// C3's board SDK. C3 inlines this file as the first script of every board page. The page reaches C3 only
// through window.c3; the shell (the parent window) carries every call and event, and the page's frame has
// no network of its own.
(() => {
  "use strict";

  const KINDS = ["message", "edit", "reaction", "action", "update", "status", "voice", "speech", "chat", "reveal"];
  const CONTROLS = "a[href], button, input, select, textarea, label, summary, [contenteditable]";
  const QUOTE_RUNES = 2000;
  const VIEWS = {
    message: ({ id, reply_to, part, text, at }) => ({ id, reply_to, part, text, ts: at }),
    edit: ({ id, text, status }) => ({ id, text, status }),
    reaction: ({ ref, text, from }) => ({ ref, emoji: text, from }),
    action: ({ id, op, part, text, ref, from, segment, has_more, status }) => ({ id, op, part, text, ref, from, segment, has_more, status }),
  };
  const listeners = Object.fromEntries(KINDS.map((kind) => [kind, new Set()]));
  const calls = new Map();
  let nextCall = 1;
  let board = null; // ready's fields, kept current from the shell's events
  let markReady;
  const ready = new Promise((resolve) => { markReady = resolve; });

  const toShell = (message) => parent.postMessage(message, "*");
  function request(op, fields) {
    return new Promise((resolve, reject) => {
      const call = nextCall++;
      calls.set(call, { resolve, reject });
      toShell({ op, call, ...fields });
    });
  }
  const act = (fields) => request("act", {
    body: Object.fromEntries(Object.entries(fields).filter(([, value]) => value !== undefined)),
  });
  const comment = (part, text, { quote } = {}) => act({ op: "comment", part, text, quote });
  const approve = (ref, { part, note } = {}) => act({ op: "approve", ref, part, note });
  // c3.speak(id): C3's shell reads your message id aloud (the frame has no network), in place of whatever it was reading;
  // c3.speak(null) stops. c3.on("speech") follows it: {id, state: "loading" | "playing" | "ended" | "failed", error}.
  const speak = (id) => request("speak", { value: id ?? null });

  // c3.chat(part, {title, ask, choices, quote}): C3's shell opens its chat sheet on part (or refreshes it, when that
  // part's chat is open): the thread, the composer and C3's mic are C3's. title names the part; ask is one line for the
  // top (your question, or what is decided); choices are up to 3 quick answers the owner taps (each a tap act); quote is
  // page text the owner picked. All are shown as text. c3.chat(null) shuts it. c3.on("chat") follows it: {part}, or
  // {part: null} once shut. C3 refuses "general" (invalid): the board-level chat is the owner's to open.
  // The owner's tap anywhere in the page that doesn't itself ask for a chat shuts it, as Escape does (a click the page
  // makes itself never does).
  let chatPart = null; // the part the sheet shows, from the shell's "chat" events
  let isChatAsked = false; // c3.chat was called while the current click was dispatched
  const chat = (part, { title, ask, choices, quote } = {}) => {
    isChatAsked = true;
    return request("chat", { value: part ?? null, title, ask, choices, quote });
  };
  const shutChat = () => { if (chatPart !== null) chat(null).catch(() => {}); }; // refused while the mic records: it stays
  addEventListener("click", () => { isChatAsked = false; }, true);
  addEventListener("click", (event) => { if (event.isTrusted && !isChatAsked) shutChat(); });
  addEventListener("keydown", (event) => { if (event.key === "Escape") shutChat(); });

  // c3.focus(part, {dock, hasText}): the part the owner is on, where the page's Send sits (an element C3 follows, or
  // {x, y, width, height} in frame CSS pixels) and whether its composer has text. While it is empty, C3 draws its mic
  // there; the page hides its Send (keeping its box) and shows it again once there is text. c3.focus(null) lets go.
  let composer = { part: null, dock: null, hasText: false };
  let toldComposer = ""; // the composer the shell has, so an unchanged one isn't sent again
  let isDockQueued = false;

  function focus(part, { dock = null, hasText = false } = {}) {
    composer = { part: part ?? null, dock: part == null ? null : dock, hasText: Boolean(hasText) };
    dockWatcher?.disconnect();
    if (composer.dock instanceof Element) dockWatcher?.observe(composer.dock);
    return tellComposer();
  }

  function tellComposer() {
    const { part, dock, hasText } = composer;
    const box = dock instanceof Element ? dock.getBoundingClientRect() : dock;
    const message = { value: part, dock: box && { x: box.x, y: box.y, width: box.width, height: box.height }, hasText };
    const key = JSON.stringify(message);
    if (key === toldComposer) return Promise.resolve();
    toldComposer = key;
    return request("focus", message).catch((error) => { toldComposer = ""; throw error; });
  }

  /** The followed Send moved or resized (layout, scroll, keyboard): the shell hears its new box once per frame. */
  function followDock() {
    if (isDockQueued || !(composer.dock instanceof Element)) return;
    isDockQueued = true;
    requestAnimationFrame(() => {
      isDockQueued = false;
      tellComposer().catch(reportError);
    });
  }
  const dockWatcher = typeof ResizeObserver === "function" ? new ResizeObserver(followDock) : null;
  addEventListener("resize", followDock);
  addEventListener("scroll", followDock, { capture: true, passive: true });

  function on(kind, handler) {
    if (!Object.hasOwn(listeners, kind)) throw new TypeError(`c3.on: unknown event "${kind}" (one of ${KINDS.join(", ")})`);
    listeners[kind].add(handler);
    return () => listeners[kind].delete(handler);
  }

  function emit(kind, event) {
    for (const handler of listeners[kind]) {
      try { handler(event); } catch (error) { reportError(error); }
    }
  }

  addEventListener("message", (event) => {
    const message = event.data;
    if (event.source !== parent || parent === window || !message || typeof message.c3 !== "string") return;
    if (message.c3 === "ready") {
      if (!board) start(message.ready);
    } else if (!board) {
      return;
    } else if (message.c3 === "result") {
      finish(message);
    } else if (message.c3 === "status") {
      Object.assign(board, message.status);
      draw();
      emit("status", { ...message.status });
    } else if (message.c3 === "entry" && Object.hasOwn(VIEWS, message.kind)) {
      board.log.push(message.entry);
      draw();
      emit(message.kind, VIEWS[message.kind](message.entry));
    } else if (message.c3 === "page") {
      applyPage(message);
    } else if (message.c3 === "voice") {
      emit("voice", { ...message.voice });
    } else if (message.c3 === "speech") {
      emit("speech", { ...message.speech });
    } else if (message.c3 === "chat") {
      chatPart = message.chat.part;
      emit("chat", { part: chatPart });
    } else if (message.c3 === "reveal") {
      reveal(message.reveal.part);
    }
  });

  function start(state) {
    board = { ...state, log: [...state.log], typing: false };
    markReady({ ...state, log: [...state.log], kept: state.kept === undefined ? undefined : JSON.parse(state.kept) });
    draw();
  }

  function finish({ call, error, value }) {
    const waiter = calls.get(call);
    if (!waiter) return;
    calls.delete(call);
    if (error) waiter.reject(new Error(error));
    else waiter.resolve(value);
  }

  // A new stored revision: the shell hands over the previous and the new text. Parts that changed are
  // swapped in place; anything else asks the shell to reload the frame, which is always right.
  function applyPage({ entry, previous, next }) {
    board.log.push(entry);
    board.rev = entry.rev;
    draw();
    if (!updateParts(entry.rev, previous, next)) toShell({ op: "reload" });
  }

  function updateParts(rev, previous, next) {
    let changes;
    try {
      changes = previous === null ? null : diff(previous, next);
    } catch (error) {
      reportError(error);
      return false;
    }
    if (!changes) return false;
    for (const { part, tag, html, data } of changes) {
      let isHandled = false;
      for (const handler of listeners.update) {
        try {
          isHandled = handler({ rev, part, html, data }) === true || isHandled;
        } catch (error) {
          reportError(error);
          return false;
        }
      }
      if (!isHandled && !swap(tag, part, html)) return false;
    }
    return true;
  }

  /** The element's markup with every part inside it emptied: what a change outside those parts shows in. */
  function skeleton(element, isInnerOnly) {
    const copy = element.cloneNode(true);
    for (const part of copy.querySelectorAll("[data-c3-part]")) part.replaceChildren();
    return isInnerOnly ? copy.innerHTML : copy.outerHTML;
  }

  /** The outermost changed parts between two stored texts, or null when anything outside parts changed. */
  function diff(previous, next) {
    const [before, after] = [previous, next].map((text) => new DOMParser().parseFromString(text, "text/html"));
    if (skeleton(before.documentElement) !== skeleton(after.documentElement)) return null;
    const [oldParts, newParts] = [before, after].map((parsed) => [...parsed.querySelectorAll("[data-c3-part]")]);
    const address = (part) => part.getAttribute("data-c3-part");
    const twins = new Map(oldParts.map((part) => [address(part), part]));
    if (twins.size !== oldParts.length || new Set(newParts.map(address)).size !== newParts.length) return null; // a reused address
    // A part with no twin was added inside a part whose skeleton changed with it, so only that outer part is kept.
    const changed = newParts.filter((part) => !twins.has(address(part)) || skeleton(part, true) !== skeleton(twins.get(address(part)), true));
    return changed.filter((part) => !changed.some((other) => other !== part && other.contains(part))).map((part) => ({
      part: part.getAttribute("data-c3-part"), tag: part.localName, html: part.innerHTML,
      data: part.matches('script[type="application/json"]') ? JSON.parse(part.textContent) : null,
    }));
  }

  function swap(tag, part, html) {
    const live = document.querySelector(`${CSS.escape(tag)}[data-c3-part="${CSS.escape(part)}"]`);
    if (!live) return false;
    live.innerHTML = html;
    flash(live);
    return true;
  }

  /** A short outline on a part C3 just changed or brought into view. */
  const flash = (element) => element.animate([{ outline: "3px solid #d4a72c", outlineOffset: "2px" }, { outline: "3px solid transparent", outlineOffset: "2px" }],
    { duration: 1200, easing: "ease-out" });

  /** The shell asks for part to be shown (the owner picked its unread reply in the panel; its chat opens there). A
   *  c3.on("reveal") handler that returns true has shown it; else the part's element scrolls to the top and flashes. */
  function reveal(part) {
    let isHandled = false;
    for (const handler of listeners.reveal) {
      try { isHandled = handler({ part }) === true || isHandled; } catch (error) { reportError(error); }
    }
    const element = isHandled ? null : livePart(part);
    element?.scrollIntoView({ block: "start", behavior: "smooth" });
    if (element) flash(element);
  }

  // c3.threads(): a tap on a part opens its chat in C3's sheet, titled by the part's first heading; words the owner
  // selected in the part come along as a quote. Parts whose last approve a new page answered get a "settled" line.
  let isThreaded = false;
  let foldStyle = null;
  let pressedQuote = "";

  const partOf = (node) => (node instanceof Element ? node.closest("[data-c3-part]") : null);
  const livePart = (part) => document.querySelector(`[data-c3-part="${CSS.escape(part)}"]`);

  /** The text selected inside element, clipped to the quote limit C3 takes. */
  function selectionIn(element) {
    const selection = getSelection();
    if (!element || !selection || selection.isCollapsed || !selection.rangeCount) return "";
    if (!element.contains(selection.getRangeAt(0).commonAncestorContainer)) return "";
    const runes = [...selection.toString().trim()];
    return runes.length > QUOTE_RUNES ? `${runes.slice(0, QUOTE_RUNES - 1).join("")}…` : runes.join("");
  }

  function threads() {
    if (isThreaded) return;
    isThreaded = true;
    document.addEventListener("pointerdown", (event) => { pressedQuote = selectionIn(partOf(event.target)); }, true);
    document.addEventListener("click", (event) => {
      const element = partOf(event.target);
      if (!element || event.defaultPrevented || event.target.closest(CONTROLS)) return;
      const title = element.querySelector("h1, h2, h3, h4, h5, h6")?.textContent.trim();
      chat(element.getAttribute("data-c3-part"), { title, quote: selectionIn(element) || pressedQuote }).catch(reportError);
    });
    draw();
  }

  /** Parts whose last approve was answered by a new page, and not talked about since. From the log alone. */
  function settledParts() {
    const approves = new Map(), isSettled = new Map();
    for (const entry of board.log) {
      if (entry.dir === "in" && entry.op === "approve") approves.set(entry.id, entry.part);
      if (entry.dir === "in" && (entry.op === "comment" || entry.op === "voice")) isSettled.set(entry.part, false);
      if (entry.dir === "out" && entry.op === "page" && approves.has(entry.reply_to)) isSettled.set(approves.get(entry.reply_to), true);
    }
    return [...isSettled].filter(([part, settled]) => settled && part).map(([part]) => part);
  }

  function draw() {
    if (!board || !isThreaded) return;
    const selectors = settledParts().map((part) => `[data-c3-part="${CSS.escape(part)}"]::before`).join(",");
    foldStyle ??= (document.head ?? document.documentElement).appendChild(document.createElement("style"));
    foldStyle.textContent = `[data-c3-part]{cursor:pointer}` + (selectors && `${selectors}{${FOLD_CSS}}` +
      `@media (prefers-color-scheme:dark){${selectors}{color:#3fb950;background:#3fb95026}}`);
  }

  const FOLD_CSS = "content:'✓ Settled · tap for its thread';display:block;width:fit-content;margin:0 0 .5em;padding:.2em .6em;" +
    "border-radius:6px;font:600 13px/1.4 system-ui,-apple-system,sans-serif;color:#1a7f37;background:#1a7f3718";

  window.c3 = Object.freeze({
    ready, comment, approve, on, threads, chat, focus, speak,
    tap: (part, value, { ref } = {}) => act({ op: "tap", part, value, ref }),
    react: (ref, emoji) => act({ op: "react", ref, emoji }),
    keep: (value) => request("keep", { value: JSON.stringify(value) }),
  });
  if (parent !== window) toShell({ op: "hello" });
})();
