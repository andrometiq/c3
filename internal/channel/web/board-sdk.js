// C3's board SDK. C3 inlines this file as the first script of every board page. The page reaches C3 only
// through window.c3; the shell (the parent window) carries every call and event, and the page's frame has
// no network of its own.
(() => {
  "use strict";

  const KINDS = ["message", "edit", "reaction", "action", "update", "status"];
  const CONTROLS = "a[href], button, input, select, textarea, label, summary, [contenteditable]";
  const QUOTE_RUNES = 2000;
  const REASONS = { closed: "the board is closed", rate: "too many this minute, wait a moment", too_big: "too long",
    offline: "C3 can't be reached, try again", invalid: "C3 refused it" };
  const VIEWS = {
    message: ({ id, reply_to, part, text, at }) => ({ id, reply_to, part, text, ts: at }),
    edit: ({ id, text }) => ({ id, text }),
    reaction: ({ ref, text, from }) => ({ ref, emoji: text, from }),
    action: ({ id, op, part, text, ref, from }) => ({ id, op, part, text, ref, from }),
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
    const answered = board.log.find((line) => line.dir === "in" && line.op === "approve" && line.id === entry.reply_to);
    if (answered && answered.part === openPart) closeSheet();
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
    live.animate([{ outline: "3px solid #d4a72c", outlineOffset: "2px" }, { outline: "3px solid transparent", outlineOffset: "2px" }],
      { duration: 1200, easing: "ease-out" });
    return true;
  }

  // c3.threads(): a tap on a part opens its breakout, a sheet drawn outside every part, so no swap touches it.
  const drafts = new Map();
  const quotes = new Map();
  const approving = new Set();
  let isThreaded = false;
  let sheet = null;
  let foldStyle = null;
  let openPart = null;
  let pressedQuote = "";

  function el(tag, attributes = {}, ...children) {
    const node = document.createElement(tag);
    for (const [key, value] of Object.entries(attributes)) node.setAttribute(key, value);
    node.append(...children.filter((child) => child !== null && child !== false && child !== undefined));
    return node;
  }

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
      openSheet(element.getAttribute("data-c3-part"), selectionIn(element) || pressedQuote);
    });
    document.addEventListener("selectionchange", () => {
      const quote = openPart === null ? "" : selectionIn(livePart(openPart));
      if (quote) {
        quotes.set(openPart, quote);
        draw();
      }
    });
    document.addEventListener("keydown", (event) => { if (event.key === "Escape" && openPart !== null) closeSheet(); });
    draw();
  }

  function makeSheet() {
    const host = el("c3-thread");
    const root = host.attachShadow({ mode: "open" });
    const parts = {
      address: el("strong"), state: el("span", { class: "state" }),
      close: el("button", { type: "button", class: "close", "aria-label": "Close the thread" }, "×"),
      lines: el("ol", { class: "lines" }),
      typing: el("p", { class: "typing" }, el("span", { class: "spinner", "aria-hidden": "true" }), "Typing…"),
      error: el("p", { class: "error", role: "alert" }),
      quote: el("blockquote", { class: "quote" }),
      dropQuote: el("button", { type: "button", "aria-label": "Drop the quote" }, "×"),
      text: el("textarea", { rows: "2", "aria-label": "Your message" }),
      send: el("button", { type: "button", class: "send" }, "Send"),
    };
    const style = el("style");
    style.textContent = SHEET_CSS;
    parts.quoteRow = el("div", { class: "quote-row" }, parts.quote, parts.dropQuote);
    root.append(style, el("section", { class: "sheet", role: "dialog", "aria-label": "Thread" },
      el("header", {}, parts.address, parts.state, parts.close), parts.lines, parts.typing, parts.error, parts.quoteRow,
      el("div", { class: "compose" }, parts.text, parts.send)));
    parts.close.addEventListener("click", closeSheet);
    parts.dropQuote.addEventListener("click", () => { quotes.delete(openPart); draw(); });
    parts.text.addEventListener("input", () => drafts.set(openPart, parts.text.value));
    parts.text.addEventListener("keydown", (event) => { if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) send(); });
    parts.send.addEventListener("click", send);
    parts.lines.addEventListener("click", (event) => {
      const button = event.target.closest("[data-approve]");
      if (button) approveAnswer(Number(button.getAttribute("data-approve")));
    });
    (document.body ?? document.documentElement).append(host);
    return { host, ...parts };
  }

  function openSheet(part, quote) {
    sheet ??= makeSheet();
    openPart = part;
    if (quote) quotes.set(part, quote);
    sheet.text.value = drafts.get(part) ?? "";
    sheet.error.textContent = "";
    sheet.host.hidden = false;
    draw();
    if (!matchMedia("(pointer: coarse)").matches) sheet.text.focus();
  }

  function closeSheet() {
    openPart = null;
    if (sheet) sheet.host.hidden = true;
  }

  async function send() {
    const part = openPart, text = sheet.text.value.trim();
    if (!text || board.closed) return;
    sheet.send.disabled = true;
    try {
      await comment(part, text, { quote: quotes.get(part) });
      drafts.delete(part);
      quotes.delete(part);
      if (openPart === part) sheet.text.value = "";
      sheet.error.textContent = "";
    } catch (error) {
      sheet.error.textContent = `Not sent: ${REASONS[error.message] ?? error.message}.`;
    } finally {
      draw();
    }
  }

  async function approveAnswer(id) {
    approving.add(id);
    draw();
    try {
      await approve(id, { part: openPart });
      sheet.error.textContent = "";
    } catch (error) {
      sheet.error.textContent = `Not approved: ${REASONS[error.message] ?? error.message}.`;
    } finally {
      approving.delete(id);
      draw();
    }
  }

  /** Parts whose last approve was answered by a new page, and not talked about since. From the log alone. */
  function settledParts() {
    const approves = new Map(), isSettled = new Map();
    for (const entry of board.log) {
      if (entry.dir === "in" && entry.op === "approve") approves.set(entry.id, entry.part);
      if (entry.dir === "in" && entry.op === "comment") isSettled.set(entry.part, false);
      if (entry.dir === "out" && entry.op === "page" && approves.has(entry.reply_to)) isSettled.set(approves.get(entry.reply_to), true);
    }
    return [...isSettled].filter(([part, settled]) => settled && part).map(([part]) => part);
  }

  function draw() {
    if (!board || !isThreaded) return;
    const settled = settledParts();
    foldStyle ??= (document.head ?? document.documentElement).appendChild(el("style"));
    const selectors = settled.map((part) => `[data-c3-part="${CSS.escape(part)}"]::before`).join(",");
    foldStyle.textContent = `[data-c3-part]{cursor:pointer}` + (selectors && `${selectors}{${FOLD_CSS}}` +
      `@media (prefers-color-scheme:dark){${selectors}{color:#3fb950;background:#3fb95026}}`);
    if (sheet && openPart !== null) drawSheet(openPart, settled.includes(openPart));
  }

  function drawSheet(part, isSettled) {
    const thread = board.log.filter((entry) => entry.part === part && !["edit", "reaction", "react"].includes(entry.op));
    const ids = new Set(thread.map((entry) => entry.id));
    const edits = new Map(), reactions = new Map(), approved = new Set();
    for (const entry of board.log) {
      if (entry.op === "edit") edits.set(entry.id, entry.text);
      if ((entry.op === "reaction" || entry.op === "react") && ids.has(entry.ref)) reactions.set(entry.ref, `${reactions.get(entry.ref) ?? ""}${entry.text}`);
      if (entry.op === "approve") approved.add(entry.ref);
    }
    const isAtBottom = sheet.lines.scrollHeight - sheet.lines.scrollTop - sheet.lines.clientHeight < 8;
    sheet.address.textContent = `On ${part}`;
    sheet.state.textContent = isSettled ? "Settled" : "";
    sheet.lines.replaceChildren(...thread.map((entry) => line(entry, edits, reactions, approved)));
    if (isAtBottom) sheet.lines.scrollTop = sheet.lines.scrollHeight;
    sheet.typing.hidden = !(board.typing && thread.at(-1)?.dir === "in");
    sheet.quote.textContent = quotes.get(part) ?? "";
    sheet.quoteRow.hidden = !quotes.has(part);
    sheet.text.disabled = sheet.send.disabled = board.closed;
    sheet.text.placeholder = board.closed ? "This board is closed." : `About ${part}…`;
  }

  function line(entry, edits, reactions, approved) {
    const who = entry.dir === "out" ? "Agent" : entry.from === board.viewer?.name ? "You" : entry.from;
    const text = {
      approve: `Approved message ${entry.ref}.`, tap: `Tapped “${entry.text}”.`,
      page: entry.text || "Updated the page.", message: edits.get(entry.id) ?? entry.text,
    }[entry.op] ?? entry.text;
    const isAnswer = entry.dir === "out" && entry.op === "message";
    const mark = isAnswer && (approved.has(entry.id) ? el("span", { class: "approved" }, "Approved ✓")
      : !board.closed && el("button", { type: "button", "data-approve": String(entry.id), ...(approving.has(entry.id) && { disabled: "" }) },
        approving.has(entry.id) ? "Approving…" : "Approve"));
    return el("li", { class: entry.dir }, el("span", { class: "who" }, who), el("span", { class: "text" }, text),
      reactions.has(entry.id) && el("span", { class: "reactions" }, reactions.get(entry.id)), mark);
  }

  const FOLD_CSS = "content:'✓ Settled · tap for its thread';display:block;width:fit-content;margin:0 0 .5em;padding:.2em .6em;" +
    "border-radius:6px;font:600 13px/1.4 system-ui,-apple-system,sans-serif;color:#1a7f37;background:#1a7f3718";
  const SHEET_CSS = `
:host { all: initial; --ink: #1f2328; --muted: #59636e; --paper: #fff; --soft: #f0f2f4; --mine: #ddf4ff; --line: #d1d9e0;
  --accent: #0969da; --good: #1a7f37; --bad: #cf222e; }
@media (prefers-color-scheme: dark) { :host { --ink: #e6edf3; --muted: #9198a1; --paper: #151b23; --soft: #212830;
  --mine: #0c2d6b; --line: #3d444d; --accent: #4493f8; --good: #3fb950; --bad: #f85149; } }
:host([hidden]), [hidden] { display: none !important; }
.sheet { position: fixed; left: 0; right: 0; bottom: 0; z-index: 2147483647; box-sizing: border-box; max-width: 640px;
  max-height: 70vh; margin: 0 auto; display: flex; flex-direction: column; gap: 8px;
  padding: 12px 16px calc(12px + env(safe-area-inset-bottom)); border: 1px solid var(--line); border-bottom: 0;
  border-radius: 14px 14px 0 0; background: var(--paper); color: var(--ink);
  font: 15px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; box-shadow: 0 -6px 24px rgb(0 0 0 / 20%); }
header { display: flex; align-items: center; gap: 10px; }
.state { color: var(--good); font-weight: 600; }
.close { margin-left: auto; }
.lines { flex: 1 1 auto; min-height: 0; overflow-y: auto; margin: 0; padding: 0; list-style: none; display: flex;
  flex-direction: column; gap: 6px; }
li { max-width: 85%; padding: 6px 10px; border-radius: 10px; background: var(--soft); white-space: pre-wrap; overflow-wrap: anywhere; }
li.in { align-self: flex-end; background: var(--mine); }
.who { display: block; font-size: 12px; color: var(--muted); }
.reactions { margin-left: 6px; }
.approved { display: block; margin-top: 4px; color: var(--good); font-weight: 600; font-size: 13px; }
li button { display: block; margin-top: 6px; }
button { font: inherit; min-height: 36px; padding: 4px 12px; border: 1px solid var(--line); border-radius: 8px;
  background: var(--paper); color: var(--ink); cursor: pointer; }
.send { background: var(--accent); border-color: var(--accent); color: #fff; }
button:disabled { opacity: .55; cursor: default; }
.typing { margin: 0; display: flex; align-items: center; gap: 8px; color: var(--muted); }
.spinner { width: 14px; height: 14px; box-sizing: border-box; border: 2px solid var(--line); border-top-color: var(--accent);
  border-radius: 50%; animation: spin .8s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
.error { margin: 0; color: var(--bad); }
.error:empty { display: none; }
.quote-row { display: flex; align-items: flex-start; gap: 8px; }
.quote { flex: 1; min-width: 0; max-height: 4.5em; overflow: auto; margin: 0; padding: 2px 10px; border-left: 3px solid var(--accent);
  color: var(--muted); white-space: pre-wrap; }
.compose { display: flex; align-items: flex-end; gap: 8px; }
textarea { flex: 1; min-width: 0; box-sizing: border-box; resize: vertical; padding: 8px 10px; border: 1px solid var(--line);
  border-radius: 8px; background: var(--paper); color: var(--ink); font: inherit; font-size: 16px; }
`;

  window.c3 = Object.freeze({
    ready, comment, approve, on, threads,
    tap: (part, value, { ref } = {}) => act({ op: "tap", part, value, ref }),
    react: (ref, emoji) => act({ op: "react", ref, emoji }),
    keep: (value) => request("keep", { value: JSON.stringify(value) }),
  });
  if (parent !== window) toShell({ op: "hello" });
})();
