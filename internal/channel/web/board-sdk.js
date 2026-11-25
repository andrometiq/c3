// C3's board SDK. C3 inlines this file as the first script of every board page. The page reaches C3 only
// through window.c3; the shell (the parent window) carries every call and event, and the page's frame has
// no network of its own.
(() => {
  "use strict";

  const SDK_SCRIPT = document.currentScript; // C3's own script, which a live page update never touches
  const KINDS = ["message", "edit", "reaction", "action", "update", "status", "voice", "speech", "chat", "reveal"];
  const CONTROLS = "a[href], button, input, select, textarea, label, summary, [contenteditable]";
  const QUOTE_RUNES = 2000;
  const VIEWS = {
    message: ({ id, reply_to, part, text, at }) => ({ id, reply_to, part, text, ts: at }),
    edit: ({ id, text, status }) => ({ id, text, status }),
    reaction: ({ ref, text, from }) => ({ ref, emoji: text, from }),
    action: ({ id, op, part, text, ref, from, segment, has_more, status, selection }) => ({ id, op, part, text, ref, from, segment, has_more, status, selection }),
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
    } else if (message.c3 === "pencil") {
      toShell({ op: "pencil", key: message.pencil.key, value: pencilHits(message.pencil) });
    } else if (message.c3 === "scroll") {
      const { x, y } = message.scroll;
      if (Number.isFinite(x) && Number.isFinite(y)) scrollBy({ left: x, top: y, behavior: "instant" });
    }
  });

  // The shell hears where the page is scrolled to (once a frame), so a shape the owner drew over the page moves with it.
  let isScrollQueued = false;
  addEventListener("scroll", () => {
    if (isScrollQueued || !board) return;
    isScrollQueued = true;
    requestAnimationFrame(() => {
      isScrollQueued = false;
      toShell({ op: "scroll", value: { x: scrollX, y: scrollY } });
    });
  }, { passive: true });

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
  // swapped in place; a change outside parts is patched into the live page (morph); anything else asks the shell
  // to reload the frame, which is always right.
  function applyPage({ entry, previous, next }) {
    board.log.push(entry);
    board.rev = entry.rev;
    draw();
    let changes;
    try {
      changes = previous === null ? null : diff(previous, next);
    } catch (error) {
      reportError(error);
      return toShell({ op: "reload" });
    }
    const isShown = changes ? updateParts(entry.rev, changes) : previous !== null && morphPage(previous, next);
    if (!isShown) toShell({ op: "reload" });
  }

  function updateParts(rev, changes) {
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
    return changedParts(before, after);
  }

  /** The outermost parts whose own content changed between two parsed texts, or null when an address is reused. */
  function changedParts(before, after) {
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

  // ---------- live update: a change outside parts patched into the live page ----------

  /** Thrown when a change can't be patched in place: a script changed, moved or came in new (it must run). */
  class NeedsReload extends Error {}
  const MORPH_FLASHES = 12;
  const isDataScript = (node) => node.localName === "script" && /^application\/(?:ld\+)?json$/i.test(node.getAttribute("type") ?? "");
  const isScript = (node) => node.nodeType === Node.ELEMENT_NODE && node.localName === "script" && !isDataScript(node);
  /** A node's children the morph works on: never C3's own script or style, nor blank text in head (where C3's inlined
   *  script makes the parser keep some that the stored text alone doesn't have). */
  const childrenOf = (node) => [...node.childNodes].filter((child) => child !== SDK_SCRIPT && child !== foldStyle
    && !(node.localName === "head" && child.nodeType === Node.TEXT_NODE && !child.nodeValue.trim()));
  const isSameKind = (a, b) => a.nodeType === b.nodeType && (a.nodeType !== Node.ELEMENT_NODE || a.localName === b.localName);

  /** Patches the live document from the previous stored text to the next, three ways: only what the agent changed
   *  between the two texts moves, so what the page's own scripts drew elsewhere stays, and so do the scroll, the open
   *  chat and the page's state. The changed elements flash. False when it can't (the shell then reloads the frame). */
  function morphPage(previous, next) {
    const [before, after] = [previous, next].map((text) => new DOMParser().parseFromString(text, "text/html"));
    const changed = new Set();
    try {
      // A part's change is the page's to draw (c3.on("update")) or a JSON part's script draws it at load: patching the
      // markup alone would leave such a page stale, so a mixed change touching one reloads. So does a changed
      // <template> (its content isn't in the node tree the patch walks).
      const parts = changedParts(before, after);
      if (!parts || parts.some((part) => part.data !== null) || parts.length && listeners.update.size) throw new NeedsReload();
      const templates = (parsed) => [...parsed.querySelectorAll("template")].map((template) => template.outerHTML).join("\n");
      if (templates(before) !== templates(after)) throw new NeedsReload();
      morphAttributes(document.documentElement, before.documentElement, after.documentElement, changed);
      for (const section of ["head", "body"]) {
        if (!document[section] || !before[section] || !after[section]) throw new NeedsReload();
        morphAttributes(document[section], before[section], after[section], changed);
        morphChildren(document[section], before[section], after[section], changed);
      }
    } catch (error) {
      if (!(error instanceof NeedsReload)) reportError(error);
      return false;
    }
    const outermost = [...changed].filter((element) => element.isConnected && element !== document.body && element !== document.documentElement
      && document.body.contains(element) && ![...changed].some((other) => other !== element && other.contains?.(element)));
    for (const element of outermost.slice(0, MORPH_FLASHES)) flash(element);
    return true;
  }

  function morphAttributes(live, before, after, changed) {
    const names = new Set([...before.getAttributeNames(), ...after.getAttributeNames()]);
    for (const name of names) {
      const value = after.getAttribute(name);
      if (before.getAttribute(name) === value) continue;
      if (value === null) live.removeAttribute(name);
      else live.setAttribute(name, value);
      changed.add(live);
    }
  }

  /** Walks the previous and the next children side by side, the live ones with the previous: one inserted or removed
   *  node is found as such, a node of the same kind is patched, and anything else replaced. Live children that no longer
   *  match the previous text (the page's script redrew them) take the next text's whole. */
  function morphChildren(live, before, after, changed) {
    const [liveKids, oldKids, newKids] = [childrenOf(live), childrenOf(before), childrenOf(after)];
    if (oldKids.length === newKids.length && oldKids.every((kid, index) => kid.isEqualNode(newKids[index]))) return;
    if (liveKids.length !== oldKids.length || liveKids.some((kid, index) => !isSameKind(kid, oldKids[index]))) {
      for (const kid of [...liveKids, ...oldKids, ...newKids]) if (isScript(kid)) throw new NeedsReload();
      liveKids.forEach((kid) => kid.remove());
      live.append(...newKids.map(imported));
      changed.add(live);
      return;
    }
    const tail = liveKids.at(-1)?.nextSibling ?? null; // appended nodes go before it, in order
    let [oldAt, newAt] = [0, 0];
    while (oldAt < oldKids.length || newAt < newKids.length) {
      const [old, fresh, current] = [oldKids[oldAt], newKids[newAt], liveKids[oldAt]];
      if (old && fresh && old.isEqualNode(fresh)) {
        oldAt++; newAt++;
      } else if (fresh && (!old || newKids[newAt + 1]?.isEqualNode(old))) { // fresh was inserted
        const node = imported(fresh);
        live.insertBefore(node, current ?? tail);
        markChanged(node, changed);
        newAt++;
      } else if (old && (!fresh || oldKids[oldAt + 1]?.isEqualNode(fresh))) { // old was removed
        if (isScript(current)) throw new NeedsReload();
        markChanged(current.parentNode, changed);
        current.remove();
        oldAt++;
      } else if (isSameKind(old, fresh)) {
        morphNode(current, old, fresh, changed);
        oldAt++; newAt++;
      } else {
        if (isScript(current)) throw new NeedsReload();
        const node = imported(fresh);
        current.replaceWith(node);
        markChanged(node, changed);
        oldAt++; newAt++;
      }
    }
  }

  function morphNode(live, before, after, changed) {
    if (live.nodeType !== Node.ELEMENT_NODE) {
      if (live.nodeValue !== after.nodeValue) {
        live.nodeValue = after.nodeValue;
        markChanged(live.parentNode, changed);
      }
      return;
    }
    if (isScript(after)) throw new NeedsReload(); // scripts differ: only a reload runs the new one
    morphAttributes(live, before, after, changed);
    if (isDataScript(after)) {
      if (live.textContent !== after.textContent) {
        live.textContent = after.textContent;
        changed.add(live);
      }
      return;
    }
    morphChildren(live, before, after, changed);
  }

  /** A node from a parsed text, made the live document's; one holding a script refuses (it must run: reload). */
  function imported(node) {
    if (isScript(node) || node.querySelector?.("script:not([type='application/json']):not([type='application/ld+json'])")) throw new NeedsReload();
    return document.importNode(node, true);
  }

  const markChanged = (node, changed) => {
    const element = node?.nodeType === Node.ELEMENT_NODE ? node : node?.parentElement;
    if (element) changed.add(element);
  };

  // ---------- the Magic Pencil: what lies under the owner's selection ----------

  // The shell draws the shape over the frame and asks what is under it; only this script can read the page. The
  // answer names the visible elements mostly inside the shape (the outermost of them, in page order), the innermost
  // element holding the whole shape, the words inside the shape, the viewport, scroll and document size, and the
  // location hash. All boxes are the frame's viewport CSS pixels, as the shape's points are. Nothing in the page
  // changes: the found elements only flash.
  const PENCIL = { elements: 12, textRunes: 160, wordsRunes: 500, coverage: 0.6, gridMin: 4, gridMax: 10, gridStepPx: 24, selectorDepth: 12,
    skip: new Set(["script", "style", "head", "meta", "link", "title", "template", "noscript", "br", "wbr"]) };

  function pencilHits({ kind, points }) {
    if (!Array.isArray(points) || points.length < 3) return null;
    const bounds = boxOf(points);
    const isInside = kind === "rectangle" ? (x, y) => x >= bounds.x && x <= bounds.right && y >= bounds.y && y <= bounds.bottom
      : (x, y) => isInPolygon(x, y, points);
    const covered = new Map();
    for (const element of document.body?.querySelectorAll("*") ?? []) {
      if (PENCIL.skip.has(element.localName)) continue;
      const box = element.getBoundingClientRect();
      if (box.width < 1 || box.height < 1 || box.right < bounds.x || box.x > bounds.right || box.bottom < bounds.y || box.y > bounds.bottom) continue;
      if (element.checkVisibility?.({ opacityProperty: true, visibilityProperty: true }) === false) continue; // the owner can't see it
      const coverage = coverageOf(box, isInside);
      if (coverage >= PENCIL.coverage) covered.set(element, coverage);
    }
    let elements = [...covered.keys()].filter((element) => !covered.has(element.parentElement)).map((element) => innermost(element, covered));
    const container = containerOf(bounds);
    if (!elements.length && container && container !== document.body) elements = [container];
    for (const element of elements.slice(0, PENCIL.elements)) flash(element);
    const view = (element) => element && { ...describe(element), ...(covered.has(element) && { coverage: Math.round(covered.get(element) * 100) / 100 }) };
    const parts = new Set(elements.map((element) => partOf(element)?.getAttribute("data-c3-part") ?? null));
    return {
      part: parts.size === 1 && !parts.has(null) ? [...parts][0] : partOf(container)?.getAttribute("data-c3-part") ?? null,
      viewport: { width: innerWidth, height: innerHeight }, scroll: { x: scrollX, y: scrollY },
      document: { width: document.documentElement.scrollWidth, height: document.documentElement.scrollHeight },
      hash: location.hash.slice(0, 200),
      text: wordsIn(bounds, isInside),
      container: container && container !== document.body && container !== document.documentElement ? view(container) : null,
      elements: elements.slice(0, PENCIL.elements).map(view),
    };
  }

  /** The words the owner sees inside the shape, in page order: each word counts when its middle is inside. */
  function wordsIn(bounds, isInside) {
    const words = [];
    let runes = 0;
    const isOutside = (box) => box.right < bounds.x || box.x > bounds.right || box.bottom < bounds.y || box.y > bounds.bottom;
    const walker = document.createTreeWalker(document.body ?? document.documentElement, NodeFilter.SHOW_TEXT, { acceptNode: (node) =>
      (node.nodeValue.trim() && !PENCIL.skip.has(node.parentElement?.localName) ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT) });
    const range = document.createRange();
    for (let node = walker.nextNode(); node && runes < PENCIL.wordsRunes; node = walker.nextNode()) {
      range.selectNodeContents(node);
      if (isOutside(range.getBoundingClientRect())) continue;
      for (const match of node.nodeValue.matchAll(/\S+/gu)) {
        range.setStart(node, match.index);
        range.setEnd(node, match.index + match[0].length);
        const box = range.getBoundingClientRect();
        if (!box.width || !isInside(box.x + box.width / 2, box.y + box.height / 2)) continue;
        words.push(match[0]);
        runes += [...match[0]].length + 1;
        if (runes >= PENCIL.wordsRunes) break;
      }
    }
    const text = words.join(" ");
    return [...text].length > PENCIL.wordsRunes ? `${[...text].slice(0, PENCIL.wordsRunes - 1).join("")}…` : text;
  }

  /** A covered wrapper whose only covered child holds all its content stands for that child: a header around a title
   *  names the title. */
  function innermost(element, covered) {
    for (;;) {
      const kids = [...element.children].filter((kid) => covered.has(kid));
      const hasOwnText = [...element.childNodes].some((node) => node.nodeType === Node.TEXT_NODE && node.nodeValue.trim());
      if (kids.length !== 1 || hasOwnText) return element;
      element = kids[0];
    }
  }

  function boxOf(points) {
    const xs = points.map(([x]) => x), ys = points.map(([, y]) => y);
    const [x, y, right, bottom] = [Math.min(...xs), Math.min(...ys), Math.max(...xs), Math.max(...ys)];
    return { x, y, right, bottom, width: right - x, height: bottom - y };
  }

  /** Ray casting: whether (x, y) lies inside the closed polygon. */
  function isInPolygon(x, y, polygon) {
    let isIn = false;
    for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
      const [[xi, yi], [xj, yj]] = [polygon[i], polygon[j]];
      if ((yi > y) !== (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi) isIn = !isIn;
    }
    return isIn;
  }

  /** The share of an element's visible box inside the shape, from a grid of sample points that is finer on a larger box. */
  function coverageOf(box, isInside) {
    const [left, top] = [Math.max(box.x, 0), Math.max(box.y, 0)];
    const [right, bottom] = [Math.min(box.right, innerWidth), Math.min(box.bottom, innerHeight)];
    if (right <= left || bottom <= top) return 0;
    const grid = Math.min(Math.max(Math.ceil(Math.max(right - left, bottom - top) / PENCIL.gridStepPx), PENCIL.gridMin), PENCIL.gridMax);
    let inside = 0;
    for (let row = 0; row < grid; row++) {
      for (let column = 0; column < grid; column++) {
        const x = left + (column + 0.5) * (right - left) / grid, y = top + (row + 0.5) * (bottom - top) / grid;
        if (isInside(x, y)) inside++;
      }
    }
    return inside / grid ** 2;
  }

  /** The innermost element whose box holds the whole shape. */
  function containerOf(bounds) {
    let element = document.elementFromPoint(bounds.x + bounds.width / 2, bounds.y + bounds.height / 2);
    while (element && element !== document.documentElement) {
      const box = element.getBoundingClientRect();
      if (box.x <= bounds.x && box.y <= bounds.y && box.right >= bounds.right && box.bottom >= bounds.bottom) return element;
      element = element.parentElement;
    }
    return element;
  }

  function describe(element) {
    const box = element.getBoundingClientRect();
    const part = partOf(element)?.getAttribute("data-c3-part");
    return { selector: selectorOf(element), tag: element.localName, text: textOf(element), ...(part && { part }),
      box: { x: box.x, y: box.y, width: box.width, height: box.height } };
  }

  /** A selector that finds element: its id when unique, else a path of tags (with up to two classes) and positions
   *  from the nearest ancestor with a unique id, or from body. */
  function selectorOf(element) {
    const unique = (node) => node.id && document.querySelectorAll(`#${CSS.escape(node.id)}`).length === 1;
    const steps = [];
    for (let node = element; node && node !== document.body && node !== document.documentElement; node = node.parentElement) {
      if (unique(node)) {
        steps.unshift(`#${CSS.escape(node.id)}`);
        return steps.join(" > ");
      }
      const classes = [...node.classList].slice(0, 2).map((name) => `.${CSS.escape(name)}`).join("");
      const twins = node.parentElement ? [...node.parentElement.children].filter((sibling) => sibling.localName === node.localName) : [node];
      steps.unshift(`${node.localName}${classes}${twins.length > 1 ? `:nth-of-type(${twins.indexOf(node) + 1})` : ""}`);
      if (steps.length >= PENCIL.selectorDepth) break;
    }
    steps.unshift("body");
    return steps.join(" > ").slice(0, 300);
  }

  /** An element's first words as the owner sees them (an image's alt, a field's value or placeholder). */
  function textOf(element) {
    const raw = element.innerText || element.getAttribute("alt") || element.getAttribute("aria-label") || element.value || element.getAttribute("placeholder") || "";
    const runes = [...String(raw).replace(/\s+/g, " ").trim()];
    return runes.length > PENCIL.textRunes ? `${runes.slice(0, PENCIL.textRunes - 1).join("")}…` : runes.join("");
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
