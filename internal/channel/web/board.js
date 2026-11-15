// C3's board shell: the strip, and the bridge between this board's sandboxed frame (the agent's page) and C3.
// Only messages from the frame's own window are taken; agent strings reach this document only as text.
(() => {
  "use strict";

  const LIMITS = { textRunes: 16000, quoteRunes: 2000, valueBytes: 64, keepBytes: 64 << 10, logEntries: 500 };
  const PART = /^[A-Za-z0-9._:-]{1,64}$/;
  const ACT_FIELDS = { comment: ["part", "text", "quote"], approve: ["ref", "part", "note"], tap: ["part", "value", "ref"], react: ["ref", "emoji"] };
  const RULES = { 409: "closed", 413: "too_big", 429: "rate", 503: "offline" };
  const STREAM_KINDS = ["message", "edit", "reaction", "action", "page", "status", "notice"];
  const REFUSALS = { 401: "Signed out. Reload to sign in.", 403: "This board can't be opened.", 404: "This board can't be opened." };
  const RETRIES = 3;
  const RETRY_MS = 1000;
  const RETRY_MAX_MS = 30000;
  const TYPING_MS = 10000;

  const name = location.pathname.split("/")[2] ?? "";
  const base = `/board/${name}`;
  const frame = document.getElementById("page");
  const strip = Object.fromEntries(["strip", "title", "session", "waiting", "typing", "notice", "close"]
    .map((id) => [id, document.getElementById(id)]));
  const early = [];
  let state = null; // /state, kept current from the stream: the one copy this shell holds
  let lastSeq = 0;
  let pageText = null; // the stored text of state.rev, which the frame's next diff starts from
  let pages = Promise.resolve();
  let kept;
  let isFrameReady = false;
  let isSynced = false; // false from a stream open until /state is read: stream events wait in `early`
  let syncs = 0;
  let retryMs = RETRY_MS;
  let typingTimer = 0;

  const sleep = (ms) => new Promise((resolve) => { setTimeout(resolve, ms); });
  const toFrame = (message) => frame.contentWindow.postMessage(message, "*");
  const pageURL = (rev) => `${base}/page?cap=${encodeURIComponent(state.cap)}${rev ? `&rev=${rev}&raw=1` : ""}`;
  /** The stored text of rev, or null when it can't be fetched or read in full. */
  const readPage = (rev) => fetch(pageURL(rev)).then((response) => (response.ok ? response.text() : null)).catch(() => null);
  const frameStatus = () => ({ live: state.live, waiting: state.waiting, closed: state.closed, typing: state.typing });
  const messages = (count) => `${count} message${count === 1 ? "" : "s"}`;

  function drawStrip() {
    const { live, waiting, closed, typing, holder } = state;
    const title = state.title || name;
    strip.title.textContent = title;
    document.title = `${title} · board`;
    strip.strip.dataset.mode = closed ? "closed" : live ? "live" : "ended";
    const folder = holder?.cwd?.split(/[\\/]/).filter(Boolean).pop() ?? holder?.cwd;
    strip.session.textContent = closed ? "Closed: read-only." : !live ? `Session ended.${waiting ? ` ${messages(waiting)} ${waiting === 1 ? "waits" : "wait"} for it.` : ""}`
      : holder ? `${holder.cli} · ${folder}` : "Live";
    strip.waiting.textContent = live && !closed && waiting ? `${messages(waiting)} waiting` : "";
    strip.typing.hidden = !(live && typing);
    strip.close.hidden = closed;
  }

  function setTyping(isTyping, isStatusNew = false) {
    clearTimeout(typingTimer);
    if (isTyping) typingTimer = setTimeout(() => setTyping(false), TYPING_MS);
    if (state.typing === isTyping && !isStatusNew) return;
    state.typing = isTyping;
    drawStrip();
    if (isFrameReady) toFrame({ c3: "status", status: frameStatus() });
  }

  function receive(kind, data) {
    if (!isSynced) return early.push([kind, data]);
    if (kind === "notice") {
      strip.notice.textContent = data.text; // strip only: never the frame
    } else if (kind === "status") {
      if (data.live !== state.live || data.closed !== state.closed) strip.notice.textContent = "";
      Object.assign(state, { live: data.live, waiting: data.waiting, closed: data.closed });
      if ("holder" in data) state.holder = data.holder;
      setTyping(Boolean(data.typing), true);
    } else if (data.seq > lastSeq) {
      lastSeq = data.seq;
      state.log.push(data);
      state.log.splice(0, state.log.length - LIMITS.logEntries);
      if (data.dir === "out" && (kind === "message" || kind === "page")) setTyping(false);
      if (kind === "page") {
        state.rev = data.rev;
        pages = pages.then(() => showPage(data));
      } else if (isFrameReady) {
        toFrame({ c3: "entry", kind, entry: data });
      }
    }
  }

  /** Hands the frame the previous and the new stored text; when that can't be done, reloads the frame. */
  async function showPage(entry) {
    const next = await readPage(entry.rev);
    const title = next === null ? "" : new DOMParser().parseFromString(next, "text/html").title.trim();
    if (title) state.title = title;
    drawStrip();
    if (next === null || !isFrameReady) {
      pageText = next;
      return reloadFrame();
    }
    toFrame({ c3: "page", entry, previous: pageText, next });
    pageText = next;
  }

  function reloadFrame() {
    isFrameReady = false;
    frame.src = pageURL();
  }

  function sendReady() {
    isFrameReady = true;
    const { board, title, rev, viewer, live, waiting, closed, log } = state;
    toFrame({ c3: "ready", ready: { board, title, rev, viewer, live, waiting, closed, log, kept } });
    toFrame({ c3: "status", status: frameStatus() });
  }

  /** An act body from the frame: the op's own fields only, each of its type and within its limit. */
  function checkAct(body) {
    const fields = body && Object.hasOwn(ACT_FIELDS, body.op) ? ACT_FIELDS[body.op] : null;
    if (!fields || Object.keys(body).some((key) => key !== "op" && !fields.includes(key))) return "invalid";
    const { part, ref, ...texts } = body;
    if (part !== undefined && !(typeof part === "string" && PART.test(part))) return "invalid";
    if (ref !== undefined && !(Number.isSafeInteger(ref) && ref > 0)) return "invalid";
    if (Object.values(texts).some((value) => typeof value !== "string")) return "invalid";
    const runes = (text) => [...(text ?? "")].length;
    if (runes(body.text) > LIMITS.textRunes || runes(body.note) > LIMITS.textRunes || runes(body.quote) > LIMITS.quoteRunes) return "too_big";
    if (new TextEncoder().encode(body.value ?? "").length > LIMITS.valueBytes) return "too_big";
    return "";
  }

  /** Posts one act; a 503 or a lost connection is retried with the same client_id, so C3 takes it once. */
  async function postAct(body) {
    const request = JSON.stringify({ client_id: [...crypto.getRandomValues(new Uint8Array(16))]
      .map((byte) => byte.toString(16).padStart(2, "0")).join(""), ...body });
    for (let attempt = 0; ; attempt++) {
      const response = await fetch(`${base}/act`, { method: "POST", headers: { "Content-Type": "application/json" }, body: request })
        .catch(() => null);
      if (response?.ok) return { value: { id: (await response.json()).id } };
      if (response && response.status !== 503 || attempt === RETRIES) {
        const rule = response && (await response.json().catch(() => ({}))).error;
        return { error: rule || (response ? RULES[response.status] ?? "unavailable" : "offline") };
      }
      await sleep(Math.min(Number(response?.headers.get("Retry-After")) || 2, 10) * 1000);
    }
  }

  async function onFrameMessage({ op, call, body, value }) {
    if (op === "hello") return sendReady();
    if (op === "reload") return reloadFrame();
    if (op === "keep") {
      const isKept = typeof value === "string" && new TextEncoder().encode(value).length <= LIMITS.keepBytes;
      if (isKept) kept = value;
      return toFrame({ c3: "result", call, ...(isKept ? {} : { error: "too_big" }) });
    }
    if (op !== "act") return undefined;
    const problem = checkAct(body);
    return toFrame({ c3: "result", call, ...(problem ? { error: problem } : await postAct(body)) });
  }

  addEventListener("message", (event) => { // only the sandboxed frame: its window, its opaque origin
    if (event.source !== frame.contentWindow || event.origin !== "null" || !state || !event.data || typeof event.data !== "object") return;
    onFrameMessage(event.data);
  });

  strip.close.addEventListener("click", async () => {
    if (!confirm("Close this board? It becomes read-only and its session lets it go.")) return;
    const response = await fetch(`${base}/close`, { method: "POST" }).catch(() => null);
    if (response?.ok) receive("status", { ...state, closed: true });
    else strip.notice.textContent = "Close failed. Try again.";
  });

  // The stream carries no log line it missed while down (a tab that saw no `id:` sends no Last-Event-ID), so each
  // open re-reads /state; C3 registers the stream before answering, so that /state holds every line it won't carry.
  function connect() {
    const stream = new EventSource(`${base}/events`);
    for (const kind of STREAM_KINDS) stream.addEventListener(kind, (event) => receive(kind, JSON.parse(event.data)));
    stream.addEventListener("open", () => sync(stream));
    stream.addEventListener("error", () => {
      if (state) showOffline("Reconnecting…");
      if (stream.readyState === EventSource.CLOSED) reconnect(); // a refused reconnect: the browser stops trying
    });
  }

  /** The strip while the stream is down or refused: the amber dot, never the last live state. */
  function showOffline(text) {
    strip.strip.dataset.mode = "offline";
    strip.session.textContent = text;
  }

  async function reconnect() {
    if (state) showOffline("Reconnecting…");
    await sleep(retryMs);
    retryMs = Math.min(retryMs * 2, RETRY_MAX_MS);
    const { status } = await fetch(`${base}/state`).catch(() => ({}));
    if (REFUSALS[status]) showOffline(REFUSALS[status]);
    else connect();
  }

  async function sync(stream) {
    const turn = ++syncs;
    isSynced = false;
    const response = await fetch(`${base}/state`).catch(() => null);
    const fresh = response?.ok ? await response.json().catch(() => null) : null;
    if (turn !== syncs || stream.readyState === EventSource.CLOSED) return; // a later open, or the stream's own reconnect, reads it again
    if (!fresh) {
      stream.close();
      if (REFUSALS[response?.status]) showOffline(REFUSALS[response.status]);
      else reconnect();
      return;
    }
    retryMs = RETRY_MS;
    isSynced = true;
    if (state) catchUp(fresh);
    else begin(fresh);
    for (const [kind, data] of early.splice(0)) receive(kind, data);
  }

  function begin(fresh) {
    state = { ...fresh, typing: false };
    lastSeq = state.log.at(-1)?.seq ?? 0;
    drawStrip();
    const { rev } = state;
    pages = pages.then(async () => { pageText = rev > 0 ? await readPage(rev) : null; });
    reloadFrame();
  }

  /** After a reconnect: the lines and status the stream missed, through receive(), which drops lines already held. */
  function catchUp(fresh) {
    Object.assign(state, { cap: fresh.cap, title: fresh.title });
    for (const entry of fresh.log) receive(entry.dir === "in" ? "action" : entry.op, entry);
    receive("status", fresh);
    if (fresh.rev === state.rev) return;
    const { rev } = fresh; // its page line fell out of the history: show that revision from scratch
    state.rev = rev;
    pages = pages.then(async () => {
      pageText = await readPage(rev);
      reloadFrame();
    });
  }

  connect();
})();
