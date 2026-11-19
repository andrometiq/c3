// C3's board shell: the status circle and its panel, the mic, and the bridge between this board's sandboxed frame
// (the agent's page, which fills the window) and C3. Only messages from the frame's own window are taken; agent
// strings reach this document only as text.
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
  // Voice, as the web chat records it; a long recording goes out as numbered parts while it goes on.
  const VOICE = { minMs: 400, segmentMs: 5 * 60 * 1000, timesliceMs: 1000, retryMs: 2000 };
  const NOTICE_MS = 5000; // a notice shows this long under the circle, then fades
  // The mic is a circle this wide, kept this far inside the frame's visible box and from the status circle; the
  // recording bar sits gapPx beside it. A page that names no spot gets it at the frame's bottom-right corner.
  const MIC = { minPx: 44, maxPx: 56, marginPx: 4, gapPx: 6, defaultPx: 48, cornerPx: 16 };
  const VOICE_TYPES = ["audio/webm;codecs=opus", "audio/webm", "audio/mp4"];
  const VOICE_REASONS = { closed: "the board is read-only", rate: "too many this minute", too_big: "too long",
    unreadable: "C3 couldn't read the recording", unauthorized: "signed out, reload to sign in", not_found: "this board can't be opened" };

  const name = location.pathname.split("/")[2] ?? "";
  const base = `/board/${name}`;
  const frame = document.getElementById("page");
  const controls = Object.fromEntries(["status", "badge", "panel", "title", "session", "waiting", "typing", "notice", "close", "toast",
    "mic", "recorder", "clock", "about", "cancel"].map((id) => [id, document.getElementById(id)]));
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
  let isPanelOpen = false;
  let isNoticeUnseen = false; // a kept notice came while the panel was shut: the circle shows its badge
  let toastFade = null;
  // Only this shell opens the microphone, after a real tap on its own button. The page only names the part the
  // owner is on, where its Send sits and whether its composer has text (c3.focus); a recording keeps its part.
  const canRecord = Boolean(window.isSecureContext && navigator.mediaDevices?.getUserMedia && window.MediaRecorder);
  let composer = { part: null, dock: null, hasText: false }; // dock: the page's Send in frame CSS pixels, or null
  let recording = null; // {part, started, isOpening, stop(isCancelled)} while the mic is on
  let voiceKeys = 0;
  let isClosing = false; // Make read-only is waiting for the voice acts on their way
  const uploads = new Map(); // key -> the frame's view of a recording on its way to C3
  const sending = new Set(); // every voice act's promise until C3 answers it

  const sleep = (ms) => new Promise((resolve) => { setTimeout(resolve, ms); });
  const toFrame = (message) => frame.contentWindow.postMessage(message, "*");
  const pageURL = (rev) => `${base}/page?cap=${encodeURIComponent(state.cap)}${rev ? `&rev=${rev}&raw=1` : ""}`;
  /** The stored text of rev, or null when it can't be fetched or read in full. */
  const readPage = (rev) => fetch(pageURL(rev)).then((response) => (response.ok ? response.text() : null)).catch(() => null);
  const frameStatus = () => ({ live: state.live, waiting: state.waiting, closed: state.closed, typing: state.typing });
  const messages = (count) => `${count} message${count === 1 ? "" : "s"}`;
  const randomID = () => [...crypto.getRandomValues(new Uint8Array(16))].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  const clock = (ms) => `${Math.floor(ms / 60000)}:${String(Math.floor(ms / 1000) % 60).padStart(2, "0")}`;

  function drawStatus() {
    const { live, waiting, closed, typing, holder } = state;
    const title = state.title || name;
    controls.title.textContent = title;
    document.title = `${title} · board`;
    document.body.dataset.mode = closed ? "closed" : !live ? "ended" : typing ? "typing" : "live";
    const folder = holder?.cwd?.split(/[\\/]/).filter(Boolean).pop() ?? holder?.cwd;
    controls.session.textContent = closed ? "Read-only." : !live ? `Session ended.${waiting ? ` ${messages(waiting)} ${waiting === 1 ? "waits" : "wait"} for it.` : ""}`
      : holder ? `${holder.cli} · ${folder}` : "Live";
    controls.waiting.textContent = live && !closed && waiting ? `${messages(waiting)} waiting` : "";
    controls.typing.hidden = !(live && typing);
    controls.close.hidden = closed;
    drawBadge();
    drawVoice();
  }

  /** The circle's badge: something waits for the session, or a kept notice the panel hasn't shown yet. */
  function drawBadge() {
    controls.badge.hidden = !(isNoticeUnseen || state && !state.closed && state.waiting > 0);
  }

  /** A notice under the circle for a few seconds, then it fades. A kept one (the default) stays in the panel and badges
   *  the circle until the panel opens; notify("") clears it. Text only, and never the frame. */
  function notify(text, isKept = true) {
    if (isKept) {
      controls.notice.textContent = text;
      isNoticeUnseen = Boolean(text) && !isPanelOpen;
      drawBadge();
    }
    toastFade?.cancel();
    controls.toast.textContent = text;
    controls.toast.hidden = !text;
    if (!text) return;
    toastFade = controls.toast.animate([{ opacity: 1, offset: 0.85 }, { opacity: 0 }], NOTICE_MS);
    toastFade.finished.then(() => { controls.toast.hidden = true; }, () => {});
  }

  /** The mic shows where the page's Send sits while its composer is empty (not while that spot has no size: the page
   *  hides it), and through a whole recording. */
  function drawVoice() {
    const hasSpot = composer.dock === null || composer.dock.width > 0 && composer.dock.height > 0;
    const isShown = Boolean(recording) || composer.part !== null && !composer.hasText && hasSpot && Boolean(state) && !state.closed;
    controls.mic.hidden = !isShown;
    controls.recorder.hidden = !recording;
    if (!isShown) return;
    const frameBox = frame.getBoundingClientRect();
    const mic = micBox(frameBox);
    place(controls.mic, mic);
    controls.mic.disabled = recording ? recording.isOpening : isClosing;
    controls.mic.ariaPressed = String(Boolean(recording));
    if (!recording) return;
    place(controls.recorder, recorderBox(frameBox, mic));
    controls.clock.textContent = recording.isOpening ? "" : clock(performance.now() - recording.started);
    controls.about.textContent = recording.isOpening ? "Opening the mic…" : `About ${recording.part}`;
  }

  const clamp = (value, low, high) => Math.min(Math.max(value, low), high);
  /** The part of the frame's box the window shows, MIC.marginPx in from each side. */
  const visibleBox = (frameBox) => ({ left: Math.max(frameBox.left, 0) + MIC.marginPx, top: Math.max(frameBox.top, 0) + MIC.marginPx,
    right: Math.min(frameBox.right, innerWidth) - MIC.marginPx, bottom: Math.min(frameBox.bottom, innerHeight) - MIC.marginPx });

  /** Where the mic goes: centred on the page's Send spot, mapped through the frame's box, as a circle MIC.minPx to
   *  MIC.maxPx wide, inside the frame's visible box, its row below the status circle. So no page can put it out of
   *  sight, cover the circle with it or with the recording bar beside it, or shrink or blow up the shell's control. */
  function micBox(frameBox) {
    const corner = { x: frameBox.width - MIC.cornerPx - MIC.defaultPx, y: frameBox.height - MIC.cornerPx - MIC.defaultPx, width: MIC.defaultPx, height: MIC.defaultPx };
    const dock = composer.dock ?? corner;
    const size = clamp(Math.min(dock.width, dock.height), MIC.minPx, MIC.maxPx);
    const view = visibleBox(frameBox);
    const left = clamp(frameBox.left + dock.x + (dock.width - size) / 2, view.left, view.right - size);
    let top = clamp(frameBox.top + dock.y + (dock.height - size) / 2, view.top, view.bottom - size);
    const circleBottom = controls.status.getBoundingClientRect().bottom + MIC.marginPx;
    if (top < circleBottom) top = circleBottom;
    return { left, top, width: size, height: size };
  }

  /** The recording bar: the mic's row, on whichever side of it has more room inside the frame. */
  function recorderBox(frameBox, mic) {
    const view = visibleBox(frameBox);
    const isLeft = mic.left - view.left >= view.right - (mic.left + mic.width);
    const left = isLeft ? view.left : mic.left + mic.width + MIC.gapPx;
    const right = isLeft ? mic.left - MIC.gapPx : view.right;
    return { left, top: mic.top, width: right - left, height: mic.height };
  }

  /** Puts a shell control at box: numbers from micBox or recorderBox, never a page string. */
  function place(element, box) {
    for (const side of ["left", "top", "width", "height"]) element.style.setProperty(side, `${Math.round(box[side])}px`);
  }

  async function startRecording() {
    const part = composer.part;
    if (recording || !part || state.closed || isClosing) return;
    let isCancelled = false;
    recording = { part, started: performance.now(), isOpening: true, stop: () => { isCancelled = true; } };
    drawVoice();
    const stream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true } })
      .catch(() => null);
    if (!stream || isCancelled) {
      stream?.getTracks().forEach((track) => track.stop());
      recording = null;
      if (!stream) notify("Microphone unavailable: check the browser's permission.");
      return drawVoice();
    }
    recording = record(part, stream);
    drawVoice();
  }

  /** The mic on for part: every VOICE.segmentMs the recording so far goes out as a numbered part, and recording goes on.
   *  Once a part has gone out, the recording always ends with a last part the session sees: its audio, or a line that
   *  it was not sent. */
  function record(part, stream) {
    const started = performance.now();
    const type = VOICE_TYPES.find((candidate) => MediaRecorder.isTypeSupported?.(candidate)) ?? "";
    let segment = 1;
    let finished = null; // finish()'s promise: settled once C3 has answered the recording's last act
    let current = startSegment();
    const ticker = setInterval(drawVoice, 250);
    const rotation = setInterval(() => {
      const ended = current;
      current = startSegment(); // the next part starts before this one stops, so no word falls between them
      beforeClosing(ended.end({ segment, hasMore: true }));
      segment += 1;
    }, VOICE.segmentMs);

    /** A recorder for the next part; end(fields) stops it and returns its upload, settled once C3 answers it. */
    function startSegment() {
      const recorder = type ? new MediaRecorder(stream, { mimeType: type }) : new MediaRecorder(stream);
      const chunks = [];
      let ending = null; // end()'s fields for the upload, or false to drop it
      const uploaded = new Promise((resolve) => {
        recorder.addEventListener("stop", () => {
          if (ending === null) finish(false); // the microphone went away: what it heard goes out as the last part
          resolve(ending && upload({ part, blob: new Blob(chunks, { type: recorder.mimeType || type || "audio/webm" }), ...ending }));
        }, { once: true });
      });
      recorder.addEventListener("dataavailable", (event) => { if (event.data.size) chunks.push(event.data); });
      recorder.addEventListener("error", () => finish(false), { once: true });
      recorder.start(VOICE.timesliceMs);
      return { end: (fields) => { ending = fields; if (recorder.state !== "inactive") recorder.stop(); return uploaded; } };
    }

    function finish(isCancelled) {
      if (finished) return finished;
      clearInterval(ticker);
      clearInterval(rotation);
      const isTooShort = segment === 1 && performance.now() - started < VOICE.minMs;
      const isDropped = isCancelled || isTooShort;
      const last = segment > 1 ? { part, segment } : { part };
      finished = beforeClosing(current.end(isDropped ? false : { segment: last.segment })
        .then(() => isDropped && last.segment && sendUnsent(last, "cancelled")));
      stream.getTracks().forEach((track) => track.stop());
      recording = null;
      const outcome = isTooShort && !isCancelled ? "Too short: nothing sent." : isCancelled && last.segment ? `Recording cancelled after part ${segment - 1}.` : "";
      if (outcome) notify(outcome, false);
      drawVoice();
      return finished;
    }
    return { part, started, isOpening: false, stop: finish };
  }

  /** Make read-only waits for promise: a voice act on its way to C3. */
  function beforeClosing(promise) {
    const done = () => sending.delete(promise);
    sending.add(promise);
    promise.then(done, done);
    return promise;
  }

  const voiceHeaders = ({ part, segment, hasMore }) => ({ "X-Board-Part": part,
    ...(segment && { "X-Voice-Segment": String(segment) }), ...(hasMore && { "X-Voice-More": "1" }) });

  /** Posts one voice act; a lost connection, 429 or 503 is retried with the same client_id, so C3 takes it once. */
  async function postVoice(headers, body, onAttempt = () => {}) {
    const request = { method: "POST", headers: { ...headers, "X-Client-Id": randomID() }, body };
    for (let attempt = 0; ; attempt++) {
      onAttempt(attempt);
      const response = await fetch(`${base}/act`, request).catch(() => null);
      if (response?.ok) return { id: (await response.json().catch(() => ({}))).id };
      if (response && response.status !== 429 && response.status !== 503) return { error: (await response.json().catch(() => ({}))).error || "unavailable" };
      await sleep(Number(response?.headers.get("Retry-After")) * 1000 || Math.min(VOICE.retryMs * 2 ** attempt, RETRY_MAX_MS));
    }
  }

  /** The act for a part whose audio never reached C3 (cancelled or refused), so the session still sees every part. */
  const sendUnsent = (fields, reason) => postVoice({ "Content-Type": "audio/webm", ...voiceHeaders(fields), "X-Voice-Unsent": reason }, "");

  /** Sends one recording. A part C3 refuses still reaches the session, as a line saying it was not sent. */
  async function upload({ part, blob, segment, hasMore }) {
    const voice = { key: `voice-${++voiceKeys}`, part, segment, has_more: Boolean(hasMore) };
    const sent = await postVoice({ "Content-Type": blob.type, ...voiceHeaders({ part, segment, hasMore }) }, blob,
      (attempt) => tellVoice({ ...voice, status: "sending", attempt }));
    if (!sent.error) return tellVoice({ ...voice, status: "sent", id: sent.id });
    tellVoice({ ...voice, status: "not_sent", error: sent.error });
    const unsent = segment ? await sendUnsent({ part, segment, hasMore }, "refused") : {};
    // The refused part's line in the log takes its row's place in the frame.
    if (unsent.id && isFrameReady) toFrame({ c3: "voice", voice: { ...voice, status: "not_sent", error: sent.error, id: unsent.id } });
    return undefined;
  }

  /** An upload's status for the page (c3.on("voice"), which shows it in its chat); one C3 refused is also a notice. */
  function tellVoice(voice) {
    if (voice.status === "sending") uploads.set(voice.key, voice);
    else uploads.delete(voice.key);
    if (voice.status === "not_sent") notify(`${voice.segment ? `Part ${voice.segment}` : "Voice note"} not sent: ${VOICE_REASONS[voice.error] ?? "C3 refused it"}.`);
    if (isFrameReady) toFrame({ c3: "voice", voice });
  }

  function setTyping(isTyping, isStatusNew = false) {
    clearTimeout(typingTimer);
    if (isTyping) typingTimer = setTimeout(() => setTyping(false), TYPING_MS);
    if (state.typing === isTyping && !isStatusNew) return;
    state.typing = isTyping;
    drawStatus();
    if (isFrameReady) toFrame({ c3: "status", status: frameStatus() });
  }

  function receive(kind, data) {
    if (!isSynced) return early.push([kind, data]);
    if (kind === "notice") {
      notify(data.text); // the shell only: never the frame
    } else if (kind === "status") {
      if (data.live !== state.live || data.closed !== state.closed) notify("");
      if (data.closed && recording) {
        recording.stop(true);
        notify("The board is read-only now: the recording stopped.");
      }
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
    drawStatus();
    if (next === null || !isFrameReady) {
      pageText = next;
      return reloadFrame();
    }
    toFrame({ c3: "page", entry, previous: pageText, next });
    pageText = next;
  }

  function reloadFrame() {
    isFrameReady = false;
    composer = { ...composer, part: null, hasText: false }; // the new page names its part again
    drawVoice();
    frame.src = pageURL();
  }

  function sendReady() {
    isFrameReady = true;
    const { board, title, rev, viewer, live, waiting, closed, log } = state;
    toFrame({ c3: "ready", ready: { board, title, rev, viewer, live, waiting, closed, log, kept } });
    toFrame({ c3: "status", status: frameStatus() });
    for (const voice of uploads.values()) toFrame({ c3: "voice", voice });
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
    const request = JSON.stringify({ client_id: randomID(), ...body });
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

  /** The composer a focus message names, or null if any field is malformed: the part (null: none), its Send's box in
   *  frame CSS pixels (null: the default corner; kept for a recording when the part goes), whether it has text. */
  function readComposer({ value, dock = null, hasText = false }) {
    const isPart = value === null || typeof value === "string" && PART.test(value);
    const isDock = dock === null || typeof dock === "object" && ["x", "y", "width", "height"].every((side) => Number.isFinite(dock[side]))
      && dock.width >= 0 && dock.height >= 0;
    if (!isPart || !isDock || typeof hasText !== "boolean") return null;
    if (value === null) return { part: null, dock: composer.dock, hasText: false };
    return { part: value, dock: dock && { x: dock.x, y: dock.y, width: dock.width, height: dock.height }, hasText };
  }

  async function onFrameMessage({ op, call, body, value, dock, hasText }) {
    if (op === "hello") return sendReady();
    if (op === "reload") return reloadFrame();
    if (op === "focus") {
      const next = readComposer({ value, dock, hasText });
      if (next) composer = next;
      drawVoice();
      return toFrame({ c3: "result", call, ...(next ? {} : { error: "invalid" }) });
    }
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

  // A recording going on ends first (after a part has gone out, with its last act), and every voice act on its way
  // reaches C3 before the board stops taking acts.
  controls.close.addEventListener("click", async () => {
    if (!confirm("This makes the board read-only for good.")) return;
    controls.panel.hidePopover?.();
    const wasRecording = Boolean(recording);
    isClosing = true;
    recording?.stop(true);
    if (sending.size) notify("Sending the voice notes on their way first…");
    await Promise.all(sending);
    const response = await fetch(`${base}/close`, { method: "POST" }).catch(() => null);
    isClosing = false;
    if (response?.ok) receive("status", { ...state, closed: true });
    notify(!response?.ok ? "Making it read-only failed. Try again." : wasRecording ? "The board is read-only now: the recording stopped." : "");
    drawVoice();
  });
  controls.panel.addEventListener("toggle", (event) => {
    isPanelOpen = event.newState === "open";
    if (isPanelOpen) { // the panel shows the notice itself
      isNoticeUnseen = false;
      toastFade?.cancel();
      controls.toast.hidden = true;
    }
    drawBadge();
  });

  controls.mic.addEventListener("click", (event) => { // a real tap only: the shell's own control, never the page
    if (!event.isTrusted) return;
    if (!canRecord) return notify("Voice needs the HTTPS link.", false);
    if (recording) recording.stop(false);
    else startRecording();
  });
  controls.cancel.addEventListener("click", () => recording?.stop(true));
  addEventListener("resize", drawVoice);
  addEventListener("beforeunload", (event) => { if (recording || sending.size) event.preventDefault(); });

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

  /** The circle while the stream is down or refused: the amber dot, never the last live state. */
  function showOffline(text) {
    document.body.dataset.mode = "offline";
    controls.session.textContent = text;
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
    drawStatus();
    const { rev } = state;
    pages = pages.then(async () => { pageText = rev > 0 ? await readPage(rev) : null; });
    reloadFrame();
  }

  /** After a reconnect: the lines and status the stream missed, through receive(), which drops lines already held. */
  function catchUp(fresh) {
    Object.assign(state, { cap: fresh.cap, title: fresh.title });
    for (const entry of fresh.log) receive(entry.dir === "in" && entry.op !== "edit" ? "action" : entry.op, entry);
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
