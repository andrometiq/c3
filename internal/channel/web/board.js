// C3's board shell: the status circle and its panel, the chat sheet, the mic, and the bridge between this board's
// sandboxed frame (the agent's page, which fills the window) and C3. Only messages from the frame's own window are
// taken; agent and page strings reach this document only as text.
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
  // The docked mic (a page that draws its own chat) is a circle this wide, kept this far inside the frame's visible box
  // and from the status circle; the recording bar sits gapPx beside it. A page that names no spot gets the corner.
  const MIC = { minPx: 44, maxPx: 56, marginPx: 4, gapPx: 6, defaultPx: 48, cornerPx: 16 };
  // A press on a mic counts only if it began on that mic at least this long after the mic appeared or last moved: a
  // page that opens the sheet or moves a docked mic as a finger lands never starts a recording.
  const MIC_SETTLE_MS = 500;
  const READ_AFTER_MS = 1000; // a part's replies count as read once its chat has shown this long in a visible tab
  const VOICE_TYPES = ["audio/webm;codecs=opus", "audio/webm", "audio/mp4"];
  const VOICE_REASONS = { closed: "the board is read-only", rate: "too many this minute", too_big: "too long",
    unreadable: "C3 couldn't read the recording", unauthorized: "signed out, reload to sign in", not_found: "this board can't be opened" };
  // A voice note's line in the chat: the upload's status, then C3's (heard: the transcript). A part of a long recording
  // that never reached C3 is a voice act with its own status, cancelled or refused.
  const VOICE_STATES = { sending: "Sending…", sent: "Transcribing…", transcribing: "Transcribing…",
    slow: "Transcribing… taking longer, C3 keeps trying.", no_speech: "No speech heard.", failed: "Couldn't transcribe it.",
    cancelled: "Cancelled, not sent.", refused: "Not sent: C3 refused it." };
  // The recording wave, as Telegram draws a voice note: one bar per stepMs (its loudest moment), newest on the right; a
  // bar is a dot in silence and grows with the voice. Loudness is RMS in dB, from gateDb above a noise floor that
  // follows the quiet level (down at once, up slowly, never above floorMaxDb) to ceilingDb at full height.
  const WAVE = { stepMs: 100, barPx: 3, gapPx: 2, levels: 200, ceilingDb: -10, gateDb: 6, floorStartDb: -60, floorMinDb: -90,
    floorMaxDb: -30, floorRise: 0.01, ringEase: 0.35 };
  // Spoken replies: why one wasn't read, and the setting's key in this shell's own storage (the frame has none).
  const SPEECH_REASONS = { not_found: "it is no longer on the board", rate: "too many this minute", unavailable: "speech is unavailable",
    offline: "C3 can't be reached", unauthorized: "signed out, reload to sign in", unplayable: "the audio didn't play" };
  const SPEECH_BLOCKED = "Tap the circle at the top right to hear replies.";
  const READ_ALOUD_KEY = "c3.board.readAloud";
  // The chat sheet. The page names a part and may give a title, one line for the top (its question, or what is decided)
  // and quick answers; C3 clips the strings to these and shows them as text.
  const CHAT = { titleRunes: 120, askRunes: 600, choices: 3, followPx: 80 };
  const CHAT_REASONS = { closed: "the board is read-only", rate: "too many this minute, wait a moment", too_big: "too long",
    offline: "C3 can't be reached, try again", invalid: "C3 refused it" };
  const GENERAL = "general"; // the board-level chat: the circle's "Talk to the agent"
  const GENERAL_TITLE = "General";
  const THREAD_OPS = new Set(["comment", "tap", "approve", "voice", "voice_ended", "message", "page"]);
  // An agent message whose last line is "[apply: <label>]" is a proposal: that line is drawn as its Approve button.
  const PROPOSAL = /(?:^|\n)\[apply: ([^\]\n]{1,24})\]$/u;

  const name = location.pathname.split("/")[2] ?? "";
  const base = `/board/${name}`;
  const READ_KEY = `c3.board.read.${name}`; // the owner's read marks for this board, in this browser
  const frame = document.getElementById("page");
  const controls = Object.fromEntries(["status", "badge", "panel", "title", "session", "typing", "unread", "talk", "notice", "close", "waiting",
    "toast", "shield", "mic", "recorder", "clock", "dock-wave", "cancel", "aloud", "player", "chat", "chat-title", "chat-close", "chat-scroll",
    "chat-ask", "chat-choices", "chat-lines", "chat-new", "chat-typing", "chat-error", "chat-quote-row", "chat-quote", "chat-drop-quote",
    "chat-text", "chat-recorder", "chat-cancel", "chat-clock", "chat-wave", "chat-send", "chat-mic"]
    .map((id) => [id.replace(/-(\w)/g, (_, letter) => letter.toUpperCase()), document.getElementById(id)]));
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
  let toastFade = null;
  let isPanelOpen = false;
  // Only this shell opens the microphone, after a real tap on its own button. The page only names the part (c3.chat,
  // or c3.focus for a page that draws its own chat, with where its Send sits); a recording keeps its part.
  const canRecord = Boolean(window.isSecureContext && navigator.mediaDevices?.getUserMedia && window.MediaRecorder);
  let composer = { part: null, dock: null, hasText: false }; // c3.focus: dock is the page's Send in frame CSS pixels, or null
  let recording = null; // {part, started, isOpening, stop(isCancelled)} while the mic is on
  let voiceKeys = 0;
  let isClosing = false; // Make read-only is waiting for the voice acts on their way
  let micSettlesAt = 0; // a press on a mic that began before this performance.now() is ignored
  let micPress = null; // {mic, at}: the last trusted pointerdown on a mic, which its click must follow
  let placedMic = ""; // where the docked mic was last put, to see it move
  let audioContext = null; // the wave's, made in the first tap on a mic: a phone runs one only after a tap
  const uploads = new Map(); // key -> the latest status of a recording on its way to C3
  const sending = new Set(); // every voice act's promise until C3 answers it
  // Spoken replies play here, through one reusable player: the frame has no network. A phone lets a page play sound
  // only after a tap; Chromium counts a tap in the frame for this shell too, and a tap on the shell's circle always counts.
  const speechQueue = []; // replies waiting to be read: {id, audio}, audio a promise of {token} or {error}
  let speaking = null; // the queue entry the player is on
  let speechState = null; // {id, state, error}: the last one the frame heard, told again after a frame reload
  let isReadingAloud = readSetting(); // "Read replies aloud": each new agent message is read as it arrives
  let isAudioBlocked = false; // the browser refused to play until a tap: the reply waits at the queue's head
  let isAudioUnlocked = false; // the shell's silent clip played from a tap on it
  // The chat sheet's state lives here; the page only asks for a part's chat.
  let chat = null; // {part, title, ask, choices} while the sheet is open
  const drafts = new Map(); // part -> the owner's unsent text
  const quotes = new Map(); // part -> the page's words the owner quoted
  const pendingActs = new Map(); // part -> {op, text, at, id} from the owner's Send or quick answer until its line is in the log
  const approving = new Set(); // proposals whose approve is on its way, until its line is in the log
  const voiceShown = new Map(); // a voice row's key -> the status last drawn, so only a change fades in
  const pageTitles = new Map(); // part -> the title the page's markup gives it
  const chatTitles = new Map(); // part -> the title the page last opened its chat with
  let shownRows = 0; // rows the open chat had when last drawn: only new ones follow the bottom
  let readingPart = null; // the part the sheet has shown for READ_AFTER_MS in a visible tab: its replies are read
  let readTimer = 0;
  let marks = { from: 0, parts: new Map() }; // read marks: the agent's replies on a part after its mark are unread

  const sleep = (ms) => new Promise((resolve) => { setTimeout(resolve, ms); });
  const toFrame = (message) => frame.contentWindow.postMessage(message, "*");
  const pageURL = (rev) => `${base}/page?cap=${encodeURIComponent(state.cap)}${rev ? `&rev=${rev}&raw=1` : ""}`;
  /** The stored text of rev, or null when it can't be fetched or read in full. */
  const readPage = (rev) => fetch(pageURL(rev)).then((response) => (response.ok ? response.text() : null)).catch(() => null);
  const frameStatus = () => ({ live: state.live, waiting: state.waiting, closed: state.closed, typing: state.typing });
  const messages = (count) => `${count} message${count === 1 ? "" : "s"}`;
  const randomID = () => [...crypto.getRandomValues(new Uint8Array(16))].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  const clock = (ms) => `${Math.floor(ms / 60000)}:${String(Math.floor(ms / 1000) % 60).padStart(2, "0")}`;
  const clip = (text, runes) => ([...text].length > runes ? `${[...text].slice(0, runes - 1).join("")}…` : text);
  const isReply = (entry) => entry.dir === "out" && entry.op === "message" && Boolean(entry.part);
  const isTouch = () => matchMedia("(pointer: coarse)").matches;

  /** A new element with its classes, holding text (as text nodes) and elements; empty ones are left out. */
  function element(tag, className, ...children) {
    const node = document.createElement(tag);
    if (className) node.classList.add(...className.split(" "));
    node.append(...children.filter(Boolean));
    return node;
  }

  function drawStatus() {
    const { live, waiting, closed, typing, holder } = state;
    const title = state.title || name;
    controls.title.textContent = title;
    document.title = `${title} · board`;
    document.body.dataset.mode = closed ? "closed" : !live ? "ended" : typing ? "typing" : "live";
    const folder = holder?.cwd?.split(/[\\/]/).filter(Boolean).pop() ?? holder?.cwd;
    controls.session.textContent = closed ? "Read-only." : !live ? `Session ended.${waiting ? ` ${messages(waiting)} ${waiting === 1 ? "waits" : "wait"} for it.` : ""}`
      : holder ? `${holder.cli} · ${folder}` : "Live";
    // Waiting is a line at the panel's foot, never a badge: only unread replies ask for the owner's eye.
    controls.waiting.textContent = live && !closed && waiting ? `${messages(waiting)} waiting for the session` : "";
    controls.typing.hidden = !(live && typing);
    controls.close.hidden = closed;
    drawBadge();
    drawVoice();
    drawChat();
  }

  /** The circle's badge counts the agent's replies the owner hasn't seen, and the panel lists them; nothing else badges it. */
  function drawBadge() {
    const items = unreadParts();
    const count = items.reduce((sum, item) => sum + item.count, 0);
    controls.badge.hidden = count === 0;
    controls.badge.textContent = count > 9 ? "9+" : String(count);
    controls.unread.hidden = items.length === 0;
    controls.unread.replaceChildren(...items.map(({ part, count: replies, newest }) => {
      const button = element("button", "", element("strong", "", element("span", "", titleOf(part)), replies > 1 && element("span", "", String(replies))),
        element("span", "snippet", snippetOf(newest)));
      button.addEventListener("click", () => openFromPanel(part));
      return element("li", "", button);
    }));
  }

  /** The parts holding replies after their read mark, newest first: {part, count, newest}. */
  function unreadParts() {
    const byPart = new Map();
    for (const entry of state?.log ?? []) {
      if (!isReply(entry) || entry.id <= Math.max(marks.from, marks.parts.get(entry.part) ?? 0)) continue;
      byPart.set(entry.part, { part: entry.part, count: (byPart.get(entry.part)?.count ?? 0) + 1, newest: entry });
    }
    return [...byPart.values()].sort((a, b) => b.newest.id - a.newest.id);
  }

  /** The id of part's first reply after its read mark, or null when it has none unread. */
  const firstUnread = (part) => state.log.find((entry) => isReply(entry) && entry.part === part
    && entry.id > Math.max(marks.from, marks.parts.get(part) ?? 0))?.id ?? null;

  /** A reply's first line as it now reads (its latest edit, without a proposal's marker line). */
  function snippetOf(reply) {
    const text = state.log.findLast((entry) => entry.op === "edit" && entry.dir === "out" && entry.id === reply.id)?.text ?? reply.text;
    return (proposalOf(text)?.body ?? text).trim().split("\n")[0];
  }

  /** Starts the clock on the open part: after READ_AFTER_MS on screen in a visible tab its replies count as read. A chat
   *  shut, switched or hidden before then marks nothing, so a page flashing chats open can't clear the badge. */
  function watchReading() {
    clearTimeout(readTimer);
    readingPart = null;
    if (!chat || document.hidden) return;
    const { part } = chat;
    readTimer = setTimeout(() => {
      readingPart = part;
      markRead(part);
    }, READ_AFTER_MS);
  }

  /** The owner's read mark on part moves to its newest reply, once the sheet has shown part long enough (watchReading). */
  function markRead(part) {
    if (document.hidden || !state || part !== readingPart) return;
    const newest = state.log.findLast((entry) => isReply(entry) && entry.part === part)?.id ?? 0;
    if (newest <= (marks.parts.get(part) ?? 0)) return;
    marks.parts.set(part, newest);
    try { localStorage.setItem(READ_KEY, JSON.stringify({ from: marks.from, parts: Object.fromEntries(marks.parts) })); } catch { /* this tab keeps them */ }
    drawBadge();
  }

  /** The read marks this browser keeps for the board. On a first visit what is on the board already counts as read. */
  function loadMarks(log) {
    try {
      const stored = JSON.parse(localStorage.getItem(READ_KEY));
      if (Number.isSafeInteger(stored?.from) && stored.parts && typeof stored.parts === "object") {
        return { from: stored.from, parts: new Map(Object.entries(stored.parts).filter(([, id]) => Number.isSafeInteger(id))) };
      }
    } catch { /* unreadable: start over */ }
    const fresh = { from: log.findLast(isReply)?.id ?? 0, parts: new Map() };
    try { localStorage.setItem(READ_KEY, JSON.stringify({ from: fresh.from, parts: {} })); } catch { /* this tab keeps them */ }
    return fresh;
  }

  /** A part's name for the sheet and the panel: the title the page opened its chat with, else its markup's, else its address. */
  const titleOf = (part) => (part === GENERAL ? GENERAL_TITLE : chatTitles.get(part) || pageTitles.get(part) || part);

  /** Each part's title from the stored page: a JSON part's "title", else the part's first heading. Returns the parsed page. */
  function readTitles(text) {
    if (text === null) return null;
    pageTitles.clear();
    const parsed = new DOMParser().parseFromString(text, "text/html");
    for (const part of parsed.querySelectorAll("[data-c3-part]")) {
      let title = part.querySelector("h1, h2, h3, h4, h5, h6")?.textContent;
      if (part.matches('script[type="application/json"]')) {
        try { title = JSON.parse(part.textContent)?.title; } catch { title = null; }
      }
      if (typeof title === "string" && title.trim()) pageTitles.set(part.getAttribute("data-c3-part"), clip(title.trim(), CHAT.titleRunes));
    }
    return parsed;
  }

  /** A notice under the circle for a few seconds, then it fades. A kept one (the default) stays in the panel;
   *  notify("") clears it. Text only, and never the frame. */
  function notify(text, isKept = true) {
    if (isKept) controls.notice.textContent = text;
    toastFade?.cancel();
    controls.toast.textContent = text;
    controls.toast.hidden = !text;
    if (!text) return;
    toastFade = controls.toast.animate([{ opacity: 1, offset: 0.85 }, { opacity: 0 }], NOTICE_MS);
    toastFade.finished.then(() => { controls.toast.hidden = true; }, () => {});
  }

  /** A mic that just appeared or moved waits MIC_SETTLE_MS before it takes a tap. */
  function settleMic() {
    micSettlesAt = performance.now() + MIC_SETTLE_MS;
  }

  /** The docked mic shows where the page's Send sits while its composer is empty (not while that spot has no size: the
   *  page hides it), and through a whole recording; never while C3's own chat sheet is open, which has its mic. */
  function drawVoice() {
    if (chat) drawComposer();
    const hasSpot = composer.dock === null || composer.dock.width > 0 && composer.dock.height > 0;
    const isShown = !chat && (Boolean(recording) || composer.part !== null && !composer.hasText && hasSpot && Boolean(state) && !state.closed);
    controls.mic.hidden = !isShown;
    controls.recorder.hidden = !(isShown && recording);
    controls.clock.textContent = controls.chatClock.textContent = recording && !recording.isOpening ? clock(performance.now() - recording.started) : "";
    if (!isShown) {
      placedMic = "";
      return;
    }
    const frameBox = frame.getBoundingClientRect();
    const mic = micBox(frameBox);
    place(controls.mic, mic);
    const spot = [mic.left, mic.top, mic.width].map(Math.round).join();
    if (spot !== placedMic) settleMic(); // it appeared or the page moved it
    placedMic = spot;
    controls.mic.disabled = recording ? recording.isOpening : isClosing;
    controls.mic.ariaPressed = String(Boolean(recording));
    if (recording) place(controls.recorder, recorderBox(frameBox, mic));
  }

  const clamp = (value, low, high) => Math.min(Math.max(value, low), high);
  /** The part of the frame's box the window shows, MIC.marginPx in from each side. */
  const visibleBox = (frameBox) => ({ left: Math.max(frameBox.left, 0) + MIC.marginPx, top: Math.max(frameBox.top, 0) + MIC.marginPx,
    right: Math.min(frameBox.right, innerWidth) - MIC.marginPx, bottom: Math.min(frameBox.bottom, innerHeight) - MIC.marginPx });

  /** Where the docked mic goes: centred on the page's Send spot, mapped through the frame's box, as a circle MIC.minPx to
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
    const part = chat?.part ?? composer.part;
    if (recording || !part || state.closed || isClosing) return;
    stopSpeech(); // recording and speaking never overlap
    if (!audioContext && window.AudioContext) audioContext = new AudioContext(); // in the tap, so a phone lets it run
    audioContext?.resume().catch(() => {});
    let isCancelled = false;
    recording = { part, started: performance.now(), isOpening: true, stop: () => { isCancelled = true; } };
    drawVoice();
    const stream = await navigator.mediaDevices.getUserMedia({ audio: { echoCancellation: true, noiseSuppression: true, autoGainControl: true } })
      .catch(() => null);
    if (!stream || isCancelled) {
      stream?.getTracks().forEach((track) => track.stop());
      recording = null;
      if (!stream) notify("Microphone unavailable: check the browser's permission.");
      playNext();
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
    const stopListening = listen(stream);
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
      stopListening();
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
      playNext(); // replies that came while the mic was on
      return finished;
    }
    return { part, started, isOpening: false, stop: finish };
  }

  /** The wave and the ring: the mic's loudness, as a bar every WAVE.stepMs moving left and as a ring around the mic
   *  that swells with the voice, so the owner sees at a glance that the mic hears them. Returns what stops it. */
  function listen(stream) {
    if (!audioContext) return () => {};
    const analyser = audioContext.createAnalyser();
    const source = audioContext.createMediaStreamSource(stream);
    source.connect(analyser);
    const samples = new Float32Array(analyser.fftSize);
    const levels = [];
    let floorDb = WAVE.floorStartDb, loudestDb = -Infinity, ring = 0, barStarted = performance.now();
    let request = requestAnimationFrame(function step(now) {
      request = requestAnimationFrame(step);
      analyser.getFloatTimeDomainData(samples);
      const decibels = decibelsOf(samples);
      loudestDb = Math.max(loudestDb, decibels);
      ring += (levelAt(decibels, floorDb) - ring) * WAVE.ringEase; // smoothed over a few frames
      ringTo(ring);
      if (now - barStarted < WAVE.stepMs) return;
      barStarted = now;
      levels.push(levelAt(loudestDb, floorDb));
      levels.splice(0, levels.length - WAVE.levels);
      floorDb = clamp(floorDb + (loudestDb - floorDb) * (loudestDb < floorDb ? 1 : WAVE.floorRise), WAVE.floorMinDb, WAVE.floorMaxDb);
      loudestDb = -Infinity;
      drawWave(levels);
    });
    return () => {
      cancelAnimationFrame(request);
      source.disconnect();
      ringTo(0);
    };
  }

  /** The RMS loudness of samples in dB below full scale. */
  function decibelsOf(samples) {
    const meanSquare = samples.reduce((sum, sample) => sum + sample * sample, 0) / samples.length;
    return 10 * Math.log10(Math.max(meanSquare, 1e-12));
  }

  /** decibels as a level: 0 up to gateDb above the noise floor (silence), 1 at ceilingDb and above. */
  const levelAt = (decibels, floorDb) => clamp((decibels - floorDb - WAVE.gateDb) / (WAVE.ceilingDb - floorDb - WAVE.gateDb), 0, 1);

  /** The ring behind both mics: a number from 0 (at the button's edge) to 1, never a page value. It doesn't move the
   *  button or widen what it takes taps on (the ring takes none). */
  function ringTo(level) {
    for (const mic of [controls.mic, controls.chatMic]) mic.style.setProperty("--level", level.toFixed(2));
  }

  /** Draws levels (0 to 1) as round centred bars, the newest at the right, on whichever wave canvas shows: silence is a
   *  dot as tall as it is wide. */
  function drawWave(levels) {
    for (const canvas of [controls.chatWave, controls.dockWave]) {
      if (!canvas.clientWidth) continue;
      const scale = devicePixelRatio || 1;
      canvas.width = Math.round(canvas.clientWidth * scale);
      canvas.height = Math.round(canvas.clientHeight * scale);
      const context = canvas.getContext("2d");
      context.fillStyle = getComputedStyle(canvas).color;
      const [width, step] = [WAVE.barPx * scale, (WAVE.barPx + WAVE.gapPx) * scale];
      const shown = levels.slice(-Math.floor(canvas.width / step));
      context.beginPath();
      shown.forEach((level, index) => {
        const height = width + level * (canvas.height - width);
        (context.roundRect ?? context.rect).call(context, canvas.width - (shown.length - index) * step, (canvas.height - height) / 2, width, height, width / 2);
      });
      context.fill();
    }
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
    // The refused part's line in the log takes its row's place.
    if (unsent.id) tellVoice({ ...voice, status: "not_sent", error: sent.error, id: unsent.id }, false);
    return undefined;
  }

  /** An upload's status: a row in C3's chat until its voice act is in the log, and an event for the page (c3.on("voice"));
   *  one C3 refused is also a notice. */
  function tellVoice(voice, isNew = true) {
    if (voice.id && state.log.some((entry) => entry.id === voice.id)) uploads.delete(voice.key); // its line is in already
    else uploads.set(voice.key, { ...voice, at: uploads.get(voice.key)?.at ?? new Date().toISOString() });
    if (isNew && voice.status === "not_sent") notify(`${voice.segment ? `Part ${voice.segment}` : "Voice note"} not sent: ${VOICE_REASONS[voice.error] ?? "C3 refused it"}.`);
    if (isFrameReady) toFrame({ c3: "voice", voice });
    drawChat();
  }

  /** Reply id's audio from C3: {token}, or {error} with C3's rule. */
  async function fetchSpeech(id) {
    const response = await fetch(`${base}/speak`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ id }) })
      .catch(() => null);
    const body = (await response?.json().catch(() => null)) ?? {};
    if (response?.ok && /^[0-9a-f]{32}$/.test(body.token)) return { token: body.token };
    return { error: response ? body.error || "unavailable" : "offline" };
  }

  /** Queues reply id and asks C3 for its audio at once, so it is ready when its turn comes. While the browser waits for a
   *  tap, only the newest reply waits with it. */
  function queueSpeech(id) {
    if (isAudioBlocked) speechQueue.splice(0);
    speechQueue.push({ id, audio: fetchSpeech(id) });
    playNext();
  }

  /** Plays the next queued reply, unless one plays, the mic is on or the browser wants a tap first. */
  async function playNext() {
    if (speaking || recording || isAudioBlocked || !speechQueue.length) return;
    const entry = speechQueue.shift();
    speaking = entry;
    tellSpeech(entry.id, "loading");
    const { token, error } = await entry.audio;
    if (speaking !== entry) return; // stopped meanwhile
    if (error) return endSpeech(error);
    playAudio(token).then(() => {
      if (speaking !== entry) return;
      tellSpeech(entry.id, "playing");
      if (controls.notice.textContent === SPEECH_BLOCKED) notify(""); // the browser plays again
    }, (problem) => {
      if (speaking !== entry) return;
      if (problem.name !== "NotAllowedError") return endSpeech("unplayable");
      speaking = null;
      speechQueue.unshift(entry);
      isAudioBlocked = true;
      tellSpeech(entry.id, "failed", "blocked");
      notify(SPEECH_BLOCKED);
    });
  }

  /** Plays name on the shell's one player: a token C3 gave for this board's audio (fetchSpeech checks it), or "unlock",
   *  C3's silent clip. */
  function playAudio(name) {
    controls.player.src = `${base}/audio/${name}`;
    return controls.player.play();
  }

  /** The player is done with the reply (error: why it wasn't read); the next one plays. */
  function endSpeech(error = "") {
    const { id } = speaking;
    speaking = null;
    tellSpeech(id, error ? "failed" : "ended", error);
    if (error) notify(`Couldn't read the reply aloud: ${SPEECH_REASONS[error] ?? "C3 refused it"}.`, false);
    playNext();
  }

  /** Stops the player and empties the queue: a tap on a reply, c3.speak(null), the mic or "Read replies aloud" off. */
  function stopSpeech() {
    speechQueue.splice(0);
    if (!speaking) return;
    const { id } = speaking;
    speaking = null;
    controls.player.pause();
    tellSpeech(id, "ended");
  }

  /** A reply's place in the player, for the chat's play buttons and the page (c3.on("speech")): loading, playing, ended,
   *  or failed with C3's rule (blocked: the browser waits for a tap). */
  function tellSpeech(id, phase, error) {
    speechState = { id, state: phase, ...(error && { error }) };
    if (isFrameReady) toFrame({ c3: "speech", speech: speechState });
    drawChat();
  }

  /** c3.speak(id) from the page, or a play button in the chat: id plays now, in place of whatever played or waited; null
   *  stops. Not while the mic is on. Returns the rule refusing it, or "". */
  function speakForPage(id) {
    if (id !== null && !(Number.isSafeInteger(id) && id > 0)) return "invalid";
    if (id !== null && recording) {
      notify("Stop recording to listen.", false);
      return "recording";
    }
    stopSpeech();
    if (id === null) return "";
    isAudioBlocked = false; // the tap that asked may have let the shell play: try
    queueSpeech(id);
    return "";
  }

  /** A new agent message, read aloud when the owner turned that on. */
  function readAloud(id) {
    if (isReadingAloud) queueSpeech(id);
  }

  /** A real tap on the shell lets its player play on a phone: the silent clip plays, then the reply that waited. Only when
   *  sound is wanted, since any sound pauses the owner's own music. */
  function unlockAudio(event) {
    const isWanted = isAudioBlocked || isReadingAloud && !isAudioUnlocked;
    if (!event.isTrusted || speaking || !isWanted) return;
    playAudio("unlock").then(() => {
      isAudioUnlocked = true;
      isAudioBlocked = false;
      playNext();
    }, () => {});
  }

  /** "Read replies aloud" is this browser's, kept in the shell's storage; off where storage fails. */
  function readSetting() {
    try { return localStorage.getItem(READ_ALOUD_KEY) === "on"; } catch { return false; }
  }

  function saveSetting(isOn) {
    try { localStorage.setItem(READ_ALOUD_KEY, isOn ? "on" : "off"); } catch { /* this tab keeps it */ }
  }

  // ---------- the chat sheet ----------

  /** Opens (or refreshes) the sheet on part. options are the page's, checked by readChat; null when the shell opens it
   *  (the panel), which keeps what the page gave last. A chat that opens marks where its unread replies begin, and the
   *  mark stays put while it is open. A recording keeps its own part on screen. Returns the rule refusing it, or "". */
  function openChat(part, options) {
    if (!state) return "offline";
    if (recording && part !== recording.part) return "recording";
    const isNew = chat?.part !== part;
    const fields = options ?? (isNew ? { title: "", ask: "", choices: [] } : chat);
    chat = { part, title: fields.title, ask: fields.ask, choices: fields.choices, unreadFrom: isNew ? firstUnread(part) : chat.unreadFrom };
    if (fields.title) chatTitles.set(part, fields.title);
    if (options?.quote) quotes.set(part, options.quote);
    if (isNew) {
      shownRows = 0;
      controls.chatText.value = drafts.get(part) ?? "";
      showChatError("");
      controls.chat.hidden = false;
      settleMic(); // the sheet's mic just appeared, at a spot the page chose the moment of
      if (isFrameReady) toFrame({ c3: "chat", chat: { part } });
    }
    if (isNew) watchReading();
    drawVoice();
    drawChat();
    if (isNew && !isTouch() && !state.closed) controls.chatText.focus(); // on a keyboard, straight to typing
    else if (isNew) controls.chatTitle.focus(); // on touch no keyboard pops up; the shell holds the focus
    return "";
  }

  /** Shuts the sheet, except while it records (the recording's part stays on screen). Returns the rule refusing it, or "". */
  function closeChat() {
    if (!chat) return "";
    if (recording) return "recording";
    chat = null;
    controls.chat.hidden = true;
    watchReading();
    if (isFrameReady) toFrame({ c3: "chat", chat: { part: null } });
    drawVoice();
    return "";
  }

  /** An unread reply in the panel: the page brings its part into view (c3.on("reveal")) and its chat opens. */
  function openFromPanel(part) {
    controls.panel.hidePopover?.();
    if (isFrameReady) toFrame({ c3: "reveal", reveal: { part } });
    if (openChat(part, null)) notify("Stop recording first.", false);
  }

  /** A chat request from the page: the part (null closes; never the board-level chat) and its strings, each a string or absent, clipped to fit; up to
   *  CHAT.choices quick answers, each one C3 takes as a tap. Null if any field is malformed. */
  function readChat({ value, title, ask, choices = [], quote }) {
    const isText = (text) => text === undefined || text === null || typeof text === "string";
    const isChoice = (choice) => typeof choice === "string" && choice.trim() !== "" && new TextEncoder().encode(choice).length <= LIMITS.valueBytes;
    if (!(value === null || typeof value === "string" && PART.test(value) && value !== GENERAL) || ![title, ask, quote].every(isText)) return null;
    if (!Array.isArray(choices) || choices.length > CHAT.choices || !choices.every(isChoice)) return null;
    return { part: value, title: clip(title ?? "", CHAT.titleRunes), ask: clip(ask ?? "", CHAT.askRunes), choices: [...choices],
      quote: clip(quote ?? "", LIMITS.quoteRunes) };
  }

  function showChatError(text) {
    controls.chatError.textContent = text;
    controls.chatError.hidden = !text;
  }

  /** Draws the open sheet from the log: the page's title, line and quick answers, the thread, the composer. */
  function drawChat() {
    if (!chat || !state) return;
    const { part } = chat;
    const pending = pendingActs.get(part);
    controls.chatTitle.textContent = titleOf(part);
    controls.chatAsk.textContent = chat.ask;
    controls.chatAsk.hidden = !chat.ask;
    controls.chatChoices.replaceChildren(...chat.choices.map((choice) => {
      const button = element("button", "", choice);
      button.disabled = state.closed || Boolean(pending);
      button.addEventListener("click", () => choose(part, choice));
      return button;
    }));
    controls.chatChoices.hidden = chat.choices.length === 0;
    const scroller = controls.chatScroll;
    const isNearBottom = scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight <= CHAT.followPx;
    const rows = threadRows(part);
    controls.chatLines.replaceChildren(...(rows.length ? rows : [element("li", "empty", part === GENERAL ? "Talk to the agent about anything here."
      : "Type, or tap the mic to talk.")]));
    const unreadLine = shownRows === 0 && controls.chatLines.querySelector(".unread-line");
    if (unreadLine) controls.chatScroll.scrollTop = unreadLine.offsetTop - 8; // opens where the owner stopped reading
    else if (rows.length > shownRows && (isNearBottom || shownRows === 0)) scrollChat();
    else if (rows.length > shownRows) controls.chatNew.hidden = false;
    shownRows = rows.length;
    const lastLine = state.log.findLast((entry) => entry.part === part && THREAD_OPS.has(entry.op));
    controls.chatTyping.hidden = !(state.typing && (pending || lastLine?.dir === "in"));
    controls.chatQuote.textContent = quotes.get(part) ?? "";
    controls.chatQuoteRow.hidden = !quotes.has(part);
    drawComposer();
  }

  function scrollChat() {
    controls.chatScroll.scrollTop = controls.chatScroll.scrollHeight;
    controls.chatNew.hidden = true;
  }

  /** As in Telegram: an empty box has the mic, text turns it into Send; while recording, the box gives way to Cancel,
   *  the time and the wave, and the mic (pressed) sends. */
  function drawComposer() {
    const hasText = controls.chatText.value.trim() !== "";
    const isRecording = Boolean(recording);
    const wasMicHidden = controls.chatMic.hidden;
    controls.chatText.hidden = isRecording;
    controls.chatRecorder.hidden = !isRecording;
    controls.chatSend.hidden = isRecording || !hasText || state.closed;
    controls.chatMic.hidden = !isRecording && (hasText || state.closed);
    if (wasMicHidden && !controls.chatMic.hidden) settleMic();
    controls.chatMic.disabled = recording ? recording.isOpening : isClosing;
    controls.chatMic.ariaPressed = String(isRecording);
    controls.chatSend.disabled = pendingActs.has(chat.part);
    controls.chatText.disabled = state.closed;
    controls.chatText.placeholder = state.closed ? "This board is read-only." : chat.part === GENERAL ? "Message the agent" : "Message";
    controls.chatClose.disabled = isRecording;
  }

  /** The open part's rows: its lines from the log (edits, statuses and reactions folded in), the owner's recordings C3 hasn't
   *  logged yet, and the owner's comment or quick answer on its way. */
  function threadRows(part) {
    const folded = { edits: new Map(), statuses: new Map(), reactions: new Map(), approved: new Set(), logged: new Set() };
    for (const entry of state.log) {
      folded.logged.add(entry.id);
      if (entry.op === "edit" && entry.dir === "out") folded.edits.set(entry.id, entry.text);
      if (entry.op === "edit" && entry.dir === "in" && entry.status) folded.statuses.set(entry.id, entry);
      if (entry.op === "reaction" || entry.op === "react") folded.reactions.set(entry.ref, `${folded.reactions.get(entry.ref) ?? ""}${entry.text}`);
      if (entry.op === "approve") folded.approved.add(entry.ref);
    }
    const lines = state.log.filter((entry) => entry.part === part && THREAD_OPS.has(entry.op));
    const latestReply = lines.findLast(isReply)?.id;
    const rows = lines.flatMap((entry) => (entry.id === chat.unreadFrom ? [element("li", "unread-line", "Unread"), rowOf(entry, folded, latestReply)]
      : [rowOf(entry, folded, latestReply)]));
    for (const voice of uploads.values()) {
      if (voice.part === part && !(voice.id && folded.logged.has(voice.id))) rows.push(voiceRow({ ...voice, upload: voice }, folded));
    }
    const pending = pendingActs.get(part);
    if (pending && !(pending.id && folded.logged.has(pending.id))) {
      rows.push(element("li", "msg mine", element("div", "body", pending.text), element("div", "meta", "sending…")));
    }
    return rows;
  }

  function rowOf(entry, folded, latestReply) {
    if (entry.op === "message") return replyRow(entry, folded, entry.id === latestReply);
    if (entry.op === "voice") return voiceRow(entry, folded);
    if (entry.op === "approve") { // the message as it read when approved, never a later edit
      const approved = state.log.findLast((line) => line.seq < entry.seq && line.dir === "out" && line.id === entry.ref && (line.op === "message" || line.op === "edit"));
      const proposal = proposalOf(approved?.text ?? "");
      const what = proposal ? `“${proposal.label}”` : "the agent's answer";
      return element("li", "note", entry.from_page ? `Sent by the page: approved ${what}.` : `You approved ${what}.`);
    }
    if (entry.op === "page") return element("li", "note", "The agent updated the page.");
    if (entry.op === "voice_ended") return element("li", "note", entry.text);
    if (entry.from_page) return element("li", "msg page", element("span", "label", "Sent by the page"), element("div", "body", entry.text), meta(entry));
    return element("li", "msg mine", element("div", "body", entry.text), meta(entry)); // the owner's comment or quick answer
  }

  /** The agent's reply: its text as it now reads, its reactions, its time and a play button; a proposal, while it is the
   *  agent's latest word here and not yet approved, carries its Approve button. */
  function replyRow(entry, folded, isLatest) {
    const text = folded.edits.get(entry.id) ?? entry.text;
    const proposal = proposalOf(text);
    const row = element("li", "msg", element("div", "body", proposal ? proposal.body : text),
      folded.reactions.has(entry.id) && element("div", "", folded.reactions.get(entry.id)));
    if (proposal && isLatest && !folded.approved.has(entry.id) && !state.closed) {
      const apply = element("button", "apply", approving.has(entry.id) ? "Approving…" : proposal.label);
      apply.disabled = approving.has(entry.id);
      apply.addEventListener("click", () => approveReply(entry.id));
      row.append(apply);
    }
    row.append(meta(entry, playButton(entry.id), folded.edits.has(entry.id) && "edited"));
    return row;
  }

  /** The owner's voice note: where the upload is, then C3's status, then the transcript fading in where the status was. */
  function voiceRow(entry, folded) {
    const settled = entry.upload ? null : folded.statuses.get(entry.id);
    const status = entry.upload?.status ?? settled?.status ?? entry.status ?? "transcribing";
    const text = status === "heard" ? settled.text : status === "not_sent"
      ? `Not sent: ${VOICE_REASONS[entry.upload.error] ?? "C3 refused it"}.` : VOICE_STATES[status] ?? VOICE_STATES.transcribing;
    const body = element("div", status === "heard" ? "body" : "body state", text);
    const key = entry.upload?.key ?? entry.id;
    if (voiceShown.has(key) && voiceShown.get(key) !== status) body.classList.add("fresh");
    voiceShown.set(key, status);
    return element("li", "msg mine", element("span", "label", entry.segment ? `🎤 Part ${entry.segment}` : "🎤 Voice note"), body, meta(entry));
  }

  /** A row's foot: what the caller adds, then the time. */
  function meta(entry, ...extras) {
    const at = new Date(entry.at ?? Date.now());
    return element("div", "meta", ...extras, element("span", "", at.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })));
  }

  /** A reply's play button, as the web chat's: ▶ reads it aloud, … while C3 makes the audio, ⏸ while it plays (a tap stops). */
  function playButton(id) {
    const phase = speechState?.id === id && ["loading", "playing"].includes(speechState.state) ? speechState.state : "";
    const [glyph, label] = { loading: ["…", "Preparing spoken reply"], playing: ["⏸", "Stop spoken reply"] }[phase] ?? ["▶", "Play spoken reply"];
    const button = element("button", "round play", glyph);
    button.ariaLabel = label;
    button.ariaBusy = String(phase === "loading");
    button.addEventListener("click", () => speakForPage(phase ? null : id));
    return button;
  }

  /** An agent message's proposal: its text without the marker line, and its button's label; else null. */
  function proposalOf(text) {
    const match = PROPOSAL.exec(text.trimEnd());
    return match && match[1].trim() ? { body: text.trimEnd().slice(0, match.index).trimEnd(), label: match[1].trim() } : null;
  }

  /** Posts the owner's comment or quick answer on part, shown as sending until its line is in the log; returns C3's rule refusing
   *  it, or "". */
  async function deliver(part, body, shown) {
    pendingActs.set(part, { op: body.op, text: shown, at: new Date().toISOString() });
    showChatError("");
    drawChat();
    if (chat?.part === part) scrollChat();
    const problem = checkAct(body);
    const { value, error } = problem ? { error: problem } : await postAct(body);
    if (error || state.log.some((entry) => entry.id === value.id)) pendingActs.delete(part); // its line may already be in
    else pendingActs.set(part, { ...pendingActs.get(part), id: value.id });
    drawChat();
    return error ?? "";
  }

  async function sendText() {
    const part = chat.part, text = controls.chatText.value.trim();
    if (!text || state.closed || pendingActs.has(part)) return;
    const quote = quotes.get(part);
    drafts.delete(part);
    quotes.delete(part);
    controls.chatText.value = "";
    const error = await deliver(part, { op: "comment", part, text, ...(quote && { quote }) }, text);
    if (!error) return;
    if (quote && !quotes.has(part)) quotes.set(part, quote);
    if (chat?.part === part && !controls.chatText.value) controls.chatText.value = text;
    else if (!drafts.has(part)) drafts.set(part, text);
    showChatError(`Not sent: ${CHAT_REASONS[error] ?? error}. Your text is back in the box.`);
    drawChat();
  }

  async function choose(part, choice) {
    if (state.closed || pendingActs.has(part)) return;
    const error = await deliver(part, { op: "tap", part, value: choice }, choice);
    if (error) showChatError(`Not sent: ${CHAT_REASONS[error] ?? error}.`);
  }

  async function approveReply(id) {
    const part = chat.part;
    approving.add(id);
    drawChat();
    const body = { op: "approve", ref: id, part };
    const problem = checkAct(body);
    const { error } = problem ? { error: problem } : await postAct(body);
    if (error || state.log.some((entry) => entry.op === "approve" && entry.ref === id)) approving.delete(id);
    showChatError(error ? `Not approved: ${CHAT_REASONS[error] ?? error}.` : "");
    drawChat();
  }

  function setTyping(isTyping, isStatusNew = false) {
    clearTimeout(typingTimer);
    if (isTyping) typingTimer = setTimeout(() => setTyping(false), TYPING_MS);
    if (state.typing === isTyping && !isStatusNew) return;
    state.typing = isTyping;
    drawStatus();
    if (isFrameReady) toFrame({ c3: "status", status: frameStatus() });
  }

  /** A stream event; isLive is false for the lines a reconnect catches up on. */
  function receive(kind, data, isLive = true) {
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
      if (isLive && data.dir === "out" && kind === "message") readAloud(data.id);
      if (pendingActs.get(data.part)?.id === data.id) pendingActs.delete(data.part); // the owner's line is in: its row is the log's now
      for (const [key, voice] of uploads) if (voice.id === data.id) uploads.delete(key); // so is a recording's
      if (data.op === "approve") approving.delete(data.ref); // and an approve's: its button is gone for good
      if (chat && isReply(data) && data.part === chat.part) markRead(chat.part); // the owner is looking at it
      if (kind === "page" && chat && isApproveOn(data.reply_to, chat.part) && !drafts.get(chat.part)) closeChat(); // show the change
      if (kind === "page") {
        state.rev = data.rev;
        pages = pages.then(() => showPage(data));
      } else if (isFrameReady) {
        toFrame({ c3: "entry", kind, entry: data });
      }
      drawBadge();
      drawChat();
    }
  }

  const isApproveOn = (id, part) => state.log.some((entry) => entry.id === id && entry.dir === "in" && entry.op === "approve" && entry.part === part);

  /** Hands the frame the previous and the new stored text; when that can't be done, reloads the frame. */
  async function showPage(entry) {
    const next = await readPage(entry.rev);
    const title = readTitles(next)?.title.trim();
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
    for (const voice of uploads.values()) if (voice.status === "sending") toFrame({ c3: "voice", voice });
    if (speaking && speechState) toFrame({ c3: "speech", speech: speechState });
    if (chat) toFrame({ c3: "chat", chat: { part: chat.part } }); // the sheet stayed open across the reload
  }

  /** An act body: the op's own fields only, each of its type and within its limit. */
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

  async function onFrameMessage(message) {
    const { op, call, body, value, dock, hasText } = message;
    if (op === "hello") return sendReady();
    if (op === "reload") return reloadFrame();
    if (op === "chat") {
      const request = readChat(message);
      const problem = !request ? "invalid" : request.part === null ? closeChat() : openChat(request.part, request);
      return toFrame({ c3: "result", call, ...(problem ? { error: problem } : {}) });
    }
    if (op === "focus") {
      const next = readComposer({ value, dock, hasText });
      if (next) composer = next;
      drawVoice();
      return toFrame({ c3: "result", call, ...(next ? {} : { error: "invalid" }) });
    }
    if (op === "speak") {
      const problem = speakForPage(value);
      return toFrame({ c3: "result", call, ...(problem ? { error: problem } : {}) });
    }
    if (op === "keep") {
      const isKept = typeof value === "string" && new TextEncoder().encode(value).length <= LIMITS.keepBytes;
      if (isKept) kept = value;
      return toFrame({ c3: "result", call, ...(isKept ? {} : { error: "too_big" }) });
    }
    if (op !== "act") return undefined;
    const problem = checkAct(body); // refuses a from_page of the page's own
    return toFrame({ c3: "result", call, ...(problem ? { error: problem } : await postAct({ ...body, from_page: true })) });
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
  // While the panel is open a clear shell layer covers the frame: a tap anywhere else shuts the panel (the popover's own
  // light dismiss) and never reaches the page, and the page can't shut it.
  controls.panel.addEventListener("toggle", (event) => {
    isPanelOpen = event.newState === "open";
    controls.shield.hidden = !isPanelOpen;
    if (!isPanelOpen) return;
    toastFade?.cancel(); // the panel shows the notice itself
    controls.toast.hidden = true;
  });
  controls.talk.addEventListener("click", () => {
    controls.panel.hidePopover?.();
    if (openChat(GENERAL, null)) notify("Stop recording first.", false);
  });

  /** A real press on one of the shell's own mics, judged when it went down: it must have begun on this mic once the mic
   *  had sat still for MIC_SETTLE_MS. A keyboard or assistive click (no press, detail 0) needs the shell's focus on the mic,
   *  which the page can't give it. */
  function onMicTap(event) {
    const press = micPress;
    micPress = null;
    if (!event.isTrusted) return;
    if (press ? press.mic !== event.currentTarget || press.at < micSettlesAt : event.detail !== 0) return;
    if (!canRecord) return notify("Voice needs the HTTPS link.", false);
    if (recording) recording.stop(false);
    else startRecording();
  }
  function onMicPress(event) {
    micPress = event.isTrusted ? { mic: event.currentTarget, at: performance.now() } : null;
  }
  for (const mic of [controls.mic, controls.chatMic]) mic.addEventListener("pointerdown", onMicPress);
  controls.mic.addEventListener("click", onMicTap);
  controls.chatMic.addEventListener("click", onMicTap);
  controls.cancel.addEventListener("click", () => recording?.stop(true));
  controls.chatCancel.addEventListener("click", () => recording?.stop(true));
  controls.chatClose.addEventListener("click", closeChat);
  controls.chatSend.addEventListener("click", sendText);
  controls.chatNew.addEventListener("click", scrollChat);
  controls.chatScroll.addEventListener("scroll", () => {
    const scroller = controls.chatScroll;
    if (scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight <= CHAT.followPx) controls.chatNew.hidden = true;
  });
  controls.chatDropQuote.addEventListener("click", () => {
    quotes.delete(chat.part);
    drawChat();
  });
  controls.chatText.addEventListener("input", () => {
    drafts.set(chat.part, controls.chatText.value);
    drawComposer();
  });
  controls.chatText.addEventListener("keydown", (event) => {
    // As the web chat: Enter sends and Shift+Enter breaks the line on a keyboard; on touch Enter is a newline.
    if (event.key !== "Enter" || event.shiftKey || event.isComposing || isTouch()) return;
    event.preventDefault();
    sendText();
  });
  addEventListener("keydown", (event) => { if (event.key === "Escape" && chat && !isPanelOpen) closeChat(); });
  document.addEventListener("visibilitychange", watchReading);
  controls.status.addEventListener("click", unlockAudio);
  controls.aloud.checked = isReadingAloud;
  controls.aloud.addEventListener("change", (event) => {
    isReadingAloud = controls.aloud.checked;
    saveSetting(isReadingAloud);
    if (isReadingAloud) unlockAudio(event);
    else stopSpeech();
  });
  // Only a reply that is playing ends here: the unlock clip may end while the next reply still loads.
  const isReplyPlaying = () => Boolean(speaking) && speechState.state === "playing";
  controls.player.addEventListener("ended", () => { if (isReplyPlaying()) endSpeech(); });
  controls.player.addEventListener("error", () => { if (isReplyPlaying()) endSpeech("unplayable"); });
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
    marks = loadMarks(state.log);
    drawStatus();
    const { rev } = state;
    pages = pages.then(async () => {
      pageText = rev > 0 ? await readPage(rev) : null;
      readTitles(pageText);
      drawBadge();
    });
    reloadFrame();
  }

  /** After a reconnect: the lines and status the stream missed, through receive(), which drops lines already held. */
  function catchUp(fresh) {
    Object.assign(state, { cap: fresh.cap, title: fresh.title });
    const seen = lastSeq;
    for (const entry of fresh.log) receive(entry.dir === "in" && entry.op !== "edit" ? "action" : entry.op, entry, false);
    const missed = fresh.log.findLast((entry) => entry.seq > seen && entry.dir === "out" && entry.op === "message");
    if (missed) readAloud(missed.id); // of the replies missed while away, only the newest is read
    receive("status", fresh);
    if (fresh.rev === state.rev) return;
    const { rev } = fresh; // its page line fell out of the history: show that revision from scratch
    state.rev = rev;
    pages = pages.then(async () => {
      pageText = await readPage(rev);
      readTitles(pageText);
      reloadFrame();
    });
  }

  connect();
})();
