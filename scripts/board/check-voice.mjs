#!/usr/bin/env node
// Board shell browser check: the REAL Go board routes (TestBoardBrowserHarness, whose fake agent answers every line, plays
// the broker's STT readback for a voice note, proposes when asked to "propose" and answers an approve with a new page),
// C3's real shell and SDK, and headless Chromium with a fake microphone (--use-fake-device-for-media-stream
// --use-fake-ui-for-media-stream). Nobody in the loop. The shell is served with one change: VOICE.segmentMs 2500 instead
// of 5 minutes, so the numbered-parts path runs in seconds.
// Native chat: C3 draws the chat sheet itself (c3.chat; c3.threads() on the starter page), with a Telegram-style
// composer, a moving wave while recording, unread replies on the circle, the general chat and the panel's light dismiss.
// The docked mic (c3.focus) stays for pages that draw their own chat.
// Composer: files from the paperclip (the browser's own picker), replies picked by a swipe or a hover Reply button,
// quote headers that jump to the message, a reply dropped once its message leaves the log, and a play button drawn as a
// filled disc.
// Usage: check-voice.mjs <c3 checkout> [--shots <dir>]. Prints PASS/FAIL per check; exit 1 on any FAIL.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, execFileSync } from "node:child_process";
import { Browser } from "./cdp.mjs";
import { findChromium } from "./chromium.mjs";

const [checkout = process.cwd(), , shots = ""] = process.argv.slice(2);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const work = fs.mkdtempSync(path.join(os.tmpdir(), "c3-voice-check-"));
const results = [];
const report = (isPass, name, detail) => { results.push(isPass); console.log(`${isPass ? "PASS" : "FAIL"}  ${name.padEnd(9)} ${detail}`); };

// Assets: the checkout's own, with a short part length.
const web = path.join(checkout, "internal/channel/web");
const assets = path.join(work, "assets");
fs.mkdirSync(assets);
for (const name of ["board.html", "board-sdk.js", "board-starter.html"]) fs.copyFileSync(path.join(web, name), path.join(assets, name));
const shellSource = fs.readFileSync(path.join(web, "board.js"), "utf8");
if (!shellSource.includes("segmentMs: 5 * 60 * 1000")) throw new Error("board.js has no 5-minute segmentMs to shorten");
fs.writeFileSync(path.join(assets, "board.js"), shellSource.replace("segmentMs: 5 * 60 * 1000", "segmentMs: 2500"));

const tone = path.join(work, "tone.mp3");
execFileSync("ffmpeg", ["-v", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=1.5", "-ac", "1", "-ar", "24000", "-c:a", "libmp3lame", "-b:a", "32k", tone]);
const info = path.join(work, "harness.json");
const harness = spawn("go", ["test", "./internal/channel/web/", "-run", "^TestBoardBrowserHarness$", "-count=1"], {
  cwd: checkout, stdio: "ignore",
  env: { ...process.env, C3_BOARD_HARNESS: info, C3_BOARD_HARNESS_SECONDS: "360", C3_BOARD_HARNESS_ASSETS: assets, C3_BOARD_HARNESS_SPEECH: tone },
});
for (let waited = 0; !fs.existsSync(info); waited += 200) {
  if (waited > 120000) throw new Error("the harness did not start");
  await sleep(200);
}
const { url, cookies, boards, control } = JSON.parse(fs.readFileSync(info, "utf8"));

const wrapper = path.join(work, "chromium");
// The fake mic plays this file on a loop from each capture's start: 1 s of silence, 1 s of loud speech-like sound (a voiced
// tone with syllable-rate swells), 1 s of silence, so the wave and the ring can be checked without anyone talking.
const speech = path.join(work, "speech.wav");
execFileSync("ffmpeg", ["-v", "error", "-f", "lavfi", "-i",
  "aevalsrc=exprs='if(between(t,1,2),(0.45*sin(2*PI*180*t)+0.15*sin(2*PI*540*t))*(0.6+0.4*sin(2*PI*4*t)),0)':s=48000:d=3",
  "-ac", "1", "-c:a", "pcm_s16le", speech]);
fs.writeFileSync(wrapper, `#!/bin/sh\nexec ${JSON.stringify(findChromium())} --use-fake-device-for-media-stream --use-fake-ui-for-media-stream --use-file-for-fake-audio-capture=${speech} "$@"\n`, { mode: 0o755 });
process.env.CHROMIUM = wrapper;
const browser = Browser.launch({ width: 390, height: 844 });
const errors = [];
try {
  const session = await browser.newPage();
  let frameSession = null;
  browser.onEvent(({ method, params, sessionId }) => {
    if (sessionId !== session && sessionId !== frameSession) return;
    if (method === "Runtime.exceptionThrown") errors.push(params.exceptionDetails.exception?.description ?? params.exceptionDetails.text);
    if (method === "Target.attachedToTarget" && params.targetInfo.type === "iframe") {
      frameSession = params.sessionId;
      browser.send("Runtime.enable", {}, frameSession).catch(() => {});
    }
  });
  const send = (method, params) => browser.send(method, params, session);
  for (const domain of ["Page", "Runtime", "Network"]) await send(`${domain}.enable`);
  // Keep each AudioContext the shell makes, so a check can suspend it as a phone does.
  await send("Page.addScriptToEvaluateOnNewDocument", { source: `(() => { const Base = window.AudioContext; if (!Base) return;
    window.__audioContexts = []; window.AudioContext = class extends Base { constructor(...args) { super(...args); window.__audioContexts.push(this); } }; })()` });
  await send("Target.setAutoAttach", { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });
  await send("Emulation.setDeviceMetricsOverride", { width: 390, height: 844, deviceScaleFactor: 2, mobile: true });
  await send("Network.setCookie", { ...cookies[0], url });
  await send("Page.navigate", { url: boards.plan });

  async function evaluate(expression, sessionId = session) {
    const result = await browser.send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true }, sessionId);
    if (result.exceptionDetails) throw new Error(`threw: ${result.exceptionDetails.exception?.description ?? result.exceptionDetails.text}`);
    return result.result.value;
  }
  const inShell = (expression) => evaluate(expression);
  const inFrame = (expression) => evaluate(expression, frameSession);
  async function until(check, what, ms = 10000) {
    for (const end = Date.now() + ms; Date.now() < end; await sleep(100)) if (await check().catch(() => false)) return true;
    throw new Error(`timed out waiting for ${what}`);
  }
  async function click({ x, y }) {
    for (const type of ["mousePressed", "mouseReleased"]) await send("Input.dispatchMouseEvent", { type, x, y, button: "left", clickCount: 1 });
    await sleep(100);
  }
  const center = (selector) => inShell(`(() => { const r = document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; })()`);
  const frameTop = () => inShell(`(() => { const r = document.getElementById("page").getBoundingClientRect(); return { x: r.x, y: r.y }; })()`);
  /** The middle of an element in the frame, in the shell's coordinates. */
  async function inFrameAt(selector) {
    const [top, spot] = [await frameTop(), await inFrame(`(() => { const r = document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect(); return { x: r.x + 20, y: r.y + 16 }; })()`)];
    return { x: top.x + spot.x, y: top.y + spot.y };
  }
  // The shell's own controls: boxes while shown (null when hidden), the sheet's texts, the circle and the panel.
  const ui = () => inShell(`(() => { const box = (id) => { const e = document.getElementById(id); if (!e || e.hidden || !e.checkVisibility()) return null;
      const r = e.getBoundingClientRect(); return { x: r.x, y: r.y, width: r.width, height: r.height }; };
    const text = (id) => document.getElementById(id).textContent;
    const mic = document.getElementById("chat-mic");
    return { chat: box("chat"), title: text("chat-title"), ask: box("chat-ask") ? text("chat-ask") : "", choices: [...document.querySelectorAll("#chat-choices button")].map((b) => b.textContent),
      lines: text("chat-lines"), mic: box("chat-mic"), send: box("chat-send"), field: box("chat-text"), recorder: box("chat-recorder"), pressed: mic.ariaPressed,
      clock: text("chat-clock"), placeholder: document.getElementById("chat-text").placeholder, applies: [...document.querySelectorAll("#chat-lines .apply")].map((b) => b.textContent),
      docked: box("mic"), dockBar: box("recorder"), dockPressed: document.getElementById("mic").ariaPressed, dockClock: text("clock"),
      status: box("status"), frame: box("page"), badge: document.getElementById("badge").hidden ? "" : text("badge"), panel: Boolean(box("panel")),
      unread: [...document.querySelectorAll("#unread button")].map((b) => b.textContent), waiting: text("waiting"), session: text("session"),
      notice: text("notice"), toast: document.getElementById("toast").hidden ? "" : text("toast"), error: box("chat-error") ? text("chat-error") : "",
      reply: box("chat-reply-row") ? text("chat-reply-text") : "", box: document.getElementById("chat-text").value }; })()`);
  const isRecording = async () => { const now = await ui(); return now.pressed === "true" && /^\d+:\d\d$/.test(now.clock); };
  const log = () => inShell(`fetch(location.pathname + "/state").then((r) => r.json()).then((s) => s.log)`);
  const shot = async (name) => { if (shots) { fs.mkdirSync(shots, { recursive: true }); await sleep(250); fs.writeFileSync(path.join(shots, name), Buffer.from((await send("Page.captureScreenshot", { format: "png" })).data, "base64")); } };
  const isInside = (inner, outer) => inner.x >= outer.x && inner.y >= outer.y && inner.x + inner.width <= outer.x + outer.width && inner.y + inner.height <= outer.y + outer.height;
  const isApart = (a, b) => a.x + a.width <= b.x || b.x + b.width <= a.x || a.y + a.height <= b.y || b.y + b.height <= a.y;
  const round = (box) => box && Object.fromEntries(Object.entries(box).map(([key, value]) => [key, typeof value === "number" ? Math.round(value) : value]));
  const typeInSheet = async (text) => { await inShell(`document.getElementById("chat-text").focus(), true`); await send("Input.insertText", { text }); await sleep(100); };
  const press = async (key) => { for (const type of ["keyDown", "keyUp"]) await send("Input.dispatchKeyEvent", { type, key, code: key, windowsVirtualKeyCode: key === "Escape" ? 27 : 13 }); await sleep(150); };
  const window_ = { x: 0, y: 0, width: 390, height: 844 };
  const settled = () => sleep(650); // past the mic's 500 ms settle time
  const answers = async (part) => (await log()).filter((entry) => entry.dir === "out" && entry.op === "message" && entry.part === part);

  await until(() => inFrame("Boolean(window.c3) && document.querySelector('[data-c3-part]') !== null"), "the page in its frame");
  await until(async () => (await ui()).status !== null, "the status circle");
  await inFrame(`window.__seen = { chat: [], reveal: [] }; c3.on("chat", (e) => __seen.chat.push(e.part)); c3.on("reveal", (e) => { __seen.reveal.push(e.part); return false; }); true`);
  await sleep(300);
  const idle = await ui();
  report(!idle.chat && !idle.docked && idle.frame.width === 390 && idle.frame.height === 844 && idle.status.x + idle.status.width <= 390 && idle.status.y < 20 && !idle.badge, "closed",
    `nothing open -> sheet ${idle.chat ? "shown" : "hidden"}, docked mic ${idle.docked ? "shown" : "hidden"}, frame ${JSON.stringify(round(idle.frame))}, circle ${JSON.stringify(round(idle.status))}, badge "${idle.badge}"`);

  // The page can't hear: no microphone in the frame, and no frame message starts the recorder.
  const framed = await inFrame(`navigator.mediaDevices ? navigator.mediaDevices.getUserMedia({ audio: true }).then(() => "opened", (e) => e.name) : "no mediaDevices"`);
  const before = (await log()).length;
  await inFrame(`parent.postMessage({ op: "act", call: 900, body: { op: "voice", part: "1" } }, "*"), parent.postMessage({ op: "record", call: 901 }, "*"), true`);
  await sleep(500);
  report(framed !== "opened" && (await log()).length === before && (await ui()).pressed === "false", "page", `frame getUserMedia -> ${framed}; frame "act voice" and "record" messages -> log ${before} -> ${(await log()).length}`);

  // A tap on a part (c3.threads()) opens C3's own sheet: the part's title, an empty box with the mic where Send goes.
  await click(await inFrameAt('[data-c3-part="1"] p'));
  await until(async () => (await ui()).chat !== null, "the chat sheet");
  await sleep(300);
  const empty = await ui();
  await shot("chat-empty.png");
  report(empty.title === "1 First part" && empty.mic && !empty.send && empty.field && isApart(empty.chat, empty.status) && Math.round(empty.chat.y + empty.chat.height) === 844
    && /Type, or tap the mic/.test(empty.lines) && JSON.stringify(await inFrame("__seen.chat")) === '["1"]', "open",
    `tap part 1 -> sheet ${JSON.stringify(round(empty.chat))} titled "${empty.title}", mic ${JSON.stringify(round(empty.mic))}, Send ${empty.send ? "shown" : "hidden"}; page heard chat ${JSON.stringify(await inFrame("__seen.chat"))}`);

  // Typing turns the mic into Send; Send posts the comment, the agent's answer lands in the thread; an empty box has the mic.
  await typeInSheet("Keep the intro short.");
  const typing = await ui();
  await shot("chat-typing.png");
  await click(await center("#chat-send"));
  await until(async () => /Answer to msg \d+ on part "1"/.test((await ui()).lines), "the agent's answer in the thread");
  const answered = await ui();
  const comment = (await log()).find((entry) => entry.op === "comment" && entry.part === "1");
  report(!typing.mic && typing.send && comment?.text === "Keep the intro short." && answered.mic && !answered.send && /Keep the intro short\./.test(answered.lines), "typing",
    `text in the box -> mic ${typing.mic ? "shown" : "hidden"}, Send ${typing.send ? "shown" : "hidden"}; Send -> log comment ${JSON.stringify(comment && { part: comment.part, text: comment.text })}; `
    + `emptied -> mic ${answered.mic ? "back" : "gone"}; thread "${answered.lines.slice(0, 120)}"`);

  // A mic the page just made appear takes no tap for 500 ms (a page can't slide it under a finger).
  await press("Escape");
  await until(async () => (await ui()).chat === null, "Escape to shut the sheet");
  await inFrame(`c3.chat("1"), true`);
  await until(async () => (await ui()).mic !== null, "the mic in the reopened sheet");
  await click(await center("#chat-mic"));
  await sleep(400);
  const early = await ui();
  // A press that began before the mic settled and ended after it: judged by where and when it went down.
  await press("Escape");
  await until(async () => (await ui()).chat === null, "Escape to shut the sheet");
  await inFrame(`c3.chat("1"), true`);
  await until(async () => (await ui()).mic !== null, "the mic in the sheet again");
  const spot = await center("#chat-mic");
  await send("Input.dispatchMouseEvent", { type: "mousePressed", ...spot, button: "left", clickCount: 1 });
  await sleep(700);
  await send("Input.dispatchMouseEvent", { type: "mouseReleased", ...spot, button: "left", clickCount: 1 });
  await sleep(300);
  const straddled = await ui();
  await settled();
  await click(await center("#chat-mic"));
  await until(isRecording, "recording after the mic settled");
  await sleep(900); // the whole step stays under the shortened 2.5 s part, so this note is one part
  // The wave's bar heights, from its pixels: bars of different heights mean it follows the level, not a fixed pattern.
  const waveBars = (id) => inShell(`(() => { const c = document.getElementById(${JSON.stringify(id)}); const d = c.getContext("2d").getImageData(0, 0, c.width, c.height).data;
    const heights = []; for (let x = 0; x < c.width; x++) { let n = 0; for (let y = 0; y < c.height; y++) if (d[(y * c.width + x) * 4 + 3]) n++; if (n) heights.push(n); }
    return { inked: heights.length, low: Math.min(...heights), high: Math.max(...heights), key: heights.join() }; })()`);
  const wave = await waveBars("chat-wave");
  await sleep(300);
  const wave2 = await waveBars("chat-wave");
  const recordingUi = await ui();
  await shot("chat-recording.png");
  report(early.pressed === "false" && straddled.pressed === "false" && recordingUi.recorder && !recordingUi.field && recordingUi.mic && wave.inked > 20 && wave2.inked > wave.inked
    && /^0:0[01]$/.test(recordingUi.clock), "record",
    `tap 0.1 s after the page opened the sheet -> ${early.pressed === "true" ? "RECORDING" : "ignored"}; pressed at once, released 0.7 s later -> ${straddled.pressed === "true" ? "RECORDING" : "ignored"}; after it settled -> recording ${recordingUi.clock}, box gave way to ${JSON.stringify(round(recordingUi.recorder))}; `
    + `wave ${wave.inked} -> ${wave2.inked} inked columns (moving)`);

  // Stop and send: the row reads Sending…, Transcribing…, then the transcript; then the agent answers.
  await inShell(`window.__voiceStates = []; new MutationObserver(() => { const rows = [...document.querySelectorAll("#chat-lines .msg.mine")].filter((r) => r.querySelector(".label"));
    const text = rows.at(-1)?.querySelector(".body")?.textContent; if (text && __voiceStates.at(-1) !== text) __voiceStates.push(text); }).observe(document.getElementById("chat-lines"), { childList: true, subtree: true }); true`);
  await click(await center("#chat-mic"));
  await until(async () => /harness transcript of[\s\S]*Answer to msg/.test((await ui()).lines), "the transcript, then the answer, in the thread");
  const states = await inShell("__voiceStates");
  const voices = (await log()).filter((entry) => entry.op === "voice" || entry.status);
  report(states.includes("Transcribing…") && /harness transcript of \d+ bytes/.test(states.at(-1)) && voices[0]?.part === "1" && voices[1]?.status === "heard", "voicerow",
    `the voice row read ${JSON.stringify(states)}; log ${JSON.stringify(voices.map(({ id, op, part, status }) => ({ id, op, part, status })))}`);

  await settled();
  await click(await center("#chat-mic"));
  await until(isRecording, "recording again");
  await click(await center("#chat-mic"));
  await sleep(400);
  const short = await ui();
  const afterShort = (await log()).length;
  report(short.toast === "Too short: nothing sent." && !short.badge, "short", `tap, tap -> toast "${short.toast}", badge "${short.badge}"`);

  await settled();
  await click(await center("#chat-mic"));
  await until(isRecording, "recording to cancel");
  await sleep(800);
  await click(await center("#chat-cancel"));
  await sleep(1200);
  const cancelled = await ui();
  report((await log()).length === afterShort && cancelled.pressed === "false" && !cancelled.recorder && cancelled.mic && cancelled.field, "cancel",
    `Cancel -> log ${afterShort} -> ${(await log()).length}, box back ${Boolean(cancelled.field)}, mic ${cancelled.mic ? "back" : "gone"}`);

  await settled();
  await click(await center("#chat-mic"));
  await until(isRecording, "a long recording");
  await inFrame(`c3.chat(null).then(() => "shut", (e) => e.message)`).then((answer) => { globalThis.closeWhileRecording = answer; });
  await sleep(6200);
  const stillRecording = (await ui()).pressed;
  await click(await center("#chat-mic"));
  await until(async () => (await log()).filter((entry) => entry.op === "voice").length === 4 && (await log()).filter((entry) => entry.status === "heard").length === 4, "three parts heard", 15000);
  const parts = (await log()).filter((entry) => entry.op === "voice").slice(1).map(({ segment, has_more }) => `${segment}${has_more ? "+" : ""}`);
  report(stillRecording === "true" && parts.join(",") === "1+,2+,3" && globalThis.closeWhileRecording === "recording", "parts",
    `6.2 s at 2.5 s parts -> recording still on (${stillRecording}); voice acts ${parts.join(", ")}; the page shutting the sheet meanwhile -> ${globalThis.closeWhileRecording}`);

  // After a part has gone out, the recording always ends with a last part the session sees.
  const voiceActs = async () => (await log()).filter((entry) => entry.op === "voice");
  const describe = (acts) => acts.map(({ segment, has_more, status }) => `${segment}${has_more ? "+" : ""}${status ? ` ${status}` : ""}`).join(", ");
  async function recordPastOnePart() {
    const before = (await voiceActs()).length;
    await settled();
    await click(await center("#chat-mic"));
    await until(isRecording, "recording past one part");
    await until(async () => (await voiceActs()).length === before + 1, "part 1 to go out", 8000);
    return before;
  }
  let partsBefore = await recordPastOnePart();
  await click(await center("#chat-cancel"));
  await until(async () => (await voiceActs()).length === partsBefore + 2, "the cancelled last part", 8000);
  const cancelledParts = (await voiceActs()).slice(partsBefore);
  const cancelNote = (await ui()).toast;
  report(describe(cancelledParts) === "1+, 2 cancelled" && cancelNote === "Recording cancelled after part 1.", "cancel+",
    `a part out, then Cancel -> voice acts ${describe(cancelledParts)}; toast "${cancelNote}"`);

  await inShell(`(() => { const real = window.fetch; window.fetch = (url, init) => { const headers = init?.headers ?? {};
    const isLastAudio = headers["X-Voice-Segment"] === "2" && !headers["X-Voice-More"] && !headers["X-Voice-Unsent"];
    return isLastAudio ? Promise.resolve(new Response('{"error":"unreadable"}', { status: 422, headers: { "Content-Type": "application/json" } })) : real(url, init); };
    window.__restoreFetch = () => { window.fetch = real; }; return true; })()`);
  partsBefore = await recordPastOnePart();
  await click(await center("#chat-mic"));
  await until(async () => (await voiceActs()).length === partsBefore + 2, "the refused last part's line", 8000);
  await inShell("window.__restoreFetch(), true");
  const refusedParts = (await voiceActs()).slice(partsBefore);
  const refusedNote = await ui();
  report(describe(refusedParts) === "1+, 2 refused" && refusedNote.notice === "Part 2 not sent: C3 couldn't read the recording." && !refusedNote.badge, "refused",
    `a part out, then the last part refused (422) -> voice acts ${describe(refusedParts)}; notice "${refusedNote.notice}" (no badge: "${refusedNote.badge}")`);

  // The wave and the ring follow the voice (the fake mic's file: 1 s silence, 1 s loud, 1 s silence). Silence draws
  // round dots, the loud second tall bars; the ring behind the mic swells while it is loud and the button doesn't move,
  // so a tap on it while the ring is large still stops the recording.
  await settled();
  await click(await center("#chat-mic"));
  await until(isRecording, "recording the loud second");
  const micBox = await inShell(`(() => { const r = document.getElementById("chat-mic").getBoundingClientRect(); return { x: r.x, y: r.y, width: r.width, height: r.height }; })()`);
  const sample = () => inShell(`(() => { const mic = document.getElementById("chat-mic"), r = mic.getBoundingClientRect();
    return { ring: Number(getComputedStyle(mic).getPropertyValue("--level")) || 0, x: r.x, y: r.y,
      outside: document.elementFromPoint(r.x - 6, r.y + r.height / 2) === mic }; })()`);
  const rings = [];
  let movedOrWider = false, isLoudShot = false;
  for (const started = Date.now(); Date.now() - started < 3200; await sleep(40)) {
    const now = await sample();
    rings.push({ at: Date.now() - started, ring: now.ring });
    movedOrWider ||= now.x !== micBox.x || now.y !== micBox.y || now.outside;
    if (now.ring > 0.6 && !isLoudShot) {
      isLoudShot = true;
      await shot("chat-recording-loud.png");
    }
  }
  await shot("chat-recording-quiet.png");
  const bars = await inShell(`(() => { const c = document.getElementById("chat-wave"), d = c.getContext("2d").getImageData(0, 0, c.width, c.height).data, runs = [];
    for (let x = 0, run = null; x <= c.width; x++) { let n = 0; for (let y = 0; x < c.width && y < c.height; y++) if (d[(y * c.width + x) * 4 + 3] > 128) n++;
      if (n && !run) runs.push(run = { height: n }); else if (n) run.height = Math.max(run.height, n); else run = null; }
    const dot = 3 * devicePixelRatio; return { height: c.height, shape: runs.map((bar) => (bar.height <= dot + 2 ? "d" : bar.height >= c.height / 2 ? "T" : "m")).join("") }; })()`);
  await click(await center("#chat-mic")); // the ring is the voice's, not the button's: this tap stops the recording
  await until(async () => (await ui()).pressed === "false", "the tap to stop the recording");
  const loudRing = Math.max(...rings.map(({ ring }) => ring)), quietRing = Math.min(...rings.filter(({ at }) => at > 2600).map(({ ring }) => ring));
  report(/d{5,}[dm]*T{5,}[Tm]*d{5,}/.test(bars.shape) && loudRing > quietRing + 0.4 && !movedOrWider, "loudness",
    `bars, oldest first (d dot, T tall, m between): ${bars.shape}; ring ${loudRing.toFixed(2)} while loud, ${quietRing.toFixed(2)} in the last silence; `
    + `the mic ${movedOrWider ? "MOVED or took taps outside itself" : "stayed put and took no taps outside itself"}; a tap stopped the recording`);

  // A phone may suspend the wave's audio context once the mic opens (seen on Android: flat dots, no ring): while it
  // records, the shell resumes it.
  await settled();
  await click(await center("#chat-mic"));
  await until(isRecording, "recording for the suspend check");
  const suspendedTo = await inShell(`(async () => { const context = __audioContexts.at(-1); await context.suspend(); return context.state; })()`);
  await sleep(600);
  const resumedTo = await inShell(`__audioContexts.at(-1).state`);
  await click(await center("#chat-mic"));
  await until(async () => (await ui()).pressed === "false", "the tap to stop the suspend check's recording");
  report(suspendedTo === "suspended" && resumedTo === "running", "suspend",
    `audio context suspended mid-recording -> ${suspendedTo}, 0.6 s later -> ${resumedTo}`);

  // A proposal: the agent's [apply: …] line is drawn as one button; it approves that answer on this part, and the page
  // the agent sends for it shuts the sheet so the owner sees the change.
  // The harness still answers the parts above; a later answer would rightly take the proposal's button away.
  for (let last = -1, now = (await log()).length; now !== last; await sleep(1500)) [last, now] = [now, (await log()).length];
  await typeInSheet("Please propose a shorter intro.");
  await click(await center("#chat-send"));
  await until(async () => (await ui()).applies.length === 1, "the proposal's button");
  const proposal = await ui();
  await shot("chat-proposal.png");
  const proposed = (await answers("1")).at(-1);
  await click(await center("#chat-lines .apply"));
  await until(async () => (await ui()).chat === null, "the sheet to shut for the new page", 8000);
  await until(() => inFrame(`document.querySelector('[data-c3-part="1"]').textContent.includes("approved")`), "the part to change in place");
  const approved = (await log()).find((entry) => entry.op === "approve");
  await inFrame(`c3.chat("1"), true`);
  await until(async () => /You approved “Make it so”\./.test((await ui()).lines), "the approve's note in the reopened chat");
  report(JSON.stringify(proposal.applies) === '["Make it so"]' && !proposal.lines.includes("[apply:") && approved?.ref === proposed.id && approved.part === "1", "proposal",
    `answer with [apply: Make it so] -> buttons ${JSON.stringify(proposal.applies)}, marker line ${proposal.lines.includes("[apply:") ? "shown" : "hidden"}; tap -> log approve ${JSON.stringify(approved && { ref: approved.ref, part: approved.part })}; page changed, sheet shut`);

  // The page's own title, line and quick answers; a quick answer is a tap on that part.
  await inFrame(`c3.chat("2", { title: "Second part", ask: "Keep this part as it is?", choices: ["Keep", "Drop"] }), true`);
  await until(async () => (await ui()).choices.length === 2, "the quick answers");
  await settled();
  await click(await center("#chat-choices button"));
  await until(async () => (await answers("2")).length === 1, "the answer to the choice");
  const tap = (await log()).find((entry) => entry.op === "tap");
  const chose = await ui();
  report(chose.title === "Second part" && chose.ask === "Keep this part as it is?" && tap?.part === "2" && tap.text === "Keep" && /Keep/.test(chose.lines), "choice",
    `c3.chat("2", {title, ask, choices}) -> "${chose.title}" / "${chose.ask}" / ${JSON.stringify(chose.choices)}; tap Keep -> log ${JSON.stringify(tap && { op: tap.op, part: tap.part, text: tap.text })}`);

  // A reply's play button plays it on the shell's player. It is a filled disc (32 px seen, 44 px to tap) with a play
  // triangle, a spinner while C3 makes the audio and two bars while it plays (CSS draws them; sampled every 20 ms).
  await until(async () => (await inShell(`document.querySelectorAll("#chat-lines .play").length`)) > 0, "a play button");
  const playLook = () => inShell(`(() => { const b = document.querySelector("#chat-lines .msg:not(.mine) .play"), r = b.getBoundingClientRect(), disc = getComputedStyle(b, "::before");
    return { hit: Math.round(r.width) + "x" + Math.round(r.height), disc: disc.width + " " + disc.height + " " + disc.borderRadius + " " + disc.backgroundColor, label: b.ariaLabel, text: b.textContent }; })()`);
  const idlePlay = await playLook();
  await inShell(`window.__icons = []; window.__sampleIcon = () => { const b = document.querySelector("#chat-lines .msg:not(.mine) .play"); if (!b) return;
    const s = getComputedStyle(b, "::after"), icon = s.animationName === "spin" ? "spinner" : s.clipPath.startsWith("polygon") ? "triangle" : s.borderLeftWidth !== "0px" ? "bars" : "none";
    if (__icons.at(-1) !== icon) __icons.push(icon); }; __sampleIcon(); window.__iconTimer = setInterval(__sampleIcon, 20); true`);
  await click(await center("#chat-lines .msg:not(.mine) .play"));
  await until(() => inShell(`!document.getElementById("player").paused && Boolean(document.querySelector("#chat-lines .play[aria-label='Stop spoken reply']"))`), "the reply to play");
  const playing = await inShell(`({ src: document.getElementById("player").currentSrc.replace(location.origin, ""), label: document.querySelector("#chat-lines .play[aria-label='Stop spoken reply']")?.ariaLabel ?? "" })`);
  await shot("chat-play.png");
  const icons = await inShell("clearInterval(__iconTimer), __icons");
  report(/^\/board\/plan\/audio\/[0-9a-f]{32}$/.test(playing.src) && playing.label === "Stop spoken reply" && idlePlay.hit === "44x44" && /^32px 32px 50% rgb/.test(idlePlay.disc)
    && !idlePlay.disc.endsWith("rgba(0, 0, 0, 0)") && idlePlay.text === "" && icons.join() === "triangle,spinner,bars", "play",
    `button ${idlePlay.hit} to tap, disc "${idlePlay.disc}", "${idlePlay.label}"; tap -> icons ${icons.join(" -> ")}, player ${playing.src}, "${playing.label}"`);
  await sleep(1800);

  // Files. The paperclip opens the browser's own picker (a real tap); the box's text goes with the file as its caption and
  // the box clears; the row shows 📎 name · size, the caption and the upload's progress, then its time; the agent answers.
  await inFrame(`c3.chat("1"), true`);
  await until(async () => (await ui()).title === "1 First part", "part 1's chat");
  await send("Page.setInterceptFileChooserDialog", { enabled: true });
  async function pickFile(file) {
    const opened = browser.waitForEvent("Page.fileChooserOpened", session, 5000);
    await click(await center("#chat-attach"));
    const { backendNodeId } = await opened;
    await send("DOM.setFileInputFiles", { files: [file], backendNodeId });
    await sleep(150);
  }
  const notes = path.join(work, "notes.txt");
  fs.writeFileSync(notes, "a line of notes\n".repeat(4000)); // 64,000 bytes
  await typeInSheet("Here are my notes.");
  await inShell(`window.__fileMeta = []; new MutationObserver(() => { const row = [...document.querySelectorAll("#chat-lines .msg.mine")].findLast((r) => r.querySelector(".label")?.textContent.startsWith("📎"));
    const text = row?.querySelector(".meta").textContent; if (text && __fileMeta.at(-1) !== text) __fileMeta.push(text); }).observe(document.getElementById("chat-lines"), { childList: true, subtree: true }); true`);
  await pickFile(notes);
  const boxAfterPick = (await ui()).box;
  await until(async () => (await log()).some((entry) => entry.op === "file"), "the file act in the log");
  const fileAct = (await log()).find((entry) => entry.op === "file");
  await until(async () => (await answers("1")).some((entry) => entry.reply_to === fileAct.id), "the agent's answer to the file");
  await sleep(300);
  const fileRow = await inShell(`(() => { const row = [...document.querySelectorAll("#chat-lines .msg.mine")].find((r) => r.querySelector(".label")?.textContent.startsWith("📎"));
    row.scrollIntoView({ block: "center" }); return { label: row.querySelector(".label").textContent, body: row.querySelector(".body")?.textContent, links: row.querySelectorAll("a, img").length }; })()`);
  await shot("chat-file.png");
  await send("Emulation.setEmulatedMedia", { features: [{ name: "prefers-color-scheme", value: "dark" }] });
  await shot("chat-file-dark.png");
  await send("Emulation.setEmulatedMedia", { features: [] });
  const fileMeta = await inShell("__fileMeta");
  report(fileAct.part === "1" && fileAct.text === "Here are my notes." && fileAct.name === "notes.txt" && fileAct.size === 64000 && fileAct.mime === "text/plain"
    && boxAfterPick === "" && fileRow.label === "📎 notes.txt · 63 KB" && fileRow.body === "Here are my notes." && fileRow.links === 0 && fileMeta.some((meta) => meta.startsWith("sending…")), "file",
    `caption typed, paperclip, picked notes.txt -> box "${boxAfterPick}"; log ${JSON.stringify({ op: fileAct.op, part: fileAct.part, text: fileAct.text, name: fileAct.name, size: fileAct.size, mime: fileAct.mime })}; `
    + `row "${fileRow.label}" / "${fileRow.body}" (no link or image), its foot ${JSON.stringify(fileMeta)}; agent answered`);

  // Over 20 MB the shell says so and nothing leaves; neither the page nor a script click can open the picker.
  const big = path.join(work, "big.bin");
  fs.closeSync(fs.openSync(big, "w"));
  fs.truncateSync(big, 20 * 1024 * 1024 + 1);
  const fileActs = async () => (await log()).filter((entry) => entry.op === "file").length;
  await pickFile(big);
  await sleep(400);
  const tooBig = { error: (await ui()).error, acts: await fileActs() };
  await typeInSheet("long ".repeat(1300)); // 6.5 KB as sent: too much for a header
  await pickFile(notes);
  await sleep(400);
  const longCaption = { error: (await ui()).error, acts: await fileActs(), kept: (await ui()).box.length };
  await inShell(`(() => { const box = document.getElementById("chat-text"); box.value = ""; box.dispatchEvent(new Event("input")); return true; })()`);
  let isPickerOpened = false;
  const stopWatching = browser.onEvent(({ method }) => { if (method === "Page.fileChooserOpened") isPickerOpened = true; });
  await inFrame(`window.__results = []; addEventListener("message", (e) => { if (e.data?.c3 === "result" && e.data.call >= 960) __results.push(e.data); });
    parent.postMessage({ op: "file", call: 960 }, "*"); parent.postMessage({ op: "act", call: 961, body: { op: "file", part: "1", text: "x" } }, "*");
    parent.postMessage({ op: "act", call: 962, body: { op: "comment", part: "1", text: "page reply", ref: ${fileAct.id} } }, "*"); true`);
  await inShell(`document.getElementById("chat-attach").click(), true`); // untrusted, as no page can reach it anyway
  await sleep(700);
  stopWatching();
  const pageResults = await inFrame("__results");
  const pageReply = (await log()).some((entry) => entry.text === "page reply");
  report(tooBig.error === "Not sent: big.bin is over 20 MB." && tooBig.acts === 1 && longCaption.error === "Caption too long for a file; send it as a message after it."
    && longCaption.acts === 1 && longCaption.kept > 6000 && !isPickerOpened && await fileActs() === 1 && !pageReply
    && JSON.stringify(pageResults.map(({ call, error }) => [call, error])) === '[[961,"invalid"],[962,"invalid"]]', "files",
    `a 20 MB + 1 file -> "${tooBig.error}"; a 6.5 KB caption -> "${longCaption.error}" (caption kept in the box); file acts still ${longCaption.acts}; `
    + `the page's "file" and file-act messages and a script click on the paperclip -> picker ${isPickerOpened ? "OPENED" : "never opened"}, file acts ${await fileActs()}; `
    + `the page's comment answering msg ${fileAct.id} -> ${JSON.stringify(pageResults)}, logged ${pageReply}`);

  // Replies on touch: a swipe right on a message puts it in the reply bar, and what is sent next answers it; the new row
  // opens with a quote of that message, and a tap on the quote scrolls the thread to it and flashes it.
  await send("Emulation.setTouchEmulationEnabled", { enabled: true, maxTouchPoints: 1 });
  const rowAt = (text) => inShell(`(() => { const row = [...document.querySelectorAll("#chat-lines .msg")].find((r) => r.querySelector(".body")?.textContent === ${JSON.stringify(text)});
    row.scrollIntoView({ block: "center" }); const r = row.getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; })()`);
  async function touch({ x, y }, moves, holdMs = 0) {
    await send("Input.dispatchTouchEvent", { type: "touchStart", touchPoints: [{ x, y }] });
    for (const dx of moves) {
      await send("Input.dispatchTouchEvent", { type: "touchMove", touchPoints: [{ x: x + dx, y }] });
      await sleep(30);
    }
    await sleep(holdMs);
    await send("Input.dispatchTouchEvent", { type: "touchEnd", touchPoints: [] });
    await sleep(250);
  }
  const firstComment = (await log()).find((entry) => entry.op === "comment" && entry.part === "1");
  await touch(await rowAt(firstComment.text), [12, 24, 36, 48, 60]);
  const swiped = await ui();
  await shot("chat-reply-bar.png");
  await typeInSheet("Is it short enough now?");
  await click(await center("#chat-send"));
  await until(async () => (await log()).some((entry) => entry.text === "Is it short enough now?"), "the reply in the log");
  const reply = (await log()).find((entry) => entry.text === "Is it short enough now?");
  await until(async () => (await answers("1")).some((entry) => entry.reply_to === reply.id), "the agent's answer to the reply");
  await sleep(300);
  const quoteOf = (text) => inShell(`[...document.querySelectorAll("#chat-lines .msg")].find((r) => r.querySelector(".body")?.textContent.startsWith(${JSON.stringify(text)}))?.querySelector(".quoted")?.textContent ?? null`);
  const quoted = { reply: await quoteOf("Is it short enough now?"), answer: await quoteOf(`Answer to msg ${reply.id} `) };
  await shot("chat-reply.png");
  await send("Emulation.setEmulatedMedia", { features: [{ name: "prefers-color-scheme", value: "dark" }] });
  await shot("chat-reply-dark.png");
  await send("Emulation.setEmulatedMedia", { features: [] });
  const target = () => inShell(`(() => { const row = [...document.querySelectorAll("#chat-lines .msg")].find((r) => r.querySelector(".body")?.textContent === ${JSON.stringify(firstComment.text)});
    const view = document.getElementById("chat-scroll").getBoundingClientRect(), box = row.getBoundingClientRect();
    return { isInView: box.top >= view.top && box.bottom <= view.bottom, isFlashing: row.classList.contains("flash") }; })()`);
  const beforeJump = await target();
  await click(await inShell(`(() => { const r = [...document.querySelectorAll("#chat-lines .msg")].find((row) => row.querySelector(".body")?.textContent === "Is it short enough now?")
    .querySelector(".quoted").getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; })()`));
  await sleep(300);
  const afterJump = await target();
  await shot("chat-reply-jump.png");
  report(swiped.reply === firstComment.text && reply.ref === firstComment.id && quoted.reply === firstComment.text && quoted.answer === null && (await ui()).reply === ""
    && !beforeJump.isInView && afterJump.isInView && afterJump.isFlashing, "swipe",
    `swipe right on "${firstComment.text}" -> bar "${swiped.reply}"; Send -> log ref ${reply.ref} (want ${firstComment.id}), bar cleared; its row's quote "${quoted.reply}", the agent's answer right after it quotes ${JSON.stringify(quoted.answer)}; `
    + `tap the quote -> the message ${beforeJump.isInView ? "was already" : "scrolled"} in view (${afterJump.isInView}), flashing ${afterJump.isFlashing}`);

  // A long press stays the browser's (it picks nothing); the bar's × drops a picked message.
  const fileAnswer = (await answers("1")).find((entry) => entry.reply_to === fileAct.id);
  await touch(await rowAt(fileAnswer.text), [], 700);
  const pressed = (await ui()).reply;
  await touch(await rowAt(fileAnswer.text), [12, 24, 36, 48, 60]);
  const swipedAgain = (await ui()).reply;
  await click(await center("#chat-drop-reply"));
  report(pressed === "" && swipedAgain === fileAnswer.text && (await ui()).reply === "", "drop",
    `hold 0.7 s on "${fileAnswer.text}" -> bar "${pressed}"; swipe -> "${swipedAgain}"; × -> bar "${(await ui()).reply}"`);
  await send("Emulation.setTouchEmulationEnabled", { enabled: false });

  // On a mouse a message shows a Reply button while hovered; a voice note sent with the bar answers that message. (The
  // button is hidden only on a coarse pointer, which touch emulation gave the steps above.)
  await send("Emulation.setDeviceMetricsOverride", { width: 1024, height: 844, deviceScaleFactor: 1, mobile: false });
  await sleep(400);
  const fileBox = await inShell(`(() => { const row = [...document.querySelectorAll("#chat-lines .msg.mine")].find((r) => r.querySelector(".label")?.textContent.startsWith("📎"));
    row.scrollIntoView({ block: "center" }); const r = row.getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2, hidden: getComputedStyle(row.querySelector(".reply")).opacity }; })()`);
  await send("Input.dispatchMouseEvent", { type: "mouseMoved", x: fileBox.x, y: fileBox.y });
  await sleep(200);
  const hovered = await inShell(`(() => { const button = [...document.querySelectorAll("#chat-lines .msg.mine")].find((r) => r.querySelector(".label")?.textContent.startsWith("📎")).querySelector(".reply");
    const r = button.getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2, opacity: getComputedStyle(button).opacity }; })()`);
  await shot("chat-reply-hover.png");
  if (hovered.opacity !== "1") throw new Error(`the Reply button didn't show on hover (opacity ${hovered.opacity})`);
  await click(hovered);
  const hoverBar = (await ui()).reply;
  const voicesBefore = (await log()).filter((entry) => entry.op === "voice").length;
  await settled();
  await click(await center("#chat-mic"));
  await until(isRecording, "recording the reply");
  await sleep(900);
  await click(await center("#chat-mic"));
  await until(async () => (await log()).filter((entry) => entry.op === "voice").length === voicesBefore + 1, "the voice reply in the log");
  const voiceReply = (await log()).filter((entry) => entry.op === "voice").at(-1);
  await until(async () => (await answers("1")).some((entry) => entry.reply_to === voiceReply.id), "the agent's answer to the voice reply", 15000);
  const voiceQuote = await inShell(`[...document.querySelectorAll("#chat-lines .msg.mine")].findLast((r) => r.querySelector(".label")?.textContent.startsWith("🎤"))?.querySelector(".quoted")?.textContent ?? null`);
  report(fileBox.hidden === "0" && hovered.opacity === "1" && hoverBar === "📎 notes.txt" && voiceReply.ref === fileAct.id && voiceQuote === "📎 notes.txt" && (await ui()).reply === "", "hover",
    `mouse over the file's row -> Reply ${fileBox.hidden} -> ${hovered.opacity} opaque; click -> bar "${hoverBar}"; a voice note -> log ref ${voiceReply.ref} (want ${fileAct.id}), its row's quote "${voiceQuote}", bar cleared`);

  // A picked message that leaves the log (its newest 500 lines) is dropped from the bar, and the next message answers
  // nothing, so C3 never refuses it for a ref it no longer holds.
  await send("Input.dispatchMouseEvent", { type: "mouseMoved", x: fileBox.x, y: fileBox.y });
  const again = await inShell(`(() => { const row = [...document.querySelectorAll("#chat-lines .msg.mine")].find((r) => r.querySelector(".label")?.textContent.startsWith("📎"));
    row.scrollIntoView({ block: "center" }); const r = row.querySelector(".reply").getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; })()`);
  await send("Input.dispatchMouseEvent", { type: "mouseMoved", ...again });
  await sleep(200);
  await click(again);
  const beforeFlood = (await ui()).reply;
  fs.writeFileSync(control, "flood plan\n");
  await until(async () => (await ui()).reply === "", "the bar to drop the reply once its message left the log", 30000);
  await typeInSheet("After the flood.");
  await click(await center("#chat-send"));
  await until(async () => (await log()).some((entry) => entry.text === "After the flood."), "the message after the flood", 10000);
  const afterFlood = (await log()).find((entry) => entry.text === "After the flood.");
  report(beforeFlood === "📎 notes.txt" && afterFlood.ref === 0 && !(await log()).some((entry) => entry.id === fileAct.id) && (await ui()).error === "", "stale",
    `bar "${beforeFlood}"; the log moved past msg ${fileAct.id} -> bar gone; the next message sent with ref ${afterFlood.ref}, no error`);
  await until(async () => (await answers("1")).some((entry) => entry.reply_to === afterFlood.id), "the answer after the flood");
  await send("Emulation.setDeviceMetricsOverride", { width: 390, height: 844, deviceScaleFactor: 2, mobile: true });
  await sleep(1200); // the answers are read: the chat is open on them
  await inFrame(`c3.chat("2"), true`); // back to the short thread the steps below start from
  await until(async () => (await ui()).title === "Second part", "part 2's chat again");

  // A tap in the page that asks for no chat shuts the sheet; a tap on another part switches to it.
  await click(await inFrameAt('[data-c3-part="3"] p'));
  await until(async () => (await ui()).title === "3 Third part", "the sheet to switch to part 3");
  await click(await inFrameAt("h1"));
  await until(async () => (await ui()).chat === null, "a tap on the page to shut the sheet");
  report(true, "dismiss", `tap part 3 -> sheet on "3 Third part"; tap the page's title -> sheet shut; page heard ${JSON.stringify((await inFrame("__seen.chat")).slice(-3))}`);

  // The circle's panel shuts on a tap anywhere else, through the shell's own layer: the tap never reaches the page.
  await click(await center("#status"));
  await until(async () => (await ui()).panel, "the panel");
  const heardBefore = (await inFrame("__seen.chat")).length;
  await click(await inFrameAt('[data-c3-part="1"] p'));
  await until(async () => !(await ui()).panel, "a tap over the page to shut the panel", 3000);
  await sleep(300);
  const afterPanel = { chat: (await ui()).chat, heard: (await inFrame("__seen.chat")).length };
  report(!afterPanel.chat && afterPanel.heard === heardBefore, "panel",
    `circle -> panel open; tap over part 1 -> panel shut, sheet ${afterPanel.chat ? "OPENED (the tap reached the page)" : "still shut"}, page chat events ${heardBefore} -> ${afterPanel.heard}`);

  // Talk to the agent: the panel opens the board-level chat on part "general".
  await click(await center("#status"));
  await until(async () => (await ui()).panel, "the panel");
  await click(await center("#talk"));
  await until(async () => (await ui()).chat !== null, "the general chat");
  await typeInSheet("How is the board going overall?");
  await click(await center("#chat-send"));
  await until(async () => (await answers("general")).length === 1, "the answer in the general chat");
  await sleep(300);
  const general = await ui();
  await shot("chat-general.png");
  await send("Emulation.setEmulatedMedia", { features: [{ name: "prefers-color-scheme", value: "dark" }] });
  await shot("chat-general-dark.png");
  await send("Emulation.setEmulatedMedia", { features: [] });
  const generalLine = (await log()).find((entry) => entry.op === "comment" && entry.part === "general");
  await sleep(1100); // the owner reads it: a reply counts as read once its chat has shown for a second
  const pageGeneral = await inFrame(`c3.chat("general").then(() => "opened", (e) => e.message)`);
  const stillGeneral = (await ui()).title;
  // The owner taps another part (here the page's script does): the sheet follows, so the next words go to that part.
  const pageSwitch = await inFrame(`c3.chat("1").then(() => "switched", (e) => e.message)`);
  const switched = (await ui()).title;
  report(general.title === "General" && general.placeholder === "Message the agent" && generalLine?.text === "How is the board going overall?" && /on part "general"/.test(general.lines)
    && pageGeneral === "invalid" && stillGeneral === "General" && pageSwitch === "switched" && switched === "1 First part", "general",
    `Talk to the agent -> "${general.title}" (${general.placeholder}); log ${JSON.stringify(generalLine && { part: generalLine.part, text: generalLine.text })}; `
    + `the page opening "general" itself -> ${pageGeneral}; the page switching it to part 1 -> ${pageSwitch}, sheet on "${switched}"`);
  await press("Escape");

  // Unread: a reply on a part whose chat is shut badges the circle; the panel lists it; a tap reveals the part and opens
  // its chat, which reads it. Waiting actions never badge.
  await inFrame(`c3.comment("3", "Anything new on the third part?"), true`);
  await until(async () => (await ui()).badge === "1", "the unread badge");
  await inFrame(`c3.chat("3").then(() => c3.chat(null)), true`); // a page flashing the chat open can't read it
  await sleep(1500);
  const flashed = await ui();
  const badged = await ui();
  await shot("circle-unread.png");
  await click(await center("#status"));
  await until(async () => (await ui()).panel, "the panel");
  await sleep(200);
  const listed = await ui();
  await shot("panel-unread.png");
  await click(await center("#unread button"));
  await until(async () => (await ui()).chat !== null, "the unread part's chat");
  await sleep(400);
  const halfSecond = await ui();
  await until(async () => (await ui()).badge === "", "the reply to count as read after a second", 3000);
  const revealed = await ui();
  await shot("reveal-open.png");
  report(flashed.badge === "1" && halfSecond.badge === "1" && /Sent by the page\s*Anything new on the third part\?/.test(revealed.lines) && badged.badge === "1" && listed.unread.length === 1 && /^3 Third part/.test(listed.unread[0]) && /Answer to msg \d+ on part "3"/.test(listed.unread[0])
    && revealed.title === "3 Third part" && !revealed.badge && JSON.stringify(await inFrame("__seen.reveal")) === '["3"]', "unread",
    `reply on part 3 -> badge "${badged.badge}", still "${flashed.badge}" after the page flashed its chat; the page's own comment labelled "Sent by the page"; 0.4 s open -> "${halfSecond.badge}"; panel lists ${JSON.stringify(listed.unread)}; tap -> page revealed ${JSON.stringify(await inFrame("__seen.reveal"))}, chat on "${revealed.title}", badge "${revealed.badge}"`);
  await press("Escape");

  // The unread line: two replies came while part 1's chat was shut (its thread is long by now); it opens at a quiet
  // "Unread" line above the first of them, in view, and the line stays put after the read mark moves.
  const answeredBefore = (await answers("1")).length;
  // The second question is long, so what follows the line is taller than the sheet: opening at the bottom would hide it.
  const longQuestion = `Second question while away: ${"how does this part change if we keep the old flow for a week? ".repeat(20).trim()}`;
  for (const [index, text] of ["First question while away", longQuestion].entries()) {
    await inFrame(`c3.comment("1", ${JSON.stringify(text)}), true`);
    await until(async () => (await answers("1")).length === answeredBefore + index + 1, `answer ${index + 1} while the chat is shut`);
  }
  await until(async () => (await ui()).badge === "2", "two unread replies");
  const firstAway = (await log()).find((entry) => entry.op === "comment" && entry.text === "First question while away").id;
  await click(await center("#status"));
  await until(async () => (await ui()).panel, "the panel");
  await click(await center("#unread button"));
  await until(async () => (await ui()).chat !== null, "part 1's chat");
  await sleep(300);
  const unreadAt = () => inShell(`(() => { const line = document.querySelector("#chat-lines .unread-line"); if (!line) return null;
    const first = line.nextElementSibling, view = document.getElementById("chat-scroll").getBoundingClientRect(), box = first.getBoundingClientRect();
    const scroller = document.getElementById("chat-scroll");
    return { lines: document.querySelectorAll("#chat-lines .unread-line").length, first: first.querySelector(".body").textContent,
      isInView: box.top >= view.top && box.bottom <= view.bottom, isAtBottom: scroller.scrollHeight - scroller.scrollTop - scroller.clientHeight < 2,
      scrolls: scroller.scrollHeight > scroller.clientHeight }; })()`);
  const opened = await unreadAt();
  await until(async () => (await ui()).badge === "", "the replies to count as read", 3000);
  const kept = await unreadAt();
  await shot("chat-unread.png");
  report(opened?.lines === 1 && opened.first.startsWith(`Answer to msg ${firstAway} `) && opened.isInView && opened.scrolls && !opened.isAtBottom
    && kept?.first === opened.first, "unreadline",
    `two replies while shut -> badge 2; open -> one "Unread" line above "${opened?.first}", in view ${opened?.isInView}, thread scrolls ${opened?.scrolls}, `
    + `at the bottom ${opened?.isAtBottom}; read a second later -> line still above "${kept?.first}"`);
  await press("Escape");

  // The dot rule: a session that is away holds the owner's lines; that never badges the circle, it is a line in the panel.
  fs.writeFileSync(control, "end plan\n");
  await until(async () => (await ui()).session.startsWith("Session ended"), "the session to go away");
  await inFrame(`c3.comment("2", "This waits for the session."), true`);
  await until(async () => /1 message waits/.test((await ui()).session), "the held line");
  const held = await ui();
  fs.writeFileSync(control, "resume plan\n");
  await until(async () => (await ui()).badge === "1", "the held line's answer to badge the circle", 10000);
  report(!held.badge, "dots", `session away, one line held -> badge "${held.badge}", panel "${held.session}"; session back, it answers -> badge "${(await ui()).badge}"`);

  // The docked mic, for a page that draws its own chat (c3.focus): it sits on the page's Send spot, can't be put over
  // the shell's controls, takes no tap for 500 ms after it appears or moves, and records with the wave in its bar.
  const dock = { x: 300, y: 760, width: 52, height: 52 };
  await inFrame(`c3.focus("1", { dock: ${JSON.stringify(dock)}, hasText: false }), true`);
  await until(async () => (await ui()).docked !== null, "the docked mic");
  await click(await center("#mic"));
  await sleep(300);
  const dockEarly = await ui();
  const hostile = { circle: { x: 350, y: 6, width: 36, height: 36 }, huge: { x: -500, y: -500, width: 5000, height: 5000 }, away: { x: 2000, y: 3000, width: 10, height: 10 }, topLeft: { x: 0, y: 0, width: 44, height: 44 } };
  const placed = {};
  for (const [label, spot] of Object.entries(hostile)) {
    await inFrame(`parent.postMessage({ op: "focus", call: 950, value: "1", dock: ${JSON.stringify(spot)}, hasText: false }, "*"), true`);
    await sleep(150);
    placed[label] = (await ui()).docked;
  }
  const isSafe = (mic) => mic && isInside(mic, window_) && isApart(mic, idle.status) && mic.width >= 44 && mic.width <= 56 && mic.width === mic.height;
  await inFrame(`c3.focus("1", { dock: ${JSON.stringify(dock)}, hasText: false }), true`);
  await sleep(150);
  await click(await center("#mic")); // it just moved back: ignored
  await sleep(300);
  const movedTap = (await ui()).dockPressed;
  await settled();
  await click(await center("#mic"));
  await until(async () => (await ui()).dockPressed === "true" && /^\d+:\d\d$/.test((await ui()).dockClock), "the docked recording");
  await sleep(1200);
  const docked = await ui();
  const dockWave = (await waveBars("dock-wave")).inked;
  await click(await center("#cancel"));
  await sleep(600);
  report(dockEarly.dockPressed === "false" && movedTap === "false" && Object.values(placed).every(isSafe) && docked.dockBar && isApart(docked.dockBar, docked.docked) && dockWave > 20, "dock",
    `docked mic tapped as it appeared -> ${dockEarly.dockPressed === "true" ? "RECORDING" : "ignored"}, right after it moved -> ${movedTap === "true" ? "RECORDING" : "ignored"}; hostile spots ${Object.entries(placed).map(([label, mic]) => `${label} ${JSON.stringify(round(mic))}`).join(", ")}; `
    + `settled -> recording, bar ${JSON.stringify(round(docked.dockBar))} with ${dockWave} wave pixels`);
  await inFrame(`c3.focus(null), true`);

  // Make read-only mid-recording: the last part's line reaches C3 before the board stops taking acts.
  await inFrame(`c3.chat("1"), true`);
  await until(async () => (await ui()).mic !== null, "the sheet again");
  partsBefore = await recordPastOnePart();
  await inShell("window.confirm = () => true, true");
  await click(await center("#status"));
  await until(async () => (await ui()).panel, "the panel");
  await click(await center("#close"));
  await until(() => inShell(`fetch(location.pathname + "/state").then((r) => r.json()).then((s) => s.closed)`), "the board to be read-only", 15000);
  await sleep(300);
  const closedParts = (await voiceActs()).slice(partsBefore);
  const closed = await ui();
  report(describe(closedParts) === "1+, 2 cancelled" && closed.notice === "The board is read-only now: the recording stopped." && !closed.mic && !closed.recorder && closed.placeholder === "This board is read-only.", "readonly",
    `a part out, then Make read-only -> voice acts ${describe(closedParts)} before it closed; notice "${closed.notice}", mic ${closed.mic ? "shown" : "gone"}, box "${closed.placeholder}"`);
  report(errors.length === 0, "errors", `${errors.length} errors${errors.length ? `: ${errors.join(" | ")}` : ""}`);
} catch (error) {
  report(false, "run", error.message);
} finally {
  await browser.close();
  fs.rmSync(info, { force: true });
  harness.kill();
  fs.rmSync(work, { recursive: true, force: true });
}
const failed = results.filter((isPass) => !isPass).length;
console.log(failed ? `${failed} check(s) failed` : `${results.length} checks passed`);
process.exit(failed ? 1 : 0);
