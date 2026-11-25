#!/usr/bin/env node
// Spoken-replies browser check: the REAL Go board routes (TestBoardBrowserHarness: its fake agent answers every comment, its
// fake synthesizer returns a 2.5 s tone as MP3), C3's real shell and SDK, and headless Chromium with a phone's autoplay
// rule (--autoplay-policy=document-user-activation-required) and a fake microphone. Nobody in the loop.
// Native chat: the play buttons and the mic are in C3's own chat sheet (the shell), which a tap on a part opens.
// Usage: check-speak.mjs <c3 checkout> [--shots <dir>]. Prints PASS/FAIL per check; exit 1 on any FAIL.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, execFileSync } from "node:child_process";
import { Browser } from "./cdp.mjs";
import { findChromium } from "./chromium.mjs";

const [checkout = process.cwd(), , shots = ""] = process.argv.slice(2);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const work = fs.mkdtempSync(path.join(os.tmpdir(), "c3-speak-check-"));
const results = [];
const report = (isPass, name, detail) => { results.push(isPass); console.log(`${isPass ? "PASS" : "FAIL"}  ${name.padEnd(9)} ${detail}`); };

const tone = path.join(work, "tone.mp3");
execFileSync("ffmpeg", ["-v", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=2.5", "-ac", "1", "-ar", "24000", "-c:a", "libmp3lame", "-b:a", "32k", tone]);
const info = path.join(work, "harness.json");
const harness = spawn("go", ["test", "./internal/channel/web/", "-run", "^TestBoardBrowserHarness$", "-count=1"], {
  cwd: checkout, stdio: "ignore", env: { ...process.env, C3_BOARD_HARNESS: info, C3_BOARD_HARNESS_SECONDS: "240", C3_BOARD_HARNESS_SPEECH: tone },
});
for (let waited = 0; !fs.existsSync(info); waited += 200) {
  if (waited > 120000) throw new Error("the harness did not start");
  await sleep(200);
}
const { url, cookies, boards } = JSON.parse(fs.readFileSync(info, "utf8"));

const wrapper = path.join(work, "chromium");
fs.writeFileSync(wrapper, `#!/bin/sh\nexec ${JSON.stringify(findChromium())} --autoplay-policy=document-user-activation-required --use-fake-device-for-media-stream --use-fake-ui-for-media-stream "$@"\n`, { mode: 0o755 });
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
  await send("Target.setAutoAttach", { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });
  // window.__micDelayMs holds the mic's opening back, as a phone's permission prompt does.
  await send("Page.addScriptToEvaluateOnNewDocument", { source: `(() => { const media = navigator.mediaDevices; if (!media) return; const open = media.getUserMedia.bind(media);
    media.getUserMedia = (constraints) => new Promise((resolve) => setTimeout(resolve, window.__micDelayMs ?? 0)).then(() => open(constraints)); })();` });
  await send("Emulation.setDeviceMetricsOverride", { width: 390, height: 844, deviceScaleFactor: 2, mobile: true });
  await send("Network.setCookie", { ...cookies[0], url });

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
  const shot = async (name) => { if (shots) { fs.mkdirSync(shots, { recursive: true }); fs.writeFileSync(path.join(shots, name), Buffer.from((await send("Page.captureScreenshot", { format: "png" })).data, "base64")); } };
  const center = (selector) => inShell(`(() => { const r = document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; })()`);
  const frameTop = () => inShell(`(() => { const r = document.getElementById("page").getBoundingClientRect(); return { x: r.x, y: r.y }; })()`);
  /** A spot in the frame, in the shell's coordinates. */
  async function inFrameAt(expression) {
    const [top, spot] = [await frameTop(), await inFrame(`(() => { const r = (${expression}).getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; })()`)];
    return { x: top.x + spot.x, y: top.y + spot.y };
  }
  // A reply's play button in C3's sheet: the harness answers comment N with "Answer to msg N …".
  const commentOf = new Map(); // answer id -> the comment it answers
  const playButton = (id) => `[...document.querySelectorAll("#chat-lines .msg:not(.mine)")].find((row) => row.querySelector(".body").textContent.startsWith("Answer to msg ${commentOf.get(id)} "))?.querySelector(".play")`;
  // The button's state, as the glyph it once showed (CSS draws a triangle, a spinner or two bars).
  const playLabel = (id) => inShell(`({ "Play spoken reply": "▶", "Preparing spoken reply": "…", "Stop spoken reply": "⏸" })[${playButton(id)}?.ariaLabel] ?? null`);
  const shellAt = (expression) => inShell(`(() => { const r = (${expression}).getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; })()`);
  const log = () => inShell(`fetch(location.pathname + "/state").then((r) => r.json()).then((s) => s.log)`);
  const speeches = () => inFrame("window.__speech");
  const statesOf = async (id) => (await speeches()).filter((event) => event.id === id).map((event) => event.state + (event.error ? `:${event.error}` : ""));
  const player = () => inShell(`(() => { const p = document.getElementById("player"); return { paused: p.paused, src: p.currentSrc.replace(location.origin, ""), time: p.currentTime }; })()`);
  const shell = () => inShell(`({ toast: document.getElementById("toast").hidden ? "" : document.getElementById("toast").textContent, notice: document.getElementById("notice").textContent,
    badge: !document.getElementById("badge").hidden, activated: navigator.userActivation.hasBeenActive, aloud: document.getElementById("aloud").checked,
    stored: (() => { try { return localStorage.getItem("c3.board.readAloud"); } catch { return "throws"; } })() })`);
  /** The owner comments on part 1 from the page's script (no tap); resolves with the agent's answer id. */
  async function answerTo(text) {
    const { id } = await inFrame(`c3.comment("1", ${JSON.stringify(text)})`);
    let answer = 0;
    await until(async () => { answer = (await log()).find((entry) => entry.dir === "out" && entry.op === "message" && entry.reply_to === id)?.id ?? 0; return answer > 0; }, `the answer to ${id}`);
    commentOf.set(answer, id);
    return { comment: id, answer };
  }

  // The owner turned "Read replies aloud" on in this browser earlier; this load has had no tap at all.
  await send("Page.navigate", { url: boards.plan });
  await until(() => inShell("document.readyState === 'complete'"), "the shell");
  await inShell(`localStorage.setItem("c3.board.readAloud", "on"), true`);
  frameSession = null;
  await send("Page.reload", {});
  await until(() => inFrame("Boolean(window.c3) && document.querySelector('[data-c3-part]') !== null"), "the page in its frame");
  await inFrame("window.__speech = [], c3.on('speech', (event) => window.__speech.push(event)), true");
  const loaded = await shell();
  report(loaded.aloud && !loaded.activated, "setting", `stored "${loaded.stored}" -> Read replies aloud ${loaded.aloud ? "on" : "off"}; no tap yet (shell activated: ${loaded.activated})`);

  // A new reply with no tap anywhere: the browser refuses to play; the shell says so and the reply waits.
  const first = await answerTo("first comment");
  await until(async () => (await statesOf(first.answer)).includes("failed:blocked"), "the reply to be blocked");
  const blocked = await shell();
  report(blocked.notice === "Tap the circle at the top right to hear replies." && blocked.toast === blocked.notice && (await player()).paused, "blocked",
    `reply ${first.answer} read aloud with no tap -> ${JSON.stringify(await statesOf(first.answer))}; notice and toast "${blocked.notice}"`);

  // A tap inside the frame lets the shell play (user activation reaches the frame's ancestors in Chromium).
  const partSpot = await inFrameAt(`document.querySelector('[data-c3-part="1"]')`);
  await click(partSpot);
  await until(() => inShell(`Boolean(${playButton(first.answer)})`), "the thread with the answer's play button");
  const afterFrameTap = await shell();
  const idleLabel = await playLabel(first.answer);
  await click(await shellAt(playButton(first.answer)));
  await until(async () => (await statesOf(first.answer)).includes("playing"), "the tapped reply to play");
  const playing = { player: await player(), label: await playLabel(first.answer), shell: await shell() };
  await shot("speak-playing.png");
  report(afterFrameTap.activated && idleLabel === "▶" && !playing.player.paused && /^\/board\/plan\/audio\/[0-9a-f]{32}$/.test(playing.player.src) && playing.label === "⏸" && playing.shell.notice === "",
    "frametap", `tap on part 1 in the frame -> shell activated ${afterFrameTap.activated}; ▶ on reply ${first.answer} -> ${JSON.stringify(await statesOf(first.answer))}, player ${playing.player.src} playing, button "${playing.label}", blocked notice cleared ${playing.shell.notice === ""}`);

  await until(async () => (await statesOf(first.answer)).at(-1) === "ended", "the reply to end", 8000);
  report(await playLabel(first.answer) === "▶", "ended", `reply ${first.answer} -> ${JSON.stringify(await statesOf(first.answer))}, button back to "${await playLabel(first.answer)}"`);

  // Read replies aloud: each new reply plays as it arrives, in order, with no tap.
  const [second, third] = await Promise.all([answerTo("second comment"), sleep(50).then(() => answerTo("third comment"))]);
  await until(async () => (await statesOf(third.answer)).at(-1) === "ended", "both replies read", 15000);
  const order = (await speeches()).filter((event) => event.id === second.answer || event.id === third.answer).map((event) => `${event.id}:${event.state}`);
  const expected = [second.answer, third.answer].flatMap((id) => ["loading", "playing", "ended"].map((state) => `${id}:${state}`));
  report(JSON.stringify(order) === JSON.stringify(expected), "auto", `two new replies -> ${order.join(", ")}`);

  // Starting the mic stops playback; asking to listen while it records is refused.
  await click(await shellAt(playButton(first.answer)));
  await until(async () => (await statesOf(first.answer)).at(-1) === "playing", "the reply to play again");
  await click(await center("#chat-mic"));
  await until(async () => (await statesOf(first.answer)).at(-1) === "ended", "the mic to stop playback", 3000);
  const micOn = { player: await player(), pressed: await inShell(`document.getElementById("chat-mic").ariaPressed`) };
  const whileRecording = await inFrame(`c3.speak(${first.answer}).then(() => "taken", (error) => error.message)`);
  const recordingToast = (await shell()).toast;
  await click(await center("#chat-cancel"));
  await sleep(500);
  report(micOn.player.paused && micOn.pressed === "true" && whileRecording === "recording" && recordingToast === "Stop recording to listen.", "mic",
    `mic tapped mid-reply -> ${JSON.stringify(await statesOf(first.answer))} (player paused ${micOn.player.paused}, recording ${micOn.pressed}); c3.speak while recording -> ${whileRecording}, toast "${recordingToast}"`);

  // ⏸ stops; a bad id is refused (C3 speaks only the session's messages on this board).
  await click(await shellAt(playButton(first.answer)));
  await until(async () => (await playLabel(first.answer)) === "⏸", "⏸");
  await click(await shellAt(playButton(first.answer)));
  await until(async () => (await statesOf(first.answer)).at(-1) === "ended", "⏸ to stop");
  report((await player()).paused && await playLabel(first.answer) === "▶", "stop", `▶ then ⏸ -> ${JSON.stringify((await statesOf(first.answer)).slice(-3))}`);

  const refusals = {};
  for (const [label, id] of [["unknown 999999", 999999], [`owner's comment ${first.comment}`, first.comment]]) {
    await inFrame(`c3.speak(${id})`);
    await until(async () => (await statesOf(id)).at(-1)?.startsWith("failed"), `the refusal of ${id}`);
    refusals[label] = (await statesOf(id)).at(-1);
  }
  const refusedToast = (await shell()).toast;
  refusals["string id"] = await inFrame(`c3.speak("7").then(() => "taken", (error) => error.message)`);
  refusals["other board"] = await inShell(`fetch("/board/other/speak", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ id: ${first.answer} }) })
    .then(async (r) => r.status + " " + (await r.json()).error)`);
  refusals["frame network"] = await inFrame(`fetch("/board/plan/speak", { method: "POST", body: "{}" }).then(() => "reached", (error) => error.name)`);
  report(refusals["unknown 999999"] === "failed:not_found" && refusals[`owner's comment ${first.comment}`] === "failed:not_found" && refusals["string id"] === "invalid"
    && refusals["other board"] === "404 not_found" && refusals["frame network"] !== "reached" && refusedToast === "Couldn't read the reply aloud: it is no longer on the board.",
  "badid", `${JSON.stringify(refusals)}; toast "${refusedToast}"`);

  // The setting lives in the panel: off stops reading new replies, and the browser remembers it.
  await click(await center("#status"));
  await until(() => inShell(`document.getElementById("panel").matches(":popover-open")`), "the panel");
  await sleep(200);
  await shot("speak-panel.png");
  await click(await center("#aloud"));
  const off = await shell();
  await send("Input.dispatchKeyEvent", { type: "keyDown", key: "Escape", code: "Escape", windowsVirtualKeyCode: 27 });
  await send("Input.dispatchKeyEvent", { type: "keyUp", key: "Escape", code: "Escape", windowsVirtualKeyCode: 27 });
  const quiet = await answerTo("fourth comment");
  await sleep(1500);
  report(!off.aloud && off.stored === "off" && (await statesOf(quiet.answer)).length === 0, "toggle",
    `tap "Read replies aloud" -> ${off.aloud ? "on" : "off"}, stored "${off.stored}"; reply ${quiet.answer} then -> ${JSON.stringify(await statesOf(quiet.answer))} (not read)`);
  // A reply that comes while the mic is on waits, and plays once the recording ends.
  await click(await center("#status"));
  await until(() => inShell(`document.getElementById("panel").matches(":popover-open")`), "the panel");
  await click(await center("#aloud"));
  await send("Input.dispatchKeyEvent", { type: "keyDown", key: "Escape", code: "Escape", windowsVirtualKeyCode: 27 });
  await send("Input.dispatchKeyEvent", { type: "keyUp", key: "Escape", code: "Escape", windowsVirtualKeyCode: 27 });
  await click(await center("#chat-mic"));
  await until(async () => (await inShell(`document.getElementById("chat-mic").ariaPressed`)) === "true", "recording");
  const held = await answerTo("fifth comment");
  await sleep(1500);
  const duringRecording = await statesOf(held.answer);
  await click(await center("#chat-cancel"));
  await until(async () => (await statesOf(held.answer)).includes("playing"), "the held reply after the recording");
  report(duringRecording.length === 0, "held", `reply ${held.answer} while recording -> ${JSON.stringify(duringRecording)}; after Cancel -> ${JSON.stringify(await statesOf(held.answer))}`);
  await inFrame("c3.speak(null)");

  // Cancel while the mic still opens frees the shell at once, and the mic that opens late is let go.
  await inShell("window.__micDelayMs = 2000, true");
  await click(await center("#chat-mic"));
  await until(async () => (await inShell(`document.getElementById("chat-mic").ariaPressed`)) === "true", "the mic opening");
  await click(await center("#chat-cancel"));
  const afterCancel = await inShell(`document.getElementById("chat-mic").ariaPressed`);
  const listen = await inFrame(`c3.speak(${held.answer}).then(() => "taken", (error) => error.message)`);
  await inFrame("c3.speak(null)");
  await sleep(2300);
  const afterOpen = await inShell(`document.getElementById("chat-mic").ariaPressed`);
  await inShell("window.__micDelayMs = 0, true");
  report(afterCancel === "false" && listen === "taken" && afterOpen === "false", "opening",
    `Cancel while the mic opened -> mic pressed ${afterCancel}, c3.speak ${listen}; once the mic opened -> pressed ${afterOpen}`);

  // A fresh load (no tap yet) with the setting on: the reply waits for a tap, and a tap on the circle, the shell's own
  // control (what a phone that ignores a tap in the frame needs), plays it.
  frameSession = null;
  await send("Page.reload", {});
  await until(() => inFrame("Boolean(window.c3) && document.querySelector('[data-c3-part]') !== null"), "the page after a reload");
  await inFrame("window.__speech = [], c3.on('speech', (event) => window.__speech.push(event)), true");
  const reloaded = await answerTo("sixth comment");
  await until(async () => (await statesOf(reloaded.answer)).includes("failed:blocked"), "the reply to wait for a tap");
  await click(await center("#status"));
  await until(async () => (await statesOf(reloaded.answer)).includes("playing"), "the circle's tap to play it");
  const circle = await shell();
  report(circle.aloud && circle.notice === "", "circle", `reload, reply ${reloaded.answer} -> blocked; tap the circle -> ${JSON.stringify(await statesOf(reloaded.answer))}, blocked notice cleared`);
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
