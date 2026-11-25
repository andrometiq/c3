#!/usr/bin/env node
// Magic Link tools browser check: the REAL Go board routes (TestBoardBrowserHarness, whose fake agent answers every line,
// plays C3's STT readback for a voice note, and answers a Magic Pencil command by changing the selected element and
// narrating), C3's real shell and SDK, and headless Chromium with a fake microphone and camera
// (--use-fake-device-for-media-stream --use-fake-ui-for-media-stream). Nobody in the loop.
// It proves the toolbar's parts on a phone (390 px, touch, 3x pixels) and a desktop (1280 px, mouse): the Magic Pencil's
// selection bounds and elements against the known element positions on a scrolled page, the boxes drawn over what it
// found (and that they follow the page's scroll), the mic (tap and hold to talk, with the recording state, timer and
// level), the camera (a photo and a video), files of any type with previews before Send, and narration (C3's voice,
// the browser's own voice when C3 has none, and the toggle).
// Usage: check-magic-tools.mjs <c3 checkout> [--shots <dir>]. Prints PASS/FAIL per check; exit 1 on any FAIL.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, execFileSync } from "node:child_process";
import { Browser } from "./cdp.mjs";
import { findChromium } from "./chromium.mjs";

const [checkout = process.cwd(), , shots = ""] = process.argv.slice(2);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const work = fs.mkdtempSync(path.join(os.tmpdir(), "c3-magic-tools-check-"));
const results = [];
const report = (isPass, name, detail) => { results.push(isPass); console.log(`${isPass ? "PASS" : "FAIL"}  ${name.padEnd(12)} ${detail}`); };

// A page that scrolls, with cards at known places: two columns on a phone, three on a desktop.
const page = path.join(work, "page.html");
fs.writeFileSync(page, `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Shop</title>
<style>
  body { margin: 0; font: 16px/1.5 system-ui, sans-serif; }
  header { padding: 20px 24px; background: #f6f8fa; }
  h1 { margin: 0; font-size: 28px; }
  .grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; padding: 16px; }
  @media (min-width: 900px) { .grid { grid-template-columns: repeat(3, 1fr); } }
  .card { min-height: 140px; padding: 12px; border: 1px solid #d1d9e0; border-radius: 10px; }
  .card h3 { margin: 0 0 6px; }
  .spacer { height: 1600px; }
</style>
</head>
<body>
<header><h1 id="title">Welcome to the shop</h1></header>
<div class="grid">
  <article class="card" id="c1"><h3>Alpha plan</h3><p>The first card.</p></article>
  <article class="card" id="c2"><h3>Beta plan</h3><p>The second card.</p></article>
  <article class="card" id="c3"><h3>Gamma plan</h3><p>The third card.</p></article>
  <article class="card" id="c4"><h3>Delta plan</h3><p>The fourth card.</p></article>
  <article class="card" id="c5"><h3>Epsilon plan</h3><p>The fifth card.</p></article>
  <article class="card" id="c6"><h3>Zeta plan</h3><p>The sixth card.</p></article>
</div>
<div class="spacer"></div>
<script>window.__boot = Math.random();</script>
</body>
</html>
`);

const tone = path.join(work, "tone.mp3");
execFileSync("ffmpeg", ["-v", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=1.5", "-ac", "1", "-ar", "24000", "-c:a", "libmp3lame", "-b:a", "32k", tone]);
const info = path.join(work, "harness.json");
const inbounds = path.join(work, "inbounds.jsonl");
const harness = spawn("go", ["test", "./internal/channel/web/", "-run", "^TestBoardBrowserHarness$", "-count=1"], {
  cwd: checkout, stdio: "ignore",
  env: { ...process.env, C3_BOARD_HARNESS: info, C3_BOARD_HARNESS_SECONDS: "420", C3_BOARD_HARNESS_PAGE: page, C3_BOARD_HARNESS_SPEECH: tone,
    C3_BOARD_HARNESS_INBOUNDS: inbounds },
});
for (let waited = 0; !fs.existsSync(info); waited += 200) {
  if (waited > 120000) throw new Error("the harness did not start");
  await sleep(200);
}
const { url, cookies, boards } = JSON.parse(fs.readFileSync(info, "utf8"));

// The fake mic: 0.2 s of silence, then a loud tone; the fake camera is Chromium's own moving test picture.
const speech = path.join(work, "speech.wav");
execFileSync("ffmpeg", ["-v", "error", "-f", "lavfi", "-i",
  "aevalsrc=exprs='if(between(t,0.2,1.8),0.45*sin(2*PI*180*t)*(0.6+0.4*sin(2*PI*4*t)),0)':s=48000:d=2", "-ac", "1", "-c:a", "pcm_s16le", speech]);
const wrapper = path.join(work, "chromium");
fs.writeFileSync(wrapper, `#!/bin/sh\nexec ${JSON.stringify(findChromium())} --use-fake-device-for-media-stream --use-fake-ui-for-media-stream --use-file-for-fake-audio-capture=${speech} --autoplay-policy=user-gesture-required "$@"\n`, { mode: 0o755 });
process.env.CHROMIUM = wrapper;
// Any file type: a note and a binary file the owner attaches.
const note = path.join(work, "notes.txt");
fs.writeFileSync(note, "Shipping notes.\n");
const binary = path.join(work, "archive.bin");
fs.writeFileSync(binary, Buffer.from(Array.from({ length: 4096 }, (_, index) => index % 251)));

// The browser's own voice, recorded: the shell falls back to it when C3 has no voice for a reply.
const VOICE_STUB = `(() => { window.__spoken = []; window.__clicks = 0; addEventListener("click", () => { window.__clicks++; }, true); const synth = { speaking: false, cancel() {}, getVoices() { return []; },
  speak(utterance) { if (!utterance.text) return; window.__spoken.push(utterance.text);
    setTimeout(() => utterance.dispatchEvent(new Event("start")), 50); setTimeout(() => utterance.dispatchEvent(new Event("end")), 900); } };
  Object.defineProperty(window, "speechSynthesis", { value: synth, configurable: true }); })();`;

const browser = Browser.launch({ width: 1280, height: 900 });
const errors = [];
let blockSpeak = false;
let touchMisses = 0;
const speakAsks = [];
try {
  const session = await browser.newPage();
  let frameSession = null;
  let chooser = null;
  browser.onEvent(({ method, params, sessionId }) => {
    if (sessionId !== session && sessionId !== frameSession) return;
    if (method === "Runtime.exceptionThrown") errors.push(params.exceptionDetails.exception?.description ?? params.exceptionDetails.text);
    if (method === "Target.attachedToTarget" && params.targetInfo.type === "iframe") {
      frameSession = params.sessionId;
      browser.send("Runtime.enable", {}, frameSession).catch(() => {});
    }
    if (method === "Page.fileChooserOpened") chooser = params;
    if (method === "Fetch.requestPaused") {
      speakAsks.push(blockSpeak ? "blocked" : "served");
      const body = Buffer.from(JSON.stringify({ error: "unavailable" })).toString("base64");
      (blockSpeak ? browser.send("Fetch.fulfillRequest", { requestId: params.requestId, responseCode: 503, body,
        responseHeaders: [{ name: "Content-Type", value: "application/json" }] }, session)
        : browser.send("Fetch.continueRequest", { requestId: params.requestId }, session)).catch(() => {});
    }
  });
  const send = (method, params) => browser.send(method, params, session);
  for (const domain of ["Page", "Runtime", "Network", "DOM"]) await send(`${domain}.enable`);
  await send("Fetch.enable", { patterns: [{ urlPattern: "*/speak" }] });
  await send("Page.addScriptToEvaluateOnNewDocument", { source: VOICE_STUB });
  await send("Target.setAutoAttach", { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });
  await send("Network.setCookie", { ...cookies[0], url });

  async function evaluate(expression, sessionId = session) {
    const result = await browser.send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true }, sessionId);
    if (result.exceptionDetails) throw new Error(`threw: ${result.exceptionDetails.exception?.description ?? result.exceptionDetails.text}`);
    return result.result.value;
  }
  const inShell = (expression) => evaluate(expression);
  const inFrame = (expression) => evaluate(expression, frameSession);
  async function until(check, what, ms = 10000) {
    for (const end = Date.now() + ms; Date.now() < end; await sleep(100)) if (await Promise.resolve().then(check).catch(() => false)) return true;
    throw new Error(`timed out waiting for ${what}`);
  }
  let isTouch = false;
  const mouse = (type, x, y, buttons = 0) => send("Input.dispatchMouseEvent", { type, x, y, button: type === "mouseMoved" ? (buttons ? "left" : "none") : "left", buttons, clickCount: 1 });
  const touch = (type, points) => send("Input.dispatchTouchEvent", { type, touchPoints: points.map(([x, y]) => ({ x, y, radiusX: 1, radiusY: 1, force: 1 })) });
  async function press([x, y], holdMs = 0) {
    if (isTouch) {
      const clicks = await inShell("window.__clicks ?? 0");
      await touch("touchStart", [[x, y]]);
      await sleep(holdMs || 80);
      await touch("touchEnd", []);
      await sleep(150);
      // Chromium's emulated touch sometimes makes no click from a tap after a drawn stroke; a mouse click stands in, counted.
      if (!holdMs && await inShell("window.__clicks ?? 0") === clicks) {
        touchMisses++;
        await mouse("mousePressed", x, y, 1);
        await mouse("mouseReleased", x, y);
      }
    } else {
      await mouse("mouseMoved", x, y);
      await mouse("mousePressed", x, y, 1);
      if (holdMs) await sleep(holdMs);
      await mouse("mouseReleased", x, y);
    }
    await sleep(200);
  }
  async function stroke(points) {
    if (isTouch) {
      await touch("touchStart", [points[0]]);
      for (const point of points.slice(1)) await touch("touchMove", [point]);
      await touch("touchEnd", []);
    } else {
      await mouse("mouseMoved", ...points[0]);
      await mouse("mousePressed", ...points[0], 1);
      for (const point of points.slice(1)) await mouse("mouseMoved", ...point, 1);
      await mouse("mouseReleased", ...points.at(-1));
    }
    await sleep(150);
  }
  const center = async (selector) => {
    const box = await inShell(`(() => { const r = document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect(); return [r.x + r.width / 2, r.y + r.height / 2]; })()`);
    return box;
  };
  const tap = async (selector, holdMs = 0) => press(await center(selector), holdMs);
  const frameBox = (selector) => inFrame(`(() => { const r = document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect(); return { x: r.x, y: r.y, width: r.width, height: r.height }; })()`);
  const shown = (id) => inShell(`(() => { const e = document.getElementById(${JSON.stringify(id)}); return Boolean(e && !e.hidden && e.checkVisibility({ visibilityProperty: true })); })()`);
  const text = (id) => inShell(`document.getElementById(${JSON.stringify(id)}).textContent`);
  const received = () => (fs.existsSync(inbounds) ? fs.readFileSync(inbounds, "utf8").trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)) : []);
  const selectionOf = (line) => JSON.parse(line.split("\n").find((row) => row.startsWith("Magic Pencil selection (msg ")).replace(/^Magic Pencil selection \(msg \d+; [^)]*\): /, ""));
  const picks = () => inShell(`[...document.querySelectorAll("#pencil-picks .pick")].map((p) => { const r = p.getBoundingClientRect(); return { x: r.x, y: r.y, width: r.width, height: r.height }; })`);
  const near = (a, b, slack = 1.5) => ["x", "y", "width", "height"].every((side) => Math.abs(a[side] - b[side]) <= slack);
  const shot = async (name) => {
    if (!shots) return;
    fs.mkdirSync(shots, { recursive: true });
    await sleep(350);
    fs.writeFileSync(path.join(shots, name), Buffer.from((await send("Page.captureScreenshot", { format: "png" })).data, "base64"));
  };
  async function open(width, height, isPhone) {
    isTouch = isPhone;
    await send("Emulation.setDeviceMetricsOverride", { width, height, deviceScaleFactor: isPhone ? 3 : 1, mobile: isPhone });
    await send("Emulation.setTouchEmulationEnabled", { enabled: isPhone, maxTouchPoints: 5 });
    frameSession = null;
    await send("Page.navigate", { url: boards.plan });
    await until(() => frameSession && inFrame("Boolean(window.c3) && Boolean(window.__boot)"), "the page in its frame", 15000);
    await until(() => shown("magic"), "the toolbar");
    await sleep(500); // the toolbar's entrance, and the mic's settle time
  }
  const waitInbound = async (count, what, ms = 20000) => { await until(() => received().length >= count, what, ms); return received()[count - 1]; };

  /** Draws kind around the frame boxes (union, margin px), checks the found elements and their boxes, the bounds and the
   *  scroll, and returns the selection the command carries. */
  async function selectAround(kind, selectors, margin) {
    // The page scrolls first, so the target sits mid-screen with a scroll offset the selection must report.
    const scroll = await inFrame(`(() => { const top = document.querySelector(${JSON.stringify(selectors[0])}).getBoundingClientRect().top + scrollY; scrollTo(0, Math.max(0, top - 180)); return scrollY; })()`);
    await sleep(300);
    const boxes = await Promise.all(selectors.map(frameBox));
    const [left, top] = [Math.min(...boxes.map((b) => b.x)) - margin, Math.min(...boxes.map((b) => b.y)) - margin];
    const [right, bottom] = [Math.max(...boxes.map((b) => b.x + b.width)) + margin, Math.max(...boxes.map((b) => b.y + b.height)) + margin];
    await tap(kind === "freeform" ? "#pencil-freeform" : "#pencil-rectangle");
    await until(() => shown("pencil-layer"), "the drawing layer");
    if (kind === "rectangle") {
      await stroke([[left, top], [(left + right) / 2, (top + bottom) / 2], [right, bottom]]);
    } else {
      const [cx, cy, rx, ry] = [(left + right) / 2, (top + bottom) / 2, (right - left) / 2 * 1.2, (bottom - top) / 2 * 1.3];
      const view = await inShell("[innerWidth, innerHeight]");
      const inView = ([x, y]) => [Math.min(Math.max(x, 1), view[0] - 1), Math.min(Math.max(y, 1), view[1] - 1)];
      await stroke(Array.from({ length: 49 }, (_, index) => inView([cx + rx * Math.cos(index * Math.PI / 24), cy + ry * Math.sin(index * Math.PI / 24)])));
    }
    await until(() => shown("chat"), `the command bar after the ${kind}`);
    await until(async () => (await picks()).length > 0, "the boxes over what was found");
    await sleep(700); // their entrance (a small scale) is over
    const found = await picks();
    const isExact = found.length === boxes.length && boxes.every((box) => found.some((pick) => near(pick, box)));
    const mode = await inShell(`document.getElementById("chat").dataset.mode`);
    return { boxes, found, isExact, mode, scroll, drawn: { left, top, right, bottom } };
  }

  for (const [width, height, isPhone, name] of [[390, 844, true, "390"], [1280, 900, false, "1280"]]) {
    await open(width, height, isPhone);
    await shot(`${name}-1-toolbar.png`);
    const bar = await inShell(`(() => { const r = document.getElementById("magic").getBoundingClientRect(); return { x: r.x, width: r.width, bottom: innerHeight - r.bottom }; })()`);
    report(Math.abs(bar.x + bar.width / 2 - width / 2) < 2 && bar.width <= width - 16 && bar.bottom >= 8, `${name} toolbar`,
      `bottom centre, ${Math.round(bar.width)} px wide in ${width}, ${Math.round(bar.bottom)} px from the bottom`);

    // The page scrolls before the owner points: boxes are the frame's viewport pixels, scroll says where it was.
    await inFrame("scrollTo(0, 120), true");
    await sleep(300);

    // 1. Rectangle around one card: exactly that card.
    const before = received().length;
    const rectangle = await selectAround("rectangle", ["#c4"], 6);
    await shot(`${name}-2-selected.png`);
    await inShell(`document.getElementById("chat-text").focus(), true`);
    await send("Input.insertText", { text: "make this card red" });
    await sleep(150);
    await tap("#chat-send");
    const typed = await waitInbound(before + 1, "the rectangle's command");
    const typedSelection = selectionOf(typed.Text);
    const bounds = typedSelection.bounds;
    const drawn = rectangle.drawn;
    const isBounds = Math.abs(bounds.x - Math.round(drawn.left)) <= 1 && Math.abs(bounds.y - Math.round(drawn.top)) <= 1
      && Math.abs(bounds.x + bounds.width - Math.round(drawn.right)) <= 1 && Math.abs(bounds.y + bounds.height - Math.round(drawn.bottom)) <= 1;
    report(rectangle.isExact && rectangle.mode === "command" && JSON.stringify(typedSelection.elements.map((e) => e.selector)) === '["#c4"]' && isBounds
      && typedSelection.scroll.y === Math.round(rectangle.scroll) && rectangle.scroll > 0 && near(typedSelection.elements[0].box, rectangle.boxes[0], 1) && typedSelection.viewport.width === width, `${name} rectangle`,
      `found ${JSON.stringify(typedSelection.elements.map((e) => e.selector))}, box ${JSON.stringify(typedSelection.elements[0]?.box)} vs page ${JSON.stringify(rectangle.boxes[0])}, bounds ${JSON.stringify(bounds)}, scroll ${typedSelection.scroll.y} (page ${rectangle.scroll}), ${rectangle.found.length} box drawn exactly over it: ${rectangle.isExact}, bar ${rectangle.mode}`);
    // The command bar steps aside, the narration says what happens, the agent's answer is narrated and read aloud.
    await until(() => shown("narration"), "the narration");
    const early = await text("narration-text");
    await until(async () => (await text("narration-text")).startsWith("Changed #c4"), "the narration's answer", 15000);
    await until(() => inShell(`!document.getElementById("narration-bars").hidden && document.getElementById("player").src.includes("/audio/")`), "the answer read aloud", 10000);
    await shot(`${name}-3-narration.png`);
    report(!(await shown("chat")) && /Sending|Thinking/.test(early) && await inFrame(`document.getElementById("c4").textContent.startsWith("Changed by msg")`), `${name} narration`,
      `bar shut, narration "${early}" then "${await text("narration-text")}", read aloud by C3's voice (player on /audio/, bars moving)`);

    // 2. Freeform around one title: the title, and its words.
    const lasso = await selectAround("freeform", ["#c2 > h3"], 8);
    const followBefore = await picks();
    await inFrame("scrollBy(0, 50), true");
    await sleep(400);
    const followAfter = await picks();
    report(Math.abs(followBefore[0].y - followAfter[0].y - 50) <= 1.5, `${name} follow`, `page scrolled 50 px: the box moved ${Math.round(followBefore[0].y - followAfter[0].y)} px with it`);
    await inFrame("scrollBy(0, -50), true");
    await sleep(300);
    await inShell(`document.getElementById("chat-text").focus(), true`);
    await send("Input.insertText", { text: "rename this plan" });
    await sleep(150);
    await tap("#chat-send");
    const lassoLine = await waitInbound(before + 2, "the freeform's command");
    const lassoSelection = selectionOf(lassoLine.Text);
    report(lasso.isExact && JSON.stringify(lassoSelection.elements.map((e) => e.selector)) === '["#c2 > h3"]' && lassoSelection.text === "Beta plan"
      && lassoSelection.shape === "freeform", `${name} freeform`,
      `found ${JSON.stringify(lassoSelection.elements.map((e) => e.selector))}, words "${lassoSelection.text}", box over it exact: ${lasso.isExact}`);
    await until(() => inShell(`!document.getElementById("narration-play").hidden`), "the second answer", 15000);

    // 3. Rectangle over two cards in a row: both, in page order.
    const pair = await selectAround("rectangle", ["#c5", "#c6"], 6);
    await shot(`${name}-4-two.png`);
    await inShell(`document.getElementById("chat-text").focus(), true`);
    await send("Input.insertText", { text: "swap these" });
    await sleep(150);
    await tap("#chat-send");
    const pairSelection = selectionOf((await waitInbound(before + 3, "the pair's command")).Text);
    report(pair.isExact && JSON.stringify(pairSelection.elements.map((e) => e.selector)) === '["#c5","#c6"]', `${name} two`,
      `found ${JSON.stringify(pairSelection.elements.map((e) => e.selector))}, ${pair.found.length} boxes exact: ${pair.isExact}`);
    await until(() => inShell(`!document.getElementById("narration-play").hidden`), "the third answer", 15000);
    await tap("#narration-close");
  }

  // ---------- the phone: mic, camera, files, narration's fallback and toggle ----------
  await open(390, 844, true);
  // 4. Tap the toolbar's mic: the command bar opens recording, with the time and a level; a second tap sends it.
  let count = received().length;
  await tap("#magic-mic");
  await until(() => inShell(`document.getElementById("chat-mic").ariaPressed === "true"`), "the recording");
  let peak = 0;
  for (let index = 0; index < 14; index++) {
    peak = Math.max(peak, Number(await inShell(`getComputedStyle(document.getElementById("chat-mic")).getPropertyValue("--level")`)) || 0);
    await sleep(100);
  }
  const recordingState = await inShell(`(() => { const c = document.getElementById("chat-wave"); const d = c.getContext("2d").getImageData(0, 0, c.width, c.height).data;
    let lit = 0; for (let i = 3; i < d.length; i += 4) if (d[i]) lit++;
    return { clock: document.getElementById("chat-clock").textContent, recorder: !document.getElementById("chat-recorder").hidden, lit, mode: document.getElementById("chat").dataset.mode }; })()`);
  await shot("390-5-recording.png");
  await tap("#chat-mic");
  const voice = await waitInbound(count + 1, "the voice note");
  report(recordingState.recorder && /^0:0[1-9]$/.test(recordingState.clock) && recordingState.lit > 100 && peak > 0.2 && recordingState.mode === "command"
    && voice.Attachments?.[0]?.Kind === "voice" && /^\[board plan · general · msg \d+\] Voice note:/.test(voice.Text), "mic tap",
    `recording bar ${recordingState.recorder}, clock ${recordingState.clock}, wave ${recordingState.lit} lit px, level peak ${peak.toFixed(2)}; sent ${JSON.stringify(voice.Text.split("\n")[0])} with a ${voice.Attachments?.[0]?.Kind} attachment of ${voice.Attachments?.[0]?.Size} bytes`);
  await until(async () => (await text("narration-said")).includes("harness transcript"), "the transcript in the narration", 15000);
  await until(async () => (await text("narration-text")).startsWith("Answer to msg"), "the voice note's answer", 15000);
  await shot("390-6-voice-answer.png");
  report(true, "mic answer", `narration "${await text("narration-said")}" then "${await text("narration-text")}"`);
  await tap("#narration-close");

  // 5. Hold the toolbar's mic to talk; let go to send.
  count = received().length;
  await tap("#magic-mic", 2000);
  const held = await waitInbound(count + 1, "the held voice note");
  report(held.Attachments?.[0]?.Kind === "voice" && held.Attachments[0].Size > 1000, "mic hold", `held 2 s, let go: ${JSON.stringify(held.Text.split("\n")[0])}, ${held.Attachments?.[0]?.Size} bytes`);
  await until(async () => (await text("narration-text")).startsWith("Answer to msg"), "the held note's answer", 15000);
  await tap("#narration-close");

  // 6. The camera: a photo, then a video; both wait in the tray with previews.
  await tap("#magic-camera");
  await until(() => inShell(`!document.getElementById("camera").hidden && document.getElementById("camera-view").videoWidth > 0`), "the camera's preview");
  await sleep(500);
  await shot("390-7-camera.png");
  await tap("#camera-shutter");
  await until(() => inShell(`document.querySelectorAll("#chat-tray li").length === 1 && document.querySelector("#chat-tray img")?.naturalWidth > 0`), "the photo in the tray");
  await tap("#chat-camera");
  await until(() => inShell(`document.getElementById("camera-view").videoWidth > 0`), "the camera again");
  await tap("#camera-video");
  await tap("#camera-shutter");
  await until(() => inShell(`document.getElementById("camera-shutter").ariaPressed === "true"`), "the video recording");
  await sleep(1800);
  const videoClock = await text("camera-clock");
  await tap("#camera-shutter");
  await until(() => inShell(`document.querySelectorAll("#chat-tray li").length === 2`), "the video in the tray");
  // 7. Files of any type join the tray too, each with a preview; one is removed before sending.
  const { root } = await send("DOM.getDocument", { depth: -1 });
  const { nodeId } = await send("DOM.querySelector", { nodeId: root.nodeId, selector: "#chat-file" });
  await send("DOM.setFileInputFiles", { nodeId, files: [note, binary] });
  await until(() => inShell(`document.querySelectorAll("#chat-tray li").length === 4`), "the picked files in the tray");
  const trayBefore = await inShell(`[...document.querySelectorAll("#chat-tray li")].map((li) => li.querySelector(".thumb").className + ":" + li.querySelector(".name").textContent)`);
  await shot("390-8-tray.png");
  await tap("#chat-tray li:last-child .drop");
  await until(() => inShell(`document.querySelectorAll("#chat-tray li").length === 3`), "the removal");
  await inShell(`document.getElementById("chat-text").focus(), true`);
  await send("Input.insertText", { text: "the shots" });
  await sleep(150);
  count = received().length;
  await tap("#chat-send");
  const files = [await waitInbound(count + 1, "the photo"), await waitInbound(count + 2, "the video"), await waitInbound(count + 3, "the note")];
  const names = files.map((line) => line.Attachments?.[0]?.Name ?? "");
  report(trayBefore.length === 4 && /image/.test(trayBefore[0]) && /video/.test(trayBefore[1]) && /^0:0[1-9]/.test(videoClock)
    && /^photo-\d{8}-\d{6}\.jpg$/.test(names[0]) && /^video-\d{8}-\d{6}\.(webm|mp4)$/.test(names[1]) && names[2] === "notes.txt"
    && files[0].Text.includes("the shots") && files.every((line) => line.Attachments?.[0]?.Size > 0), "camera+files",
    `tray ${JSON.stringify(trayBefore)} (video recorded ${videoClock}); after removing archive.bin, sent ${JSON.stringify(names)} with sizes ${JSON.stringify(files.map((line) => line.Attachments?.[0]?.Size))}, caption on the first`);
  await until(async () => (await text("narration-text")).startsWith("Answer to msg"), "the files' answer", 15000);
  await tap("#narration-close");

  // 8. A phone without a camera the browser can open: the camera button opens the phone's camera app (<input capture>).
  await send("Page.setInterceptFileChooserDialog", { enabled: true });
  await inShell(`Object.defineProperty(navigator.mediaDevices, "getUserMedia", { value: undefined, configurable: true }), true`);
  await tap("#magic-camera");
  await until(() => chooser, "the camera app's chooser");
  const capture = await inShell(`(() => { const input = document.getElementById("camera-file"); return input.getAttribute("capture") + " " + input.accept; })()`);
  report(Boolean(chooser) && capture === "environment image/*,video/*" && await inShell(`document.getElementById("camera").hidden`), "camera app",
    `no getUserMedia: the tap opened the file chooser of <input capture="environment" accept="image/*,video/*"> (${chooser?.mode})`);
  await send("Page.setInterceptFileChooserDialog", { enabled: false });
  await tap("#chat-close");

  // 9. Narration when C3 has no voice: the browser's own voice reads the answer.
  blockSpeak = true;
  await open(390, 844, true); // a fresh shell: getUserMedia back, the stub's record empty
  await tap("#pencil-rectangle");
  const title = await frameBox("#title");
  await stroke([[title.x - 6, title.y - 6], [title.x + title.width + 6, title.y + title.height + 6]]);
  await until(() => shown("chat"), "the command bar");
  await inShell(`document.getElementById("chat-text").focus(), true`);
  await send("Input.insertText", { text: "friendlier title" });
  await sleep(150);
  await tap("#chat-send");
  await until(async () => (await text("narration-text")).startsWith("Changed #title"), "the answer", 15000);
  await until(() => inShell(`window.__spoken.length > 0`), "the browser's voice", 10000);
  const spoken = await inShell("window.__spoken");
  report(spoken.at(-1) === await text("narration-text") && speakAsks.includes("blocked"), "local voice",
    `C3's /speak answered 503 unavailable; speechSynthesis read ${JSON.stringify(spoken.at(-1))}`);
  blockSpeak = false;

  // 10. Narration off: the next answer is shown but not read; on again from the toolbar.
  await tap("#narration-close");
  await tap("#magic-narrate");
  const stored = await inShell(`localStorage.getItem("c3.board.readAloud")`);
  const asksBefore = speakAsks.length;
  const spokenBefore = (await inShell("window.__spoken")).length;
  await tap("#pencil-rectangle");
  const pricing = await frameBox("#c1");
  await stroke([[pricing.x - 6, pricing.y - 6], [pricing.x + pricing.width + 6, pricing.y + pricing.height + 6]]);
  await until(() => shown("chat"), "the command bar");
  await inShell(`document.getElementById("chat-text").focus(), true`);
  await send("Input.insertText", { text: "quieter" });
  await sleep(150);
  await tap("#chat-send");
  await until(async () => (await text("narration-text")).startsWith("Changed #c1"), "the quiet answer", 15000);
  await sleep(1500);
  const isQuiet = speakAsks.length === asksBefore && (await inShell("window.__spoken")).length === spokenBefore;
  await tap("#magic-narrate");
  report(stored === "off" && isQuiet && await inShell(`document.getElementById("magic-narrate").ariaPressed === "true" && localStorage.getItem("c3.board.readAloud") === "on"`),
    "narrate off", `toggle off stored "${stored}", the answer was not read (${speakAsks.length - asksBefore} /speak asks); toggled on again`);
  await shot("390-9-narration-off.png");

  // 11. The toolbar folds to its pencil and opens again.
  await tap("#pencil");
  await sleep(450);
  const folded = await inShell(`document.getElementById("magic").getBoundingClientRect().width`);
  await shot("390-10-folded.png");
  await tap("#pencil");
  await sleep(450);
  const unfolded = await inShell(`document.getElementById("magic").getBoundingClientRect().width`);
  report(folded < 60 && unfolded > 300, "fold", `toolbar ${Math.round(folded)} px folded, ${Math.round(unfolded)} px open`);

  await open(1280, 900, false);
  await tap("#magic-camera");
  await until(() => inShell(`document.getElementById("camera-view").videoWidth > 0`), "the desktop camera");
  await shot("1280-5-camera.png");
  await tap("#camera-close");
  await tap("#chat-close"); // the camera opened the command bar; the toolbar comes back once it shuts
  await sleep(600); // the toolbar came back into view: its mic waits out its settle time
  await tap("#magic-mic");
  await until(() => inShell(`document.getElementById("chat-mic").ariaPressed === "true"`), "the desktop recording");
  await sleep(1200);
  await shot("1280-6-recording.png");
  await tap("#chat-cancel");
  report(errors.length === 0, "errors", `${errors.length ? errors.join(" | ") : "no script errors in the shell or the page"}; emulated touch taps that made no click (a mouse click stood in): ${touchMisses}`);
} catch (error) {
  report(false, "run", error.stack ?? String(error));
} finally {
  await browser.close();
  fs.rmSync(info, { force: true });
  harness.kill();
  fs.rmSync(work, { recursive: true, force: true });
}
const failed = results.filter((isPass) => !isPass).length;
console.log(failed ? `${failed} FAILED` : `all ${results.length} passed`);
process.exit(failed ? 1 : 0);
