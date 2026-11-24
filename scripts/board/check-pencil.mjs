#!/usr/bin/env node
// Magic Link browser check: the REAL Go board routes (TestBoardBrowserHarness, whose fake agent answers a Magic Pencil
// command by changing the selected element in the page, sending the page again and narrating), C3's real shell and SDK,
// and headless Chromium with a fake microphone. Nobody in the loop.
// It draws a freeform selection and a rectangle over a plain page (no parts) with the mouse, gives one command typed and
// one spoken, and checks what the session receives (the harness records every inbound), that the page changes live
// without a reload (its boot mark and the content its own script drew stay), and that the narration shows.
// Usage: check-pencil.mjs <c3 checkout> [--shots <dir>]. Prints PASS/FAIL per check; exit 1 on any FAIL.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, execFileSync } from "node:child_process";
import { Browser } from "./cdp.mjs";
import { findChromium } from "./chromium.mjs";

const [checkout = process.cwd(), , shots = ""] = process.argv.slice(2);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const work = fs.mkdtempSync(path.join(os.tmpdir(), "c3-pencil-check-"));
const results = [];
const report = (isPass, name, detail) => { results.push(isPass); console.log(`${isPass ? "PASS" : "FAIL"}  ${name.padEnd(10)} ${detail}`); };

// A plain page: no parts, a script that marks when it booted and draws content of its own.
const page = path.join(work, "page.html");
fs.writeFileSync(page, `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Shop</title>
<style>
  body { margin: 0; font: 16px/1.5 system-ui, sans-serif; }
  header { padding: 24px 72px; background: #f6f8fa; }
  h1 { margin: 0; font-size: 28px; }
  section { margin: 24px 72px; padding: 16px; border: 1px solid #d1d9e0; border-radius: 10px; }
  #runtime { margin: 0 72px; color: #59636e; }
</style>
</head>
<body>
<header><h1 id="title">Welcome to the shop</h1></header>
<section id="pricing"><h2 id="pricing-title">Pricing</h2><p id="basic">Basic plan, ten a month.</p><p id="pro">Pro plan, thirty a month.</p></section>
<div id="runtime"></div>
<script>window.__boot = Math.random(); document.getElementById("runtime").textContent = "drawn by the page's script";</script>
</body>
</html>
`);

const tone = path.join(work, "tone.mp3");
execFileSync("ffmpeg", ["-v", "error", "-f", "lavfi", "-i", "sine=frequency=440:duration=1.5", "-ac", "1", "-ar", "24000", "-c:a", "libmp3lame", "-b:a", "32k", tone]);
const info = path.join(work, "harness.json");
const inbounds = path.join(work, "inbounds.jsonl");
const harness = spawn("go", ["test", "./internal/channel/web/", "-run", "^TestBoardBrowserHarness$", "-count=1"], {
  cwd: checkout, stdio: "ignore",
  env: { ...process.env, C3_BOARD_HARNESS: info, C3_BOARD_HARNESS_SECONDS: "240", C3_BOARD_HARNESS_PAGE: page, C3_BOARD_HARNESS_SPEECH: tone,
    C3_BOARD_HARNESS_INBOUNDS: inbounds },
});
for (let waited = 0; !fs.existsSync(info); waited += 200) {
  if (waited > 120000) throw new Error("the harness did not start");
  await sleep(200);
}
const { url, cookies, boards } = JSON.parse(fs.readFileSync(info, "utf8"));

const speech = path.join(work, "speech.wav");
execFileSync("ffmpeg", ["-v", "error", "-f", "lavfi", "-i",
  "aevalsrc=exprs='if(between(t,0.2,1.2),0.45*sin(2*PI*180*t),0)':s=48000:d=2", "-ac", "1", "-c:a", "pcm_s16le", speech]);
const wrapper = path.join(work, "chromium");
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
    for (const end = Date.now() + ms; Date.now() < end; await sleep(100)) if (await Promise.resolve().then(check).catch(() => false)) return true;
    throw new Error(`timed out waiting for ${what}`);
  }
  const mouse = (type, x, y, buttons = 0) => send("Input.dispatchMouseEvent", { type, x, y, button: type === "mouseMoved" ? (buttons ? "left" : "none") : "left", buttons, clickCount: 1 });
  async function click({ x, y }) {
    await mouse("mousePressed", x, y, 1);
    await mouse("mouseReleased", x, y);
    await sleep(150);
  }
  async function stroke(points) {
    await mouse("mouseMoved", ...points[0]);
    await mouse("mousePressed", ...points[0], 1);
    for (const point of points.slice(1)) await mouse("mouseMoved", ...point, 1);
    await mouse("mouseReleased", ...points.at(-1));
    await sleep(150);
  }
  const center = (selector) => inShell(`(() => { const r = document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; })()`);
  /** An element's box in the frame, in the shell's coordinates (the frame fills the window). */
  const frameBox = (selector) => inFrame(`(() => { const r = document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect(); return { x: r.x, y: r.y, width: r.width, height: r.height }; })()`);
  const shown = (id) => inShell(`(() => { const e = document.getElementById(${JSON.stringify(id)}); return Boolean(e && !e.hidden && e.checkVisibility()); })()`);
  const text = (id) => inShell(`document.getElementById(${JSON.stringify(id)}).textContent`);
  const log = () => inShell(`fetch(location.pathname + "/state").then((r) => r.json()).then((s) => s.log)`);
  const received = () => (fs.existsSync(inbounds) ? fs.readFileSync(inbounds, "utf8").trim().split("\n").filter(Boolean).map((line) => JSON.parse(line)) : []);
  const selectionOf = (line) => JSON.parse(line.split("\n").find((row) => row.startsWith("Magic Pencil selection (msg ")).replace(/^Magic Pencil selection \(msg \d+\): /, ""));
  const shot = async (name) => { if (shots) { fs.mkdirSync(shots, { recursive: true }); await sleep(250); fs.writeFileSync(path.join(shots, name), Buffer.from((await send("Page.captureScreenshot", { format: "png" })).data, "base64")); } };

  await until(() => inFrame("Boolean(window.c3) && Boolean(window.__boot)"), "the page in its frame");
  await until(() => shown("pencil"), "the pencil");
  const boot = await inFrame("window.__boot");
  const pencilBox = await inShell(`(() => { const r = document.getElementById("pencil").getBoundingClientRect(); return { x: r.x, y: r.y, width: r.width }; })()`);
  report(pencilBox.x < 40 && pencilBox.width >= 40, "pencil", `floating on the left at x=${Math.round(pencilBox.x)}, ${Math.round(pencilBox.width)} px wide`);

  // 1. Freeform around the title, a typed command.
  await click(await center("#pencil"));
  const isMenu = await shown("pencil-menu");
  await click(await center("#pencil-freeform"));
  const isLayer = await shown("pencil-layer");
  report(isMenu && isLayer && (await text("pencil-hint")).includes("Draw around"), "menu", `menu ${isMenu}, drawing layer ${isLayer}, hint "${await text("pencil-hint")}"`);
  const title = await frameBox("#title");
  const [cx, cy, rx, ry] = [title.x + title.width / 2, title.y + title.height / 2, title.width / 2 + 24, title.height / 2 + 22];
  const loop = Array.from({ length: 41 }, (_, index) => [cx + rx * Math.cos(index * Math.PI / 20), cy + ry * Math.sin(index * Math.PI / 20)]);
  await stroke(loop);
  await until(() => shown("chat"), "the chat after the selection");
  await shot("1-selected.png");
  const ink = await inShell(`(() => { const c = document.getElementById("pencil-ink"); const d = c.getContext("2d").getImageData(0, 0, c.width, c.height).data;
    let inked = 0; for (let i = 3; i < d.length; i += 4) if (d[i]) inked++; return inked; })()`);
  const reply = await text("chat-reply-text");
  const pencilEntry = (await log()).findLast((entry) => entry.op === "pencil");
  report(Boolean(pencilEntry) && /Magic Pencil: freeform around “Welcome to the shop”/.test(pencilEntry.text) && reply.includes("Magic Pencil") && ink > 1000
    && received().length === 0, "selection",
    `logged "${pencilEntry?.text}", reply bar "${reply}", shape drawn (${ink} inked pixels), ${received().length} lines at the session before a command`);

  await inShell(`document.getElementById("chat-text").focus(), true`);
  await send("Input.insertText", { text: "make the title friendlier" });
  await sleep(150);
  await click(await center("#chat-send"));
  await until(() => received().length >= 1, "the command at the session");
  const typed = received()[0];
  const typedSelection = selectionOf(typed.Text);
  const header = typed.Text.split("\n")[0];
  report(header.startsWith(`[board plan · page · msg ${pencilEntry.id + 1} · reply to msg ${pencilEntry.id} "Magic Pencil: freeform`) && header.endsWith("] make the title friendlier")
    && typedSelection.shape === "freeform" && typedSelection.points.length >= 20 && typedSelection.elements[0]?.selector === "#title"
    && typedSelection.elements[0].text === "Welcome to the shop" && typedSelection.viewport.width === 390 && typedSelection.rev === 1
    && typedSelection.url.endsWith("/board/plan") && typed.ReplyTo?.MessageID === pencilEntry.id, "session",
    `header ${JSON.stringify(header)}; selection ${typedSelection.shape}, ${typedSelection.points.length} points, elements ${JSON.stringify(typedSelection.elements.map((e) => e.selector))}, viewport ${typedSelection.viewport.width}x${typedSelection.viewport.height}, rev ${typedSelection.rev}`);
  await until(async () => !(await shown("chat")), "the chat to shut once the command is in").catch(() => {});
  const isChatShut = !(await shown("chat"));
  report(isChatShut && await shown("narration") && !(await shown("pencil-layer")), "sent", `chat ${isChatShut ? "shut" : "open"}, narration "${await text("narration-text")}"`);

  await until(() => inFrame(`document.getElementById("title").textContent.startsWith("Changed by msg")`), "the live change");
  await until(async () => (await text("narration-text")).startsWith("Changed #title"), "the narration");
  await shot("2-changed.png");
  const live = await inFrame(`({ boot: window.__boot, title: document.getElementById("title").textContent, runtime: document.getElementById("runtime").textContent, scroll: scrollY })`);
  report(live.boot === boot && live.runtime === "drawn by the page's script" && live.title === `Changed by msg ${pencilEntry.id + 1}`, "live",
    `title "${live.title}", frame ${live.boot === boot ? "not reloaded" : "RELOADED"}, page script's content "${live.runtime}"`);
  report(await shown("narration") && await shown("narration-play") && (await text("narration-text")).includes("freeform selection of 1 elements"), "narration",
    `"${await text("narration-text")}" with a play button`);

  // 2. Rectangle around the basic plan, a spoken command.
  await click(await center("#pencil"));
  await click(await center("#pencil-rectangle"));
  const basic = await frameBox("#basic");
  await stroke([[basic.x - 10, basic.y - 8], [basic.x + basic.width / 2, basic.y + 4], [basic.x + basic.width + 10, basic.y + basic.height + 8]]);
  await until(() => shown("chat"), "the chat after the rectangle");
  await sleep(700); // past the mic's settle time
  await click(await center("#chat-mic"));
  await until(() => inShell(`document.getElementById("chat-mic").ariaPressed === "true"`), "the recording");
  await sleep(1500);
  await click(await center("#chat-mic"));
  await until(() => received().length >= 2, "the spoken command at the session", 20000);
  const spoken = received()[1];
  const spokenSelection = selectionOf(spoken.Text);
  report(/\] Voice note:\nMagic Pencil selection \(msg \d+\): /.test(spoken.Text) && spoken.Attachments?.[0]?.Kind === "voice" && spokenSelection.shape === "rectangle"
    && spokenSelection.points.length === 4 && spokenSelection.elements.some((element) => element.selector === "#basic") && spokenSelection.rev === 2, "voice",
    `line ${JSON.stringify(spoken.Text.split("\n")[0])}, ${spoken.Attachments?.length} attachment, selection ${spokenSelection.shape} ${JSON.stringify(spokenSelection.elements.map((e) => e.selector))}, rev ${spokenSelection.rev}`);
  await until(() => inFrame(`document.getElementById("basic").textContent.startsWith("Changed by msg")`), "the second live change", 15000);
  await until(async () => (await text("narration-text")).includes("rectangle selection"), "the second narration");
  await shot("3-voice.png");
  const after = await inFrame(`({ boot: window.__boot, title: document.getElementById("title").textContent })`);
  report(after.boot === boot && after.title === live.title, "live-2", `frame ${after.boot === boot ? "not reloaded" : "RELOADED"}, earlier change kept: "${after.title}"`);

  // 3. Escape drops a drawing in progress; nothing is logged.
  const before = (await log()).length;
  await click(await center("#pencil"));
  await click(await center("#pencil-freeform"));
  await send("Input.dispatchKeyEvent", { type: "keyDown", key: "Escape", code: "Escape", windowsVirtualKeyCode: 27 });
  await send("Input.dispatchKeyEvent", { type: "keyUp", key: "Escape", code: "Escape", windowsVirtualKeyCode: 27 });
  await sleep(300);
  report(!(await shown("pencil-layer")) && (await log()).length === before, "escape", `layer ${await shown("pencil-layer") ? "still shown" : "gone"}, log ${before} -> ${(await log()).length}`);
  report(errors.length === 0, "errors", errors.length ? errors.join(" | ") : "no script errors in the shell or the page");
} catch (error) {
  report(false, "run", error.stack ?? String(error));
} finally {
  browser.close();
  fs.rmSync(info, { force: true });
  harness.kill();
}
const failed = results.filter((isPass) => !isPass).length;
console.log(failed ? `${failed} FAILED` : `all ${results.length} passed`);
process.exit(failed ? 1 : 0);
