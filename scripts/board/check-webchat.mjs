#!/usr/bin/env node
// Web chat voice browser check: the REAL Go web chat routes (TestWebChatBrowserHarness, whose fake broker plays
// the STT readback and answers), the real page, and headless Chromium with a fake microphone
// (--use-fake-device-for-media-stream --use-fake-ui-for-media-stream). Nobody in the loop. The page is served with one
// change: VOICE_PART_MS 2500 instead of 5 minutes, so the numbered-parts path runs in seconds. C3's own login is served
// on 127.0.0.1, the authenticated front on this host's first non-loopback address.
// Usage: check-webchat.mjs <c3 checkout> [--shots <dir>]. Prints PASS/FAIL per check; exit 1 on any FAIL.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { Browser } from "./cdp.mjs";
import { findChromium } from "./chromium.mjs";

const [checkout = process.cwd(), , shots = ""] = process.argv.slice(2);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const work = fs.mkdtempSync(path.join(os.tmpdir(), "c3-webchat-check-"));
const results = [];
const report = (isPass, name, detail) => { results.push(isPass); console.log(`${isPass ? "PASS" : "FAIL"}  ${name.padEnd(9)} ${detail}`); };

const info = path.join(work, "harness.json");
const harness = spawn("go", ["test", "./internal/channel/web/", "-run", "^TestWebChatBrowserHarness$", "-count=1"], {
  cwd: checkout, stdio: "ignore",
  env: { ...process.env, GOFLAGS: "-buildvcs=false", C3_WEBCHAT_HARNESS: info, C3_WEBCHAT_HARNESS_SECONDS: "300", C3_WEBCHAT_HARNESS_PART_MS: "2500" },
});
for (let waited = 0; !fs.existsSync(info); waited += 200) {
  if (waited > 180000) throw new Error("the harness did not start");
  await sleep(200);
}
await sleep(200);
const harnessInfo = JSON.parse(fs.readFileSync(info, "utf8"));
const inbounds = () => (fs.existsSync(harnessInfo.inbounds) ? fs.readFileSync(harnessInfo.inbounds, "utf8").split("\n").filter(Boolean).map((line) => JSON.parse(line)) : []);

const wrapper = path.join(work, "chromium");
fs.writeFileSync(wrapper, `#!/bin/sh\nexec ${JSON.stringify(findChromium())} --use-fake-device-for-media-stream --use-fake-ui-for-media-stream "$@"\n`, { mode: 0o755 });
process.env.CHROMIUM = wrapper;
const browser = Browser.launch({ width: 390, height: 844 });
const errors = [];
try {
  async function openPage() {
    const session = await browser.newPage();
    for (const domain of ["Page", "Runtime", "Network"]) await browser.send(`${domain}.enable`, {}, session);
    await browser.send("Emulation.setDeviceMetricsOverride", { width: 390, height: 844, deviceScaleFactor: 2, mobile: true }, session);
    return session;
  }
  browser.onEvent(({ method, params }) => {
    if (method === "Runtime.exceptionThrown") errors.push(params.exceptionDetails.exception?.description ?? params.exceptionDetails.text);
  });
  const own = await openPage();
  const send = (method, params, session = own) => browser.send(method, params, session);
  async function evaluate(expression, session = own) {
    const result = await send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true }, session);
    if (result.exceptionDetails) throw new Error(`threw: ${result.exceptionDetails.exception?.description ?? result.exceptionDetails.text}`);
    return result.result.value;
  }
  async function until(check, what, ms = 15000) {
    for (const end = Date.now() + ms; Date.now() < end; await sleep(100)) if (await check().catch(() => false)) return true;
    throw new Error(`timed out waiting for ${what}`);
  }
  async function mouse(type, { x, y }, session = own) {
    await send("Input.dispatchMouseEvent", { type, x, y, button: "left", buttons: type === "mouseReleased" ? 0 : 1, clickCount: 1 }, session);
  }
  async function click(point) {
    await mouse("mousePressed", point);
    await mouse("mouseReleased", point);
    await sleep(100);
  }
  const center = (selector, session = own) => evaluate(`(() => { const r = document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect(); return { x: r.x + r.width / 2, y: r.y + r.height / 2 }; })()`, session);
  const shot = async (name, session = own) => { if (shots) { fs.mkdirSync(shots, { recursive: true }); fs.writeFileSync(path.join(shots, name), Buffer.from((await send("Page.captureScreenshot", { format: "png" }, session)).data, "base64")); } };
  const control = (id, session = own) => evaluate(`(() => { const node = document.getElementById(${JSON.stringify(id)}); return { hidden: node.hidden, shown: node.checkVisibility() }; })()`, session);
  // Every voice row the page draws, oldest first: its label, what it says under it, its state and its joins.
  const voiceRows = () => evaluate(`[...document.querySelectorAll(".message.mine")].filter((row) => row.querySelector(".voice-label")).map((row) => ({
    label: row.querySelector(".voice-label").textContent, state: row.querySelector(".body").dataset.voiceState,
    text: row.querySelector(".voice-transcript")?.textContent ?? "", isFresh: Boolean(row.querySelector(".voice-fresh")),
    delivery: row.querySelector(".delivery")?.textContent ?? "", continues: row.classList.contains("voice-continues"), continued: row.classList.contains("voice-continued") }))`);
  const mic = () => evaluate(`({ text: document.getElementById("mic").textContent, pressed: document.getElementById("mic").getAttribute("aria-pressed"), disabled: document.getElementById("mic").disabled })`);
  const agentTexts = () => evaluate(`[...document.querySelectorAll(".message.agent .body")].map((body) => body.textContent)`);
  // Each voice row's states in the order the page drew them, so a passing state (sending, transcribing) is seen.
  const watchStates = () => evaluate(`(() => {
    window.voiceStates = [];
    const ids = new WeakMap();
    const note = (row) => {
      const state = row?.classList?.contains("mine") ? row.querySelector(".body")?.dataset.voiceState : "";
      if (!state) return;
      if (!ids.has(row)) ids.set(row, window.voiceStates.push([]) - 1);
      const states = window.voiceStates[ids.get(row)];
      if (states.at(-1) !== state) states.push(state);
    };
    new MutationObserver((records) => records.forEach((record) => record.type === "childList"
      ? record.addedNodes.forEach((node) => node.nodeType === 1 && note(node)) : note(record.target.closest(".message"))))
      .observe(document.getElementById("message-list"), { subtree: true, childList: true, attributes: true, attributeFilter: ["data-voice-state"] });
    return true;
  })()`);

  await send("Network.setCookie", { ...harnessInfo.cookie, url: harnessInfo.url });
  await send("Page.navigate", { url: `${harnessInfo.url}/` });
  await until(() => evaluate(`document.getElementById("connection").textContent.includes("harness-agent")`), "the session's presence");
  await until(async () => !(await control("logout")).hidden, "C3's own login to show Logout", 5000).catch(() => {});

  // Live mode (Drive) and Hands-free (Chat) are off: hidden, while the rest of Drive and Chat stays.
  const controls = { live: await control("drive-live"), handsFree: await control("hands-free-toggle"), pill: await control("hands-free-state"),
    replay: await control("drive-replay"), mic: await control("mic"), logout: await control("logout") };
  await shot("webchat-drive.png");
  report(!controls.live.shown && !controls.handsFree.shown && !controls.pill.shown && controls.replay.shown && controls.mic.shown, "controls",
    `Live ${controls.live.shown ? "shown" : "hidden"}, Hands-free ${controls.handsFree.shown ? "shown" : "hidden"}, its pill ${controls.pill.shown ? "shown" : "hidden"}; Replay ${controls.replay.shown ? "shown" : "hidden"}, mic ${controls.mic.shown ? "shown" : "hidden"}`);

  await evaluate(`document.getElementById("drive-chat").click(), true`);
  await until(() => evaluate(`document.getElementById("view-shell").classList.contains("show-chat")`), "the Chat view");
  await sleep(300);
  await watchStates();

  // A short note: sending, then "Transcribing…" once C3 has it, then the transcript fading in where that was.
  await click(await center("#mic"));
  await until(async () => (await mic()).pressed === "true", "recording");
  await sleep(1500);
  await click(await center("#mic"));
  await until(async () => (await voiceRows()).at(-1)?.state === "heard" && (await agentTexts()).some((text) => /Answer to msg \d+/.test(text)), "the transcript, then the answer");
  const short = (await voiceRows()).at(-1);
  const shortStates = (await evaluate("window.voiceStates")).at(-1);
  const shortInbound = inbounds().filter((inbound) => inbound.voice).at(-1);
  await shot("webchat-short.png");
  report(shortStates.join(">") === "sending>transcribing>heard" && short.isFresh && /^🎤 Voice note · 0:0\d$/.test(short.label) && /harness transcript of \d+ bytes/.test(short.text)
    && short.delivery === "sent" && shortInbound?.text === "", "short",
    `states ${shortStates.join(" > ")}; row "${short.label}" / "${short.text}" (${short.isFresh ? "faded in" : "no fade"}, ${short.delivery}); session read ${JSON.stringify(shortInbound?.text)} + audio`);

  // No speech heard: C3's status on the note, not "couldn't transcribe".
  fs.writeFileSync(harnessInfo.control, "no-speech\n");
  await sleep(300);
  await click(await center("#mic"));
  await until(async () => (await mic()).pressed === "true", "recording a quiet note");
  await sleep(1200);
  await click(await center("#mic"));
  await until(async () => (await voiceRows()).at(-1)?.state === "no_speech", "no speech heard");
  const quiet = (await voiceRows()).at(-1);
  report(quiet.text === "🔇 No speech heard in that voice note." && !/transcri/i.test(quiet.text), "nospeech", `row "${quiet.label}" says "${quiet.text}" (${quiet.state})`);

  // A long note: every part goes out while recording goes on, numbered; the mic's clock never restarts.
  const before = inbounds().length;
  await until(async () => !(await mic()).disabled, "the mic to be free");
  await click(await center("#mic"));
  await until(async () => (await mic()).pressed === "true", "a long recording");
  await sleep(3500);
  const midway = await mic();
  const midRows = await voiceRows();
  await shot("webchat-recording-part2.png");
  await sleep(2700);
  await click(await center("#mic"));
  await until(async () => (await voiceRows()).slice(-3).every((row) => row.state === "heard") && inbounds().length >= before + 3, "three parts heard", 20000);
  const parts = (await voiceRows()).slice(-3);
  const partLines = inbounds().slice(before).map((inbound) => inbound.text);
  await shot("webchat-parts.png");
  report(midway.pressed === "true" && /^● 0:0[3-4] · part 2$/.test(midway.text) && midRows.at(-1).label.startsWith("🎤 Voice note · part 1 · ")
    && /part 1 · /.test(parts[0].label) && /part 2 · /.test(parts[1].label) && /part 3, the last/.test(parts[2].label)
    && parts[0].continues && parts[1].continues && parts[1].continued && !parts[2].continues && parts[2].continued
    && partLines.join("|") === "Voice note (part 1; more follows):|Voice note (part 2; more follows):|Voice note (part 3, the last):", "parts",
    `at 3.5 s mic "${midway.text}" (pressed ${midway.pressed}), newest row "${midRows.at(-1).label}"; rows ${parts.map((row) => `"${row.label}" ${row.state}${row.continues ? " ↓" : ""}`).join(", ")}; session read ${JSON.stringify(partLines)}`);

  // Cancel after a part (Drive's locked recording, then its Cancel held): the session still gets a last line.
  const beforeCancel = inbounds().length;
  await evaluate(`document.getElementById("chat-drive").click(), true`);
  await until(() => evaluate(`!document.getElementById("view-shell").classList.contains("show-chat")`), "the Drive view");
  await sleep(400);
  const circle = await center("#drive-circle");
  await mouse("mousePressed", circle);
  await sleep(150);
  for (const step of [20, 40, 60, 80, 95]) {
    await mouse("mouseMoved", { x: circle.x, y: circle.y - step });
    await sleep(40);
  }
  await mouse("mouseReleased", { x: circle.x, y: circle.y - 95 });
  await until(() => evaluate(`!document.getElementById("drive-cancel").hidden`), "the locked recording's Cancel");
  await until(async () => inbounds().slice(beforeCancel).some((inbound) => inbound.text.startsWith("Voice note (part 1;")), "part 1 of the locked recording", 10000);
  await sleep(300);
  await shot("webchat-drive-locked.png");
  const cancelButton = await center("#drive-cancel");
  await mouse("mousePressed", cancelButton);
  await sleep(950);
  await mouse("mouseReleased", cancelButton);
  await until(async () => inbounds().length >= beforeCancel + 2, "the cancelled line", 10000);
  const cancelLines = inbounds().slice(beforeCancel).map((inbound) => `${inbound.text}${inbound.voice ? " +audio" : ""}`);
  const driveLine = await evaluate(`document.getElementById("drive-transcript").textContent`);
  await evaluate(`document.getElementById("drive-chat").click(), true`);
  await until(async () => (await voiceRows()).at(-1)?.state === "unsent", "the cancelled part's row");
  await until(async () => (await voiceRows()).at(-2)?.state === "heard", "part 1 heard");
  const cancelRows = (await voiceRows()).slice(-2);
  const status = await evaluate(`document.getElementById("voice-status").textContent`);
  await sleep(400);
  await shot("webchat-cancelled.png");
  report(cancelLines.join("|") === "Voice note (part 1; more follows): +audio|Voice note (part 2, the last): the recording was cancelled; this part was not sent."
    && /part 1 · /.test(cancelRows[0].label) && cancelRows[1].label === "🎤 Voice note · part 2, the last" && cancelRows[1].text === "Cancelled, not sent." && cancelRows[1].delivery === "",
    "cancel", `session read ${JSON.stringify(cancelLines)}; rows ${cancelRows.map((row) => `"${row.label}" ${row.state} "${row.text.slice(0, 40)}"`).join(", ")}; status "${status}"; Drive line "${driveLine}"`);

  // A part C3 refuses (the page's fetch answers 422 for part 2 once): its row says so, and the session still gets
  // the part's line.
  const beforeRefused = inbounds().length;
  await evaluate(`(() => { const realFetch = window.fetch; let isRefused = false;
    window.fetch = (url, init = {}) => { const headers = init.headers || {};
      if (url === "/voice-note" && headers["X-Voice-Segment"] === "2" && !headers["X-Voice-Unsent"] && !isRefused) {
        isRefused = true; return Promise.resolve(new Response(JSON.stringify({ error: "could not decode audio" }), { status: 422 })); }
      return realFetch(url, init); }; return true; })()`);
  await until(async () => !(await mic()).disabled, "the mic to be free");
  await click(await center("#mic"));
  await until(async () => (await mic()).pressed === "true", "a recording to refuse");
  await sleep(3500);
  await click(await center("#mic"));
  await until(async () => inbounds().length >= beforeRefused + 2 && (await voiceRows()).at(-1)?.state === "unsent", "the refused part's line", 15000);
  const refusedLines = inbounds().slice(beforeRefused).map((inbound) => `${inbound.text}${inbound.voice ? " +audio" : ""}`);
  const refused = (await voiceRows()).at(-1);
  await sleep(400);
  await shot("webchat-refused.png");
  report(refusedLines.join("|") === "Voice note (part 1; more follows): +audio|Voice note (part 2, the last): C3 couldn't take this part; it was not sent."
    && /part 2, the last/.test(refused.label) && refused.text === "Not sent: C3 refused it." && refused.delivery === "not sent — could not decode audio", "refused",
    `session read ${JSON.stringify(refusedLines)}; row "${refused.label}" "${refused.text}" (${refused.delivery})`);

  // A reload draws the same rows from history: parts joined, each with its final state. Lengths aren't compared: a
  // row keeps the page's own measure when C3 answers before its echo, and a refused part's row then has none.
  const placeOf = ({ label, state, text, continues, continued }) => JSON.stringify({ label: label.replace(/ · \d+:\d\d$/, ""), state, text, continues, continued });
  const live = (await voiceRows()).map(placeOf);
  await send("Page.reload", {});
  await until(() => evaluate(`document.getElementById("view-chat").classList.contains("history-ready") && document.querySelectorAll(".message.mine").length > 0`), "history after reload");
  await sleep(500);
  const reloaded = (await voiceRows()).map(placeOf);
  const differing = reloaded.filter((row, index) => row !== live[index]);
  report(reloaded.length === live.length && differing.length === 0, "reload", `${reloaded.length} voice rows after reload, ${differing.length} differ${differing.length ? `: ${differing.join(" ")}` : ""}`);

  // Logout: kept for C3's own login, hidden for a viewer the front signed in (POST /logout doesn't exist there).
  const ownLogout = await control("logout");
  const front = await openPage();
  await browser.send("Network.setExtraHTTPHeaders", { headers: { [harnessInfo.front_header]: harnessInfo.front_identity } }, front);
  await browser.send("Page.navigate", { url: `${harnessInfo.front_url}/` }, front);
  await until(() => evaluate(`document.getElementById("connection").textContent.includes("harness-agent")`, front), "the front viewer's presence");
  await evaluate(`document.getElementById("drive-chat").click(), true`, front);
  await sleep(400);
  const frontLogout = await control("logout", front);
  await shot("webchat-front-chat.png", front);
  await evaluate(`document.getElementById("chat-drive").click(), true`, own);
  report(!ownLogout.hidden && ownLogout.shown && frontLogout.hidden && !frontLogout.shown, "logout",
    `C3 login: Logout ${ownLogout.shown ? "shown" : "hidden"}; front viewer: Logout ${frontLogout.shown ? "shown" : "hidden"}`);

  report(errors.length === 0, "errors", errors.length ? errors.join(" | ") : "no page exceptions");
} catch (error) {
  report(false, "run", `${error.stack ?? error}\n  session read since start: ${JSON.stringify(inbounds().map((inbound) => inbound.text).slice(-6))}`);
} finally {
  await browser.close();
  fs.rmSync(info, { force: true });
  await new Promise((resolve) => { harness.once("exit", resolve); setTimeout(() => { harness.kill(); resolve(); }, 5000); });
  fs.rmSync(work, { recursive: true, force: true });
}
const failed = results.filter((isPass) => !isPass).length;
console.log(`${results.length - failed}/${results.length} passed`);
process.exit(failed ? 1 : 0);
