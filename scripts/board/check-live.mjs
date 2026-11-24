#!/usr/bin/env node
// Live page update browser check: the REAL Go board routes (TestBoardBrowserHarness, whose control file sends page
// revisions as the session would), C3's real shell and SDK, and headless Chromium. Nobody in the loop.
// It sends revisions of one page with a JSON part drawn by the page's own script and a c3.on("update") handler, and
// checks which way each lands: a change outside parts is patched in place (scroll, a typed field and its focus kept);
// a part-only change reaches the update handler; a mixed change touching the JSON part, a changed <template> and a
// changed script reload the frame, so nothing is left stale.
// Usage: check-live.mjs <c3 checkout> [--shots <dir>]. Prints PASS/FAIL per check; exit 1 on any FAIL.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { Browser } from "./cdp.mjs";

const [checkout = process.cwd(), , shots = ""] = process.argv.slice(2);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const work = fs.mkdtempSync(path.join(os.tmpdir(), "c3-live-check-"));
const results = [];
const report = (isPass, name, detail) => { results.push(isPass); console.log(`${isPass ? "PASS" : "FAIL"}  ${name.padEnd(9)} ${detail}`); };

/** One revision of the page: its title, the JSON part's number, the template's text and a script marker. */
const page = ({ title = "One", n = 1, template = "t1", script = "v1" } = {}) => `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Live</title>
<style>body { margin: 0; font: 16px/1.5 system-ui, sans-serif; } .tall { height: 3000px; }</style>
</head>
<body>
<h1 id="title">${title}</h1>
<input id="field" aria-label="Field">
<template id="tpl"><p>${template}</p></template>
<div id="view"></div>
<script type="application/json" data-c3-part="data">{"n": ${n}}</script>
<div class="tall"></div>
<script>/* ${script} */
window.__boot = Math.random();
const show = (n) => { document.getElementById("view").textContent = "n=" + n; };
show(JSON.parse(document.querySelector('[data-c3-part="data"]').textContent).n);
c3.on("update", (event) => { if (event.part !== "data") return false; show(event.data.n); window.__updates = (window.__updates ?? 0) + 1; return true; });
</script>
</body>
</html>
`;
let revision = 0;
const file = (fields) => {
  const target = path.join(work, `page-${++revision}.html`);
  fs.writeFileSync(target, page(fields));
  return target;
};

const info = path.join(work, "harness.json");
const harness = spawn("go", ["test", "./internal/channel/web/", "-run", "^TestBoardBrowserHarness$", "-count=1"], {
  cwd: checkout, stdio: "ignore", env: { ...process.env, C3_BOARD_HARNESS: info, C3_BOARD_HARNESS_SECONDS: "180", C3_BOARD_HARNESS_PAGE: file({}) },
});
for (let waited = 0; !fs.existsSync(info); waited += 200) {
  if (waited > 120000) throw new Error("the harness did not start");
  await sleep(200);
}
const { url, cookies, boards, control } = JSON.parse(fs.readFileSync(info, "utf8"));
const browser = Browser.launch({ width: 390, height: 844 });
const errors = [];
try {
  const session = await browser.newPage();
  let frameSession = null;
  browser.onEvent(({ method, params, sessionId }) => {
    if (method === "Runtime.exceptionThrown" && (sessionId === session || sessionId === frameSession)) errors.push(params.exceptionDetails.exception?.description ?? params.exceptionDetails.text);
    if (method === "Target.attachedToTarget" && params.targetInfo.type === "iframe") {
      frameSession = params.sessionId;
      browser.send("Runtime.enable", {}, frameSession).catch(() => {});
    }
  });
  const send = (method, params) => browser.send(method, params, session);
  for (const domain of ["Page", "Runtime", "Network"]) await send(`${domain}.enable`);
  await send("Target.setAutoAttach", { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });
  await send("Network.setCookie", { ...cookies[0], url });
  await send("Page.navigate", { url: boards.plan });
  const inFrame = async (expression) => {
    const result = await browser.send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true }, frameSession);
    if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description ?? result.exceptionDetails.text);
    return result.result.value;
  };
  const until = async (check, what, ms = 15000) => {
    for (const end = Date.now() + ms; Date.now() < end; await sleep(150)) if (await Promise.resolve().then(check).catch(() => false)) return;
    throw new Error(`timed out waiting for ${what}`);
  };
  const look = () => inFrame(`({ boot: window.__boot, title: document.getElementById("title").textContent, view: document.getElementById("view").textContent,
    scroll: Math.round(scrollY), field: document.getElementById("field").value, focused: document.activeElement?.id ?? "", updates: window.__updates ?? 0,
    template: document.getElementById("tpl").content.textContent })`);
  const publish = (fields) => fs.appendFileSync(control, `page plan ${file(fields)}\n`);
  const shot = async (name) => { if (shots) { fs.mkdirSync(shots, { recursive: true }); fs.writeFileSync(path.join(shots, name), Buffer.from((await send("Page.captureScreenshot", { format: "png" })).data, "base64")); } };

  await until(() => inFrame(`Boolean(window.__boot) && document.getElementById("view").textContent === "n=1"`), "the page");
  const inShell = (expression) => browser.send("Runtime.evaluate", { expression, returnByValue: true }, session);
  await inShell(`document.getElementById("page").focus()`); // keyboard focus into the frame, as a tap gives it
  await inFrame(`document.getElementById("field").focus(); true`);
  await send("Input.insertText", { text: "typed by the owner" });
  await inFrame(`scrollTo(0, 1200); true`);
  const start = await look();
  if (start.field !== "typed by the owner" || start.focused !== "field" || start.scroll !== 1200) {
    throw new Error(`setup: field "${start.field}", focus #${start.focused}, scroll ${start.scroll}`);
  }

  // A change outside parts: patched in place.
  publish({ title: "Two" });
  await until(async () => (await look()).title === "Two", "the outside change");
  const outside = await look();
  report(outside.boot === start.boot && outside.scroll === 1200 && outside.field === "typed by the owner" && outside.focused === "field", "outside",
    `title "${outside.title}", frame ${outside.boot === start.boot ? "kept" : "RELOADED"}, scroll ${outside.scroll}, field "${outside.field}", focus #${outside.focused}`);

  // A change in the JSON part only: the page's update handler draws it.
  publish({ title: "Two", n: 2 });
  await until(async () => (await look()).view === "n=2", "the part update");
  const partOnly = await look();
  report(partOnly.boot === start.boot && partOnly.updates === 1 && partOnly.scroll === 1200, "part",
    `view "${partOnly.view}" by the update handler (${partOnly.updates} call), frame ${partOnly.boot === start.boot ? "kept" : "RELOADED"}`);

  // A mixed change touching the JSON part: reloaded, so the page shows the new data, never a stale view.
  publish({ title: "Three", n: 3 });
  await until(async () => { const now = await look(); return now.title === "Three" && now.view === "n=3"; }, "the mixed change", 20000);
  const mixed = await look();
  report(mixed.boot !== start.boot && mixed.view === "n=3", "mixed", `title "${mixed.title}", view "${mixed.view}", frame ${mixed.boot !== start.boot ? "reloaded" : "KEPT (stale risk)"}`);

  // A changed <template>: its content isn't in the node tree, so the frame reloads.
  publish({ title: "Three", n: 3, template: "t2" });
  await until(async () => (await look()).template === "t2", "the template change", 20000);
  const template = await look();
  report(template.boot !== mixed.boot, "template", `template "${template.template}", frame ${template.boot !== mixed.boot ? "reloaded" : "KEPT"}`);

  // A changed script: only a reload runs it.
  publish({ title: "Three", n: 3, template: "t2", script: "v2" });
  await until(async () => (await look()).boot !== template.boot, "the script reload", 20000);
  const script = await look();
  report(script.title === "Three" && script.view === "n=3", "script", `frame reloaded for the new script, view "${script.view}"`);
  await shot("live.png");
  report(errors.length === 0, "errors", errors.length ? errors.join(" | ") : "no script errors in the shell or the page");
} catch (error) {
  report(false, "run", error.stack ?? String(error));
} finally {
  await browser.close();
  harness.kill();
  fs.rmSync(work, { recursive: true, force: true });
}
const failed = results.filter((isPass) => !isPass).length;
console.log(failed ? `${failed} FAILED` : `all ${results.length} passed`);
process.exit(failed ? 1 : 0);
