#!/usr/bin/env node
// Magic Link end to end: a separate C3 broker and the real Claude adapter, both built from the checkout, with their own
// HOME, XDG dirs and ports, and a fake Telegram Bot API (C3_TELEGRAM_API_URL), so nothing touches a running broker or a
// real bot. This script plays the Claude session over MCP stdio (attach +board, publish the page, read the owner's
// pencil command with fetch_queue, change the page, narrate) and headless Chromium plays the owner (sign in with C3's
// login link, draw a freeform selection, type the command, see the page change live and the narration).
// Usage: check-magic-link.mjs <c3 checkout> [--shots <dir>]. Prints PASS/FAIL per check; exit 1 on any FAIL.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import http from "node:http";
import net from "node:net";
import { spawn, execFileSync } from "node:child_process";
import { Browser } from "./cdp.mjs";
import { findChromium } from "./chromium.mjs";

const [checkout = process.cwd(), , shots = ""] = process.argv.slice(2);
process.env.CHROMIUM = findChromium();
const work = fs.mkdtempSync(path.join(os.tmpdir(), "c3-magic-link-check-"));
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const results = [];
const report = (isPass, name, detail) => { results.push(isPass); console.log(`${isPass ? "PASS" : "FAIL"}  ${name.padEnd(9)} ${detail}`); };
const freePort = () => new Promise((resolve) => { const server = net.createServer().listen(0, "127.0.0.1", () => { const { port } = server.address(); server.close(() => resolve(port)); }); });

const dirs = Object.fromEntries(["home", "config", "state", "cache", "bin", "project"].map((name) => [name, path.join(work, name)]));
for (const dir of Object.values(dirs)) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
dirs.runtime = fs.mkdtempSync("/tmp/c3rt-"); // a unix socket path must stay under 108 bytes
execFileSync("go", ["build", "-o", dirs.bin, "./cmd/c3-broker", "./cmd/c3-claude-adapter"], { cwd: checkout, stdio: "inherit", env: { ...process.env, GOFLAGS: "-buildvcs=false" } });

// The fake Telegram: getMe, empty getUpdates, and every send recorded.
const sent = [];
let nextMessage = 100;
const telegram = http.createServer((request, response) => {
  let body = "";
  request.on("data", (chunk) => { body += chunk; });
  request.on("end", async () => {
    const method = request.url.split("/").pop().split("?")[0];
    let params = {};
    try { params = JSON.parse(body || "{}"); } catch { params = Object.fromEntries(new URLSearchParams(body)); }
    if (String(request.headers["content-type"]).startsWith("multipart/")) params = { raw: body };
    let result = true;
    if (method === "getMe") result = { id: 999, is_bot: true, first_name: "Test", username: "test_bot", can_join_groups: true, can_read_all_group_messages: true, supports_inline_queries: false };
    else if (method === "getUpdates") { await sleep(Math.min(Number(params.timeout ?? 1), 2) * 1000); result = []; }
    else if (method === "getChat") result = { id: Number(params.chat_id) || 4242, type: "private", first_name: "Owner" };
    else if (method.startsWith("send") || method === "editMessageText") {
      sent.push({ method, params });
      result = { message_id: nextMessage++, date: Math.floor(Date.now() / 1000), chat: { id: Number(params.chat_id) || 4242, type: "private" }, text: params.text ?? "" };
    }
    response.setHeader("Content-Type", "application/json");
    response.end(JSON.stringify({ ok: true, result }));
  });
}).listen(0, "127.0.0.1");
await new Promise((resolve) => telegram.on("listening", resolve));
const webPort = await freePort();
const owner = 4242;
fs.mkdirSync(path.join(dirs.config, "c3"), { recursive: true, mode: 0o700 });
fs.writeFileSync(path.join(dirs.config, "c3", "mappings.json"), JSON.stringify({
  schema_version: 1,
  channels: {
    telegram: { bot_token: "123456:fake-token-for-e2e", dm_chat_id: owner, master_user_id: owner, topics: [], debounce_ms: 300 },
    web: { enabled: true, listen: `127.0.0.1:${webPort}`, tls: false },
  },
  allowlist: { users: [owner] },
  mappings: {},
}, null, 2), { mode: 0o600 });
const env = { PATH: `${dirs.bin}:${process.env.PATH}`, HOME: dirs.home, XDG_CONFIG_HOME: dirs.config, XDG_STATE_HOME: dirs.state, XDG_CACHE_HOME: dirs.cache,
  XDG_RUNTIME_DIR: dirs.runtime, C3_TELEGRAM_API_URL: `http://127.0.0.1:${telegram.address().port}`, LANG: "C.UTF-8" };
execFileSync(path.join(dirs.bin, "c3-broker"), ["validate"], { env, stdio: "inherit" });
const brokerLog = fs.openSync(path.join(work, "broker.out"), "w");
const broker = spawn(path.join(dirs.bin, "c3-broker"), [], { env, stdio: ["ignore", brokerLog, brokerLog] });

// The session: the real Claude adapter over MCP stdio.
const adapter = spawn(path.join(dirs.bin, "c3-claude-adapter"), [], { env, cwd: dirs.project, stdio: ["pipe", "pipe", fs.openSync(path.join(work, "adapter.err"), "w")] });
const pending = new Map();
const notifications = [];
let buffer = "";
let nextID = 1;
adapter.stdout.on("data", (chunk) => {
  buffer += chunk;
  for (let index; (index = buffer.indexOf("\n")) >= 0;) {
    const line = buffer.slice(0, index);
    buffer = buffer.slice(index + 1);
    if (!line.trim()) continue;
    const message = JSON.parse(line);
    if (message.id !== undefined && pending.has(message.id)) { pending.get(message.id)(message); pending.delete(message.id); }
    else if (message.method) notifications.push(message);
  }
});
const rpc = (method, params) => new Promise((resolve) => {
  const id = nextID++;
  pending.set(id, resolve);
  setTimeout(() => { if (pending.delete(id)) resolve({ error: { message: `${method}: no answer in 30 s` } }); }, 30000);
  adapter.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
});
const tool = async (name, args) => {
  const reply = await rpc("tools/call", { name, arguments: args });
  return { isError: Boolean(reply.error || reply.result?.isError), text: reply.error?.message ?? reply.result?.content?.map((item) => item.text).join("\n") ?? "" };
};
const inboundTexts = () => notifications.filter((note) => note.method === "notifications/claude/channel").map((note) => note.params.content);

let browser = null;
try {
  const init = await rpc("initialize", { protocolVersion: "2025-06-18", capabilities: { experimental: { "claude/channel": {} } }, clientInfo: { name: "e2e-session", version: "1" } });
  adapter.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", method: "notifications/initialized" })}\n`);
  report(Boolean(init.result), "mcp", `adapter ${init.result?.serverInfo?.name} ${init.result?.serverInfo?.version ?? ""}`);
  let attach;
  for (const end = Date.now() + 30000; Date.now() < end; await sleep(500)) {
    attach = await tool("attach", { expr: "+board:shop" });
    if (!attach.isError && attach.text.includes("/board/shop")) break;
  }
  const link = attach.text.match(/https?:\/\/\S+\/board\/shop/)?.[0];
  report(Boolean(link) && link.startsWith(`http://127.0.0.1:${webPort}/`) && attach.text.includes("Magic Pencil"), "attach",
    `link ${link} (the listener's own address: no front configured); notice names the Magic Pencil rule: ${attach.text.includes("Magic Pencil")}`);

  const pagePath = path.join(dirs.project, "shop.html");
  const writePage = (title, basic) => fs.writeFileSync(pagePath, `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Shop</title>
<style>body{margin:0;font:16px/1.5 system-ui,sans-serif}header{padding:24px 72px;background:#f6f8fa}h1{margin:0;font-size:28px}
section{margin:24px 72px;padding:16px;border:1px solid #d1d9e0;border-radius:10px}</style></head>
<body><header><h1 id="title">${title}</h1></header>
<section id="pricing"><h2>Pricing</h2><p id="basic">${basic}</p></section>
<script>window.__boot = Math.random();</script></body></html>
`);
  writePage("Welcome to the shop", "Basic plan, ten a month.");
  const published = await tool("reply", { channel: "board:shop", text: "", media: [{ kind: "file", path: pagePath }] });
  report(!published.isError, "publish", published.text.split("\n")[0]);

  // The owner signs in with C3's own login link (DM'd through the fake Telegram), then opens the Magic Link.
  const linked = await new Promise((resolve) => { // async: the fake Telegram answers from this process
    const child = spawn(path.join(dirs.bin, "c3-broker"), ["web", "link"], { env });
    let output = "";
    child.stdout.on("data", (chunk) => { output += chunk; });
    child.stderr.on("data", (chunk) => { output += chunk; });
    child.on("close", (status) => resolve({ status, output }));
  });
  if (linked.status !== 0) throw new Error(`web link: ${linked.output}`);
  const login = sent.map((entry) => String(entry.params.text ?? entry.params.raw ?? "")).join("\n").match(/https?:\/\/\S+/)?.[0];
  report(Boolean(login), "login", `login link delivered by the fake Telegram DM: ${Boolean(login)}`);
  browser = Browser.launch({ width: 390, height: 844 });
  const session = await browser.newPage();
  let frameSession = null;
  const errors = [];
  browser.onEvent(({ method, params, sessionId }) => {
    if (method === "Runtime.exceptionThrown" && (sessionId === session || sessionId === frameSession)) errors.push(params.exceptionDetails.exception?.description ?? params.exceptionDetails.text);
    if (method === "Target.attachedToTarget" && params.targetInfo.type === "iframe" && sessionId === session) {
      frameSession = params.sessionId;
      browser.send("Runtime.enable", {}, frameSession).catch(() => {});
    }
  });
  const send = (method, params) => browser.send(method, params, session);
  for (const domain of ["Page", "Runtime"]) await send(`${domain}.enable`);
  await send("Target.setAutoAttach", { autoAttach: true, waitForDebuggerOnStart: false, flatten: true });
  await send("Emulation.setDeviceMetricsOverride", { width: 390, height: 844, deviceScaleFactor: 2, mobile: true });
  const evaluate = async (expression, sessionId = session) => {
    const result = await browser.send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true }, sessionId);
    if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description ?? result.exceptionDetails.text);
    return result.result.value;
  };
  const inFrame = (expression) => evaluate(expression, frameSession);
  const until = async (check, what, ms = 15000) => {
    for (const end = Date.now() + ms; Date.now() < end; await sleep(150)) if (await Promise.resolve().then(check).catch(() => false)) return;
    throw new Error(`timed out waiting for ${what}`);
  };
  const mouse = (type, x, y, buttons = 0) => send("Input.dispatchMouseEvent", { type, x, y, button: type === "mouseMoved" ? (buttons ? "left" : "none") : "left", buttons, clickCount: 1 });
  const center = (selector) => evaluate(`(() => { const r = document.querySelector(${JSON.stringify(selector)}).getBoundingClientRect(); return [r.x + r.width / 2, r.y + r.height / 2]; })()`);
  const click = async (selector) => { const [x, y] = await center(selector); await mouse("mousePressed", x, y, 1); await mouse("mouseReleased", x, y); await sleep(200); };
  const shot = async (name) => { if (shots) { fs.mkdirSync(shots, { recursive: true }); await sleep(300); fs.writeFileSync(path.join(shots, name), Buffer.from((await send("Page.captureScreenshot", { format: "png" })).data, "base64")); } };

  await send("Page.navigate", { url: login });
  await sleep(1500);
  if (await evaluate(`Boolean(document.querySelector("button, input[type=submit]"))`).catch(() => false)) { // the login page's Continue
    await evaluate(`document.querySelector("button, input[type=submit]").click(), true`);
    await sleep(1500);
  }
  await send("Page.navigate", { url: link });
  await until(() => inFrame("Boolean(window.c3) && Boolean(window.__boot)"), "the published page");
  await until(() => evaluate(`!document.getElementById("pencil").hidden`), "the pencil");
  const boot = await inFrame("window.__boot");

  await click("#pencil");
  await click("#pencil-freeform");
  const box = await inFrame(`(() => { const r = document.getElementById("title").getBoundingClientRect(); return [r.x + r.width / 2, r.y + r.height / 2, r.width / 2 + 24, r.height / 2 + 22]; })()`);
  const loop = Array.from({ length: 41 }, (_, index) => [box[0] + box[2] * Math.cos(index * Math.PI / 20), box[1] + box[3] * Math.sin(index * Math.PI / 20)]);
  await mouse("mouseMoved", ...loop[0]);
  await mouse("mousePressed", ...loop[0], 1);
  for (const point of loop.slice(1)) await mouse("mouseMoved", ...point, 1);
  await mouse("mouseReleased", ...loop.at(-1));
  await until(() => evaluate(`!document.getElementById("chat").hidden`), "the chat");
  await evaluate(`document.getElementById("chat-text").focus(), true`);
  await send("Input.insertText", { text: "say hello to the visitor" });
  await sleep(200);
  await click("#chat-send");
  await shot("e2e-1-command.png");

  // The session receives the command with the selection.
  // This adapter has no Claude Code host to render channel pushes, so the broker holds the line for fetch_queue,
  // which is how a queue-only session reads its input.
  let command = "";
  await until(async () => {
    command = inboundTexts().find((text) => text.includes("Magic Pencil selection")) ?? "";
    if (!command) command = (await tool("fetch_queue", {})).text;
    return command.includes("Magic Pencil selection");
  }, "the command at the session", 20000);
  const header = command.split("\n").find((line) => line.startsWith("[board shop")); // fetch_queue's own lines around it are C3's
  const selection = JSON.parse(command.split("\n").find((line) => line.startsWith("Magic Pencil selection")).replace(/^Magic Pencil selection \(msg \d+; [^)]*\): /, ""));
  const messageID = Number(header.match(/· msg (\d+)/)[1]);
  report(header.includes("reply to msg") && header.endsWith("] say hello to the visitor") && selection.shape === "freeform" && selection.elements[0]?.selector === "#title"
    && selection.url === link && selection.rev === 1 && selection.points.length > 10, "session",
    `the adapter delivered ${JSON.stringify(header)} with ${selection.shape} selection of ${JSON.stringify(selection.elements.map((e) => e.selector))}, url ${selection.url}, rev ${selection.rev}`);

  // The session changes the area the selector names, republishes answering the command, then narrates.
  writePage("Hello, visitor!", "Basic plan, ten a month.");
  const page = await tool("reply", { channel: "board:shop", text: "", reply_to: messageID, media: [{ kind: "file", path: pagePath }] });
  const narration = await tool("reply", { channel: "board:shop", reply_to: messageID, text: "I changed the title to greet the visitor." });
  report(!page.isError && !narration.isError, "answer", `page ${page.isError ? page.text : "sent"}, narration ${narration.isError ? narration.text : "sent"}`);
  await until(() => inFrame(`document.getElementById("title").textContent === "Hello, visitor!"`), "the live change");
  await until(() => evaluate(`document.getElementById("narration-text").textContent === "I changed the title to greet the visitor."`), "the narration");
  await shot("e2e-2-changed.png");
  report(await inFrame("window.__boot") === boot, "live", `title changed in place, frame ${await inFrame("window.__boot") === boot ? "not reloaded" : "RELOADED"}; narration shown`);
  report(errors.length === 0, "errors", errors.length ? errors.join(" | ") : "no script errors");
} catch (error) {
  report(false, "run", error.stack ?? String(error));
} finally {
  await browser?.close();
  adapter.kill();
  broker.kill("SIGTERM");
  for (let waited = 0; broker.exitCode === null && broker.signalCode === null && waited < 8000; waited += 200) await sleep(200);
  if (broker.exitCode === null && broker.signalCode === null) broker.kill("SIGKILL");
  telegram.close();
  await sleep(500);
  fs.rmSync(dirs.runtime, { recursive: true, force: true });
  if (results.some((isPass) => !isPass)) { // the broker's last words, before its directory goes
    const log = path.join(dirs.state, "c3", "broker.log");
    if (fs.existsSync(log)) console.log(fs.readFileSync(log, "utf8").trim().split("\n").slice(-20).join("\n"));
  }
  fs.rmSync(work, { recursive: true, force: true });
}
const failed = results.filter((isPass) => !isPass).length;
console.log(failed ? `${failed} FAILED` : `all ${results.length} passed`);
process.exit(failed ? 1 : 0);
