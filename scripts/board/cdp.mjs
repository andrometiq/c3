// Chrome DevTools Protocol over --remote-debugging-pipe: Chromium reads NUL-terminated JSON on
// fd 3 and writes it on fd 4. Node 20 has no global WebSocket, and the pipe needs no port.

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { findChromium } from "./chromium.mjs";

const STDERR_TAIL_LINES = 20;

export class Browser {
  #child;
  #profile;
  #buffer = "";
  #nextId = 1;
  #pending = new Map();
  #listeners = new Set();
  #stderr = [];
  #exitError = null;

  /** Starts headless Chromium with a fresh profile. */
  static launch({ width, height }) {
    return new Browser({ width, height });
  }

  constructor({ width, height }) {
    this.#profile = fs.mkdtempSync(path.join(os.tmpdir(), "mindmeld-animation-chromium-"));
    const args = [
      "--headless=new", "--remote-debugging-pipe", "--no-sandbox", "--disable-gpu", "--disable-crash-reporter",
      "--disable-breakpad", "--disable-dev-shm-usage", "--hide-scrollbars", "--mute-audio", "--no-first-run",
      "--no-default-browser-check", "--disable-extensions", "--disable-background-timer-throttling",
      "--disable-renderer-backgrounding", "--disable-backgrounding-occluded-windows",
      `--window-size=${width},${height}`, "--force-device-scale-factor=1", `--user-data-dir=${this.#profile}`,
    ];
    this.#child = spawn(findChromium(), args, { stdio: ["ignore", "ignore", "pipe", "pipe", "pipe"] });
    this.#child.stderr.setEncoding("utf8");
    this.#child.stderr.on("data", (text) => {
      this.#stderr.push(...text.split("\n").filter(Boolean));
      this.#stderr.splice(0, Math.max(0, this.#stderr.length - STDERR_TAIL_LINES));
    });
    this.#child.stdio[4].on("data", (chunk) => this.#receive(chunk));
    this.#child.stdio[3].on("error", () => {}); // a dead browser surfaces through the exit handler
    this.#child.on("exit", (code, signal) => {
      this.#exitError = new Error(`Chromium exited (${signal ?? `code ${code}`}): ${this.#stderr.slice(-5).join(" | ")}`);
      for (const { reject } of this.#pending.values()) reject(this.#exitError);
      this.#pending.clear();
    });
    this.#child.on("error", (error) => {
      this.#exitError = new Error(`cannot start Chromium: ${error.message}`);
    });
  }

  #receive(chunk) {
    this.#buffer += chunk.toString("utf8");
    let end;
    while ((end = this.#buffer.indexOf("\0")) >= 0) {
      const message = JSON.parse(this.#buffer.slice(0, end));
      this.#buffer = this.#buffer.slice(end + 1);
      if (message.id !== undefined) {
        const waiter = this.#pending.get(message.id);
        this.#pending.delete(message.id);
        if (!waiter) continue;
        if (message.error) waiter.reject(new Error(`${waiter.method}: ${message.error.message}${message.error.data ? ` (${message.error.data})` : ""}`));
        else waiter.resolve(message.result);
      } else {
        for (const listener of this.#listeners) listener(message);
      }
    }
  }

  /** Sends one command; resolves with its result. `sessionId` targets an attached page. */
  send(method, params = {}, sessionId = undefined) {
    if (this.#exitError) return Promise.reject(this.#exitError);
    const id = this.#nextId++;
    const message = { id, method, params, ...(sessionId ? { sessionId } : {}) };
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject, method });
      this.#child.stdio[3].write(JSON.stringify(message) + "\0");
    });
  }

  /** Calls `listener({method, params, sessionId})` for every event; returns an unsubscribe. */
  onEvent(listener) {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  /** Resolves on the next `method` event for `sessionId`; rejects after `timeoutMs`. */
  waitForEvent(method, sessionId, timeoutMs) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { stop(); reject(new Error(`timed out after ${timeoutMs / 1000}s waiting for ${method}`)); }, timeoutMs);
      const stop = this.onEvent((event) => {
        if (event.method === method && event.sessionId === sessionId) {
          clearTimeout(timer);
          stop();
          resolve(event.params);
        }
      });
    });
  }

  /** Opens a new page target and returns its flat-protocol session id. */
  async newPage() {
    const { targetId } = await this.send("Target.createTarget", { url: "about:blank" });
    const { sessionId } = await this.send("Target.attachToTarget", { targetId, flatten: true });
    return sessionId;
  }

  async close() {
    if (!this.#exitError) {
      const exited = new Promise((resolve) => this.#child.once("exit", resolve));
      this.send("Browser.close").catch(() => {});
      const timer = setTimeout(() => this.#child.kill("SIGKILL"), 5000);
      await exited;
      clearTimeout(timer);
    }
    fs.rmSync(this.#profile, { recursive: true, force: true });
  }
}
