#!/usr/bin/env python3
"""MCP stdio recorder/barriers. Never synthesizes a host transcript or receipt.

The hidden attach uses the real adapter's tool handler; only that private RPC's
response is consumed here. All host traffic is otherwise forwarded verbatim.
Fetch mode removes the inherited peer endpoint and launches without the channel
flag, leaving the actual adapter pull-only. Raw evidence stays in private scratch.
"""
import json
import os
import queue
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time


def wait_file(path, seconds=120):
    deadline = time.monotonic() + seconds
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(f"barrier timed out: {path.name}")
        time.sleep(0.05)


def main():
    adapter, directory, transport, topic = sys.argv[1:]
    control = Path(directory)
    pid = os.getpid()
    env = os.environ.copy()
    if transport == "fetch":
        env.pop("CLAUDE_CODE_MESSAGING_SOCKET", None)
        env.pop("CLAUDE_CODE_MESSAGING_TOKEN", None)
    # A dead scratch broker must never trigger adapter auto-spawn of a normal one.
    if not (Path(env["XDG_RUNTIME_DIR"]) / "c3.sock").is_socket():
        raise RuntimeError("private broker is absent")
    lock = threading.Lock()
    events_lock = threading.Lock()
    child = subprocess.Popen([adapter], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=open(control / "adapter.log", "ab", buffering=0), env=env)

    def event(kind, **data):
        with events_lock, (control / "events.jsonl").open("a") as out:
            out.write(json.dumps(dict(event=kind, pid=pid, time=time.time(), **data)) + "\n")

    def write_child(frame):
        with lock:
            child.stdin.write(json.dumps(frame).encode() + b"\n")
            child.stdin.flush()

    pending = {}
    attach_id = f"matrix-attach-{pid}"
    initialized = threading.Event()
    attached = threading.Event()
    attach_results = queue.Queue()

    def attach():
        initialized.wait()
        for _ in range(20):
            write_child({"jsonrpc": "2.0", "id": attach_id, "method": "tools/call",
                         "params": {"name": "attach", "arguments": {"topic_id": int(topic)}}})
            try:
                if attach_results.get(timeout=15):
                    return
            except queue.Empty:
                event("attach_failed", reason="attach RPC timed out")
                return
            time.sleep(1)
        event("attach_failed")

    def from_host():
        try:
            for raw in sys.stdin.buffer:
                frame = json.loads(raw)
                if frame.get("method") == "notifications/initialized":
                    event("initialized_waiting")
                    if (control / "gate-initialized").exists():
                        wait_file(control / f"release-initialized-{pid}")
                    event("initialized_released")
                    write_child(frame)
                    initialized.set()
                    continue
                if frame.get("method") == "tools/call":
                    pending[frame["id"]] = frame.get("params", {}).get("name", "")
                    event("tool_call", frame=frame)
                write_child(frame)
        except Exception as exc:
            event("proxy_error", reason=str(exc))
        finally:
            child.terminate()

    def stop(*_):
        child.terminate()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, stop)
    event("proxy_started", inbox_env=bool(env.get("CLAUDE_CODE_MESSAGING_SOCKET")))
    threading.Thread(target=from_host, daemon=True).start()
    threading.Thread(target=attach, daemon=True).start()
    try:
        for raw in child.stdout:
            frame = json.loads(raw)
            if frame.get("id") == attach_id:
                result = frame.get("result", {})
                if result and not result.get("isError"):
                    attached.set()
                    event("attached", frame=frame)
                    attach_results.put(True)
                else:
                    event("attach_retry", frame=frame)
                    attach_results.put(False)
                continue
            method = pending.pop(frame.get("id"), "")
            if method == "fetch_queue" and (control / "gate-fetch").exists():
                event("fetch_result_waiting", frame=frame)
                wait_file(control / "release-fetch")
                event("fetch_result_released")
            if frame.get("method") == "notifications/claude/channel":
                event("channel_notify", frame=frame)
            sys.stdout.buffer.write(raw)
            sys.stdout.buffer.flush()
    finally:
        child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()


if __name__ == "__main__":
    main()
