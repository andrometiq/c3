#!/usr/bin/env python3
"""Maintainer-only real-host matrix. Importing modules never launches a host."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time

from collect import attempt_events, collect, export_fixtures
from host import Host, read_jsonl, wait_for
from matrix import cells, selection, selection_summary


def queue_rows(root):
    directory = root / "broker/queue"
    return [r for path in directory.glob("*.jsonl") for r in read_jsonl(path) if r.get("TestInjected")]


def broker_text(root):
    path = root / "broker/broker.log"
    return path.read_text(errors="replace") if path.exists() else ""


def offered_before_ready(host, root, after):
    return (bool(attempt_events(broker_text(root)))
            or "delivered chan=test-inject " in broker_text(root)
            or bool(host.events("channel_notify", after)))


def false_held(log, count):
    active = {}
    retired = 0
    for line in log.splitlines():
        events = attempt_events(line)
        if events:
            event = events[0]
            if event["phase"] == "reserved":
                active[event["token"]] = int(event["members"])
            else:
                active.pop(event["token"], None)
                retired += int(event.get("retired", 0))
        if "TEST SINK " in line and "Held" in line:
            held = re.search(r'(\d+) messages? queued', line)
            if held and int(held[1]) > max(0, count - retired - sum(active.values())):
                return True
    return False


def run_cell(args, cell, binaries, repo, version):
    root = Path(tempfile.mkdtemp(prefix="c3mx-"))
    root.chmod(0o700)
    output = args.output / version / cell.name
    if output.exists():
        raise FileExistsError(f"refusing to overwrite {output}; choose --output or remove the old cell explicitly")
    host = None
    broker_process = None
    evidence = {"setup_errors": [], "injected": False, "rows_final": None}
    try:
        with (root / "broker-stderr.log").open("wb") as err:
            broker_process = subprocess.Popen([str(binaries / "c3-broker"), "test-serve", "--allow-test-inject", "--state", str(root / "broker")], stdout=err, stderr=err)
        wait_for(lambda: (root / "broker/c3.sock").is_socket(), 10, "scratch broker socket did not appear")
        host = Host(root, args.claude, binaries / "c3-broker", binaries / "c3-claude-adapter", Path(__file__).parent.resolve(), cell, args.setup_timeout)
        if cell.session == "resumed":
            host.launch()
            host.wait_event("attached")
            host.ready_turn()
            host.stop_session()
        final_launch = time.time()
        if cell.state == "startup":
            (host.control / "gate-initialized").touch()
        host.launch(continued=cell.session == "resumed")
        if cell.state == "startup":
            gate = host.wait_event("initialized_waiting", final_launch)
        else:
            host.wait_event("attached", final_launch)
            gate = None
            if cell.session == "resumed":
                host.ready_turn()
        if cell.state == "reconnect":
            (host.control / "gate-initialized").touch()
            gate = host.reconnect(args.reconnect_keys)
        # The check is at arrival, not launch: transcript-free means precisely
        # that. The host may create it later when the warmup prompt is submitted.
        transcript = host.transcript()
        if cell.session == "fresh" and transcript and transcript.exists() and transcript.stat().st_size:
            raise RuntimeError("fresh cell already has a transcript before injection")
        if cell.transport == "inbox":
            started = host.events("proxy_started", final_launch)
            if not started or not started[-1].get("inbox_env"):
                raise RuntimeError("host did not supply an owning-session inbox endpoint")
        if cell.state in ("foreground", "background"):
            evidence["tool_state"] = host.tool_state(cell.state == "background", args.sleep_seconds)
        if cell.transport == "fetch":
            (host.control / "gate-fetch").touch()
        if gate:
            evidence["attempt_before_ready"] = offered_before_ready(host, root, final_launch)
        command = [str(binaries / "c3-broker"), "inject", "--socket", str(root / "broker/c3.sock"),
                   "--topic", "42", "--text", "MATRIX_SAMPLE: generic delivery sample.", "--count", str(cell.count)]
        if cell.kind == "voice":
            command += ["--voice", "--voice-delay-ms", "2000"]
        elif cell.kind == "photo":
            command += ["--photo"]
        response = subprocess.run(command, check=True, capture_output=True, text=True, timeout=10)
        admitted = json.loads(response.stdout)
        evidence.update(injected=admitted["accepted"], message_ids=admitted["message_ids"], injected_at=time.time())
        if gate:
            # Allow ordinary debounce/persistence to finish while initialized is
            # demonstrably withheld. No sleep substitutes for a readiness test.
            wait_for(lambda: len(queue_rows(root)) == cell.count, 5, "startup input was not persisted")
            time.sleep(0.3)  # let the persisted batch finish scheduling while the real gate remains closed
            evidence["attempt_before_ready"] |= offered_before_ready(host, root, final_launch)
            evidence["rows_before_ready"] = len(queue_rows(root))
            (host.control / f"release-initialized-{gate['pid']}").touch()
            host.wait_event("attached", gate["time"])
        if cell.session == "fresh":
            host.ready_turn()
        if cell.transport == "fetch":
            wait_for(lambda: len(queue_rows(root)) == cell.count and all(not r.get("_c3_voice_pending") for r in queue_rows(root)), 10, "fetch rows/revisions not ready")
            if cell.state == "foreground":
                wait_for(lambda: not (host.cwd / "tool-running").exists(), args.sleep_seconds + 5, "foreground tool did not finish")
            host.send("Call c3 fetch_queue exactly once with limit='all' and ack=true. Then reply MATRIX_FETCHED. Do not repeat the fetch.")
            held = host.wait_event("fetch_result_waiting", evidence["injected_at"])
            evidence["rows_while_fetch_result_held"] = len(queue_rows(root))
            evidence["fetch_result"] = held["frame"].get("result")
            (host.control / "release-fetch").touch()
        # Wait past both live windows: missing primary receipt must expose an
        # inbox fallback and duplicate turn. Keep checking after first retirement.
        deadline = time.monotonic() + args.observe_seconds
        while time.monotonic() < deadline:
            host.pane()
            time.sleep(0.2)
        evidence["rows_final"] = len(queue_rows(root))
        evidence["false_held"] = false_held(broker_text(root), cell.count)
    except Exception as exc:
        evidence["setup_errors"].append(f"{type(exc).__name__}: {exc}")
    finally:
        # Collect before stopping: shutdown/holder death changes queue state.
        try:
            result = collect(cell, host, root, output, evidence, args.collect_only)
            if args.fixtures and (output / "records.jsonl").stat().st_size:
                export_fixtures(output, repo, version, cell)
        finally:
            if host:
                host.close()
            if broker_process:
                broker_process.terminate()
                try:
                    broker_process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    broker_process.kill()
                    broker_process.wait()
            if args.keep_scratch:
                print(f"Private scratch retained: {root}", flush=True)
            else:
                shutil.rmtree(root)
    return result


def write_report(output, version, results):
    output.mkdir(parents=True, exist_ok=True)
    lines = [f"# Live matrix — Claude {version}", "", "| Cell | Result | Reason |", "| --- | --- | --- |"]
    for cell in cells():
        result = results.get(cell.name)
        if cell.infeasible:
            status, reason = "N/A", cell.infeasible
        elif result:
            status, reason = result["status"], "; ".join(result.get("reasons", [])) or "all receipt/retirement checks passed"
            if status == "COLLECTED":
                reason = "shapes collected; no success assertion; " + reason
        else:
            status, reason = "NOT RUN", "not selected or not yet completed"
        lines.append(f"| {cell.name} | {status} | {reason.replace('|', '/')} |")
    (output / "REPORT.md").write_text("\n".join(lines) + "\n")
    (output / version).mkdir(exist_ok=True)
    shutil.copyfile(output / "REPORT.md", output / version / "REPORT.md")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--claude", type=Path, help="installed binary; default newest numeric version under ~/.local/share/claude/versions")
    parser.add_argument("--version-label", help="required for binaries whose filename is not a numeric version")
    parser.add_argument("--cell", default="*", help="cell-name glob; default all 180 cells (126 feasible)")
    parser.add_argument("--list", action="store_true", help="list all cells without launching or building anything")
    parser.add_argument("--collect-only", action="store_true")
    parser.add_argument("--fixtures", action="store_true", help="export sanitized shapes and expectation sidecars; never overwrite")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--keep-scratch", action="store_true", help="retain private raw logs/transcripts and copied credentials for debugging")
    parser.add_argument("--reconnect-keys", help="comma-separated tmux keys for /mcp menu on this host version")
    parser.add_argument("--setup-timeout", type=int, default=90)
    parser.add_argument("--sleep-seconds", type=int, default=35)
    parser.add_argument("--observe-seconds", type=int, default=45)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    lane_temp = Path.home() / ".cache/c3-lanes"
    os.environ["TMPDIR"] = str(lane_temp)
    os.environ["GOCACHE"] = str(lane_temp / "go-cache")
    lane_temp.mkdir(parents=True, exist_ok=True)
    (lane_temp / "go-cache").mkdir(exist_ok=True)
    tempfile.tempdir = str(lane_temp)
    matched, selected, _ = selection(args.cell)
    print(selection_summary(args.cell), flush=True)
    if args.list:
        for cell in matched:
            print(cell.name + "\t" + ("N/A: " + cell.infeasible if cell.infeasible else "FEASIBLE"))
        return
    if args.sleep_seconds <= 15 or args.observe_seconds < args.sleep_seconds + 5:
        parser.error("sleep must exceed 15 seconds; observation must exceed sleep by at least 5 seconds")
    if not args.claude:
        versions = [p for p in (Path.home() / ".local/share/claude/versions").glob("*") if re.fullmatch(r"\d+\.\d+\.\d+", p.name) and p.is_file()]
        if not versions:
            parser.error("no installed numeric Claude version; pass --claude and --version-label")
        args.claude = max(versions, key=lambda p: tuple(map(int, p.name.split("."))))
    args.claude = args.claude.resolve()
    version = args.version_label or args.claude.name
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:[-.][A-Za-z0-9]+)*", version):
        parser.error("supply a safe numeric --version-label")
    for command in ("tmux", "go", "python3"):
        if not shutil.which(command):
            parser.error(f"missing dependency: {command}")
    args.output = (args.output or repo / "local-notes/live-matrix").resolve()
    if not selected:
        parser.error("cell filter selects no feasible cells")
    results = {}
    for summary in (args.output / version).glob("*/summary.json"):
        result = json.loads(summary.read_text())
        results[result["cell"]] = result
    with tempfile.TemporaryDirectory(prefix="c3mx-build-") as build:
        binaries = Path(build)
        for name in ("c3-broker", "c3-claude-adapter"):
            subprocess.run(["go", "build", "-o", str(binaries / name), "./cmd/" + name], cwd=repo, check=True)
        write_report(args.output, version, results)
        for index, cell in enumerate(selected, 1):
            print(f"[{index}/{len(selected)}] {cell.name}", flush=True)
            result = run_cell(args, cell, binaries, repo, version)
            results[cell.name] = result
            write_report(args.output, version, results)
            print(result["status"] + ": " + "; ".join(result.get("reasons", [])), flush=True)
    raise SystemExit(1 if any(results[c.name]["status"] == "FAIL" for c in selected) else 0)


if __name__ == "__main__":
    main()
