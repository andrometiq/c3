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

from collect import attempt_events, build_verdict_inputs, collect, export_fixtures, notice_evidence
from capture_export import save_sanitized_failure_evidence
from host import Host, HostSetupError, read_jsonl, wait_for
from matrix import cells, selection, selection_summary
from hostdriver import Adapter, BarrierPoint, Broker, ControlJournal, DriverError, RunProfile, Scratch, Timeouts, Workload, resolve_contract
from hostdrivers import registry


def queue_rows(root):
    directory = root / "broker/queue"
    return [r for path in directory.glob("*.jsonl") for r in read_jsonl(path) if r.get("TestInjected")]


def broker_text(root):
    path = root / "broker/broker.log"
    return path.read_text(errors="replace") if path.exists() else ""


def offered_before_ready(host, root, pid, cursor):
    """Name each observation that the gated session was offered delivery early.

    Scoped to the gated proxy: a predecessor proxy's auto-attach recovery notice
    is that proxy's event, not this one's premature offer. The broker log is read
    from the cursor taken when the gate was armed, so a prior session lifetime
    cannot be re-attributed to this one, and a notification the gated proxy
    forwards after its own release is delivery working, not delivery early.
    """
    # Count complete lines, not characters: a line still being written when the
    # gate armed is unattributable, and including it fails closed.
    log = "\n".join(broker_text(root).splitlines()[cursor:])
    observations = []
    if attempt_events(log):
        observations.append("broker reserved a delivery attempt")
    if "delivered chan=test-inject " in log:
        observations.append("broker delivered to the channel")
    released = [e["time"] for e in host.events("initialized_released") if e.get("pid") == pid]
    before_ready = released[0] if released else float("inf")
    if any(e.get("pid") == pid and e["time"] < before_ready for e in host.events("channel_notify")):
        observations.append("gated proxy forwarded a channel notification")
    return observations


def false_held(log, count):
    return notice_evidence(log, count)["false_held"]


def default_driver(args):
    return registry().create('claude', backend_factory=Host, reconnect_keys=getattr(args, 'reconnect_keys', None))


def default_profile(host, args, cell, scratch, version):
    description = host.describe(version, 'matrix')
    case = next(case for case in description['capabilities']['cases'] if case['id'] == cell.name)
    scenario, _, _ = build_verdict_inputs(cell, {'run_id': scratch.run_id, 'route_id': 'test-inject/42'},
                                         collect_only=args.collect_only)
    scenario["observation_duration_ms"] = getattr(args, "observe_seconds", 80) * 1000
    setup = args.setup_timeout
    return RunProfile(description, case, scenario, resolve_contract(description, case), args.claude.resolve(), version,
                      'matrix', None, workload=Workload('sleep', getattr(args, 'sleep_seconds', 35) * 1000, 'workload'),
                      timeouts=Timeouts(checkpoint_seconds=setup, prepare_seconds=setup * 2, state_seconds=setup + 8,
                                        barrier_seconds=setup, reconnect_seconds=setup + 70,
                                        observe_seconds=getattr(args, 'observe_seconds', 80)))


def run_cell(args, cell, binaries, repo, version, *, host_driver=None, profile=None):
    root = Path(tempfile.mkdtemp(prefix="c3mx-"))
    root.chmod(0o700)
    output = args.output / version / cell.name
    if output.exists():
        raise FileExistsError(f"refusing to overwrite {output}; choose --output or remove the old cell explicitly")
    host = None
    result = None
    broker_process = None
    setup_complete = False
    evidence = {"setup_errors": [], "injected": False, "rows_final": None}
    try:
        with (root / "broker-stderr.log").open("wb") as err:
            broker_process = subprocess.Popen([str(binaries / "c3-broker"), "test-serve", "--allow-test-inject", "--state", str(root / "broker")], stdout=err, stderr=err)
        wait_for(lambda: (root / "broker/c3.sock").is_socket(), 10, "scratch broker socket did not appear")
        scratch = Scratch(root, root.name, root.name, root / 'cwd', root, ControlJournal(root / 'control'))
        host = host_driver if host_driver is not None else default_driver(args)
        profile = profile if profile is not None else default_profile(host, args, cell, scratch, version)
        host.prepare(scratch, Adapter((binaries / 'c3-claude-adapter').resolve(), 'scratch-build'),
                     Broker((binaries / 'c3-broker').resolve(), root / 'broker/c3.sock', root / 'broker',
                            scratch.run_id, profile.scenario['route_id']), profile)
        boundaries = profile.description['operations']['barriers']
        def barrier(kind, name, action='sample', release=None):
            return host.barrier(BarrierPoint(name, kind, action, boundaries[kind][name],
                                            release_id=release.release_id if release else None))
        def checkpoint(name):
            return barrier('checkpoint', name)
        def warmup():
            host.enter_state('idle', Workload('warmup'))
        def early_offers():
            log = "\n".join(broker_text(root).splitlines()[gate_cursor:])
            observations = []
            if attempt_events(log):
                observations.append('broker reserved a delivery attempt')
            if 'delivered chan=test-inject ' in log:
                observations.append('broker delivered to the channel')
            sample = scratch.journal.legacy_evidence(checkpoint('before_ready'))
            return observations + sample.get('attempt_before_ready_observations', [])
        resume_handle = None
        if cell.session == "resumed":
            host.launch()
            checkpoint('attachment')
            warmup()
            resume_handle = host.stop(True)
        gate_cursor = 0
        gate = None
        if cell.state == "startup":
            gate_cursor = broker_text(root).count("\n")
            gate = barrier('readiness', 'ready', 'arm')
        session = host.launch(resume_handle)
        if cell.state == "startup":
            gate = barrier('readiness', 'ready', 'wait', gate)
        else:
            checkpoint('attachment')
            if cell.session == "resumed":
                warmup()
        if cell.state == "reconnect":
            gate_cursor = broker_text(root).count("\n")
            gate = barrier('readiness', 'ready', 'arm')
            host.reconnect(session)
        checkpoint('session')
        if cell.state in ("foreground", "background"):
            state = host.enter_state(cell.state, profile.workload)
            evidence.update(scratch.journal.legacy_evidence(state))
        if cell.transport == "fetch":
            fetch_gate = barrier('fetch_result', 'fetch', 'arm')
        if gate:
            evidence["attempt_before_ready_observations"] = early_offers()
            evidence["attempt_before_ready"] = bool(evidence["attempt_before_ready_observations"])
        checkpoint('injection')
        setup_complete = True
        command = [str(binaries / "c3-broker"), "inject", "--socket", str(root / "broker/c3.sock"),
                   "--topic", "42", "--text", "MATRIX_SAMPLE: generic delivery sample.", "--count", str(cell.count)]
        if cell.kind == "voice":
            command += ["--voice", "--voice-delay-ms", "2000"]
        elif cell.kind == "photo":
            command += ["--photo"]
        response = subprocess.run(command, check=True, capture_output=True, text=True, timeout=10)
        admitted = json.loads(response.stdout)
        from capture_store import record_injection
        record_injection(root, admitted, cell.kind)
        evidence.update(injected=admitted["accepted"], message_ids=admitted["message_ids"], injected_at=time.time())
        if gate:
            # Allow ordinary debounce/persistence to finish while initialized is
            # demonstrably withheld. No sleep substitutes for a readiness test.
            wait_for(lambda: len(queue_rows(root)) == cell.count, 5, "startup input was not persisted")
            time.sleep(0.3)  # let the persisted batch finish scheduling while the real gate remains closed
            seen = evidence["attempt_before_ready_observations"]
            seen += [o for o in early_offers() if o not in seen]
            evidence["attempt_before_ready"] = bool(seen)
            evidence["rows_before_ready"] = len(queue_rows(root))
            barrier('readiness', 'ready', 'release', gate)
            checkpoint('attachment')
        if cell.session == "fresh":
            warmup()
        if cell.transport == "fetch":
            wait_for(lambda: len(queue_rows(root)) == cell.count and all(not r.get("_c3_voice_pending") for r in queue_rows(root)), 10, "fetch rows/revisions not ready")
            if cell.state == "foreground":
                barrier('workload_finished', 'workload', 'wait')
            host.send_prompt("Call c3 fetch_queue exactly once with limit='all' and ack=true. Then reply MATRIX_FETCHED. Do not repeat the fetch.")
            held = barrier('fetch_result', 'fetch', 'wait', fetch_gate)
            evidence["rows_while_fetch_result_held"] = len(queue_rows(root))
            evidence.update(scratch.journal.legacy_evidence(held))
            barrier('fetch_result', 'fetch', 'release', held)
        # Wait past both live windows: missing primary receipt must expose an
        # inbox fallback and duplicate turn. Keep checking after first retirement.
        deadline = time.monotonic() + profile.timeouts.observe_seconds
        while time.monotonic() < deadline:
            host.observe()
            time.sleep(0.2)
        checkpoint("final")
        evidence["rows_final"] = len(queue_rows(root))
        evidence["false_held"] = false_held(broker_text(root), cell.count)
    except Exception as exc:
        is_setup = not setup_complete or isinstance(exc, HostSetupError) or isinstance(exc.__cause__, HostSetupError)
        field = "setup_errors" if is_setup else "run_errors"
        evidence.setdefault(field, []).append(str(exc) if isinstance(exc, (HostSetupError, DriverError)) else f"{type(exc).__name__}: {exc}")
    finally:
        # Collect before stopping: shutdown/holder death changes queue state.
        try:
            from capture_store import DriverCapture
            source = DriverCapture(host, profile) if profile is not None else host
            result = collect(cell, source, root, output, evidence, args.collect_only)
            if args.fixtures and ((output / 'legacy-fixture-refused.txt').exists() or (output / "records.jsonl").stat().st_size):
                export_fixtures(output, repo, version, cell)
        finally:
            cleanup_errors = []
            def cleanup(step, action):
                try:
                    action()
                except Exception as error:
                    cleanup_errors.append(error)
                    # Persist only a fixed step and exception type, never raw
                    # teardown text which has not passed collection redaction.
                    reason = f'cleanup {step} failed: {type(error).__name__}'
                    if isinstance(result, dict):
                        result['status'] = 'FAIL'
                        result.setdefault('reasons', []).append(reason)
                        try:
                            summary = output / 'summary.json'
                            saved = json.loads(summary.read_text())
                            saved['status'] = 'FAIL'
                            saved.setdefault('reasons', []).append(reason)
                            pending = output / 'summary-cleanup.tmp'
                            pending.write_text(json.dumps(saved, indent=2) + '\n')
                            pending.replace(summary)
                        except Exception as persistence_error:
                            cleanup_errors.append(persistence_error)
            if host:
                cleanup('host stop', lambda: host.stop(False))
            if broker_process:
                cleanup('broker terminate', broker_process.terminate)
                def wait_broker():
                    try:
                        broker_process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        broker_process.kill()
                        broker_process.wait()
                cleanup('broker wait', wait_broker)
            if args.keep_scratch:
                print(f"Private scratch retained: {root}", flush=True)
            else:
                failed = isinstance(result, dict) and result.get('status') == 'FAIL'
                if failed:
                    cleanup('failure evidence export', lambda: save_failure_evidence(root, output))
                cleanup('scratch removal', lambda: shutil.rmtree(root))
                if cleanup_errors and not failed:
                    cleanup('failure evidence export', lambda: save_failure_evidence(root, output))
            if cleanup_errors:
                # An active collection error retains precedence. Otherwise the
                # cleanup exception propagates and makes the process fail loud.
                import sys
                if sys.exc_info()[0] is None:
                    raise cleanup_errors[0]
    return result


def save_failure_evidence(root, destination):
    """Retain controls already sanitized during collection, before context seal."""
    save_sanitized_failure_evidence(destination)


def write_report(output, version, results, description=None):
    output.mkdir(parents=True, exist_ok=True)
    label = description['driver_id'].capitalize() if description else 'Claude'
    lines = [f"# Live matrix — {label} {version}", "", "| Cell | Result | Reason |", "| --- | --- | --- |"]
    for cell in cells(description):
        result = results.get(cell.name)
        if cell.infeasible:
            status, reason = "N/A", cell.infeasible
        elif result:
            status, reason = result["status"], "; ".join(result.get("reasons", [])) or "all receipt/retirement checks passed"
            if result.get("not_evaluated"):
                reason += "; NOT EVALUATED: " + ", ".join(result["not_evaluated"])
            if status == "COLLECTED":
                reason = "shapes collected; no success assertion; " + reason
        else:
            status, reason = "NOT RUN", "not selected or not yet completed"
            if cell.feasibility == 'unknown':
                reason += '; unknown: ' + (cell.capability_case or {}).get('reason', 'combination recipe is not declared')
        lines.append(f"| {cell.name} | {status} | {reason.replace('|', '/')} |")
    (output / "REPORT.md").write_text("\n".join(lines) + "\n")
    (output / version).mkdir(exist_ok=True)
    shutil.copyfile(output / "REPORT.md", output / version / "REPORT.md")


def argument_parser():
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
    parser.add_argument("--observe-seconds", type=int, default=80)
    return parser


def main():
    parser = argument_parser()
    args = parser.parse_args()
    repo = Path(__file__).resolve().parents[2]
    lane_temp = Path.home() / ".cache/c3-lanes"
    os.environ["TMPDIR"] = str(lane_temp)
    os.environ["GOCACHE"] = str(lane_temp / "go-cache")
    lane_temp.mkdir(parents=True, exist_ok=True)
    (lane_temp / "go-cache").mkdir(exist_ok=True)
    tempfile.tempdir = str(lane_temp)
    description = default_driver(args).describe(args.version_label or '2.1.267', 'matrix')
    matched, selected, _ = selection(args.cell, description)
    if args.list:
        print(selection_summary(args.cell, description), flush=True)
        for cell in matched:
            status = "N/A: " + cell.infeasible if cell.infeasible else cell.feasibility.upper()
            if cell.feasibility == 'unknown':
                status += ': ' + (cell.capability_case or {}).get('reason', 'combination recipe is not declared')
            print(cell.name + "\t" + status)
        return
    if args.sleep_seconds <= 15 or args.observe_seconds < max(75, args.sleep_seconds + 5):
        parser.error("sleep must exceed 15 seconds; observation must be at least 75 seconds and exceed sleep by at least 5 seconds")
    if not args.claude:
        versions = [p for p in (Path.home() / ".local/share/claude/versions").glob("*") if re.fullmatch(r"\d+\.\d+\.\d+", p.name) and p.is_file()]
        if not versions:
            parser.error("no installed numeric Claude version; pass --claude and --version-label")
        args.claude = max(versions, key=lambda p: tuple(map(int, p.name.split("."))))
    args.claude = args.claude.resolve()
    version = args.version_label or args.claude.name
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:[-.][A-Za-z0-9]+)*", version):
        parser.error("supply a safe numeric --version-label")
    description = default_driver(args).describe(version, 'matrix')
    matched, selected, _ = selection(args.cell, description)
    print(selection_summary(args.cell, description), flush=True)
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
            subprocess.run(["go", "build", "-p", "1", "-o", str(binaries / name), "./cmd/" + name], cwd=repo, check=True)
        write_report(args.output, version, results, description)
        for index, cell in enumerate(selected, 1):
            print(f"[{index}/{len(selected)}] {cell.name}", flush=True)
            result = run_cell(args, cell, binaries, repo, version)
            results[cell.name] = result
            write_report(args.output, version, results, description)
            print(result["status"] + ": " + "; ".join(result.get("reasons", [])), flush=True)
    raise SystemExit(1 if any(results[c.name]["status"] == "FAIL" for c in selected) else 0)


if __name__ == "__main__":
    main()
