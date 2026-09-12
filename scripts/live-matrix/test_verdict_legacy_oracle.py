"""Frozen pre-extraction oracle. Test-only; never import from production.

Source: scripts/live-matrix/collect.py, before Phase 1 extraction.
Source SHA-256: 796b6b47ba32dfbc84481ac510bb8001a874993433a4d033d748ae238ca28dde
Functions copied verbatim; report wrapper preserves lines 337–342.
Use only to generate synthetic golden data, never expected results at test time.
"""

def route_line_limit(cell):
    # Automatic route diagnostics never belong in chat.
    return 0


def verdict(cell, evidence):
    failures = list(evidence.get("setup_errors", []))
    if not evidence.get("injected"):
        failures.append("injection was not completed")
    if evidence.get("rows_final") != 0:
        failures.append("durable rows remain")
    if evidence.get("attempt_before_ready"):
        # Name the observation: a system notice is not a reserved attempt.
        observed = evidence.get("attempt_before_ready_observations") or ["observation not recorded"]
        failures.append("delivery offered before host initialized (" + "; ".join(observed) + ")")
    if evidence.get("no_false_held") is not True or evidence.get("false_held"):
        failures.append("no false Held assertion failed or missing")
    count = evidence.get("route_line_count")
    if not isinstance(count, int) or not 0 <= count <= route_line_limit(cell):
        failures.append("route line count assertion failed or missing")
    if cell.transport == "fetch":
        if any(e.get("phase") == "reserved" and e.get("transport") != "fetch" for e in evidence.get("attempts", [])):
            failures.append("live offer in fetch-only cell")
        if evidence.get("rows_while_fetch_result_held") != cell.count:
            failures.append("rows retired before host tool-result receipt (baseline consume-on-fetch)")
        if not evidence.get("fetch_tool_result"):
            failures.append("no successful fetch tool-result record")
        reserved = [e for e in evidence.get("attempts", []) if e.get("phase") == "reserved" and e.get("transport") == "fetch"]
        confirmed = [e for e in evidence.get("attempts", []) if e.get("phase") == "confirmed" and e.get("transport") == "fetch"]
        if not reserved or {e["token"] for e in reserved} != {e["token"] for e in confirmed} or any(int(e.get("elapsed_ms", 60000)) >= 60000 for e in confirmed):
            failures.append("fetch group confirmation missing or outside 60-second window")
        if sum(int(e.get("retired", 0)) for e in confirmed) != cell.count:
            failures.append("fetch group retirement count differs from injected sources")
        if not evidence.get("fetch_trailer_complete"):
            failures.append("fetch tool result lacks the complete matching receipt trailer")
        if not evidence.get("fetch_token"):
            failures.append("fetch result has no broker receipt token")
        if evidence.get("fetch_source_occurrences") != cell.count:
            failures.append("fetch did not return each injected source exactly once")
    else:
        reserved = [e for e in evidence.get("attempts", []) if e.get("phase") == "reserved"]
        confirmed = [e for e in evidence.get("attempts", []) if e.get("phase") == "confirmed"]
        if not reserved:
            failures.append("no negotiated attempt observed")
        if any(e.get("transport") != cell.transport for e in reserved):
            failures.append("fallback/wrong transport attempted")
        if sum(int(e.get("members", 0)) for e in reserved) != cell.count:
            failures.append("source attempted more or less than once")
        if {e["token"] for e in reserved} != {e["token"] for e in confirmed} or any(int(e.get("elapsed_ms", 15000)) >= 15000 for e in confirmed):
            failures.append("receipt missing or outside 15-second window")
        if sum(int(e.get("retired", 0)) for e in confirmed) != cell.count:
            failures.append("retirement count differs from injected source count")
        if list(evidence.get("received", {}).values()) != [1] * cell.count:
            failures.append("host did not receive each source exactly once")
    return failures


def delivery_assertions(cell):
    common = ["injection completed", "durable rows retired", "no attempt before initialization",
              "no false Held", "route line count"]
    if cell.transport == "fetch":
        return common + ["no live offer in fetch-only cell", "rows retained until fetch receipt",
                         "successful fetch tool result", "fetch confirmation within 60 seconds",
                         "fetch retirement count", "complete fetch receipt trailer",
                         "broker fetch receipt token", "each source fetched exactly once"]
    return common + ["negotiated attempt observed", "no fallback or wrong transport",
                     "each source attempted once", "receipt within 15 seconds",
                     "retirement count", "each source received exactly once"]

def old_report(cell, evidence, collect_only=False):
    """Reproduce the historical collector lifecycle without artifact I/O."""
    failures = (evidence.get("setup_errors", [])[:1]
                if evidence.get("setup_errors")
                else evidence.get("run_errors", []) + verdict(cell, evidence))
    status = ("COLLECTED"
              if collect_only and not evidence.get("setup_errors") and not evidence.get("run_errors")
              else ("FAIL" if failures else "PASS"))
    not_evaluated = delivery_assertions(cell) if evidence.get("setup_errors") else []
    return {"status": status, "reasons": failures, "not_evaluated": not_evaluated}
