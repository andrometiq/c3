"""Evidence selection, redaction, receipt-shape classification and report verdicts."""
from collections import Counter
import json
from pathlib import Path
import re
import shutil

from host import read_jsonl

TOKEN = re.compile(r'c3_delivery_id=["\']([^"\']+)["\']')
ATTEMPT = re.compile(r'c3_attempt=["\']([^"\']+)["\']')
PEER_PREFIX = "Another Claude session sent a message:\n"


def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for item in value.values():
            yield from strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from strings(item)


def intake_text(record):
    if record.get("type") == "user" and record.get("message", {}).get("role") == "user":
        content = record["message"].get("content", "")
        if isinstance(content, str):
            return content
        return "\n".join(b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text") if isinstance(content, list) else ""
    if record.get("type") == "attachment" and record.get("attachment", {}).get("type") == "queued_command":
        prompt = record["attachment"].get("prompt", "")
        return prompt if isinstance(prompt, str) else ""
    if record.get("type") == "queue-operation":
        content = record.get("content", "")
        return content if isinstance(content, str) else ""
    return ""


def classify(record):
    """Only observed, checked-in shapes become positives; new shapes are TODO."""
    text = intake_text(record)
    token, attempt = TOKEN.search(text), ATTEMPT.search(text)
    transport = "inbox" if attempt and attempt[1].startswith(("inbox:", "cross-session:")) else "channel"
    result = dict(transport=transport, token=token[1] if token else "TOKEN", attempt=attempt[1] if attempt else "channel:1", accept=None,
                  reason="TODO: review captured host shape before adding positive coverage")
    if record.get("type") == "queue-operation" and record.get("operation") == "remove":
        result.update(accept=False, reason="queue removal is not receipt evidence")
    elif text and not token:
        result.update(accept=False, reason="captured record predates delivery token metadata")
    elif token and attempt:
        if transport == "inbox":
            if record.get("type") == "user":
                result.update(accept=bool(record.get("isMeta") and record.get("origin", {}).get("kind") == "peer"
                                          and record.get("origin", {}).get("from") == "c3" and text.startswith(PEER_PREFIX)),
                              reason="verified 2.1.263 peer user shape and prefix")
            elif record.get("type") in ("queue-operation", "attachment"):
                source = re.match(r'<channel\s[^>]*\bsource=["\']plugin:c3:c3["\']', text)
                if record.get("type") == "queue-operation":
                    provenance = record.get("operation") == "enqueue"
                else:
                    attachment = record.get("attachment", {})
                    provenance = (attachment.get("isMeta") and attachment.get("origin", {}).get("kind") == "peer"
                                  and attachment.get("origin", {}).get("from") == "c3")
                result.update(accept=bool(source and provenance), reason="verified 2.1.266 bare peer intake envelope")
        elif text.lstrip().startswith("<channel"):
            known = record.get("type") == "user" or (record.get("type") == "queue-operation" and record.get("operation") == "enqueue") or (
                record.get("type") == "attachment" and record.get("attachment", {}).get("origin", {}).get("kind") == "channel")
            if known:
                result.update(accept=True, reason="verified channel intake envelope")
    return result


def result_text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list) and all(isinstance(p, dict) and p.get("type") == "text" and isinstance(p.get("text"), str) for p in content):
        return "\n".join(p["text"] for p in content)
    return ""


def fetch_trailer(text):
    """The phase-4 grammar: final delimiter, complete unique member set."""
    marker = "[C3_FETCH_RECEIPT_V1]\n"
    start = text.rfind(marker)
    if start < 0 or (start and text[start - 1] != "\n"):
        return None
    lines = text[start:].split("\n")
    if len(lines) < 4 or lines[-1] != "[/C3_FETCH_RECEIPT_V1]":
        return None
    group = re.fullmatch(r"group ([A-Za-z0-9_-]+)", lines[1])
    if not group:
        return None
    members = []
    for line in lines[2:-1]:
        member = re.fullmatch(r"member ([A-Za-z0-9_-]+) ([0-9a-f]{64})", line)
        if not member or any(m["record_id"] == member[1] for m in members):
            return None
        members.append(dict(record_id=member[1], revision=member[2]))
    return dict(token=group[1], members=members)


def classify_fetch(record, expected, call_ids):
    """Expected identities come from the held MCP response, not the transcript."""
    result = dict(transport="fetch", token=expected["token"] if expected else "TOKEN",
                  members=expected["members"] if expected else [], tool_use_id="ID", accept=False,
                  reason="not a matching successful complete fetch tool result")
    if not expected:
        result.update(accept=None, reason="TODO: capture a complete broker fetch trailer")
        return result
    content = record.get("message", {}).get("content", [])
    if record.get("type") != "user" or not isinstance(content, list):
        return result
    for block in content:
        if not isinstance(block, dict) or block.get("type") != "tool_result" or block.get("tool_use_id") not in call_ids:
            continue
        result["tool_use_id"] = block["tool_use_id"]
        if block.get("is_error", False) is not False:
            continue
        trailer = fetch_trailer(result_text(block.get("content")))
        if trailer and trailer["token"] == expected["token"] and sorted(trailer["members"], key=lambda m: m["record_id"]) == sorted(expected["members"], key=lambda m: m["record_id"]):
            result.update(accept=True, reason="matching tool result with complete phase-4 receipt trailer")
            return result
    return result


def sanitize(value, tokens=(), key="", receipt_ids=()):
    """Keep envelope keys and types; replace identifiers and local paths.

    Numeric identity fields use 1, the numeric stand-in for ID. Host prose from
    these scratch-only transcripts is retained so peer prefix checks stay real.
    """
    identity = key.lower().replace("_", "")
    if isinstance(value, dict):
        return {k: sanitize(v, tokens, k, receipt_ids) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize(v, tokens, key, receipt_ids) for v in value]
    if identity == "recordid" and value in receipt_ids:
        return "ROW" + str(receipt_ids.index(value) + 1)
    if identity in {"uuid", "parentuuid", "promptid", "sessionid", "recordid", "tooluseid", "verifiedpeerpid", "verifiedpeerprocstart", "pid", "chatid", "topicid", "messageid", "userid", "id", "requestid"}:
        return 1 if isinstance(value, (int, float)) else "ID"
    if identity in {"user", "username"}:
        return "USER"
    if isinstance(value, str):
        for i, record_id in enumerate(receipt_ids):
            value = value.replace(record_id, "ROW" + str(i + 1))
        for token in sorted(set(tokens), key=len, reverse=True):
            value = value.replace(token, "TOKEN")
        value = TOKEN.sub('c3_delivery_id="TOKEN"', value)
        value = re.sub(r'\b((?:chat|message|user|attachment_file|reply_to_message|message_thread)_id|merged_message_ids|reply_to_user|user|ts)=["\'][^"\']*["\']',
                       lambda m: m[1] + '="' + ("USER" if m[1] in ("user", "reply_to_user") else "ID") + '"', value)
        value = re.sub(r'\b[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\b', "ID", value)
        value = re.sub(r'(?:/home/|/Users/|/tmp/|/private/|/run/)[^\s"<>\']+', "/work/ID", value)
        value = re.sub(r'\b(token|message_id|topic|pid|conn|record_id|file_id)=(?!["\'])[^\s]+',
                       lambda m: m[1] + "=" + ("TOKEN" if m[1] == "token" else "ID"), value)
        if identity in {"cwd", "transcriptpath", "path"}:
            return "/work/ID"
        if identity in {"token", "leasetoken", "deliverytoken"}:
            return "TOKEN"
    return value


def count_receives(records, tokens, message_ids):
    """Enqueue/remove are receipts/housekeeping, not extra user deliveries."""
    counts = Counter({str(i): 0 for i in message_ids})
    seen = set()
    for index, record in enumerate(records):
        if record.get("type") not in ("user", "attachment"):
            continue
        text = intake_text(record)
        if not any(m[1] in tokens for m in TOKEN.finditer(text)):
            continue
        identity = record.get("uuid") or f"record-{index}"
        if identity in seen:
            continue
        seen.add(identity)
        ids = re.search(r'merged_message_ids=["\']([^"\']+)', text) or re.search(r'message_id=["\']([^"\']+)', text)
        if ids:
            for message_id in set(ids[1].split(",")):
                if message_id in counts:
                    counts[message_id] += 1
    return dict(counts)


def attempt_events(log):
    events = []
    for line in log.splitlines():
        if "TEST ATTEMPT " in line:
            events.append(dict(re.findall(r'(\w+)=([^\s]+)', line)))
    return events


def notice_evidence(log, count):
    active, observed = {}, {}
    retired = 0
    false_held = False
    route_lines = 0
    injected = "TEST INJECT accepted" not in log
    for line in log.splitlines():
        if "TEST INJECT accepted" in line:
            injected = True
        for event in attempt_events(line):
            token = event["token"]
            if event["phase"] == "reserved":
                active[token] = int(event["members"])
            else:
                active.pop(token, None)
                retired += int(event.get("retired", 0))
                if int(event.get("retired", 0)):
                    observed.pop(token, None)
        if "attempt confirmed token=" in line:
            token = re.search(r"token=([^ ]+)", line)
            if token:
                observed[token[1]] = active.get(token[1], 0)
        if "TEST SINK " in line and injected:
            if 'text="Live route:' in line or 'text="📨 Held —' in line:
                route_lines += line.count("Live route:")
            held = re.search(r'(\d+) messages? queued', line) if "Held" in line else None
            if held and int(held[1]) > max(0, count - retired - sum(active.values()) - sum(n for token, n in observed.items() if token not in active)):
                false_held = True
    return dict(no_false_held=not false_held, false_held=false_held, route_line_count=route_lines)


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
        failures.append("attempt reserved before host initialized")
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


def collect(cell, host, root, output, evidence, collect_only=False):
    output.mkdir(parents=True, exist_ok=True)
    broker_log = (root / "broker/broker.log").read_text(errors="replace") if (root / "broker/broker.log").exists() else ""
    adapter_log = (root / "control/adapter.log").read_text(errors="replace") if (root / "control/adapter.log").exists() else ""
    attempts = attempt_events(broker_log)
    tokens = {e["token"] for e in attempts}
    try:
        records = host.records() if host else []
    except Exception as exc:
        records = []
        evidence.setdefault("setup_errors", []).append(f"collection: {type(exc).__name__}: {exc}")
    tokens.update(match[1] for r in records for text in strings(r) for match in TOKEN.finditer(text))
    selected = [r for r in records if any(token in text for text in strings(r) for token in tokens)
                or (cell.transport == "fetch" and ("MATRIX_SAMPLE" in json.dumps(r) or "__fetch_queue" in json.dumps(r)))]
    fetch_calls = {b.get("id") for r in records for b in (r.get("message", {}).get("content", []) if isinstance(r.get("message", {}).get("content"), list) else [])
                   if isinstance(b, dict) and b.get("type") == "tool_use" and b.get("name", "").endswith("__fetch_queue")}
    expected = fetch_trailer(result_text((evidence.get("fetch_result") or {}).get("content")))
    receipt_ids = [m["record_id"] for m in expected["members"]] if expected else []
    if expected:
        tokens.add(expected["token"])
    expectations = []
    for record in selected:
        expectation = classify_fetch(record, expected, fetch_calls) if cell.transport == "fetch" else classify(record)
        expectations.append(sanitize(expectation, tokens, receipt_ids=receipt_ids))
    (output / "records.jsonl").write_text("".join(json.dumps(sanitize(r, tokens, receipt_ids=receipt_ids), ensure_ascii=False) + "\n" for r in selected))
    (output / "records.expect.json").write_text(json.dumps(expectations, indent=2) + "\n")
    for name, log in (("broker.log", broker_log), ("adapter.log", adapter_log)):
        (output / name).write_text(sanitize(log, tokens))
    if host:
        (output / "events.jsonl").write_text("".join(json.dumps(sanitize(r, tokens)) + "\n" for r in host.events()))
    evidence.update(notice_evidence(broker_log, cell.count))
    evidence["route_line_limit"] = route_line_limit(cell)
    evidence["attempts"] = attempts
    evidence["received"] = count_receives(records, tokens, evidence.get("message_ids", []))
    fetch_results = [b for r in records if r.get("type") == "user"
                     for b in (r.get("message", {}).get("content", []) if isinstance(r.get("message", {}).get("content"), list) else [])
                     if isinstance(b, dict) and b.get("type") == "tool_result" and not b.get("is_error")
                     and b.get("tool_use_id") in fetch_calls and "MATRIX_SAMPLE" in json.dumps(b.get("content"))]
    evidence["fetch_tool_result"] = bool(fetch_results)
    evidence["fetch_source_occurrences"] = sum(json.dumps(b.get("content")).count("MATRIX_SAMPLE") for b in fetch_results)
    evidence["fetch_token"] = bool(expected)
    evidence["fetch_trailer_complete"] = any(classify_fetch(r, expected, fetch_calls)["accept"] for r in records) if expected else False
    setup_errors = evidence.get("setup_errors", [])
    failures = setup_errors[:1] if setup_errors else evidence.get("run_errors", []) + verdict(cell, evidence)
    status = "COLLECTED" if collect_only and not setup_errors and not evidence.get("run_errors") else ("FAIL" if failures else "PASS")
    result = {"cell": cell.name, "status": status, "reasons": failures, "evidence": sanitize(evidence, tokens, receipt_ids=receipt_ids),
              "todo_records": sum(e["accept"] is None for e in expectations),
              "not_evaluated": delivery_assertions(cell) if setup_errors else []}
    # Counter keys must not leak raw message ids; preserve per-source order.
    result["evidence"]["received"] = {f"ID{i+1}": n for i, n in enumerate(evidence["received"].values())}
    result["evidence"]["message_ids"] = ["ID"] * len(evidence.get("message_ids", []))
    (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def export_fixtures(output, repo, version, cell):
    target = repo / "cmd/c3-claude-adapter/testdata" / ("claude-" + version)
    target.mkdir(exist_ok=True)
    for source, suffix in (("records.jsonl", ".jsonl"), ("records.expect.json", ".expect.json")):
        dest = target / (cell.name + suffix)
        if dest.exists():
            raise FileExistsError(f"refusing to overwrite fixture {dest.name}")
        shutil.copyfile(output / source, dest)
