#!/usr/bin/env python3
"""Record SessionStart's real transcript path, then run C3's actual handoff hook."""
import json
from pathlib import Path
import subprocess
import sys
import time

raw = sys.stdin.buffer.read()
entry = json.loads(raw)
with (Path(sys.argv[2]) / "sessions.jsonl").open("a") as out:
    out.write(json.dumps({"time": time.time(), **entry}) + "\n")
subprocess.run([sys.argv[1], "session-hook"], input=raw, check=True)
