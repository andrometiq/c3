#!/usr/bin/env bash
set -euo pipefail
# This driver is for the maintainer, outside the coding sandbox.
script_dir=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
export TMPDIR="$HOME/.cache/c3-lanes"
export GOCACHE="$TMPDIR/go-cache"
mkdir -p "$TMPDIR" "$GOCACHE"
exec python3 "$script_dir/driver.py" "$@"
