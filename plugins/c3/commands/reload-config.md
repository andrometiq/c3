---
description: Tell the running C3 broker to re-read ~/.config/c3/mappings.json. Non-disruptive — no process restart, no adapter recycle.
---

!CAPS="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/c3-broker.caps"; if ! pgrep -x c3-broker >/dev/null; then echo "no running broker; mappings.json will load when one starts"; elif [ ! -f "$CAPS" ] || ! grep -q "^sighup-reload$" "$CAPS"; then echo "REFUSED: running broker is an old binary without SIGHUP support"; echo "(caps file $CAPS missing or does not list sighup-reload)"; echo "sending SIGHUP would kill it, which would also kill this CLI's MCP adapter."; echo "to upgrade safely: quit Claude Code (Ctrl-D or /exit), then relaunch with 'claude --dangerously-load-development-channels=plugin:c3@c3' (append --resume to pick your session back up)."; else pkill -HUP -x c3-broker && echo "SIGHUP sent — broker will re-read mappings.json"; fi

Display the output. The slash command first probes the broker's capabilities marker (`$XDG_RUNTIME_DIR/c3-broker.caps`, written by the broker at startup). If the file confirms SIGHUP support, signal is sent and the broker swaps the in-memory mappings pointer in-place — live route claims preserved, no process churn, no adapter recycle.

If the probe refuses, the running broker is from a pre-2026-05-15 build that doesn't have a SIGHUP handler. Don't override — sending SIGHUP would kill it (Go's default handler is "terminate") and CC would close this session's MCP adapter as a side effect. The recovery is to quit Claude Code and relaunch with `claude --dangerously-load-development-channels=plugin:c3@c3` (append `--resume` to pick your session back up); the next adapter spawn auto-spawns a fresh broker with the new binary.

**What requires a real restart instead of reload:**
- Telegram bot token change (channel was initialized at broker start; reload doesn't re-init the channel)
- Telegram group additions where you need the channel to start polling new groups
- Broker binary update after `/c3:build` (the running process is the old code)
