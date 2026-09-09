---
description: Rebuild C3's core binaries and trigger open-session adapter upgrades.
---

!cd "${CLAUDE_PLUGIN_ROOT}/../.." && make install && c3-broker restart

Display the output. The build embeds `git describe --always --dirty` as its build
identity. The broker bounce triggers upgrade hints for every connected Claude
adapter. Compatible adapters finish outstanding work and self-exec with MCP
resume; older or incompatible adapters show a notice to run `/mcp` and reconnect
c3 (or restart the session). The Codex launcher remains opt-in via INSTALL.md §5.

A broker restart can cancel broker-side tool calls and permission prompts; the
adapter's exec gate protects only the subsequent adapter replacement.

If the build fails, surface the error verbatim. Check that Go meets `go.mod` and
that the plugin source directory contains a `go.mod`.
