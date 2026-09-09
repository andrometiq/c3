---
description: Update C3 to the latest checksum-verified release and trigger adapter upgrades.
---

!c3-broker update

Display the output verbatim. A successful update replaces the core binaries and
bundled STT runtime, then bounces the broker. That bounce triggers upgrade hints
on each adapter's reconnect. Compatible Claude adapters self-exec after draining
outstanding work; older or incompatible adapters show a notice to run `/mcp` and
reconnect c3 (or restart the session). A broker restart can cancel broker-side
tool calls and permission prompts before the adapter's exec gate takes effect.

The Codex launcher updates only when already installed as C3's launcher. Plugin
commands and hooks update separately through `/plugin`. Windows requires quitting
C3 and extracting the release manually. A dev build has no release version and
uses `/c3:build`; a failed check can be retried later.
