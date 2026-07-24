# C3 — On-the-Go / Voice-Channel (future design capture)

**Status:** FUTURE — captured 2026-07-24 from an extended voice brief. Not scheduled, nothing frozen. This is the elaboration of the roadmap line *"Web-chat and voice-mode channels via the `Channel` interface"* (and touches *"Stream the agent's reasoning to the channel"*, *"Be the phone surface for session managers"*, inter-agent messaging, and a possible VoIP transport). A clean-up/design pass is expected before implementation; this file exists so a future agent can be pointed straight at it.

## Why (the problem)

The operator often wants to drive C3 **100% by voice, hands- and eyes-free** — typically while driving. Today the workaround is Telegram: ask the agent to reply as voice notes, play them, reply by voice. It works, but it forces **eyes-down, small-target interactions** — hunting for the mic button to record, hunting for the play button when a reply lands. The friction is the *visual UI targeting*, not the voice itself. The next logical step for C3 is to be usable on the go, entirely by voice.

## Shape — two orthogonal axes

- **A. The surface (channel):** *where* voice in/out happens — a new pluggable `Channel`. C3 channels are pluggable by design, so the frame is "C3 can have any number of channels."
- **B. The voice/AI behavior:** *how* speech is produced and consumed — two modes (agent-aware async TTS, and live bidirectional narration).

---

### A. Surface / channel

A new **"on-the-go mode"** that sits alongside CLI mode and Telegram mode (per-session, agent-tracked, switched only on explicit request — same output-mode protocol as today). Switching it on yields an **authenticated surface**. Candidate form factors (pluggable — more than one can ship):

1. **Web channel** — for people who don't want to install an app. Turning on on-the-go mode returns a **URL**.
   - **Auth:** magic link (below) or Telegram Login. Likely scoped to the **direct bot (DM)**, not the group.
   - **Telegram Mini App:** evaluate shipping this as a Telegram mini app (mini apps run inside the bot). If it works well it's the lowest-friction entry; Telegram Login stays as the fallback.
2. **Native app** — downloaded once, authenticated once.
   - **Why an app over web:** deeper OS/telephony integration — hooking the Android call system (a "true call" so it connects to the **car's Bluetooth as a real phone call**), giving car-native controls (mute, hang-up from the head-unit/steering).
   - **But:** if the browser/web app can reach the needed integration (or the operator just puts the phone on loudspeaker nearby), the **web app suffices — a native app is not required.** Native is an optimization for car-call integration, not a prerequisite.

**Auth mechanics (deliberately simple):**
- **Magic link, single-use.** Persists while the tab/app stays open. Closed by mistake → request a fresh link and resume from it. No passwords, no long-lived session juggling.
- Native app: authenticate once and persist.

---

### B. Voice / AI behavior — two modes

**1. Agent-aware mode (asynchronous).**
Exactly like Telegram mode: the agent is *told* it's in on-the-go mode and follows on-the-go instructions. The agent emits **plain text**; **C3 converts text → speech itself** (TTS owned by C3 — the mirror image of the STT/HTTP plugin, i.e. "the reverse plugin"). Inbound voice is transcribed to text for the agent, as today.
- **Why C3 owns the TTS** (vs. the agent calling a TTS API mid-turn): keeps control in C3, keeps the API call out of the agent's turn, and lets the agent simply be instructed "write text that is easy to speak and to listen to." Provider/model swappable, mirroring the STT chain.

**2. Live / bidirectional mode (synchronous).**
The agent is **not aware** anything is happening. A live voice service **hosted as part of C3** sits between the operator and the running session.
- It uses a **low-intelligence voice model** — explicitly **not** a thinker. Its only job: **read what's happening in the target agent's session and narrate it truthfully** ("the agent said …", "the agent is working right now"), and **relay what the operator says back to the agent.** Think "a friend sitting nearby, looking at the screen, telling me what's going on." No reasoning, no decisions — faithful narration + relay only. Swap the model as they improve.
- **Reach unlocked:** once live voice exists, C3 can connect over *any* voice-capable transport — **phone calls, VoIP** (e.g. a C3 VoIP service + a VoIP app → just place a call and talk; works natively in the car).
- **Cross-topic control:** in live mode, potentially drive **multiple chat topics** from one voice session (cross-topic). Context-management for this narrator agent is an open implementation question — defer to build time.

---

## When to use which

- **Async agent-aware TTS** — when the *thinking* must stay in the full model: you're issuing real work by voice and want the model's full intelligence, just delivered as speech.
- **Live bidirectional** — when you want low-latency conversational *awareness/control* of what sessions are doing, with the heavy thinking still happening inside the underlying agents (the narrator doesn't think).

## Relationship to the existing roadmap

- **Elaborates:** *More channels → Web-chat and voice-mode channels via the `Channel` interface.*
- **Touches:** *Reach → Stream the agent's reasoning to the channel* (the live narrator reads session state); *Be the phone surface for session managers*; *Inter-agent messaging*; and a possible **VoIP transport** under *Other transports the interface already admits.*

## Open questions (for the design pass, not now)

- Mini-app vs. plain web vs. native — which to build first; how much car-call integration is reachable from the browser alone.
- Narrator context management and the multi-topic session model in live mode.
- TTS provider/chain config (mirror the STT chain); latency budget for live mode.
- Auth surface: bot-DM-only vs. group; magic-link lifetime/rotation.
- Whether live-mode narration reuses the "stream reasoning to channel" plumbing or reads a separate session-state feed.
