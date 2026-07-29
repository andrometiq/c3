package ipc

import (
	"encoding/json"
	"fmt"

	"github.com/Andrometiq/c3/internal/c3types"
)

// InboundMsg is the broker → adapter push for a single normalized inbound
// message. The adapter translates this into its host CLI's notification
// dialect (Claude Code: notifications/claude/channel; Codex: log + inbox).
type InboundMsg struct {
	Op      Op              `json:"op"` // = OpInbound
	Inbound c3types.Inbound `json:"inbound"`

	// Pending is the number of messages STILL queued for this route AFTER the
	// lines this push covered (i.e. backlog the live push did not cover). The
	// Claude adapter appends a "(N pending — call fetch_queue)" recovery nudge to
	// the push when Pending > 0, so a stuck backlog item is surfaced on the next
	// successful push — not only at the next re-attach.
	Pending int `json:"pending,omitempty"`
	// Covered is the number of durable queue lines this (possibly MERGED) push
	// covers. A debounced batch of N stored lines is delivered as ONE merged
	// notification; the adapter echoes Covered back in InboundDeliveredMsg.Count
	// so the broker Consumes exactly those N lines on ack (not just 1, which
	// would orphan N-1 as phantom backlog). Defaults to 1 (single-message push /
	// older brokers).
	Covered int `json:"covered,omitempty"`
	// DeliveryToken is a broker-minted, per-push correlation token. Adapters
	// echo it in InboundDeliveredMsg so the broker can identify the exact route
	// and durable records this ack covers even when two outstanding pushes share
	// one MessageID. Empty is the legacy wire shape.
	DeliveryToken string `json:"delivery_token,omitempty"`
}

// ToolCallReq is the adapter → broker forward of an MCP tool call. The broker
// dispatches to the channel module identified by Inbound.Channel-or-claim.
type ToolCallReq struct {
	Op   Op             `json:"op"` // = OpToolCall
	ID   string         `json:"id"`
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
}

// ToolResultMsg is the broker → adapter response to a ToolCallReq.
type ToolResultMsg struct {
	Op     Op             `json:"op"` // = OpToolResult
	ID     string         `json:"id"`
	Result map[string]any `json:"result,omitempty"`
	Error  *ErrorPayload  `json:"error,omitempty"`
}

// ErrorPayload carries a tool call's error.
type ErrorPayload struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// FetchQueueReq is the adapter → broker pull of held inbound for the stub's
// claimed route. Limit caps the batch (default applied by the adapter: 3, max
// 50); All=true overrides Limit and drains everything. Ack=true consumes
// (advances the cursor, deletes the files when drained); Ack=false peeks.
type FetchQueueReq struct {
	Op    Op     `json:"op"` // = OpFetchQueue
	ID    string `json:"id"`
	Limit int    `json:"limit,omitempty"`
	All   bool   `json:"all,omitempty"`
	Ack   bool   `json:"ack"`
}

// FetchQueueResp is the broker → adapter response to FetchQueueReq. Messages
// are the oldest up-to-Limit (or all) held inbound with full content; Remaining
// is the count still queued after this batch. Err is set (and Messages nil) on
// failure (e.g. no route claimed).
type FetchQueueResp struct {
	Op        Op                `json:"op"` // = OpFetchQueueResult
	ID        string            `json:"id"`
	Messages  []c3types.Inbound `json:"messages,omitempty"`
	Remaining int               `json:"remaining"`
	Err       string            `json:"err,omitempty"`
}

// ObserveReq is the adapter → broker READ-ONLY peek of a topic's held inbound,
// resolved by Name / Target="dm" / TopicID(+Group) exactly like attach, but
// WITHOUT claiming the route. It is the panel's "Watch" primitive: it never
// consumes and never steals, so a Claude Code session can keep the exclusive
// claim (and keep replying) while the panel displays the same inbox. Limit/All
// mirror FetchQueueReq; Ack is implicitly false (there is no consuming observe).
type ObserveReq struct {
	Op      Op     `json:"op"` // = OpObserve
	ID      string `json:"id"`
	Channel string `json:"channel,omitempty"` // "" → broker default channel
	Name    string `json:"name,omitempty"`
	Target  string `json:"target,omitempty"` // "dm"
	TopicID *int64 `json:"topic_id,omitempty"`
	Group   string `json:"group,omitempty"`
	Limit   int    `json:"limit,omitempty"`
	All     bool   `json:"all,omitempty"`
}

// ObserveResp is the broker → adapter response to ObserveReq: the peeked
// Messages (oldest up-to-Limit/all, full content — same shape as
// FetchQueueResp) PLUS the resolved topic identity and its current Holder.
//
// Status distinguishes a clean peek ("ok") from a resolution outcome the panel
// renders differently: "not_found" (offer take-over-creates), "ambiguous",
// "dm_unconfigured", "no_channel". Holder is nil when the topic is unclaimed
// (or held only by a dead session); HeldByYou is true when the CALLING stub is
// the live holder (so the panel can enable the owner-only consume affordances).
// Err carries a transient peek failure (worker busy/stopped) without changing
// the resolved identity.
type ObserveResp struct {
	Op        Op                `json:"op"` // = OpObserveResult
	ID        string            `json:"id"`
	OK        bool              `json:"ok"`
	Status    string            `json:"status"` // "ok" | "not_found" | "ambiguous" | "dm_unconfigured" | "no_channel"
	Channel   string            `json:"channel,omitempty"`
	ChatID    int64             `json:"chat_id,omitempty"`
	TopicID   *int64            `json:"topic_id,omitempty"`
	Name      string            `json:"name,omitempty"`
	Group     string            `json:"group,omitempty"`
	Holder    *Holder           `json:"holder,omitempty"`      // nil = unclaimed (or dead holder)
	HeldByYou bool              `json:"held_by_you,omitempty"` // the calling stub is the live holder
	Messages  []c3types.Inbound `json:"messages,omitempty"`
	Remaining int               `json:"remaining,omitempty"`
	Err       string            `json:"err,omitempty"`
}

// InboundDeliveredMsg is the Claude adapter → broker live-push ack. The broker
// Consumes the queued line(s) the push covered only after OK=true, so a push the
// adapter never accepted stays queued (backlog + recovery nudge). OK=false is a
// reported failure (the broker leaves it queued and may retry). Count is the
// number of durable queue lines this (possibly merged) push covered — the
// adapter echoes InboundMsg.Covered back so the broker Consumes exactly that many
// lines. DeliveryToken echoes InboundMsg.DeliveryToken and identifies the exact
// pushed route + durable records; empty invokes the fail-toward-duplicate legacy
// MessageID fallback.
type InboundDeliveredMsg struct {
	Op            Op     `json:"op"` // = OpInboundDelivered
	UpdateID      int64  `json:"update_id"`
	OK            bool   `json:"ok"`
	Count         int    `json:"count,omitempty"`
	DeliveryToken string `json:"delivery_token,omitempty"`
}

// RetranscribeReq is the adapter → broker request to re-run the STT chain over a
// cached voice attachment by FileID. MessageID is optional: when the matching
// message is still queued, its stored Text is refreshed in place.
type RetranscribeReq struct {
	Op        Op     `json:"op"` // = OpRetranscribe
	ID        string `json:"id"`
	FileID    string `json:"file_id"`
	MessageID int64  `json:"message_id,omitempty"`
}

// RetranscribeResp is the broker → adapter response to RetranscribeReq. Text is
// the fresh transcript; Err is set (Text empty) when the provider chain still
// fails.
type RetranscribeResp struct {
	Op   Op     `json:"op"` // = OpRetranscribeResult
	ID   string `json:"id"`
	Text string `json:"text,omitempty"`
	Err  string `json:"err,omitempty"`
}

// AskRegisterReq is the adapter → broker registration of a blocking, correlated
// `ask`. It carries NO route field — the broker derives the route from the
// stub's current claim (mirroring how OpToolCall is routed). AskID is generated
// adapter-side (8-char base32) and used to correlate the later OpAskResult back
// to the waiting tool call. Multi/AllowOther/AllowSkip/FreeText are accepted but
// IGNORED in Phase 1 (single-select); they are wired now so the schema/registry
// are Phase-2-ready without a wire change.
type AskRegisterReq struct {
	Op         Op       `json:"op"` // = OpAskRegister
	AskID      string   `json:"ask_id"`
	Question   string   `json:"question"`
	Options    []string `json:"options,omitempty"`
	Multi      bool     `json:"multi,omitempty"`
	AllowOther bool     `json:"allow_other,omitempty"`
	AllowSkip  bool     `json:"allow_skip,omitempty"`
	FreeText   bool     `json:"free_text,omitempty"`
}

// AskRegisteredMsg is the broker → adapter SYNCHRONOUS ack to AskRegisterReq.
// OK=true means the question + inline keyboard was sent (MessageID is the sent
// message's id); OK=false means a fast failure (ask before attach, oversized
// keyboard, channel/send error) carried in Err so the tool call bails fast
// instead of blocking the full answer timeout.
type AskRegisteredMsg struct {
	Op        Op     `json:"op"` // = OpAskRegistered
	AskID     string `json:"ask_id"`
	OK        bool   `json:"ok"`
	Err       string `json:"err,omitempty"`
	MessageID int64  `json:"message_id,omitempty"`
}

// AskResultMsg is the broker → adapter UNSOLICITED push of a human's answer to a
// previously-registered ask. Correlated to the waiting tool call by AskID;
// delivered to the route holder exactly like OpInbound. Err is set (Answer zero)
// only on an internal resolution failure.
type AskResultMsg struct {
	Op     Op        `json:"op"` // = OpAskResult
	AskID  string    `json:"ask_id"`
	Answer AskAnswer `json:"answer"`
	Err    string    `json:"err,omitempty"`
}

// AskAnswer is the channel-neutral answer payload. Phase 1 only ever sets
// Selected (one element, the tapped option) or TimedOut. Text / Skipped are
// reserved for the Phase 2 taxonomy (free-text / Other / Skip).
type AskAnswer struct {
	Selected []string `json:"selected,omitempty"`
	Text     string   `json:"text,omitempty"`
	Skipped  bool     `json:"skipped,omitempty"`
	TimedOut bool     `json:"timed_out,omitempty"`
}

// PermissionReq is the adapter → broker relay of a Claude Code tool-use
// permission prompt. Like AskRegisterReq it carries NO route — the broker derives
// it from the stub's current claim. RequestID is the harness-minted id (5 letters
// [a-km-z]) used to correlate the later OpPermissionVerdict back to the prompt.
// ToolName names the tool awaiting approval; Preview is a short, already-truncated
// input snippet (input_preview, falling back to the description) — never a secret
// body. Fire-and-forget: the broker sends no synchronous ack.
type PermissionReq struct {
	Op        Op     `json:"op"` // = OpPermissionRequest
	RequestID string `json:"request_id"`
	ToolName  string `json:"tool_name,omitempty"`
	Preview   string `json:"preview,omitempty"`
}

// PermissionVerdictMsg is the broker → adapter UNSOLICITED push of a human's
// Allow/Deny verdict for a previously-relayed permission_request. Behavior is the
// STRING "allow" | "deny" (matching the reference Telegram plugin's contract);
// the adapter emits it into Claude Code as notifications/claude/channel/permission
// {request_id, behavior}. Correlated to the prompt by RequestID; delivered to the
// route holder exactly like OpInbound.
type PermissionVerdictMsg struct {
	Op        Op     `json:"op"` // = OpPermissionVerdict
	RequestID string `json:"request_id"`
	Behavior  string `json:"behavior"`
}

// QueuedItem is one compact backlog-summary row carried in AttachedMsg. Preview
// is a short, truncated text snippet (never the full body); Unix is the
// message's timestamp.
type QueuedItem struct {
	MessageID int64  `json:"message_id"`
	Sender    string `json:"sender,omitempty"`
	Kind      string `json:"kind,omitempty"`
	Unix      int64  `json:"unix,omitempty"`
	Preview   string `json:"preview,omitempty"`
}

// HelloMsg is sent by the adapter on connect.
type HelloMsg struct {
	Op           Op       `json:"op"` // = OpHello
	CLI          string   `json:"cli"`
	PID          int      `json:"pid"`
	CWD          string   `json:"cwd"`
	Capabilities []string `json:"capabilities,omitempty"`

	// CannotRenderChannels marks a session whose HOST cannot render channel push
	// notifications — a Claude Code session launched WITHOUT the
	// development-channels flag for this plugin (typically a --fork-session
	// background job). Such a host silently DROPS notifications/claude/channel
	// frames before rendering, so an inbound the adapter "delivered" and acked
	// would vanish (the forked-session blackhole). When set, the broker never
	// marks this holder's inbound delivered: durable human messages fall through
	// to the queue + held-notice (recoverable via fetch_queue, an MCP tool-result
	// that DOES render), while the session keeps its claim for OUTBOUND.
	//
	// Inverted sense on purpose: absent/false = renderable. The adapter sets true
	// ONLY when it is confident it cannot render (see the adapter's
	// hostCanRenderChannels detection). Additive + omitempty keeps old adapters
	// (field absent → broker reads renderable → today's behavior) and old brokers
	// (unknown field ignored) compatible; the fix engages only new-adapter↔
	// new-broker, matching the single-host-lockstep note above.
	CannotRenderChannels bool `json:"cannot_render_channels,omitempty"`

	// ProtocolVersion is the IPC wire-protocol version this adapter speaks (see
	// ipc.ProtocolVersion for the bump rule). Additive + omitempty: an adapter
	// that predates versioning omits it, and absence means version 1 — the
	// broker normalizes with PeerProtocolVersion and NEVER errors on absence.
	// A mismatch is logged by the broker and the connection proceeds; C3 does
	// not refuse a connection over version, because `c3 update` routinely
	// leaves old adapters talking to a new broker on the same host.
	ProtocolVersion int `json:"protocol_version,omitempty"`
}

// RecoverSessionReq is the adapter → broker request to re-attach a resumed
// session to its last topic, keyed on the STABLE session id (the transcript /
// --resume id) which the adapter learned from the SessionStart-hook handoff.
// Sent shortly after hello on the adapter's existing connection (NOT at hello —
// the SessionStart hook fires ~2s after the adapter spawns, so recovery cannot
// happen during the handshake). The broker maps the stub→stable-id directly
// from this request, so it never needs the ephemeral instance-id.
type RecoverSessionReq struct {
	Op              Op     `json:"op"` // = OpRecoverSession
	StableSessionID string `json:"stable_session_id"`
	CWD             string `json:"cwd,omitempty"`
}

// RecoverSessionResp is the broker → adapter response to RecoverSessionReq.
// Recovered=true means the broker claimed the session's last route for the
// stub; the adapter surfaces this via a post-hello notification (with the held
// backlog count). Recovered=false (no/expired/tombstoned attachment, route held
// by another live session, or the stub was already attached) is silent — today's
// behavior, zero regression.
type RecoverSessionResp struct {
	Op          Op     `json:"op"` // = OpRecoverSessionResult
	Recovered   bool   `json:"recovered"`
	Channel     string `json:"channel,omitempty"`
	ChatID      int64  `json:"chat_id,omitempty"`
	TopicID     *int64 `json:"topic_id,omitempty"`
	Name        string `json:"name,omitempty"`
	Group       string `json:"group,omitempty"`
	QueuedCount int    `json:"queued_count,omitempty"`
	// QueuedSummary is a compact preview of the oldest held messages (up to
	// backlogSummaryMax) on the recovered route — the same shape a normal
	// attach's AttachedMsg carries. The adapter surfaces these into the resumed
	// session (sender + kind + preview + total) and instructs the agent to drain
	// the rest via fetch_queue. Additive + omitempty: nil for an empty queue and
	// for older brokers.
	QueuedSummary []QueuedItem `json:"queued_summary,omitempty"`
	Err           string       `json:"err,omitempty"`
}

// HelloAckMsg is the broker's response to HelloMsg.
type HelloAckMsg struct {
	Op           Op       `json:"op"` // = OpHelloAck
	ConnID       uint64   `json:"conn_id"`
	AutoAttached bool     `json:"auto_attached"`
	Mapping      *Mapping `json:"mapping,omitempty"`
	ClaimHolder  *Holder  `json:"claim_holder,omitempty"`
	NoConfig     bool     `json:"no_config,omitempty"`
	NoMapping    bool     `json:"no_mapping,omitempty"`

	// Capabilities carries the resolvable channel's static capability
	// manifest so the adapter can fold GuidanceFor(caps) into the agent's
	// MCP-initialize instructions (Claude) / AGENTS.md surface (Codex).
	// Additive + omitempty: nil for older brokers that predate the CMG
	// build, and nil when no channel is resolvable for the connection's
	// route (the adapter falls back to a sensible default in that case).
	// Additive-omitempty is what makes that safe across builds; the handshake
	// now ALSO carries an explicit ProtocolVersion (below) for the changes
	// additive-omitempty cannot cover.
	Capabilities *c3types.Capabilities `json:"capabilities,omitempty"`

	// ProtocolVersion is the IPC wire-protocol version the BROKER speaks (see
	// ipc.ProtocolVersion for the bump rule). Additive + omitempty: a broker
	// that predates versioning omits it, and absence means version 1. The
	// adapter logs a mismatch and carries on — the handshake still succeeds.
	ProtocolVersion int `json:"protocol_version,omitempty"`
}

// Holder identifies a claim holder for diagnostic responses.
type Holder struct {
	CLI string `json:"cli"`
	PID int    `json:"pid"`
	CWD string `json:"cwd"`
}

// Mapping is the wire-shape mirror of mappings.Mapping (avoiding a circular
// import). Populated by the broker on hello_ack when an auto-attach is
// possible.
type Mapping struct {
	Channel string `json:"channel"`
	ChatID  int64  `json:"chat_id"`
	TopicID *int64 `json:"topic_id,omitempty"`
	Name    string `json:"name"`
	Group   string `json:"group,omitempty"`
}

// ReleaseReq is sent by the adapter to drop its claim without disconnecting.
type ReleaseReq struct {
	Op Op `json:"op"` // = OpRelease
}

// ByeReq is sent by the adapter for clean disconnect.
type ByeReq struct {
	Op Op `json:"op"` // = OpBye
}

// ListTopicsReq is sent by the adapter to fetch the topics registry.
type ListTopicsReq struct {
	Op Op `json:"op"` // = OpListTopics
}

// TopicsListMsg is the broker's response to ListTopicsReq.
type TopicsListMsg struct {
	Op     Op           `json:"op"` // = OpTopicsList
	Topics []TopicEntry `json:"topics"`
}

// ListClaimsReq is sent by a status-style client (CLI subcommand or any
// adapter) to fetch a snapshot of the broker's live route-claim table.
type ListClaimsReq struct {
	Op Op `json:"op"` // = OpListClaims
}

// ClaimsListMsg is the broker's response to ListClaimsReq. Each entry is a
// (route, holder) pair as the broker currently sees it. Renders into
// `c3-broker status` output for the operator.
type ClaimsListMsg struct {
	Op     Op           `json:"op"` // = OpClaimsList
	Claims []ClaimEntry `json:"claims"`
}

// ClaimEntry is one row of ClaimsListMsg.Claims. Includes the topic name
// when the route corresponds to a known topic in mappings.json (lookup is
// best-effort; empty when the route is a DM or a yet-unregistered topic).
type ClaimEntry struct {
	Channel   string `json:"channel"`
	ChatID    int64  `json:"chat_id"`
	HasTopic  bool   `json:"has_topic"`
	TopicID   int64  `json:"topic_id,omitempty"`
	TopicName string `json:"topic_name,omitempty"`
	GroupName string `json:"group_name,omitempty"`
	HolderCLI string `json:"holder_cli"`
	HolderPID int    `json:"holder_pid"`
	HolderCWD string `json:"holder_cwd,omitempty"`
	ConnID    uint64 `json:"conn_id"`
	Connected bool   `json:"connected"`
}

// ListHealthReq is sent by a status-style client to fetch a snapshot of the
// broker's per-channel cached fetch-health (the last HealthEvent per channel).
type ListHealthReq struct {
	Op Op `json:"op"` // = OpListHealth
}

// HealthListMsg is the broker's response to ListHealthReq. One entry per channel
// that has reported at least one health edge. Renders into the `c3-broker
// status` "Channel health:" section.
type HealthListMsg struct {
	Op     Op            `json:"op"` // = OpHealthList
	Health []HealthEntry `json:"health"`
}

// HealthEntry is one channel's last-known fetch-health for status rendering.
// SinceUnix / DownForSec are wire-friendly encodings (the status client formats
// them). State is "up" | "down". LastSuccessAgoSec is the seconds since the last
// successful fetch (for the UP line); omitted (0) when unknown.
type HealthEntry struct {
	Channel    string `json:"channel"`
	State      string `json:"state"` // "up" | "down"
	SinceUnix  int64  `json:"since_unix,omitempty"`
	Consec     int    `json:"consec,omitempty"`
	Reason     string `json:"reason,omitempty"`
	DownForSec int64  `json:"down_for_sec,omitempty"`
}

// TopicEntry is one row in TopicsListMsg.Topics. Also reused by Proposal.Existing
// to describe a found-but-not-claimed topic (in which case ClaimedBy is nil).
type TopicEntry struct {
	Channel   string  `json:"channel"`
	ChatID    int64   `json:"chat_id"`
	TopicID   int64   `json:"topic_id"`
	Name      string  `json:"name"`
	Group     string  `json:"group,omitempty"`
	ClaimedBy *Holder `json:"claimed_by,omitempty"`
}

// AttachStatus is a typed enum describing the outcome of an attach IPC
// op. Added 2026-05-19 so calling agents can distinguish "user must
// configure C3" (no_topics_configured) from "CLI host policy layer
// rejected the call before it reached the adapter" (policy_rejected)
// from "success" (ok). Pre-2026-05-19 messages omit the field;
// consumers treat absence as "interpret OK/Err/Proposal as before."
//
// See docs/plans/2026-05-19-codex-policy-3state.md for the full design.
type AttachStatus string

const (
	// AttachStatusOK indicates the attach succeeded; topic + welcome flow.
	AttachStatusOK AttachStatus = "ok"

	// AttachStatusNoTopicsConfigured indicates the broker's mappings has
	// no channels / DM / topics — the user hasn't run `c3-broker setup`
	// yet. Emitted by the broker directly.
	AttachStatusNoTopicsConfigured AttachStatus = "no_topics_configured"

	// AttachStatusPolicyRejected indicates the CLI host's policy layer
	// rejected the call. ONLY ever set in response to
	// AttachReq.PolicyRejected — the broker can't detect the underlying
	// signal (it lives upstream of the adapter in the CLI host). The
	// Codex adapter exposes a `policy_rejected` tool argument the agent
	// passes on a re-invoke after observing the rejection in the Codex
	// UI; the broker then short-circuits and surfaces this status so the
	// adapter formatter can render an actionable next-step (ask tenant
	// admin to approve the Telegram destination, then retry).
	AttachStatusPolicyRejected AttachStatus = "policy_rejected"

	// AttachStatusCwdDefaultCollision indicates a BARE (cwd-default, no
	// explicit name) attach resolved its saved cwd→topic mapping to a
	// topic that is ALREADY HELD by a DIFFERENT live session (SYMPTOM-3,
	// 2026-06-04). Multiple `claude` instances launched from the same
	// parent dir report identical os.Getwd(); a bare attach from a
	// session that meant a different sub-project would otherwise silently
	// race/steal a sibling's topic. Rather than silently claim — or show
	// only the raw force_steal y/n prompt — the broker returns this status
	// with the resolved topic Name, the colliding CWD, and the live
	// Holder so the formatter can render a guided message: "did you mean a
	// different topic? attach by name … or force this topic with --steal".
	// ONLY fires for the cwd-default case; an explicit `/c3:attach <name>`
	// to a held topic still gets the normal force_steal proposal.
	AttachStatusCwdDefaultCollision AttachStatus = "cwd_default_collision"
)

// AttachReq is the adapter → broker attach request. Spec §4.4.1.
//
// Two ways to specify the target:
//
//  1. Structured: any of Name / Target / TopicID (with optional Group/Channel).
//  2. Freeform: Expr — a single user-supplied string the broker parses.
//     Lets every CLI's slash-command wrapper be a one-liner —
//     `attach(expr=$ARGUMENTS)` — instead of duplicating arg-parsing logic.
//     Parsing rules in the broker:
//     ""                  → bare attach (idempotent / session's own recover /
//     picker; never a silent cwd-saved claim)
//     "dm" (any case)     → target=dm (with disambiguation if a topic
//     named "dm" also exists)
//     "<int>"             → topic_id=<int>
//     "<name>" / "create <name>" / "-y <name>"
//     → name=<name> (create=true if prefix used)
type AttachReq struct {
	Op      Op     `json:"op"` // = OpAttach
	CWD     string `json:"cwd,omitempty"`
	Name    string `json:"name,omitempty"`
	Target  string `json:"target,omitempty"`
	TopicID *int64 `json:"topic_id,omitempty"`
	Group   string `json:"group,omitempty"`
	Channel string `json:"channel,omitempty"`
	Create  bool   `json:"create,omitempty"`
	Expr    string `json:"expr,omitempty"`

	// ChatID is an OPTIONAL cross-check for id-addressed replays. When an
	// adapter remembers a resolved topic identity for reconnect/fresh-broker
	// replay it records the topic's chat here; attachByTopicID then refuses if
	// the group named in the replay resolves to a DIFFERENT chat than this
	// value. This closes a fail-open hole: a remembered Group=="" for a topic
	// that actually lives in a non-default group would otherwise replay against
	// the DEFAULT group's chat, and if that chat happened to hold a live thread
	// with the same id, the broker would silently claim the wrong topic. Zero
	// (omitted) means "no cross-check" — preserves the behavior of callers that
	// don't set it. Only meaningful alongside TopicID.
	ChatID int64 `json:"chat_id,omitempty"`

	// Steal: when true, evict any existing alive holder of the target
	// route before claiming. Used in the force_steal proposal flow —
	// LLM asks the user for confirmation, then re-invokes with steal=true.
	// The "broker is authority" principle (claims survive PID-alive) is
	// preserved: only an explicit user-confirmed steal can displace a live
	// holder.
	Steal bool `json:"steal,omitempty"`

	// Replay: set true by the adapter when this attach is being re-sent
	// after a broker reconnect (see replayLastAttach in
	// cmd/c3-claude-adapter/main.go). The broker uses this to suppress
	// the on-attach welcome message — a broker bounce or transient
	// disconnect shouldn't surface as "👋 Hi! I'm connected…" since the
	// user didn't initiate anything. User-typed attach calls leave this
	// false (default) and get the friendly welcome.
	Replay bool `json:"replay,omitempty"`

	// Confirm carries the prior proposal for sibling-stub race detection
	// (spec §4.4.1). Optional; v1 broker doesn't yet validate it but the
	// field is plumbed for forward-compat.
	Confirm *Proposal `json:"confirm,omitempty"`

	// PolicyRejected: hint set true by the calling agent on a re-invoke
	// after observing the CLI host's policy layer reject a prior attach
	// (e.g. Codex's approvals_reviewer="auto_review" surfacing an
	// "unacceptable risk rejection"). The broker treats this as a pure
	// surface-state request: it short-circuits with
	// AttachStatusPolicyRejected so the adapter formatter can render
	// the actionable next-step (ask tenant admin to approve the Telegram
	// destination, then retry). The broker NEVER infers this on its own;
	// only the agent can observe the upstream rejection.
	PolicyRejected bool `json:"policy_rejected,omitempty"`
}

// AttachedMsg is the broker → adapter response.
type AttachedMsg struct {
	Op                Op        `json:"op"` // = OpAttached
	OK                bool      `json:"ok"`
	Channel           string    `json:"channel,omitempty"`
	ChatID            int64     `json:"chat_id,omitempty"`
	TopicID           *int64    `json:"topic_id,omitempty"`
	Name              string    `json:"name,omitempty"`
	Group             string    `json:"group,omitempty"`
	NeedsConfirmation bool      `json:"needs_confirmation,omitempty"`
	Proposal          *Proposal `json:"proposal,omitempty"`
	Err               string    `json:"err,omitempty"`

	// Status disambiguates the outcome. See AttachStatus godoc. Omitted
	// for backward compat with pre-2026-05-19 consumers that switch on
	// OK / NeedsConfirmation / Err.
	Status AttachStatus `json:"status,omitempty"`

	// CWD is the colliding launch/working directory whose saved cwd→topic
	// mapping resolved to a held topic. Set only on
	// AttachStatusCwdDefaultCollision so the formatter can name the
	// directory in the guided message. Omitted otherwise (wire-additive).
	CWD string `json:"cwd,omitempty"`

	// Holder identifies the live session currently holding the resolved
	// topic. Set only on AttachStatusCwdDefaultCollision (cli + pid drive
	// the "already held by claude pid N" line). Omitted otherwise
	// (wire-additive; the force_steal holder still travels inside
	// Proposal.Holder).
	Holder *Holder `json:"holder,omitempty"`

	// Capabilities carries the just-attached channel's static capability
	// manifest. Set only on a successful attach (OK=true) where the channel
	// is resolvable; lets a multi-channel future refresh the agent surface
	// at attach-time (the turn-time-refresh seam — spec §L5). Additive +
	// omitempty: nil on failures and for older brokers.
	Capabilities *c3types.Capabilities `json:"capabilities,omitempty"`

	// QueuedCount is the number of held inbound waiting on the just-claimed
	// route at attach time; QueuedSummary is a compact preview of the oldest few
	// (the adapter renders it and instructs the agent to call fetch_queue).
	// Additive + omitempty: zero/nil for an empty queue and for older brokers.
	QueuedCount   int          `json:"queued_count,omitempty"`
	QueuedSummary []QueuedItem `json:"queued_summary,omitempty"`
}

// Proposal describes what the broker would do if the agent confirms.
// Action is one of:
//   - "create" — create a new topic with the given Name in Group
//   - "use_existing_other_group" — adopt Existing topic from a different group
//   - "disambiguate_dm" — a topic named "dm" exists; agent asks user
//     whether they meant the topic or the actual Telegram DM (Existing
//     describes the topic; pass target="dm" to confirm DM, or topic_id to
//     confirm topic).
//   - "force_steal" — the requested route is held by Holder; agent asks
//     user for confirmation, then re-invokes attach with steal=true to
//     evict and claim.
//   - "pick_topic" — a BARE attach that resolved to neither an existing claim
//     nor the session's own recorded route. The broker claims NOTHING; it
//     returns Suggestions (ranked ≤3) + HasMore so the agent ASKS the user
//     which topic to attach. The chosen suggestion's exact re-invoke command
//     is rendered by FormatAttached; a claim happens only on that explicit
//     re-invoke. See §4 of the attach-redesign spec.
type Proposal struct {
	Action      string      `json:"action"`
	Channel     string      `json:"channel"`
	Group       string      `json:"group"`
	Name        string      `json:"name"`
	Existing    *TopicEntry `json:"existing,omitempty"`    // populated for use_existing_* / disambiguate_dm
	Alternative *Proposal   `json:"alternative,omitempty"` // recursion: e.g. "or create new in default group"
	Holder      *Holder     `json:"holder,omitempty"`      // populated for force_steal

	// pick_topic payload (bare-attach friendly picker, spec §4). Additive +
	// omitempty so every other proposal action and older brokers are byte-stable.
	Suggestions []PickSuggestion `json:"suggestions,omitempty"` // ≤3, ranked (current project, then recently used)
	Project     string           `json:"project,omitempty"`     // basename(cwd) — the label source for the create row
	HasMore     bool             `json:"has_more,omitempty"`    // registry holds more existing topics than shown → offer "See the full list"
}

// PickSuggestion is one ranked option in a "pick_topic" proposal (spec §4). The
// broker never claims when it emits these; FormatAttached renders each with its
// EXACT re-invoke command, and the claim happens only when the agent runs that
// command on the human's pick.
//
// Kind is "attach_existing" (an already-registered topic or the DM) or "create"
// (mint a new topic named Name). Group is set ONLY for an existing topic that
// lives in a NON-default group — its presence tells FormatAttached to add a
// group="…" arg to the re-invoke (a topic_id re-invoke otherwise validates
// against the default group's chat). TopicID is set iff Kind=="attach_existing"
// AND the suggestion is a real topic; a nil TopicID on an attach_existing row is
// the DM (rendered as target="dm"). ClaimedBy is set when the topic is held by a
// live session, so the human is warned — the re-invoke still goes through the
// normal force_steal confirmation, never a silent steal.
type PickSuggestion struct {
	Kind      string  `json:"kind"`                 // "attach_existing" | "create"
	Reason    string  `json:"reason,omitempty"`     // "current project" | "recently used" | "current project (new)"
	Name      string  `json:"name"`                 //
	Group     string  `json:"group,omitempty"`      // set only for a non-default-group existing topic
	ChatID    int64   `json:"chat_id,omitempty"`    //
	TopicID   *int64  `json:"topic_id,omitempty"`   // set iff attach_existing AND not the DM
	ClaimedBy *Holder `json:"claimed_by,omitempty"` // live holder, when the topic is held (ranking rule 3)
}

// PairModeStartReq is the adapter → broker request to arm a pairing
// window. Target is "dm" or "group". For "group", ChatID must be set
// (the Telegram group's chat_id, typically a -100… supergroup id).
//
// On success the broker returns a PairModeReplyMsg carrying the freshly
// generated 4-digit code and TTL. The CLI displays the code so the
// human can type it into the bot.
type PairModeStartReq struct {
	Op     Op     `json:"op"`     // = OpPairModeStart
	Target string `json:"target"` // "dm" or "group"
	ChatID int64  `json:"chat_id,omitempty"`
}

// PairModeReplyMsg is the broker's response to PairModeStartReq.
type PairModeReplyMsg struct {
	Op     Op     `json:"op"` // = OpPairModeReply
	OK     bool   `json:"ok"`
	Code   string `json:"code,omitempty"`
	Target string `json:"target,omitempty"` // echoed: "dm" or "group"
	ChatID int64  `json:"chat_id,omitempty"`
	TTLSec int    `json:"ttl_sec,omitempty"`
	Err    string `json:"err,omitempty"`
}

// PingThisSessionReq is sent by the `c3-broker ping` transient client
// (slash command `/c3:ping`) to ask the broker to send a one-shot
// identification message to whichever Telegram route the calling
// session currently holds. The transient client itself doesn't hold a
// route — the broker matches the user's actual session against the live
// adapter stubs.
//
// Matching is PID-primary, CWD-fallback (mirrors ListSessionsReq):
//
// PID: the calling CLI session's best-effort PID, walked up the PPID
// chain from the slash command's shell-out (see bestEffortSessionPID in
// cmd/c3-broker/sessions.go). When set (!=0) the broker matches the stub
// whose PID equals this — the stable identity that survives the CWD
// collapse that happens when `claude` is launched from a parent dir and
// the slash command runs from a project subdir.
//
// CWD: the calling client's working directory (typically inherited
// from the user's shell / slash-command invocation). Used only as a
// fallback when PID==0 (PPID walk failed — non-Linux / missing /proc /
// ancestors exited). The broker scans live stubs for one whose CWD
// matches AND whose CurrentRoute is non-nil; that stub's claim is the
// target of the ping reply.
type PingThisSessionReq struct {
	Op  Op     `json:"op"` // = OpPingThisSession
	PID int    `json:"pid,omitempty"`
	CWD string `json:"cwd"`
}

// PingThisSessionReplyMsg is the broker's response to PingThisSessionReq.
// On success: OK=true and the broker has already dispatched a one-shot
// reply to the matched route via the channel's SendReply. On failure
// (no matching attached stub, channel send error, etc.): OK=false and
// Err carries the cause for the CLI to surface.
type PingThisSessionReplyMsg struct {
	Op       Op     `json:"op"` // = OpPingThisSessionReply
	OK       bool   `json:"ok"`
	Channel  string `json:"channel,omitempty"`
	Topic    string `json:"topic,omitempty"`     // human label ("dm", "<name>", "topic-<id>")
	SentText string `json:"sent_text,omitempty"` // text the broker actually sent
	Err      string `json:"err,omitempty"`
}

// ListSessionsReq is sent by the `c3-broker sessions` transient client
// (slash command `/c3:sessions`) to fetch a snapshot of every live
// adapter the broker is currently tracking. The CLI passes its
// best-effort guess at the calling CLI session's PID (walked up the
// PPID chain from the slash command's shell-out) so the broker can
// tag the matching stub with IsThisSession=true; if the walk fails
// (non-Linux / missing /proc / parents already exited) PID==0 and
// no entry will be flagged.
//
// CWD: the calling client's working directory. Plumbed but not
// currently consumed by the broker — included for parity with the
// other transient-client requests and for future
// "match by cwd when PID walk fails" extensions.
//
// TODO #19(e) — maintainer 2026-05-19.
type ListSessionsReq struct {
	Op  Op     `json:"op"` // = OpListSessions
	PID int    `json:"pid,omitempty"`
	CWD string `json:"cwd,omitempty"`
}

// ListSessionsReplyMsg is the broker's response to ListSessionsReq.
// Sessions is ordered descending by ConnID (most-recently-registered
// first). The transient client stub itself (CLI=="c3-broker-cli") is
// filtered out — we never want to list ourselves.
type ListSessionsReplyMsg struct {
	Op       Op             `json:"op"` // = OpListSessionsReply
	Sessions []SessionEntry `json:"sessions"`
}

// SessionEntry is one row of ListSessionsReplyMsg.Sessions. Mirrors
// what the user would see in the rendered table.
type SessionEntry struct {
	CLI    string `json:"cli"`
	PID    int    `json:"pid"`
	CWD    string `json:"cwd"`
	ConnID uint64 `json:"conn_id"`
	// AttachedTo is the human-formatted topic label — "<name> (<group>)"
	// for a regular topic, "dm" for a DM route, "topic-<id>" when the
	// route refers to an unknown topic id, or "" when the stub has no
	// current route claim.
	AttachedTo string `json:"attached_to,omitempty"`
	// IsThisSession is true when the stub's PID matches the calling
	// client's PPID-walk seed — i.e. the user pressed enter on
	// `/c3:sessions` from THIS terminal.
	IsThisSession bool `json:"is_this_session,omitempty"`
}

// ErrorMsg is sent by either side on an unrecoverable error.
type ErrorMsg struct {
	Op  Op     `json:"op"` // = OpError
	Err string `json:"err"`
}

// PeekOp parses the "op" field from a raw JSON envelope without unmarshaling
// the full payload. Used by the dispatcher to route to the right handler.
func PeekOp(raw []byte) (Op, error) {
	var env struct {
		Op Op `json:"op"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return "", fmt.Errorf("ipc: parse envelope: %w", err)
	}
	if env.Op == "" {
		return "", fmt.Errorf("ipc: missing op field")
	}
	return env.Op, nil
}
