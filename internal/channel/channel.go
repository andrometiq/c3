// Package channel defines the contract every transport implements.
// Spec §4.1.
package channel

import (
	"context"
	"errors"

	"github.com/Andrometiq/c3/internal/c3types"
)

// ErrAttachmentTooLarge marks a failure whose cause is SIZE: the attachment is
// over the transport's download ceiling, so the bot cannot fetch it at all —
// not now, not on retry, and not through any C3 recovery path. Callers use
// errors.Is to tell this apart from a transient fetch failure and say so to the
// human ("resend it in shorter parts") instead of offering a retry that is
// guaranteed to fail. (2026-07-27 incident: a 21 MB voice note failed STT twice
// and surfaced "the audio is saved and recoverable", which was false on every
// clause.)
var ErrAttachmentTooLarge = errors.New("attachment over the channel's download limit")

// Channel is the contract every transport implements. Methods are called by
// the broker on its own goroutine — implementations must be safe for
// concurrent use, except Start/Stop which are sequenced.
type Channel interface {
	Name() string
	Start(ctx context.Context, host Host) error
	Stop() error

	// Capabilities returns this channel's static capability manifest. No
	// argument in v1 — RouteKey lives only in internal/broker; a RouteKey
	// arg would introduce a channel→broker import cycle.
	Capabilities() c3types.Capabilities

	SendReply(args c3types.ReplyArgs) (sentMessageID int64, err error)
	SendTyping(chatID int64, threadID *int64) error
	EditMessage(args c3types.EditArgs) (*c3types.EditResult, error)
	React(args c3types.ReactArgs) error
	DownloadAttachment(fileID string) (path string, err error)

	// StopPoll force-closes a bot-sent poll and returns its final aggregate
	// tally. The deterministic read path for poll results (the passive `poll`
	// update arrives only on close). Telegram-specific in v1 (like CreateTopic);
	// future channels may stub it with an unsupported error.
	StopPoll(chatID, messageID int64) (*c3types.PollResult, error)

	// Topic management (Telegram-specific in v1; future channels may stub).
	CreateTopic(chatID int64, name string) (topicID int64, err error)
	ValidateTopic(chatID int64, threadID int64) error
}

// Host is what the broker passes to a Channel. Subset of plugin.Host scoped
// to channel concerns (config + emit + log + done + gate).
type Host interface {
	Config(name string, target any) error
	// Emit submits an inbound to the broker's per-route worker pool. It returns
	// true when the inbound was accepted onto a worker queue (and will be
	// persisted there), false when it was DROPPED (worker queue full or stopped).
	// On a false return the inbound never reaches durable storage, so a caller
	// that staged any in-flight bookkeeping for it (e.g. the persisted-offset
	// tracker's msgToUpdate seam) MUST resolve that bookkeeping itself — otherwise
	// the source update_id stays in-flight forever and wedges the contiguous-prefix
	// offset for ALL inbound (I4).
	Emit(in *c3types.Inbound) bool
	Logf(format string, args ...any)
	Done() <-chan struct{}

	// NotifyHealth reports a channel fetch-health transition (UP→DOWN /
	// DOWN→UP). The broker fans this out to OUT-OF-BAND sinks (desktop
	// notification, system-event broadcast to all CLI sessions, the
	// `c3-broker status` health line, the broker log) so a dead channel can
	// still raise an alarm through a different path. MUST be called only on an
	// edge (not per attempt) and MUST NOT block — the broker fans out
	// asynchronously. The alert path never re-enters the reporting channel
	// (the dead channel can't carry its own outage alert).
	NotifyHealth(ev c3types.HealthEvent)

	// GateInbound runs an inbound through the allowlist + pairing gate.
	// Channels MUST call this before Emit and act on the decision:
	//   - GateInboundAllow:        forward to Emit.
	//   - GateInboundDrop:         silently discard (do NOT log content).
	//   - GateInboundPairConsumed: silently discard; the broker has
	//     already mutated state (allowlist + pairing window).
	// See internal/broker/pairing.go for the policy.
	GateInbound(in *c3types.Inbound) GateInboundDecision

	// HandleCommand lets the channel hand a recognized bot command (e.g.
	// "/status") to the broker for direct handling. Returns the reply text and
	// handled=true when the broker owns the command — the channel then sends the
	// reply itself and MUST NOT gate, emit, queue, or route the message. Returns
	// ("", false) for anything the broker does not handle (the channel proceeds
	// with normal gating/routing). The command is broker-handled, not agent
	// input.
	HandleCommand(in *c3types.Inbound) (reply string, handled bool)
}

// GateInboundDecision mirrors broker.GateDecision but lives at the channel
// boundary so internal/channel doesn't import internal/broker.
type GateInboundDecision int

const (
	GateInboundAllow GateInboundDecision = iota
	GateInboundDrop
	GateInboundPairConsumed
)
