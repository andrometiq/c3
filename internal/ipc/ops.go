// Package ipc defines the wire types for broker ↔ adapter communication
// over the unix socket at $XDG_RUNTIME_DIR/c3.sock (or /tmp/c3-$UID.sock).
//
// Schema reference: docs/specs/2026-05-08-c3-rearch-design.md §4.4.1.
package ipc

// Op is the op-code present on every IPC message. Adapters and broker
// dispatch on Op.
type Op string

const (
	// adapter → broker
	OpHello           Op = "hello"
	OpServerInfo      Op = "server_info"
	OpToolsList       Op = "tools_list"
	OpAttach          Op = "attach"
	OpRelease         Op = "release"
	OpListTopics      Op = "list_topics"
	OpListClaims      Op = "list_claims"
	OpListHealth      Op = "list_health"
	OpToolCall        Op = "tool_call"
	OpPairModeStart   Op = "pair_mode_start"
	OpPingThisSession Op = "ping_this_session"
	OpListSessions    Op = "list_sessions"
	OpFetchQueue      Op = "fetch_queue"
	// OpObserve asks the broker to PEEK a topic's durable queue READ-ONLY,
	// resolving the topic by name/target/topic_id WITHOUT claiming it and
	// WITHOUT mutating any route. It is the no-loss-safe, no-steal counterpart
	// to fetch_queue(ack=false): the Desktop inbox panel uses it to display any
	// topic's inbox while a Claude Code session keeps the exclusive claim (and
	// keeps replying). Carries its own topic reference — it does NOT derive the
	// route from the stub's claim. Never consumes; safe to call on a timer.
	OpObserve          Op = "observe"
	OpInboundDelivered Op = "inbound_delivered"
	OpRetranscribe     Op = "retranscribe"
	OpRecoverSession   Op = "recover_session"
	// OpAskRegister registers a blocking, correlated `ask` (question + options)
	// for round-trip resolution. The answer is pushed back later as an
	// unsolicited OpAskResult once the human taps a button. Carries NO route —
	// the broker derives it from the stub's current claim.
	OpAskRegister Op = "ask_register"
	// OpPermissionRequest relays a Claude Code tool-use permission prompt
	// (default / acceptEdits mode) to the broker so it can surface an Allow/Deny
	// inline keyboard on the stub's claimed route. Carries NO route — the broker
	// derives it from the stub's current claim. Fire-and-forget: there is no
	// blocking tool to unblock, so the broker sends no synchronous ack (unlike
	// OpAskRegister). The verdict comes back later as OpPermissionVerdict.
	OpPermissionRequest Op = "permission_request"
	OpWebLoginLink      Op = "web_login_link"
	OpBye               Op = "bye"

	// broker → adapter
	OpHelloAck             Op = "hello_ack"
	OpAttached             Op = "attached"
	OpToolResult           Op = "tool_result"
	OpInbound              Op = "inbound"
	OpTopicsList           Op = "topics_list"
	OpClaimsList           Op = "claims_list"
	OpHealthList           Op = "health_list"
	OpPairModeReply        Op = "pair_mode_reply"
	OpPingThisSessionReply Op = "ping_this_session_reply"
	OpListSessionsReply    Op = "list_sessions_reply"
	OpFetchQueueResult     Op = "fetch_queue_result"
	// OpObserveResult is the broker's response to OpObserve: the peeked
	// messages plus the resolved topic identity and its CURRENT holder (who,
	// if anyone, owns the exclusive claim) — so the panel can show "held by
	// claude-code · read-only" and offer an explicit take-over.
	OpObserveResult        Op = "observe_result"
	OpRetranscribeResult   Op = "retranscribe_result"
	OpRecoverSessionResult Op = "recover_session_result"
	// OpAskRegistered is the broker's SYNCHRONOUS ack to OpAskRegister: OK=true
	// once the question + keyboard was sent (with the sent MessageID), or
	// OK=false + Err on a fast failure (ask before attach, oversized keyboard,
	// channel/send error) so the tool call returns immediately rather than
	// blocking the full answer timeout.
	OpAskRegistered Op = "ask_registered"
	// OpAskResult is the broker's UNSOLICITED push carrying the human's answer to
	// a previously-registered ask (delivered to the route holder exactly like
	// OpInbound). Correlated to the originating tool call by AskID.
	OpAskResult Op = "ask_result"
	// OpPermissionVerdict is the broker's UNSOLICITED push of a human's Allow/Deny
	// verdict for a previously-relayed permission_request (delivered to the route
	// holder exactly like OpInbound). Correlated to the originating permission
	// prompt by RequestID; the adapter emits it into Claude Code as
	// notifications/claude/channel/permission. Fire-and-forget (no caller blocks
	// on it — a never-delivered verdict just leaves CC waiting in the TUI).
	OpPermissionVerdict Op = "permission_verdict"
	OpWebLoginLinkReply Op = "web_login_link_reply"
	OpError             Op = "error"
)
