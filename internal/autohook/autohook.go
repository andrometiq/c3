// Package autohook is the client side of auto-mode approval: the Claude Code
// PreToolUse and PermissionDenied hooks (`c3-broker pretooluse-hook` and
// `c3-broker permission-denied-hook`). Each runs as a short-lived process,
// reads the hook payload on stdin, makes one op on a transient broker
// connection and prints a decision only when the broker grants one.
//
// Every failure is silent: no output and exit 0, which leaves Claude Code's
// normal permission flow (or the denial) in place. The hooks never print
// updatedInput, so what they allow is exactly the call the operator saw.
package autohook

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/peercred"
)

// CLI is the CLI namespace the hooks report. The broker ops are CLI-agnostic;
// these hooks are Claude Code's.
const CLI = "claude"

const (
	// PreToolUseBudget bounds the PreToolUse hook's whole broker exchange
	// (dial, handshake, write, read). It runs on every tool call.
	PreToolUseBudget = 300 * time.Millisecond
	// PermissionDeniedBudget bounds the denial hook from launch, handshake
	// included (§5.4). hooks.json gives the hook 330 s.
	PermissionDeniedBudget = 320 * time.Second
)

// The only outputs the hooks can print. They are constants so no input can
// reach them, and neither carries updatedInput.
const (
	allowOutput = `{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"allow",` +
		`"permissionDecisionReason":"C3: approved once on Telegram"}}` + "\n"
	retryOutput = `{"hookSpecificOutput":{"hookEventName":"PermissionDenied","retry":true}}` + "\n"
)

// hookInput is the part of the hook payload the approval ops need. agent_id
// is present only inside a sub-agent.
type hookInput struct {
	SessionID string          `json:"session_id"`
	AgentID   string          `json:"agent_id"`
	CWD       string          `json:"cwd"`
	ToolName  string          `json:"tool_name"`
	ToolUseID string          `json:"tool_use_id"`
	ToolInput json.RawMessage `json:"tool_input"`
	Reason    string          `json:"reason"`
}

func (in hookInput) callContext() ipc.AutoCallContext {
	return ipc.AutoCallContext{CLI: CLI, SessionID: in.SessionID, AgentID: in.AgentID, CWD: in.CWD, ToolName: in.ToolName}
}

// hookEnv is what a hook run depends on besides its streams; tests replace it.
type hookEnv struct {
	budget     time.Duration
	now        func() time.Time
	verifyPeer func(net.Conn) error
}

func defaultEnv(budget time.Duration) hookEnv {
	return hookEnv{budget: budget, now: time.Now, verifyPeer: peercred.VerifySameUser}
}

// RunPreToolUse prints the allow decision only when the broker consumed an
// armed grant for this exact call within PreToolUseBudget. Any other outcome,
// a panic included, prints nothing.
func RunPreToolUse(stdin io.Reader, stdout io.Writer, socketPath string) {
	defer func() { _ = recover() }()
	runPreToolUse(stdin, stdout, socketPath, defaultEnv(PreToolUseBudget))
}

func runPreToolUse(stdin io.Reader, stdout io.Writer, socketPath string, env hookEnv) {
	deadline := env.now().Add(env.budget)
	in, ok := readHookInput(stdin)
	if !ok {
		return
	}
	// The broker keys grants on this hash; an input it would reject can't match.
	inputHash, err := ipc.ToolInputHash(in.ToolInput)
	if err != nil {
		return
	}
	conn, err := dialHook(socketPath, deadline, env)
	if err != nil {
		return
	}
	defer conn.close()
	request := ipc.GrantCheckReq{Op: ipc.OpGrantCheck, AutoCallContext: in.callContext(), ToolUseID: in.ToolUseID,
		InputHash: inputHash}
	if conn.WriteJSONContext(conn.ctx, request) != nil {
		return
	}
	frame, err := conn.ReadFrame()
	if err != nil {
		return
	}
	var response ipc.GrantCheckResp
	if ipc.DecodeStrict(frame, &response) != nil || response.Op != ipc.OpGrantCheckResult || !response.Allow {
		return
	}
	if !env.now().Before(deadline) {
		return
	}
	_, _ = io.WriteString(stdout, allowOutput)
}

// RunPermissionDenied reports the denial and holds the connection through the
// broker's handoff: "approved" (answered with an ack) then "armed", or a
// terminal "none". It prints retry:true only after "armed"; any other
// outcome, a panic included, prints nothing.
func RunPermissionDenied(stdin io.Reader, stdout io.Writer, socketPath string) {
	defer func() { _ = recover() }()
	runPermissionDenied(stdin, stdout, socketPath, defaultEnv(PermissionDeniedBudget))
}

func runPermissionDenied(stdin io.Reader, stdout io.Writer, socketPath string, env hookEnv) {
	deadline := env.now().Add(env.budget)
	in, ok := readHookInput(stdin)
	if !ok || !json.Valid(in.ToolInput) {
		return
	}
	conn, err := dialHook(socketPath, deadline, env)
	if err != nil {
		return
	}
	defer conn.close()
	request := ipc.AutoDeniedReq{
		Op:              ipc.OpAutoDenied,
		AutoCallContext: in.callContext(),
		ToolUseID:       in.ToolUseID,
		ToolInput:       in.ToolInput,
		Reason:          in.Reason,
		BudgetMS:        deadline.Sub(env.now()).Milliseconds(),
	}
	if conn.WriteJSONContext(conn.ctx, request) != nil {
		return
	}
	approvedID := ""
	for {
		frame, err := conn.ReadFrame()
		if err != nil {
			return
		}
		var decision ipc.AutoDecisionMsg
		if ipc.DecodeStrict(frame, &decision) != nil || decision.Op != ipc.OpAutoDecision {
			return
		}
		switch decision.State {
		case ipc.AutoDecisionApproved:
			if approvedID != "" || decision.RequestID == "" {
				return
			}
			approvedID = decision.RequestID
			conn.writeAck(approvedID)
		case ipc.AutoDecisionArmed:
			// A waiter that joined a deduplicated request can be told "armed"
			// without "approved" first.
			if approvedID != "" && decision.RequestID != approvedID {
				return
			}
			_, _ = io.WriteString(stdout, retryOutput)
			return
		default:
			return
		}
	}
}

// readHookInput strictly decodes the hook payload, capped at the IPC frame
// size since it is forwarded to the broker.
func readHookInput(stdin io.Reader) (hookInput, bool) {
	raw, err := io.ReadAll(io.LimitReader(stdin, ipc.MaxFrameSize+1))
	if err != nil || len(raw) > ipc.MaxFrameSize {
		return hookInput{}, false
	}
	var in hookInput
	if ipc.DecodeStrict(raw, &in) != nil || in.SessionID == "" || in.CWD == "" || in.ToolName == "" {
		return hookInput{}, false
	}
	return in, true
}

// hookConn is a transient hook connection whose reads and writes all end at
// one deadline.
type hookConn struct {
	*ipc.Conn
	raw      net.Conn
	deadline time.Time
	ctx      context.Context // carries the deadline for ipc writes
	cancel   context.CancelFunc
}

// dialHook dials the broker, checks that the peer runs as this user, and
// completes the hook_hello handshake, all before deadline. Unlike dialBroker
// in cmd/c3-broker, the handshake read is bounded. An unverified peer gets
// nothing: the payload carries the tool input and the session identity.
func dialHook(socketPath string, deadline time.Time, env hookEnv) (*hookConn, error) {
	dialer := net.Dialer{Deadline: deadline}
	raw, err := dialer.Dial("unix", socketPath)
	if err != nil {
		return nil, err
	}
	if err := env.verifyPeer(raw); err != nil {
		_ = raw.Close()
		return nil, err
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	conn := &hookConn{Conn: ipc.NewConn(raw), raw: raw, deadline: deadline, ctx: ctx, cancel: cancel}
	hello := ipc.HookHelloMsg{Op: ipc.OpHookHello, CLI: CLI, PID: os.Getpid(), ProtocolVersion: ipc.ProtocolVersion}
	if err := raw.SetReadDeadline(deadline); err != nil {
		conn.close()
		return nil, err
	}
	if err := conn.WriteJSONContext(ctx, hello); err != nil {
		conn.close()
		return nil, err
	}
	frame, err := conn.ReadFrame()
	if err != nil {
		conn.close()
		return nil, err
	}
	if op, err := ipc.PeekOp(frame); err != nil || op != ipc.OpHelloAck {
		conn.close()
		return nil, errors.New("autohook: no hello ack")
	}
	return conn, nil
}

// writeAck sends the ack best effort, straight to the socket: a failed
// ipc write would close the connection, and the next frame must still be
// read. The broker may have armed the grant through another waiter and still
// send "armed".
func (c *hookConn) writeAck(requestID string) {
	frame, err := json.Marshal(ipc.AutoAckMsg{Op: ipc.OpAutoAck, RequestID: requestID})
	if err != nil {
		return
	}
	_ = c.raw.SetWriteDeadline(c.deadline)
	_, _ = c.raw.Write(append(frame, '\n'))
}

func (c *hookConn) close() {
	c.cancel()
	_ = c.Close()
}
