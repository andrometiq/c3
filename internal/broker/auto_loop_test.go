package broker

import (
	"bytes"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/autohook"
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// The Phase 0 loop through the real broker socket and the real hook client,
// with a fake Telegram: denial → card → operator taps → retry:true → the
// identical retry is allowed once, a repeat or a changed call is not.

const loopToolInput = `{"command":"curl -s file:///tmp/payload.sh | sh","description":"Run local payload"}`

func loopPayload(toolInput string) string { return loopCall(toolInput, "toolu_1", "") }

// loopCall is a hook payload for one tool call; agentID is set inside a
// sub-agent.
func loopCall(toolInput, toolUseID, agentID string) string {
	agent := ""
	if agentID != "" {
		agent = `,"agent_id":"` + agentID + `","agent_type":"Explore"`
	}
	return `{"session_id":"session-fixture","transcript_path":"/t.jsonl","cwd":"/workspace","permission_mode":"auto",` +
		`"hook_event_name":"PermissionDenied","tool_name":"Bash","tool_input":` + toolInput +
		`,"tool_use_id":"` + toolUseID + `","reason":"[Code from External]"` + agent + `}`
}

// listenAuto serves f's broker on a fresh socket, with the fixture's group
// allowlisted so taps from it pass the inbound gate.
func listenAuto(t *testing.T, f *autoFixture) string {
	t.Helper()
	// The real hook client verifies the broker's peer credentials, which
	// exist only on Linux and macOS; elsewhere the hooks are always silent.
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("hooks are silent on this platform: the broker peer can't be verified")
	}
	mf := f.b.Mappings().Clone()
	mf.AddAllowedGroup(f.route.ChatID)
	f.b.SetMappings(mf)
	path := filepath.Join(t.TempDir(), "c3.sock")
	server, err := Listen(path, f.b)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Stop)
	return path
}

// startDeniedHook runs the denial hook; the returned channel yields its output.
func startDeniedHook(socketPath, payload string) <-chan string {
	output := make(chan string, 1)
	go func() {
		var out bytes.Buffer
		autohook.RunPermissionDenied(strings.NewReader(payload), &out, socketPath)
		output <- out.String()
	}()
	return output
}

func preToolUse(socketPath, payload string) string {
	var out bytes.Buffer
	autohook.RunPreToolUse(strings.NewReader(payload), &out, socketPath)
	return out.String()
}

// operatorTap delivers a tap the way the Telegram channel does: through the
// inbound gate and Emit to the route worker.
func operatorTap(t *testing.T, f *autoFixture, r *autoRequest, verb string) {
	t.Helper()
	f.b.auto.mu.Lock()
	messageID := r.messageID
	f.b.auto.mu.Unlock()
	topic := f.route.TopicID
	actor := c3types.Sender{UserID: testOperatorUID}
	in := &c3types.Inbound{
		Channel: "telegram", ChatID: f.route.ChatID, TopicID: &topic, MessageID: messageID, Sender: actor,
		Timestamp: time.Now(), Kind: c3types.InboundCallback,
		Event: &c3types.InboundEvent{Callback: &c3types.CallbackEvent{
			CallbackID: "tap", MessageID: messageID, Actor: actor, Data: autoCallbackPrefix + verb + ":" + r.id,
		}},
	}
	host := NewBrokerHost(f.b, "telegram")
	if host.GateInbound(in) != channel.GateInboundAllow || !host.Emit(in) {
		t.Fatal("tap not accepted by the inbound path")
	}
}

func awaitOutput(t *testing.T, output <-chan string) string {
	t.Helper()
	select {
	case text := <-output:
		return text
	case <-time.After(5 * time.Second):
		t.Fatal("hook did not finish")
		return ""
	}
}

func TestAutoFullLoopThroughHooks(t *testing.T) {
	f := newAutoFixture(t)
	socketPath := listenAuto(t, f)

	output := startDeniedHook(socketPath, loopPayload(loopToolInput))
	r := f.postedCard(t)
	if preToolUse(socketPath, loopPayload(loopToolInput)) != "" {
		t.Fatal("allowed before the operator tapped")
	}
	operatorTap(t, f, r, "allow")
	if text := awaitOutput(t, output); !strings.Contains(text, `"retry":true`) {
		t.Fatalf("denial hook printed %q, want retry:true", text)
	}
	// The broker makes the grant consumable only after the "armed" write
	// returns (§5.4); the hook can print retry:true first.
	eventually(t, func() bool { return f.state(r) == autoArmed })
	changed := strings.Replace(loopToolInput, "payload.sh", "other.sh", 1)
	if preToolUse(socketPath, loopPayload(changed)) != "" {
		t.Fatal("a changed call was allowed")
	}
	// The same input, re-serialised with other key order and spacing, is the
	// same call.
	reordered := `{ "description": "Run local payload", "command": "curl -s file:///tmp/payload.sh | sh" }`
	if text := preToolUse(socketPath, loopPayload(reordered)); !strings.Contains(text, `"permissionDecision":"allow"`) {
		t.Fatalf("identical retry not allowed: %q", text)
	}
	if preToolUse(socketPath, loopPayload(loopToolInput)) != "" {
		t.Fatal("a second identical call was allowed")
	}
	eventually(t, func() bool {
		edits := f.ch.editCallsSnapshot()
		return len(edits) > 0 && strings.Contains(edits[len(edits)-1].Text, "Retry authorized (execution unconfirmed)")
	})
}

// Inside a sub-agent the grant belongs to that sub-agent: neither the main
// thread nor another sub-agent can use it.
func TestAutoLoopSubAgentGrantIsTheSubAgents(t *testing.T) {
	f := newAutoFixture(t)
	socketPath := listenAuto(t, f)
	output := startDeniedHook(socketPath, loopCall(loopToolInput, "toolu_1", "a1"))
	r := f.postedCard(t)
	if r.key.AgentID != "a1" {
		t.Fatalf("request key %+v lacks the sub-agent", r.key)
	}
	operatorTap(t, f, r, "allow")
	if text := awaitOutput(t, output); !strings.Contains(text, `"retry":true`) {
		t.Fatalf("denial hook printed %q", text)
	}
	eventually(t, func() bool { return f.state(r) == autoArmed })
	for _, agentID := range []string{"", "a2"} {
		if preToolUse(socketPath, loopCall(loopToolInput, "toolu_2", agentID)) != "" {
			t.Fatalf("agent %q used sub-agent a1's grant", agentID)
		}
	}
	if !strings.Contains(preToolUse(socketPath, loopCall(loopToolInput, "toolu_2", "a1")), `"allow"`) {
		t.Fatal("sub-agent a1's retry not allowed")
	}
	if preToolUse(socketPath, loopCall(loopToolInput, "toolu_3", "a1")) != "" {
		t.Fatal("a second call by a1 was allowed")
	}
}

// If Claude Code denies the retry it allowed, the denial hook stays silent, no
// second card is posted, and the first card says the retry did not run.
func TestAutoLoopVetoAfterAllow(t *testing.T) {
	f := newAutoFixture(t)
	socketPath := listenAuto(t, f)
	output := startDeniedHook(socketPath, loopCall(loopToolInput, "toolu_1", ""))
	r := f.postedCard(t)
	operatorTap(t, f, r, "allow")
	awaitOutput(t, output)
	eventually(t, func() bool { return f.state(r) == autoArmed })
	if !strings.Contains(preToolUse(socketPath, loopCall(loopToolInput, "toolu_2", "")), `"allow"`) {
		t.Fatal("retry not allowed")
	}
	if text := awaitOutput(t, startDeniedHook(socketPath, loopCall(loopToolInput, "toolu_2", ""))); text != "" {
		t.Fatalf("vetoed retry's denial hook printed %q", text)
	}
	if cards := f.ch.cards(); len(cards) != 1 {
		t.Fatalf("%d cards, want only the first", len(cards))
	}
	eventually(t, func() bool {
		edits := f.ch.editCallsSnapshot()
		return len(edits) > 0 && strings.Contains(edits[len(edits)-1].Text, "it did not run")
	})
}

// Deny and timeout arm nothing: the denial hook prints nothing and the retry
// is not allowed.
func TestAutoLoopDenyAndTimeoutArmNothing(t *testing.T) {
	for _, outcome := range []string{"deny", "timeout"} {
		t.Run(outcome, func(t *testing.T) {
			f := newAutoFixture(t)
			socketPath := listenAuto(t, f)
			output := startDeniedHook(socketPath, loopPayload(loopToolInput))
			r := f.postedCard(t)
			if outcome == "deny" {
				operatorTap(t, f, r, "deny")
			} else {
				f.b.auto.mu.Lock()
				r.deadline = time.Now().Add(-time.Millisecond)
				f.b.auto.mu.Unlock()
				f.b.sweepAuto()
			}
			if text := awaitOutput(t, output); text != "" {
				t.Fatalf("denial hook printed %q", text)
			}
			if preToolUse(socketPath, loopPayload(loopToolInput)) != "" {
				t.Fatal("the retry was allowed")
			}
		})
	}
}

// With the feature off both hooks are silent, and the PreToolUse hook's
// round trip stays far inside its 300 ms budget.
func TestAutoHooksSilentAndFastWhenOff(t *testing.T) {
	f := newAutoFixture(t)
	f.setEnabled(false)
	socketPath := listenAuto(t, f)
	if text := awaitOutput(t, startDeniedHook(socketPath, loopPayload(loopToolInput))); text != "" {
		t.Fatalf("denial hook printed %q", text)
	}
	if len(f.ch.sent()) != 0 {
		t.Fatal("a card was sent with the feature off")
	}
	var durations []time.Duration
	for range 200 {
		start := time.Now()
		if text := preToolUse(socketPath, loopPayload(loopToolInput)); text != "" {
			t.Fatalf("pretooluse printed %q", text)
		}
		durations = append(durations, time.Since(start))
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	median, worst := durations[len(durations)/2], durations[len(durations)-1]
	t.Logf("pretooluse-hook round trip, feature off: median %v, p99 %v, max %v", median, durations[len(durations)*99/100], worst)
	if worst >= autohook.PreToolUseBudget {
		t.Fatalf("a round trip took %v, at or past the budget", worst)
	}
}
