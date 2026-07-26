package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/Andrometiq/c3/internal/ipc"
)

func askRequest(t *testing.T, question string, options []string) *mcp.CallToolRequest {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"question": question, "options": options})
	if err != nil {
		t.Fatalf("marshal args: %v", err)
	}
	return &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Arguments: raw}}
}

// askRequestRaw builds an ask CallToolRequest from an arbitrary args map (so a
// test can set multi / allow_skip / free_text / allow_other).
func askRequestRaw(t *testing.T, args map[string]any) *mcp.CallToolRequest {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatalf("marshal args: %v", err)
	}
	return &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Arguments: raw}}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

func resultText(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if res == nil || len(res.Content) == 0 {
		t.Fatal("empty tool result")
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content[0] is %T, want *mcp.TextContent", res.Content[0])
	}
	return tc.Text
}

// TestRenderAskAnswer covers every branch of the answer renderer (FIX-2). The
// load-bearing case is multi-select with comma-bearing options: the bulleted,
// one-per-line rendering keeps them unambiguously separable, where the old
// ", " join did not.
func TestRenderAskAnswer(t *testing.T) {
	cases := []struct {
		name string
		ans  ipc.AskAnswer
		want string
	}{
		{"single", ipc.AskAnswer{Selected: []string{"B"}}, "B"},
		{"single-with-comma", ipc.AskAnswer{Selected: []string{"Red, large"}}, "Red, large"},
		{"multi", ipc.AskAnswer{Selected: []string{"A", "C"}}, "Selected:\n• A\n• C"},
		{"multi-comma", ipc.AskAnswer{Selected: []string{"Red, large", "Blue"}}, "Selected:\n• Red, large\n• Blue"},
		{"multi-empty", ipc.AskAnswer{Selected: nil}, "Selected: (none)"},
		{"skipped", ipc.AskAnswer{Skipped: true}, "(skipped)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := renderAskAnswer(tc.ans); got != tc.want {
				t.Fatalf("renderAskAnswer(%+v) = %q, want %q", tc.ans, got, tc.want)
			}
		})
	}
	if got := renderAskAnswer(ipc.AskAnswer{TimedOut: true}); !strings.Contains(strings.ToLower(got), "timed out") {
		t.Fatalf("timeout render = %q, want it to report a timeout", got)
	}
}

// TestAskTool_ReturnsTappedAnswer drives the full adapter-side blocking flow over
// a net.Pipe fake broker: toolAsk sends an AskRegisterReq, the broker acks
// OK, then pushes the tapped answer as an OpAskResult — and the tool returns that
// answer string ("B") as its result.
func TestAskTool_ReturnsTappedAnswer(t *testing.T) {
	a, peer := adapterWithConn(t)

	resultCh := make(chan *mcp.CallToolResult, 1)
	go func() {
		res, _ := a.toolAsk(context.Background(), askRequest(t, "Pick one", []string{"A", "B", "C"}))
		resultCh <- res
	}()

	// The tool must send an AskRegisterReq carrying the question + options + askID.
	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("read ask_register frame: %v", err)
	}
	var reg ipc.AskRegisterReq
	if err := json.Unmarshal(raw, &reg); err != nil {
		t.Fatalf("unmarshal ask_register: %v", err)
	}
	if reg.Op != ipc.OpAskRegister {
		t.Fatalf("op = %q, want %q", reg.Op, ipc.OpAskRegister)
	}
	if reg.Question != "Pick one" || len(reg.Options) != 3 {
		t.Fatalf("unexpected ask_register payload: %+v", reg)
	}
	if reg.AskID == "" {
		t.Fatal("ask_register must carry a generated ask id")
	}

	// Broker acks registration OK, then pushes the tapped answer "B".
	a.dispatchAskRegistered(mustMarshal(t, ipc.AskRegisteredMsg{
		Op: ipc.OpAskRegistered, AskID: reg.AskID, OK: true, MessageID: 99,
	}))
	a.dispatchAskResult(mustMarshal(t, ipc.AskResultMsg{
		Op: ipc.OpAskResult, AskID: reg.AskID, Answer: ipc.AskAnswer{Selected: []string{"B"}},
	}))

	select {
	case res := <-resultCh:
		if res.IsError {
			t.Fatalf("unexpected tool error: %q", resultText(t, res))
		}
		if got := resultText(t, res); got != "B" {
			t.Fatalf("tool result = %q, want %q", got, "B")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ask tool did not return after the answer was pushed")
	}
}

// TestAskTool_Timeout: once registered, if no answer is pushed within the answer
// timeout the tool returns a (non-error) result that reports the timeout so the
// agent can recover.
func TestAskTool_Timeout(t *testing.T) {
	prev := askAnswerTimeout
	askAnswerTimeout = 30 * time.Millisecond
	defer func() { askAnswerTimeout = prev }()

	a, peer := adapterWithConn(t)

	resultCh := make(chan *mcp.CallToolResult, 1)
	go func() {
		res, _ := a.toolAsk(context.Background(), askRequest(t, "Pick one", []string{"A", "B"}))
		resultCh <- res
	}()

	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("read ask_register frame: %v", err)
	}
	var reg ipc.AskRegisterReq
	if err := json.Unmarshal(raw, &reg); err != nil {
		t.Fatalf("unmarshal ask_register: %v", err)
	}

	// Ack registration OK but NEVER push an answer → the tool must time out.
	a.dispatchAskRegistered(mustMarshal(t, ipc.AskRegisteredMsg{
		Op: ipc.OpAskRegistered, AskID: reg.AskID, OK: true,
	}))

	select {
	case res := <-resultCh:
		got := strings.ToLower(resultText(t, res))
		if !strings.Contains(got, "timed out") {
			t.Fatalf("timeout result = %q, want it to report a timeout", resultText(t, res))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ask tool did not return on timeout")
	}
}

// TestAskTool_BailsFastOnRegisterFailure: an OK=false registration ack (e.g.
// ask-before-attach / oversized keyboard) must return the broker's error as the
// tool result immediately, without waiting for the answer timeout.
func TestAskTool_BailsFastOnRegisterFailure(t *testing.T) {
	a, peer := adapterWithConn(t)

	resultCh := make(chan *mcp.CallToolResult, 1)
	go func() {
		res, _ := a.toolAsk(context.Background(), askRequest(t, "Pick one", []string{"A", "B"}))
		resultCh <- res
	}()

	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("read ask_register frame: %v", err)
	}
	var reg ipc.AskRegisterReq
	if err := json.Unmarshal(raw, &reg); err != nil {
		t.Fatalf("unmarshal ask_register: %v", err)
	}

	a.dispatchAskRegistered(mustMarshal(t, ipc.AskRegisteredMsg{
		Op: ipc.OpAskRegistered, AskID: reg.AskID, OK: false, Err: "ask before attach: no route claimed",
	}))

	select {
	case res := <-resultCh:
		if !res.IsError {
			t.Fatalf("a failed registration must be an in-band tool error; got non-error %q", resultText(t, res))
		}
		if !strings.Contains(resultText(t, res), "ask before attach") {
			t.Fatalf("error result = %q, want it to carry the broker's reason", resultText(t, res))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ask tool did not bail fast on a failed registration")
	}
}

// TestAskTool_RequiresOptions: an ask without options is rejected locally with a
// clear error (no broker round-trip) — free-text questions are not yet supported.
func TestAskTool_RequiresOptions(t *testing.T) {
	a, _ := adapterWithConn(t)
	res, _ := a.toolAsk(context.Background(), askRequest(t, "Free text?", nil))
	if !res.IsError {
		t.Fatalf("ask without options must be an error; got %q", resultText(t, res))
	}
}

// TestAskTool_MultiSelect_ReturnsList: an ask with multi:true forwards the flag to
// the broker and returns the broker's selected list rendered as an unambiguous
// bulleted list (FIX-2 — one option per line, so options that contain commas stay
// separable; a plain ", " join could not be re-split by the agent).
func TestAskTool_MultiSelect_ReturnsList(t *testing.T) {
	a, peer := adapterWithConn(t)

	resultCh := make(chan *mcp.CallToolResult, 1)
	go func() {
		res, _ := a.toolAsk(context.Background(), askRequestRaw(t, map[string]any{
			"question": "Pick some", "options": []any{"A", "B", "C"}, "multi": true,
		}))
		resultCh <- res
	}()

	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("read ask_register frame: %v", err)
	}
	var reg ipc.AskRegisterReq
	if err := json.Unmarshal(raw, &reg); err != nil {
		t.Fatalf("unmarshal ask_register: %v", err)
	}
	if !reg.Multi {
		t.Fatal("multi:true must be forwarded to the broker in AskRegisterReq")
	}

	a.dispatchAskRegistered(mustMarshal(t, ipc.AskRegisteredMsg{
		Op: ipc.OpAskRegistered, AskID: reg.AskID, OK: true, MessageID: 5,
	}))
	a.dispatchAskResult(mustMarshal(t, ipc.AskResultMsg{
		Op: ipc.OpAskResult, AskID: reg.AskID, Answer: ipc.AskAnswer{Selected: []string{"A", "C"}},
	}))

	select {
	case res := <-resultCh:
		if res.IsError {
			t.Fatalf("unexpected tool error: %q", resultText(t, res))
		}
		if got, want := resultText(t, res), "Selected:\n• A\n• C"; got != want {
			t.Fatalf("multi result = %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ask tool did not return after the multi answer was pushed")
	}
}

// TestAskTool_Skip_ReturnsSkipped: an ask with allow_skip:true forwards the flag
// and, when the broker pushes Skipped, the tool reports a skip to the agent.
func TestAskTool_Skip_ReturnsSkipped(t *testing.T) {
	a, peer := adapterWithConn(t)

	resultCh := make(chan *mcp.CallToolResult, 1)
	go func() {
		res, _ := a.toolAsk(context.Background(), askRequestRaw(t, map[string]any{
			"question": "Pick or skip", "options": []any{"A", "B"}, "allow_skip": true,
		}))
		resultCh <- res
	}()

	raw, err := peer.ReadFrame()
	if err != nil {
		t.Fatalf("read ask_register frame: %v", err)
	}
	var reg ipc.AskRegisterReq
	if err := json.Unmarshal(raw, &reg); err != nil {
		t.Fatalf("unmarshal ask_register: %v", err)
	}
	if !reg.AllowSkip {
		t.Fatal("allow_skip:true must be forwarded to the broker in AskRegisterReq")
	}

	a.dispatchAskRegistered(mustMarshal(t, ipc.AskRegisteredMsg{
		Op: ipc.OpAskRegistered, AskID: reg.AskID, OK: true,
	}))
	a.dispatchAskResult(mustMarshal(t, ipc.AskResultMsg{
		Op: ipc.OpAskResult, AskID: reg.AskID, Answer: ipc.AskAnswer{Skipped: true},
	}))

	select {
	case res := <-resultCh:
		if res.IsError {
			t.Fatalf("unexpected tool error: %q", resultText(t, res))
		}
		if got := strings.ToLower(resultText(t, res)); !strings.Contains(got, "skip") {
			t.Fatalf("skip result = %q, want it to report a skip", resultText(t, res))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ask tool did not return after the skip was pushed")
	}
}

// TestAskTool_RejectsUnsupported: free_text / allow_other are explicitly NOT yet
// supported; requesting either returns a local tool error without a broker
// round-trip (so the agent learns it must use single/multi-select).
func TestAskTool_RejectsUnsupported(t *testing.T) {
	a, _ := adapterWithConn(t)

	res, _ := a.toolAsk(context.Background(), askRequestRaw(t, map[string]any{
		"question": "Anything?", "options": []any{"A"}, "free_text": true,
	}))
	if !res.IsError {
		t.Fatalf("free_text must be rejected as not-yet-supported; got %q", resultText(t, res))
	}

	res2, _ := a.toolAsk(context.Background(), askRequestRaw(t, map[string]any{
		"question": "Anything?", "options": []any{"A"}, "allow_other": true,
	}))
	if !res2.IsError {
		t.Fatalf("allow_other must be rejected as not-yet-supported; got %q", resultText(t, res2))
	}
}
