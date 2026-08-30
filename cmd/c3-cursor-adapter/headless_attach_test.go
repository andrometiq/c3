package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func setCursorAncestorCommandLines(t *testing.T, commandLines [][]string) {
	t.Helper()
	previous := cursorAncestorCommandLines
	cursorAncestorCommandLines = func() [][]string { return commandLines }
	t.Cleanup(func() { cursorAncestorCommandLines = previous })
}

func callAttachAndAnswer(t *testing.T, a *adapter, broker *recoveryBroker) {
	t.Helper()
	t.Setenv("CURSOR_CONVERSATION_ID", "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	request := newRecoveryAttachReq(t, map[string]any{"name": "review-topic"})
	go func() {
		_, err := a.toolAttach(ctx, request)
		done <- err
	}()
	if op, ok := broker.nextOp(t, time.Second); !ok || op != "attach" {
		t.Fatalf("attach did not proceed (op=%q ok=%v)", op, ok)
	}
	driveAttached(t, a)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("toolAttach: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("toolAttach did not return after broker response")
	}
}

func TestHeadlessCursorRunDetection(t *testing.T) {
	tests := []struct {
		name        string
		commandLine []string
		headless    bool
	}{
		{name: "print", commandLine: []string{"cursor-agent", "--print", "review"}, headless: true},
		{name: "short print", commandLine: []string{"/usr/bin/cursor", "-p", "review"}, headless: true},
		{name: "output format", commandLine: []string{"cursor-agent", "--output-format", "json"}, headless: true},
		{name: "output format equals", commandLine: []string{"cursor", "--output-format=json"}, headless: true},
		{name: "interactive", commandLine: []string{"cursor-agent", "review"}, headless: false},
		{name: "other command", commandLine: []string{"reviewer", "--print"}, headless: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setCursorAncestorCommandLines(t, [][]string{{"shell"}, test.commandLine})
			got, commandLine := headlessCursorRun()
			if got != test.headless {
				t.Fatalf("headlessCursorRun() = (%v, %q), want headless=%v", got, commandLine, test.headless)
			}
			if len(commandLine) > 200 {
				t.Fatalf("reported command line is %d bytes, want at most 200", len(commandLine))
			}
		})
	}

	setCursorAncestorCommandLines(t, [][]string{{"cursor-agent", "--print", strings.Repeat("x", 300)}})
	if headless, commandLine := headlessCursorRun(); !headless || len(commandLine) != 200 {
		t.Fatalf("long command = (headless=%v, bytes=%d), want (true, 200)", headless, len(commandLine))
	}
}

func TestToolAttachRefusesHeadlessCursorRunWithoutBrokerFrame(t *testing.T) {
	t.Setenv("C3_ALLOW_HEADLESS_ATTACH", "")
	setCursorAncestorCommandLines(t, [][]string{{"cursor-agent", "--print", "review"}})
	sink := captureProtoLog(t)
	a, broker := newRecoveryAdapter(t)
	t.Setenv("CURSOR_CONVERSATION_ID", "")

	result, err := a.toolAttach(context.Background(), newRecoveryAttachReq(t, map[string]any{
		"expr":     "create review-topic",
		"name":     "review-topic",
		"topic_id": float64(17),
		"group":    "review-group",
		"create":   true,
		"steal":    true,
	}))
	if err != nil {
		t.Fatalf("toolAttach: %v", err)
	}
	if !result.IsError {
		t.Fatalf("headless attach result IsError=false: %+v", result)
	}
	text := textBlocks(t, result)
	if len(text) != 1 || text[0] != headlessAttachDisabledMessage {
		t.Fatalf("headless attach text = %#v, want exact %q", text, headlessAttachDisabledMessage)
	}
	if op, ok := broker.nextOp(t, 150*time.Millisecond); ok {
		t.Fatalf("headless attach wrote broker frame %q", op)
	}
	logText := strings.TrimSpace(sink.String())
	if !strings.Contains(logText, headlessAttachDisabledMessage) || strings.Count(logText, "\n") != 0 {
		t.Fatalf("headless attach must log exactly one refusal line; log was:\n%s", sink.String())
	}
}

func TestToolAttachAllowedOutsideGuard(t *testing.T) {
	tests := []struct {
		name          string
		commandLines  [][]string
		allowHeadless bool
	}{
		{name: "explicit override", commandLines: [][]string{{"cursor-agent", "--print", "review"}}, allowHeadless: true},
		{name: "interactive Cursor", commandLines: [][]string{{"cursor-agent", "review"}}},
		{name: "no readable ancestors", commandLines: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.allowHeadless {
				t.Setenv("C3_ALLOW_HEADLESS_ATTACH", "1")
			} else {
				t.Setenv("C3_ALLOW_HEADLESS_ATTACH", "")
			}
			setCursorAncestorCommandLines(t, test.commandLines)
			a, broker := newRecoveryAdapter(t)
			callAttachAndAnswer(t, a, broker)
		})
	}
}

func TestRecoverAutoAttachSkippedForHeadlessCursorRun(t *testing.T) {
	t.Setenv("C3_ALLOW_HEADLESS_ATTACH", "")
	setCursorAncestorCommandLines(t, [][]string{{"cursor-agent", "--output-format", "json"}})
	sink := captureProtoLog(t)
	a, broker := newRecoveryAdapter(t)

	a.trySessionRecover(context.Background())
	waitForLog(t, sink, "recover-session: "+headlessAttachDisabledMessage)
	if op, ok := broker.nextOp(t, 150*time.Millisecond); ok {
		t.Fatalf("headless recovery wrote broker frame %q", op)
	}
	select {
	case <-a.identityGate():
	case <-time.After(time.Second):
		t.Fatal("skipped headless recovery did not settle the identity gate")
	}
}

func TestAttachGuidanceWarnsHeadlessRuns(t *testing.T) {
	const warning = "A headless run must not attach unless the operator explicitly allowed it."
	a := newAdapter()
	if instructions := a.buildInstructions(); !strings.Contains(instructions, warning) {
		t.Fatalf("MCP instructions omit headless attach warning: %q", instructions)
	}

	server := a.buildMCPServer()
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = server.Run(ctx, serverTransport) }()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.0"}, nil)
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}
	defer session.Close()
	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	for _, tool := range tools.Tools {
		if tool.Name == "attach" {
			if !strings.Contains(tool.Description, warning) {
				t.Fatalf("attach description omits headless warning: %q", tool.Description)
			}
			return
		}
	}
	t.Fatal("attach tool not registered")
}
