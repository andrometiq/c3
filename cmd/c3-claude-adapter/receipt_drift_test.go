package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/sessionhandoff"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func appendReceiptDriftRecord(t *testing.T, path, line string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString(line + "\n")
	if closeErr := f.Close(); err != nil || closeErr != nil {
		t.Fatalf("append: %v %v", err, closeErr)
	}
}

func TestReceiptDriftConsecutiveLiveExpiries(t *testing.T) {
	isolateAdapterTest(t)
	a, _, frames, _ := negotiatedAdapter(t)
	a.receiptDiagnostics.HostVersion = "2.1.266"
	var logs safeBuffer
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })
	expire := func(grew bool, record string) {
		size, _ := transcriptOffset(a.livePath())
		o := &deliveryObserver{path: a.livePath(), offset: size, receipt: receiptObservation{size: size}, written: true, attempt: "channel:1", deadline: time.Now().Add(-time.Second)}
		a.deliveryObservers = map[string]*deliveryObserver{"T": o}
		if grew {
			appendReceiptDriftRecord(t, a.livePath(), record)
		}
		a.pollDeliveries()
		if len(a.deliveryObservers) != 0 {
			t.Fatal("expiry changed")
		}
	}
	unknown := `{"type":"future-intake","content":"private text"}`
	expire(true, unknown)
	expire(true, unknown)
	if a.receiptDiagnostics.Drift {
		t.Fatal("alarmed before three")
	}
	expire(false, "") // no transcript progress resets the streak
	expire(true, unknown)
	expire(true, unknown)
	if a.receiptDiagnostics.Drift {
		t.Fatal("nonconsecutive expiry alarm")
	}
	// A known receipt discovered after expiry is diagnostic only: no confirm.
	expire(true, `{"type":"user","message":{"role":"user","content":"<channel c3_delivery_id=\"T\" c3_attempt=\"channel:1\">body</channel>"}}`)
	noLiveFrame(t, frames)
	if a.receiptDiagnostics.Consecutive != 0 {
		t.Fatal("recognized record did not reset")
	}
	expire(true, unknown)
	expire(true, unknown)
	hint := ipc.ReceiptShapeHint("2.1.266")
	if strings.Contains(string(logs.Bytes()), hint) {
		t.Fatal("drift hint logged after only two expiries")
	}
	// A successful fetch is not a live attempt and does not break this streak.
	a.receiptConfirmed("user/tool_result")
	expire(true, unknown)
	if !a.receiptDiagnostics.Drift || strings.Count(string(logs.Bytes()), hint) != 1 {
		t.Fatalf("third expiry did not immediately log the drift hint: %s", logs.Bytes())
	}
	expire(true, unknown)
	if strings.Count(string(logs.Bytes()), hint) != 1 || strings.Contains(string(logs.Bytes()), "private text") {
		t.Fatalf("logs=%s", logs.Bytes())
	}
	a.publishReceiptDrift()
	var report ipc.DeliveryReportMsg
	raw := nextLiveFrame(t, frames)
	if json.Unmarshal(raw, &report) != nil || report.ReceiptShapeDrift != "2.1.266" || report.Op != ipc.OpDeliveryReport {
		t.Fatalf("report=%s", raw)
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	if _, ok := fields["live"]; ok {
		t.Fatal("diagnostic changed eligibility")
	}
	a.publishReceiptDrift()
	noLiveFrame(t, frames)
	// Socket/negotiation changes retain the once-per-session alarm.
	a.acceptDelivery(a.deliveryOffer(), &ipc.DeliveryAcceptance{Version: 1, Modes: []string{"channel"}})
	if a.receiptDriftHost() != "2.1.266" {
		t.Fatal("reconnect lost alarm")
	}
	a.receiptDiagnostics.Consecutive = 2
	a.receiptUnconfirmed(receiptObservation{grew: true}, false)
	if a.receiptDiagnostics.Consecutive != 0 {
		t.Fatal("transport failure did not break streak")
	}
}

func TestReceiptFirstConfirmedRecord(t *testing.T) {
	isolateAdapterTest(t)
	fixture, err := os.ReadFile("testdata/claude-2.1.266-intake.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(fixture)), "\n")
	for _, tc := range []struct{ kind, line string }{
		{"queue-operation/enqueue", lines[0]},
		{"attachment/queued_command", lines[3]},
		{"user", `{"type":"user","message":{"role":"user","content":"<channel c3_delivery_id=\"DELIVERYTOKEN-1\" c3_attempt=\"channel:1\">body</channel>"}}`},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			a, _, frames, _ := negotiatedAdapter(t)
			a.receiptDiagnostics.HostVersion = "2.1.266"
			var logs safeBuffer
			previous := log.Writer()
			log.SetOutput(&logs)
			defer log.SetOutput(previous)
			for n := 0; n < 2; n++ {
				size, _ := transcriptOffset(a.livePath())
				a.deliveryObservers = map[string]*deliveryObserver{"DELIVERYTOKEN-1": {path: a.livePath(), offset: size, receipt: receiptObservation{size: size}, written: true, attempt: "channel:1", deadline: time.Now().Add(time.Second)}}
				appendReceiptDriftRecord(t, a.livePath(), tc.line)
				a.receiptDiagnostics.Consecutive = 2
				a.pollDeliveries()
				if a.receiptDiagnostics.Consecutive != 0 {
					t.Fatal("confirmation did not break expiry streak")
				}
				var result ipc.AttemptResultMsg
				raw := nextLiveFrame(t, frames)
				if json.Unmarshal(raw, &result) != nil || result.Outcome != "confirmed" {
					t.Fatalf("result=%s", raw)
				}
			}
			want := "first delivery confirmed by record=" + tc.kind + " (host 2.1.266)"
			if strings.Count(string(logs.Bytes()), want) != 1 {
				t.Fatalf("logs=%s", logs.Bytes())
			}
		})
	}
}

func TestReceiptDriftLegacyAndSessionReset(t *testing.T) {
	isolateAdapterTest(t)
	a, _, frames := liveFixture(t, ipc.RenderQueueOnly)
	a.receiptDiagnostics = receiptDiagnostics{HostVersion: "2.1.266", Drift: true, FirstRecord: "user", Consecutive: 3}
	before := a.liveRoute()
	a.publishReceiptDrift()
	var report ipc.RenderStateMsg
	raw := nextLiveFrame(t, frames)
	if json.Unmarshal(raw, &report) != nil || report.Op != ipc.OpRenderState || report.State != "" || a.liveRoute() != before || report.ReceiptShapeDrift != "2.1.266" {
		t.Fatalf("legacy=%s", raw)
	}
	a.currentStableID = "old"
	a.setCurrentStableIdentity(sessionhandoff.Entry{StableSessionID: "new"})
	if a.receiptDiagnostics.Drift || a.receiptDiagnostics.FirstRecord != "" || a.receiptDiagnostics.Consecutive != 0 {
		t.Fatal("new session inherited diagnostics")
	}
	if a.receiptDiagnostics.HostVersion != "2.1.266" {
		t.Fatal("lost host version")
	}
}

func TestReceiptHostVersionFromInitialize(t *testing.T) {
	isolateAdapterTest(t)
	a := newAdapter()
	serverTx, clientTx := mcp.NewInMemoryTransports()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	session, err := a.buildMCPServer().Connect(ctx, serverTx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "claude-code", Version: "2.1.266"}, nil)
	peer, err := client.Connect(ctx, clientTx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	a.liveMu.Lock()
	version := a.receiptDiagnostics.HostVersion
	a.liveMu.Unlock()
	if version != "2.1.266" {
		t.Fatalf("version=%q", version)
	}
}

func TestReceiptOutcomeRetriesDoNotResetNewerStreak(t *testing.T) {
	isolateAdapterTest(t)
	a := newAdapter()
	o := &deliveryObserver{result: "failed"}
	a.recordDeliveryReceiptOutcome(o, false)
	a.receiptDiagnostics.Consecutive = 2
	a.recordDeliveryReceiptOutcome(o, true)
	if a.receiptDiagnostics.Consecutive != 2 {
		t.Fatal("old outcome reset a newer expiry streak")
	}
}

func TestReceiptHostVersionFromOlderResumePayload(t *testing.T) {
	isolateAdapterTest(t)
	state := upgradeResumeState{Contract: upgradeContract(), MCP: mcp.ServerSessionState{InitializeParams: &mcp.InitializeParams{ClientInfo: &mcp.Implementation{Version: "2.1.266"}}, InitializedParams: &mcp.InitializedParams{}}}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(upgradeResumeEnv, base64.StdEncoding.EncodeToString(raw))
	a := newAdapter()
	if _, err := a.restoreUpgrade([]string{"--mcp-resume"}); err != nil {
		t.Fatal(err)
	}
	if a.receiptDiagnostics.HostVersion != "2.1.266" {
		t.Fatal("older resume lost available host version")
	}
}
