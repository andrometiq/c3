package main

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func fetchMembers() []ipc.FetchReceiptMember {
	return []ipc.FetchReceiptMember{{RecordID: "row-1", Revision: strings.Repeat("a", 64)}, {RecordID: "row-2", Revision: strings.Repeat("b", 64)}}
}

func fetchTranscriptResult(id string, content any, isError any) []byte {
	raw, _ := json.Marshal(map[string]any{"type": "user", "message": map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": id, "is_error": isError, "content": content}}}})
	return append(raw, '\n')
}

func TestFetchTrailerGrammarAndToolResult(t *testing.T) {
	isolateAdapterTest(t)
	members := fetchMembers()
	trailer := ipc.FetchReceiptTrailer("group-1", members)
	for _, tc := range []struct {
		name, text string
		want       bool
	}{
		{"complete", "body\n\n" + trailer, true},
		{"presentation", "different body and whitespace\n" + trailer, true},
		{"reordered", ipc.FetchReceiptTrailer("group-1", []ipc.FetchReceiptMember{members[1], members[0]}), true},
		{"truncated", strings.TrimSuffix(trailer, ipc.FetchReceiptEnd), false},
		{"missing", ipc.FetchReceiptTrailer("group-1", members[:1]), false},
		{"wrong-token", ipc.FetchReceiptTrailer("other", members), false},
		{"extra-text", trailer + "\nextra", false},
		{"final-newline", trailer + "\n", false},
		{"embedded-opener", "quoted " + trailer, false},
		{"duplicate", ipc.FetchReceiptTrailer("group-1", []ipc.FetchReceiptMember{members[0], members[0]}), false},
		{"changed-revision", strings.Replace(trailer, members[0].Revision, strings.Repeat("c", 64), 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := fetchReceiptTrailerMatches(tc.text, "group-1", members); got != tc.want {
				t.Fatalf("trailer=%v want=%v", got, tc.want)
			}
			for _, content := range []any{tc.text, []any{map[string]any{"type": "text", "text": tc.text}}} {
				raw := fetchTranscriptResult("tool-1", content, false)
				if got := fetchToolReceipt(raw, "tool-1", "group-1", members); got != tc.want {
					t.Fatalf("receipt=%v want=%v", got, tc.want)
				}
				if fetchToolReceipt(raw, "other-tool", "group-1", members) || fetchToolReceipt(fetchTranscriptResult("tool-1", content, true), "tool-1", "group-1", members) {
					t.Fatal("unbound or error result confirmed")
				}
			}
		})
	}
	for _, bad := range []any{"false", true, 42, nil} {
		if fetchToolReceipt(fetchTranscriptResult("tool-1", trailer, bad), "tool-1", "group-1", members) {
			t.Fatal("invalid is_error confirmed")
		}
	}
	raw := fetchTranscriptResult("tool-1", trailer, false)
	if fetchToolReceipt([]byte(strings.Replace(string(raw), `"type":"user"`, `"type":"assistant"`, 1)), "tool-1", "group-1", members) {
		t.Fatal("assistant quote confirmed")
	}
}

func TestFetchReceiptToolObservationAndReconnect(t *testing.T) {
	isolateAdapterTest(t)
	for _, event := range []string{"confirmed", "incomplete", "error", "reconnect", "expired", "peek", "missing-id"} {
		t.Run(event, func(t *testing.T) {
			a, _, frames, ctx := negotiatedAdapter(t)
			a.acceptDelivery(a.deliveryOffer(), &ipc.DeliveryAcceptance{Version: 1, Modes: []string{"fetch_receipt"}})
			// Poll synchronously; no goroutine per receipt.
			request := &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Arguments: json.RawMessage(`{"all":true}`)}}
			request.Params.Meta = map[string]any{"claudecode/toolUseId": "tool-1"}
			if event == "missing-id" {
				request.Params.Meta = nil
			}
			if event == "peek" {
				request.Params.Arguments = json.RawMessage(`{"ack":false}`)
			}
			results := make(chan *mcp.CallToolResult, 1)
			go func() { r, _ := a.toolFetchQueue(ctx, request); results <- r }()
			if event == "missing-id" {
				if r := <-results; !r.IsError {
					t.Fatal("reservation without host correlation")
				}
				select {
				case raw := <-frames:
					t.Fatal("missing id sent fetch", string(raw))
				default:
				}
				return
			}
			raw := nextLiveFrame(t, frames)
			var fq ipc.FetchQueueReq
			if json.Unmarshal(raw, &fq) != nil || fq.Op != ipc.OpFetchQueue || (string(fq.Lease) == "true") != (event != "peek") {
				t.Fatal(string(raw))
			}
			members := fetchMembers()
			resp := ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: fq.ID, Messages: []c3types.Inbound{{Text: "one"}, {Text: "two"}}}
			if event != "peek" {
				resp.LeaseToken = "group-1"
				resp.Members = members
				resp.ReceiptTrailer = ipc.FetchReceiptTrailer(resp.LeaseToken, members)
			}
			raw, _ = json.Marshal(resp)
			a.dispatchFetchQueueResult(raw)
			result := <-results
			if result.IsError {
				t.Fatal(result)
			}
			text := result.Content[0].(*mcp.TextContent).Text
			if event == "peek" {
				if strings.Contains(text, ipc.FetchReceiptStart) || len(a.deliveryFetchObservers) != 0 {
					t.Fatal("peek reserved")
				}
				return
			}
			if !strings.HasSuffix(text, resp.ReceiptTrailer) {
				t.Fatal("trailer is not final")
			}
			if event == "incomplete" {
				text = strings.Replace(text, "member "+members[1].RecordID+" "+members[1].Revision+"\n", "", 1)
			}
			if err := os.WriteFile(a.livePath(), fetchTranscriptResult("tool-1", text, event == "error"), 0600); err != nil {
				t.Fatal(err)
			}
			if event == "reconnect" {
				peer, client := net.Pipe()
				t.Cleanup(func() { peer.Close(); client.Close() })
				a.bmu.Lock()
				a.conn = ipc.NewConn(client)
				a.bmu.Unlock()
			}
			if event == "expired" {
				a.deliveryFetchObservers[resp.LeaseToken].deadline = time.Now().Add(-time.Second)
			}
			before := a.liveRoute()
			a.pollFetchReceipts()
			if event == "confirmed" {
				raw = nextLiveFrame(t, frames)
				if string(raw) != `{"op":"attempt_result","token":"group-1","outcome":"confirmed","reason":""}` {
					t.Fatal(string(raw))
				}
			} else {
				select {
				case raw := <-frames:
					t.Fatal("invalid result confirmed", string(raw))
				default:
				}
			}
			if a.liveRoute() != before {
				t.Fatal("fetch changed display")
			}
			a.pollFetchReceipts()
			select {
			case raw := <-frames:
				t.Fatal("duplicate confirmation", string(raw))
			default:
			}
		})
	}
}

func TestDeliveryAcceptanceConfirmationWire(t *testing.T) {
	isolateAdapterTest(t)
	a, _, frames, _ := negotiatedAdapter(t)
	ack := &ipc.DeliveryAcceptance{Version: 1, Modes: []string{"future", "fetch_receipt", "channel"}}
	a.acceptDelivery(a.deliveryOffer(), ack)
	if err := a.confirmDeliveryAcceptance(a.currentConn(), ack); err != nil {
		t.Fatal(err)
	}
	raw := nextLiveFrame(t, frames)
	if string(raw) != `{"op":"delivery_report","accepted":["channel","fetch_receipt"]}` {
		t.Fatal(string(raw))
	}
}

func TestFetchReceiptReaderOversizeAndTruncation(t *testing.T) {
	isolateAdapterTest(t)
	path := filepath.Join(t.TempDir(), "transcript.jsonl")
	trailer := ipc.FetchReceiptTrailer("group-1", fetchMembers())
	valid := fetchTranscriptResult("tool-1", trailer, false)
	raw := append([]byte(strings.Repeat(" ", 65<<20)), valid...)
	raw = append(raw, valid[:len(valid)-1]...)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	offset, discarding := int64(0), false
	match := func(line []byte) bool { return fetchToolReceipt(line, "tool-1", "group-1", fetchMembers()) }
	for i := 0; i < 3; i++ {
		var found bool
		offset, found = scanReceiptRecords(path, offset, &discarding, match)
		if found {
			t.Fatal("fragment confirmed")
		}
		if i < 2 && !discarding {
			t.Fatal("discard state lost")
		}
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("\n")
	f.Close()
	if _, found := scanReceiptRecords(path, offset, &discarding, match); !found {
		t.Fatal("complete result not confirmed")
	}
	// Truncation resets both offset and discard state using the live semantics.
	if err := os.WriteFile(path, valid, 0600); err != nil {
		t.Fatal(err)
	}
	discarding = true
	if _, found := scanReceiptRecords(path, offset, &discarding, match); !found || discarding {
		t.Fatal("truncation did not reset reader")
	}
}

func TestFetchLegacyBrokerKeepsResponse(t *testing.T) {
	isolateAdapterTest(t)
	a, _, frames, ctx := negotiatedAdapter(t)
	// G1: channel-only acceptance still uses baseline consume and rendering.
	result := make(chan *mcp.CallToolResult, 1)
	go func() {
		r, _ := a.toolFetchQueue(ctx, &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Arguments: json.RawMessage(`{}`)}})
		result <- r
	}()
	raw := nextLiveFrame(t, frames)
	var req ipc.FetchQueueReq
	json.Unmarshal(raw, &req)
	if len(req.Lease) != 0 || !req.Ack {
		t.Fatal(string(raw))
	}
	resp := ipc.FetchQueueResp{Op: ipc.OpFetchQueueResult, ID: req.ID, Messages: []c3types.Inbound{{Text: "held"}}}
	raw, _ = json.Marshal(resp)
	a.dispatchFetchQueueResult(raw)
	r := <-result
	if got := r.Content[0].(*mcp.TextContent).Text; got != a.renderFetchedMessages(resp.Messages, 0, "") {
		t.Fatal(got)
	}
	if len(a.deliveryFetchObservers) != 0 {
		t.Fatal("legacy fetch registered a receipt")
	}
}
