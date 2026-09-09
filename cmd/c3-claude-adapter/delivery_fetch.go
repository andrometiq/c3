package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type deliveryFetchObserver struct {
	conn                             *ipc.Conn
	path, toolUseID, token           string
	members                          []ipc.FetchReceiptMember
	offset                           int64
	deadline                         time.Time
	discarding, confirmed, reporting bool
}

func (a *adapter) confirmDeliveryAcceptance(conn *ipc.Conn, ack *ipc.DeliveryAcceptance) error {
	if !a.deliveryAccepted.Load() {
		return nil
	}
	var modes []string
	for _, mode := range []string{"channel", "inbox", "fetch_receipt"} {
		if ack.HasMode(mode) {
			modes = append(modes, mode)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return conn.WriteJSONContext(ctx, struct {
		Op       ipc.Op   `json:"op"`
		Accepted []string `json:"accepted"`
	}{ipc.OpDeliveryReport, modes})
}

func (a *adapter) deliveryFetchEligible() bool {
	if a.deliveryHostReason() != "" {
		return false
	}
	_, readable := transcriptOffset(a.livePath())
	return readable
}

func (a *adapter) fetchReceiptAccepted() bool {
	a.liveMu.Lock()
	defer a.liveMu.Unlock()
	return a.deliveryAccepted.Load() && a.deliveryFetchAccepted
}

func (a *adapter) toolAttemptFetch(ctx context.Context, req *mcp.CallToolRequest, fq ipc.FetchQueueReq) (*mcp.CallToolResult, error) {
	conn := a.currentConn()
	if conn == nil {
		return toolErrorResult("broker reconnecting — retry fetch_queue in a moment"), nil
	}
	var observer *deliveryFetchObserver
	if fq.Ack {
		id, _ := req.Params.Meta["claudecode/toolUseId"].(string)
		path := a.livePath()
		offset, readable := transcriptOffset(path)
		if id == "" || !readable {
			return toolErrorResult("fetch receipt observation unavailable; retry or use ack:false to peek"), nil
		}
		a.liveMu.Lock()
		if len(a.deliveryFetchObservers)+a.deliveryFetchPreparing >= maxLiveReadbacks {
			a.liveMu.Unlock()
			return toolErrorResult("fetch receipt observation limit reached; retry later"), nil
		}
		a.deliveryFetchPreparing++
		a.liveMu.Unlock()
		defer func() { a.liveMu.Lock(); a.deliveryFetchPreparing--; a.liveMu.Unlock() }()
		observer = &deliveryFetchObserver{conn: conn, path: path, offset: offset, toolUseID: id, deadline: time.Now().Add(60 * time.Second)}
		fq.Lease = json.RawMessage("true")
	}
	ch := make(chan ipc.FetchQueueResp, 1)
	a.fqmu.Lock()
	a.fqPending[fq.ID] = ch
	a.fqmu.Unlock()
	defer func() { a.fqmu.Lock(); delete(a.fqPending, fq.ID); a.fqmu.Unlock() }()
	if err := conn.WriteJSONContext(ctx, fq); err != nil {
		return toolErrorResult("fetch broker write failed"), nil
	}
	select {
	case <-ctx.Done():
		return toolErrorResult("canceled"), nil
	case <-time.After(60 * time.Second):
		return toolErrorResult("fetch_queue timeout"), nil
	case resp := <-ch:
		if resp.Err != "" {
			return toolErrorResult(resp.Err), nil
		}
		text := a.renderFetchedMessages(resp.Messages, resp.Remaining, a.currentTopicName())
		if observer != nil && len(resp.Messages) > 0 {
			if resp.LeaseToken == "" || len(resp.Members) != len(resp.Messages) || !fetchReceiptTrailerMatches(resp.ReceiptTrailer, resp.LeaseToken, resp.Members) {
				return toolErrorResult("fetch receipt trailer invalid; rows remain queued until reservation expiry"), nil
			}
			observer.token, observer.members = resp.LeaseToken, slices.Clone(resp.Members)
			a.liveMu.Lock()
			if conn != a.currentConn() || !a.deliveryFetchAccepted {
				a.liveMu.Unlock()
				return toolErrorResult("broker reconnected during fetch; retry"), nil
			}
			if a.deliveryFetchObservers == nil {
				a.deliveryFetchObservers = map[string]*deliveryFetchObserver{}
			}
			a.deliveryFetchObservers[observer.token] = observer
			a.liveMu.Unlock()
			text += "\n\n" + resp.ReceiptTrailer
		}
		return toolTextResult(text), nil
	}
}

// Run on the existing shared receipt ticker. Fetch observers are bound to the
// original socket and never adopted or resent after a reconnect.
func (a *adapter) pollFetchReceipts() {
	a.liveMu.Lock()
	var reports []*deliveryFetchObserver
	for token, o := range a.deliveryFetchObservers {
		if o.conn != a.currentConn() || !time.Now().Before(o.deadline) {
			delete(a.deliveryFetchObservers, token)
			continue
		}
		if !o.confirmed {
			a.liveScanMu.Lock()
			o.offset, o.confirmed = scanReceiptRecords(o.path, o.offset, &o.discarding, func(line []byte) bool { return fetchToolReceipt(line, o.toolUseID, o.token, o.members) })
			a.liveScanMu.Unlock()
		}
		if o.confirmed && !o.reporting && time.Now().Before(o.deadline) {
			o.reporting = true
			reports = append(reports, o)
		}
	}
	a.liveMu.Unlock()
	for _, o := range reports {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := o.conn.WriteJSONContext(ctx, ipc.AttemptResultMsg{Op: ipc.OpAttemptResult, Token: o.token, Outcome: "confirmed"})
		cancel()
		a.liveMu.Lock()
		o.reporting = false
		if err == nil && a.deliveryFetchObservers[o.token] == o {
			delete(a.deliveryFetchObservers, o.token)
		}
		a.liveMu.Unlock()
	}
}

// Parse only a final, complete trailer. Body formatting and member order may
// vary, but duplicate, missing, unexpected or malformed members never confirm.
func fetchReceiptTrailerMatches(text, token string, expected []ipc.FetchReceiptMember) bool {
	start := strings.LastIndex(text, ipc.FetchReceiptStart+"\n")
	if start < 0 || (start > 0 && text[start-1] != '\n') || token == "" || len(expected) == 0 {
		return false
	}
	lines := strings.Split(text[start:], "\n")
	if len(lines) != len(expected)+3 || lines[1] != "group "+token || lines[len(lines)-1] != ipc.FetchReceiptEnd {
		return false
	}
	want := map[ipc.FetchReceiptMember]bool{}
	ids := map[string]bool{}
	for _, m := range expected {
		if !receiptAtom(m.RecordID) || !receiptRevision(m.Revision) || ids[m.RecordID] {
			return false
		}
		ids[m.RecordID], want[m] = true, true
	}
	for _, line := range lines[2 : len(lines)-1] {
		parts := strings.Split(line, " ")
		if len(parts) != 3 || parts[0] != "member" {
			return false
		}
		m := ipc.FetchReceiptMember{RecordID: parts[1], Revision: parts[2]}
		if !want[m] {
			return false
		}
		delete(want, m)
	}
	return len(want) == 0 && receiptAtom(token)
}

func receiptRevision(s string) bool {
	if len(s) != 64 || s != strings.ToLower(s) {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func (a *adapter) renderDeliveryBacklog(count int, items []ipc.QueuedItem, route string) string {
	text := renderBacklogSummary(count, items, route)
	if a.fetchReceiptAccepted() {
		if count == 0 {
			return "0 message(s) fetchable now."
		}
		text = strings.Replace(text, "were held while no session was attached", "are fetchable now", 1)
	}
	return text
}

func receiptAtom(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func fetchToolReceipt(line []byte, toolUseID, token string, expected []ipc.FetchReceiptMember) bool {
	if toolUseID == "" {
		return false
	}
	var entry struct {
		Type    string `json:"type"`
		Message struct {
			Content []json.RawMessage `json:"content"`
		} `json:"message"`
	}
	if json.Unmarshal(line, &entry) != nil || entry.Type != "user" {
		return false
	}
	for _, raw := range entry.Message.Content {
		var block struct {
			Type    string          `json:"type"`
			ID      string          `json:"tool_use_id"`
			IsError json.RawMessage `json:"is_error"`
			Content json.RawMessage `json:"content"`
		}
		if json.Unmarshal(raw, &block) != nil || block.Type != "tool_result" || block.ID != toolUseID || (len(block.IsError) != 0 && string(block.IsError) != "false") {
			continue
		}
		var text string
		if json.Unmarshal(block.Content, &text) != nil {
			var parts []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			}
			if json.Unmarshal(block.Content, &parts) != nil {
				continue
			}
			var texts []string
			valid := true
			for _, part := range parts {
				if part.Type != "text" {
					valid = false
					break
				}
				texts = append(texts, part.Text)
			}
			if !valid {
				continue
			}
			text = strings.Join(texts, "\n")
		}
		if fetchReceiptTrailerMatches(text, token, expected) {
			return true
		}
	}
	return false
}
