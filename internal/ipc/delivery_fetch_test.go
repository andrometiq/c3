package ipc

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

func TestFetchReceiptWireGolden(t *testing.T) {
	members := []FetchReceiptMember{{RecordID: "row-1", Revision: strings.Repeat("a", 64)}}
	trailer := "[C3_FETCH_RECEIPT_V1]\ngroup group-1\nmember row-1 " + strings.Repeat("a", 64) + "\n[/C3_FETCH_RECEIPT_V1]"
	if FetchReceiptTrailer("group-1", members) != trailer {
		t.Fatal("trailer grammar changed")
	}
	for _, tc := range []struct {
		value any
		want  string
	}{
		{FetchQueueReq{Op: OpFetchQueue, ID: "q", Ack: true, Lease: json.RawMessage("true")}, `{"op":"fetch_queue","id":"q","ack":true,"lease":true}`},
		{DeliveryReportMsg{Op: OpDeliveryReport, Accepted: []string{}}, `{"op":"delivery_report","live":{"channel":{"eligible":false},"inbox":{"eligible":false}},"accepted":[]}`},
		{FetchConfirmReq{Op: OpFetchConfirm, LeaseToken: "group-1"}, `{"op":"fetch_confirm","lease_token":"group-1"}`},
		{AttemptResultMsg{Op: OpAttemptResult, Token: "group-1", Outcome: "confirmed"}, `{"op":"attempt_result","token":"group-1","outcome":"confirmed","reason":""}`},
		{FetchQueueResp{Op: OpFetchQueueResult, ID: "q", Messages: []c3types.Inbound{{Text: "hello"}}, LeaseToken: "group-1", Members: members, ReceiptTrailer: trailer}, `{"op":"fetch_queue_result","id":"q","messages":[{"Channel":"","ChatID":0,"TopicID":null,"MessageID":0,"Sender":{"UserID":0,"Username":""},"Text":"hello","Attachments":null,"ReplyTo":null,"Timestamp":"0001-01-01T00:00:00Z"}],"remaining":0,"lease_token":"group-1","members":[{"record_id":"row-1","revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}],"receipt_trailer":"[C3_FETCH_RECEIPT_V1]\ngroup group-1\nmember row-1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n[/C3_FETCH_RECEIPT_V1]"}`},
	} {
		raw, err := json.Marshal(tc.value)
		if err != nil || string(raw) != tc.want {
			t.Fatalf("wire=%s err=%v want=%s", raw, err, tc.want)
		}
	}
}
