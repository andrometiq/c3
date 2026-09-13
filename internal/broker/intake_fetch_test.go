package broker

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/intake"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

func (s *Stub) setIntakeActiveForTest() { s.intakeActive.Store(true) }

func intakeFetchSource() *intake.Source {
	sender := int64(8)
	return &intake.Source{Channel: "acme", ChatID: -100, MessageID: 9007199254740993,
		SenderID: &sender, UpdateID: 9007199254740995, Text: "raw",
		Attachments: []intake.SourceAttachment{{Kind: "voice", FileID: "audio", Size: 123, MIME: "audio/ogg", Name: "note.ogg"}}}
}

const intakeInboundGolden = `{"Channel":"","ChatID":0,"TopicID":null,"MessageID":0,"Sender":{"UserID":0,"Username":""},"Text":"hello","Attachments":null,"ReplyTo":null,"Timestamp":"0001-01-01T00:00:00Z"}`
const intakeMemberGolden = `{"Channel":"","ChatID":0,"TopicID":null,"MessageID":0,"Sender":{"UserID":0,"Username":""},"Text":"hello","Attachments":null,"ReplyTo":null,"Timestamp":"0001-01-01T00:00:00Z","intake":{"source":{"channel":"acme","chat_id":"-100","topic_id":null,"message_id":"9007199254740993","sender_id":"8","update_id":"9007199254740995","text":"raw","attachments":[{"kind":"voice","file_id":"audio","size":123,"mime":"audio/ogg","name":"note.ogg"}]},"attachments_state":[{"index":0,"stt":"done","transcript":"spoken"}]}}`

func intakeFetchWire(t *testing.T, b *Broker, s *Stub, req ipc.FetchQueueReq) []byte {
	t.Helper()
	peer, conn := newConnPair(t)
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	go b.handleFetchQueue(conn, s, raw)
	return nextWireOp(t, peer, ipc.OpFetchQueueResult)
}

func assertNoPrivateIntake(t *testing.T, raw []byte) {
	t.Helper()
	for _, key := range []string{`"_c3_source"`, `"_c3_attachments_state"`} {
		if strings.Contains(string(raw), key) {
			t.Fatalf("private key %s on wire", key)
		}
	}
}

func intakeResponseWire(t *testing.T, response fetchQueueResponse) []byte {
	t.Helper()
	peer, conn := newConnPair(t)
	written := make(chan error, 1)
	go func() { written <- conn.WriteJSON(response) }()
	raw := nextWireOp(t, peer, ipc.OpFetchQueueResult)
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestIntakeFetchReceiptWireGolden(t *testing.T) {
	row := queue.TrackedInbound{Inbound: c3types.Inbound{Text: "hello"}, Source: intakeFetchSource(),
		AttachmentsState: intake.AttachmentsState{{Index: 0, STT: intake.STTDone, Transcript: "spoken"}}}
	members := []ipc.FetchReceiptMember{{RecordID: "row-1", Revision: strings.Repeat("a", 64)}}
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprint(active), func(t *testing.T) {
			resp := fetchQueueResponse{FetchQueueResp: attemptFetchResponse("q", "group-1", []c3types.Inbound{row.Inbound}, members, 0)}
			if active {
				resp.intakeMessages = []intakeFetchMessage{intakeMessage(row)}
			}
			want := `{"op":"fetch_queue_result","id":"q","messages":[{"Channel":"","ChatID":0,"TopicID":null,"MessageID":0,"Sender":{"UserID":0,"Username":""},"Text":"hello","Attachments":null,"ReplyTo":null,"Timestamp":"0001-01-01T00:00:00Z"}],"remaining":0,"lease_token":"group-1","members":[{"record_id":"row-1","revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}],"receipt_trailer":"[C3_FETCH_RECEIPT_V1]\ngroup group-1\nmember row-1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n[/C3_FETCH_RECEIPT_V1]"}`
			if active {
				want = `{"op":"fetch_queue_result","id":"q","messages":[{"Channel":"","ChatID":0,"TopicID":null,"MessageID":0,"Sender":{"UserID":0,"Username":""},"Text":"hello","Attachments":null,"ReplyTo":null,"Timestamp":"0001-01-01T00:00:00Z","intake":{"source":{"channel":"acme","chat_id":"-100","topic_id":null,"message_id":"9007199254740993","sender_id":"8","update_id":"9007199254740995","text":"raw","attachments":[{"kind":"voice","file_id":"audio","size":123,"mime":"audio/ogg","name":"note.ogg"}]},"attachments_state":[{"index":0,"stt":"done","transcript":"spoken"}]}}],"remaining":0,"lease_token":"group-1","members":[{"record_id":"row-1","revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}],"receipt_trailer":"[C3_FETCH_RECEIPT_V1]\ngroup group-1\nmember row-1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n[/C3_FETCH_RECEIPT_V1]"}`
			}
			raw := intakeResponseWire(t, resp)
			if string(raw) != want {
				t.Fatalf("wire=%s want=%s", raw, want)
			}
			assertNoPrivateIntake(t, raw)
		})
	}
}

func TestIntakeFetchGateAndEmptyMetadata(t *testing.T) {
	for _, active := range []bool{false, true} {
		for _, ack := range []bool{false, true} {
			t.Run(fmt.Sprintf("active=%v/ack=%v", active, ack), func(t *testing.T) {
				clearFetchTestEnvironment(t)
				b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
				t.Cleanup(b.Shutdown)
				key := MakeRouteKey("telegram", -100, nil)
				s, _ := negotiatedHolder(t, b, key, 1)
				receiptHolder(s)
				if active {
					s.setIntakeActiveForTest()
				}
				in := &c3types.Inbound{Text: "hello"}
				if _, err := b.Queue.AppendTrackedIntake(queueRouteKey(key), in, intakeFetchSource(),
					intake.AttachmentsState{{Index: 0, STT: intake.STTDone, Transcript: "spoken"}}); err != nil {
					t.Fatal(err)
				}
				if _, err := b.Queue.AppendTracked(queueRouteKey(key), in); err != nil {
					t.Fatal(err)
				}
				raw := intakeFetchWire(t, b, s, ipc.FetchQueueReq{ID: "q", All: true, Ack: ack, Lease: json.RawMessage("true")})
				var resp struct {
					Messages []json.RawMessage `json:"messages"`
				}
				if err := json.Unmarshal(raw, &resp); err != nil || len(resp.Messages) != 2 {
					t.Fatalf("wire=%s err=%v", raw, err)
				}
				want := []string{strings.TrimSuffix(intakeInboundGolden, "}") + `,"V":1}`, strings.TrimSuffix(intakeInboundGolden, "}") + `,"V":1}`}
				if active {
					want[0] = strings.Replace(intakeMemberGolden, `,"intake":`, `,"V":1,"intake":`, 1)
					want[1] = strings.TrimSuffix(want[1], "}") + `,"intake":{"source":null,"attachments_state":[]}}`
				} else if strings.Contains(string(raw), `"intake"`) {
					t.Fatalf("inactive connection received intake: %s", raw)
				}
				for i := range want {
					if string(resp.Messages[i]) != want[i] {
						t.Fatalf("message=%s want=%s", resp.Messages[i], want[i])
					}
				}
				if !ack {
					golden := `{"op":"fetch_queue_result","id":"q","messages":[` + strings.Join(want, ",") + `],"remaining":0}`
					if string(raw) != golden {
						t.Fatalf("wire=%s want=%s", raw, golden)
					}
				}
				var legacy ipc.FetchQueueResp
				if err := json.Unmarshal(raw, &legacy); err != nil || legacy.Messages[0].Text != "hello" || legacy.Remaining != 0 {
					t.Fatalf("legacy decode=%+v err=%v", legacy, err)
				}
				if ack && (len(legacy.Members) != 2 || legacy.ReceiptTrailer != ipc.FetchReceiptTrailer(legacy.LeaseToken, legacy.Members)) {
					t.Fatalf("receipt envelope=%+v", legacy)
				}
				if b.Queue.StatusFor(queueRouteKey(key)).Pending != 2 {
					t.Fatal("unconfirmed fetch removed rows")
				}
				assertNoPrivateIntake(t, raw)
			})
		}
	}
}

func TestIntakeFetchFrameFitAcrossRoutes(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(fmt.Sprint(active), func(t *testing.T) {
			clearFetchTestEnvironment(t)
			b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
			t.Cleanup(b.Shutdown)
			key := MakeRouteKey("telegram", -100, nil)
			s, _ := negotiatedHolder(t, b, key, 1)
			receiptHolder(s)
			if active {
				s.setIntakeActiveForTest()
			}
			other := MakeRouteKey("telegram", -100, ptrI64Val(2))
			s.AddRoute(other)
			s.MarkRouteConfirmed(other)
			b.Routes.Claim(other, s)
			for i, route := range []RouteKey{key, other} {
				for j := 0; j < 3; j++ {
					source := intakeFetchSource()
					source.UpdateID += int64(i*3 + j)
					source.Text = strings.Repeat("s", 600000)
					states := intake.AttachmentsState{{Index: 0, STT: intake.STTDone, Transcript: strings.Repeat("t", 200000)}}
					if _, err := b.Queue.AppendTrackedIntake(queueRouteKey(route), &c3types.Inbound{Text: strings.Repeat("x", 100000)}, source, states); err != nil {
						t.Fatal(err)
					}
				}
			}
			raw := intakeFetchWire(t, b, s, ipc.FetchQueueReq{ID: "q", Ack: true, All: true, Lease: json.RawMessage("true")})
			var resp ipc.FetchQueueResp
			if err := json.Unmarshal(raw, &resp); err != nil || resp.Err != "" || len(raw)+1 > ipc.MaxFrameSize {
				t.Fatalf("bytes=%d response error=%s decode=%v", len(raw), resp.Err, err)
			}
			want := 6
			if active {
				want = 4
			}
			if len(resp.Messages) != want || len(resp.Members) != want || resp.Remaining != 6-want {
				t.Fatalf("messages=%d members=%d remaining=%d want=%d", len(resp.Messages), len(resp.Members), resp.Remaining, want)
			}
			reserved := 0
			for _, a := range b.attempts.lookup(resp.LeaseToken, time.Now()) {
				reserved += len(a.Members)
			}
			if reserved != want {
				t.Fatalf("reserved=%d sent=%d", reserved, want)
			}
			var wire struct {
				Messages []struct {
					Intake *ipc.IntakeMetadata `json:"intake"`
				} `json:"messages"`
			}
			if err := json.Unmarshal(raw, &wire); err != nil {
				t.Fatal(err)
			}
			for _, msg := range wire.Messages {
				if active && (msg.Intake == nil || msg.Intake.Source == nil || len(msg.Intake.Source.Text) != 600000 || len(msg.Intake.AttachmentsState) != 1 || len(msg.Intake.AttachmentsState[0].Transcript) != 200000) {
					t.Fatal("intake missing or truncated on wire")
				}
				if !active && msg.Intake != nil {
					t.Fatal("inactive fetch carried intake")
				}
			}
			for _, route := range []RouteKey{key, other} {
				if b.Queue.StatusFor(queueRouteKey(route)).Pending != 3 {
					t.Fatal("unconfirmed frame removed rows")
				}
			}
			assertNoPrivateIntake(t, raw)
		})
	}
}

func TestIntakeFetchNilOwner(t *testing.T) {
	clearFetchTestEnvironment(t)
	_, w, _, _, ctx := negotiatedFixture(t)
	negotiatedAppend(t, w, 1, "hello")
	var owner *Stub
	if owner.intakeMetadataActive() || (&Stub{}).intakeMetadataActive() {
		t.Fatal("intake defaults active")
	}
	ch := make(chan FetchResult, 1)
	w.handleFetch(ctx, &FetchJob{All: true, ResultCh: ch})
	r := <-ch
	if r.Err != nil || len(r.Messages) != 1 || r.intakeMessages != nil {
		t.Fatalf("internal peek=%+v", r)
	}
}

func TestIntakeRowRevisionStateAndBaseline(t *testing.T) {
	row := queue.TrackedInbound{Inbound: c3types.Inbound{Text: "hello"}, RecordID: "row-1",
		Source: intakeFetchSource(), AttachmentsState: intake.AttachmentsState{{Index: 0, STT: intake.STTPending}}}
	pending := rowRevision(row)
	row.AttachmentsState = intake.AttachmentsState{{Index: 0, STT: intake.STTDone, Transcript: "spoken"}}
	if pending == rowRevision(row) {
		t.Fatal("attachment state change did not revise row")
	}
	// Preserve the pre-P4 tracked-row bytes, including its existing null fields.
	legacy := queue.TrackedInbound{Inbound: c3types.Inbound{Text: "hello"}, RecordID: "row-1"}
	const baseline = `{"Origin":"","Inbound":{"Channel":"","ChatID":0,"TopicID":null,"MessageID":0,"Sender":{"UserID":0,"Username":""},"Text":"hello","Attachments":null,"ReplyTo":null,"Timestamp":"0001-01-01T00:00:00Z"},"RecordID":"row-1","SourceRecordID":"","VoicePending":null,"Source":null,"AttachmentsState":null}`
	raw, err := json.Marshal(legacy)
	if err != nil || string(raw) != baseline || rowRevision(legacy) != fmt.Sprintf("%x", sha256.Sum256([]byte(baseline))) {
		t.Fatalf("legacy revision bytes=%s err=%v", raw, err)
	}
}

func TestIntakeFetchRevisionOnWire(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, w, s, _, _ := negotiatedFixture(t)
	receiptHolder(s)
	s.setIntakeActiveForTest()
	in := &c3types.Inbound{Text: "hello"}
	source := intakeFetchSource()
	if _, err := b.Queue.AppendTrackedIntake(queueRouteKey(w.key), in, source,
		intake.AttachmentsState{{Index: 0, STT: intake.STTPending}}, "audio"); err != nil {
		t.Fatal(err)
	}
	r := reserveFetch(t, w, s, &attemptFetchGroup{token: "group-1"}, -1)
	frame := fetchQueueResponse{attemptFetchResponse("q", "group-1", r.Messages, r.Members, r.Remaining), r.intakeMessages}
	raw := intakeResponseWire(t, frame)
	if !strings.Contains(string(raw), `"attachments_state":[{"index":0,"stt":"pending"}]`) {
		t.Fatalf("pending wire=%s", raw)
	}
	rows, err := b.Queue.PeekTracked(queueRouteKey(w.key), -1)
	if err != nil || len(rows) != 1 || r.Members[0].Revision != rowRevision(rows[0]) {
		t.Fatalf("wire revision does not identify persisted state: err=%v", err)
	}
	w.handleAttemptResult(&attemptResultJob{Owner: s, Msg: ipc.AttemptResultMsg{Token: "group-1", Outcome: "failed"}})
	resolved, done, _, err := b.Queue.ResolveVoiceOutcome(queueRouteKey(w.key), rows[0].RecordID, "audio", "hello", "too large",
		intake.STTOutcome{STT: intake.STTDone, Transcript: "spoken"}, nil)
	if err != nil || !resolved || !done {
		t.Fatalf("resolve=%v done=%v err=%v", resolved, done, err)
	}
	next := reserveFetch(t, w, s, &attemptFetchGroup{token: "group-2"}, -1)
	raw = intakeResponseWire(t, fetchQueueResponse{attemptFetchResponse("q", "group-2", next.Messages, next.Members, next.Remaining), next.intakeMessages})
	var wire struct {
		Messages []json.RawMessage        `json:"messages"`
		Members  []ipc.FetchReceiptMember `json:"members"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil || len(wire.Members) != 1 || len(wire.Messages) != 1 {
		t.Fatalf("done wire=%s err=%v", raw, err)
	}
	if wire.Members[0].RecordID != r.Members[0].RecordID || wire.Members[0].Revision == r.Members[0].Revision {
		t.Fatal("pending and done wire revisions did not distinguish the same record")
	}
	want := strings.Replace(intakeMemberGolden, `,"intake":`, `,"V":1,"intake":`, 1)
	if string(wire.Messages[0]) != want {
		t.Fatalf("done message=%s want=%s", wire.Messages[0], want)
	}
	assertNoPrivateIntake(t, raw)
}
