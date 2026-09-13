package broker

import (
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

func readyPrefixFixture(t *testing.T) (*Broker, *Stub, []RouteKey) {
	t.Helper()
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	t.Cleanup(b.Shutdown)
	first := MakeRouteKey("telegram", -100, nil)
	s, _ := negotiatedHolder(t, b, first, 1)
	receiptHolder(s)
	s.setIntakeActiveForTest()
	other := MakeRouteKey("telegram", -100, ptrI64Val(2))
	s.AddRoute(other)
	s.MarkRouteConfirmed(other)
	b.Routes.Claim(other, s)
	return b, s, []RouteKey{first, other}
}

func appendReadyPrefixRow(t *testing.T, b *Broker, route RouteKey, noSource, frozen, pending bool) queue.TrackedInbound {
	t.Helper()
	in := &c3types.Inbound{Text: "hello"}
	source := intakeFetchSource()
	states := intake.AttachmentsState{{Index: 0, STT: intake.STTDone, Transcript: "spoken"}}
	var voice []string
	if pending {
		voice = []string{"audio"}
		states = intake.AttachmentsState{{Index: 0, STT: intake.STTPending}}
	}
	if noSource {
		source, states = nil, nil
	}
	if frozen {
		in.DrainedFrom = "acme"
	}
	id, err := b.Queue.AppendTrackedIntake(queueRouteKey(route), in, source, states, voice...)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := b.Queue.PeekTracked(queueRouteKey(route), -1)
	if err != nil || len(rows) == 0 || rows[len(rows)-1].RecordID != id {
		t.Fatalf("append row: %v", err)
	}
	return rows[len(rows)-1]
}

func reserveReadyPrefixRow(t *testing.T, b *Broker, s *Stub, route RouteKey, row queue.TrackedInbound) {
	t.Helper()
	holder := shadowHolder(s)
	holder.ClaimGeneration = s.claimGeneration(route)
	b.attempts.open(attemptRecord{Negotiated: true, Token: "held-1", Transport: "fetch", Route: route,
		FetchGroup: &attemptFetchGroup{token: "held-1"}, Holder: holder,
		Members: []attemptMember{{ID: row.RecordID, Revision: rowRevision(row)}}}, time.Now())
	if len(b.attempts.lookup("held-1", time.Now())) != 1 {
		t.Fatal("reservation not admitted")
	}
}

func readyPrefixMemberGolden() string {
	return strings.Replace(intakeMemberGolden, `,"intake":`, `,"V":1,"intake":`, 1)
}

// Only broker-minted identities vary; the rest of the socket frame is literal.
func assertReadyPrefixWire(t *testing.T, raw []byte, messages []string, rows []queue.TrackedInbound, remaining int, blockedID, reason string, ack bool) ipc.FetchQueueResp {
	t.Helper()
	var resp ipc.FetchQueueResp
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	want := `{"op":"fetch_queue_result","id":"q","messages":[` + strings.Join(messages, ",") + fmt.Sprintf(`],"remaining":%d`, remaining)
	if ack && len(rows) > 0 {
		if resp.LeaseToken == "" {
			t.Fatal("missing receipt token")
		}
		var members []string
		trailer := "[C3_FETCH_RECEIPT_V1]\ngroup " + resp.LeaseToken
		for _, row := range rows {
			revision := rowRevision(row)
			members = append(members, fmt.Sprintf(`{"record_id":%q,"revision":%q}`, row.RecordID, revision))
			trailer += "\nmember " + row.RecordID + " " + revision
		}
		trailer += "\n[/C3_FETCH_RECEIPT_V1]"
		encodedTrailer, err := json.Marshal(trailer)
		if err != nil {
			t.Fatal(err)
		}
		want += fmt.Sprintf(`,"lease_token":%q,"members":[%s],"receipt_trailer":%s`, resp.LeaseToken, strings.Join(members, ","), encodedTrailer)
	}
	if blockedID != "" {
		want += fmt.Sprintf(`,"blocked_on":{"record_id":%q,"reason":%q}`, blockedID, reason)
	}
	want += "}"
	if string(raw) != want {
		t.Fatalf("wire=%s want=%s", raw, want)
	}
	if strings.Contains(string(raw), `"_c3_`) {
		t.Fatal("queue-private key on wire")
	}
	return resp
}

func TestReadyPrefixGateWire(t *testing.T) {
	for _, tc := range []struct {
		mode, want string
		active     bool
	}{
		{"ready_prefix", `{"op":"fetch_queue_result","id":"q","err":"fetch_queue: ready_prefix requires the negotiated intake_metadata capability"}`, false},
		{"acme", `{"op":"fetch_queue_result","id":"q","err":"fetch_queue: unknown mode"}`, false},
		{"acme", `{"op":"fetch_queue_result","id":"q","err":"fetch_queue: unknown mode"}`, true},
	} {
		t.Run(fmt.Sprintf("%s/active=%v", tc.mode, tc.active), func(t *testing.T) {
			clearFetchTestEnvironment(t)
			b, s, routes := readyPrefixFixture(t)
			s.intakeActive.Store(tc.active)
			row := appendReadyPrefixRow(t, b, routes[0], false, false, false)
			for _, selector := range []string{"", "missing"} {
				raw := intakeFetchWire(t, b, s, ipc.FetchQueueReq{ID: "q", Channel: selector, Mode: tc.mode, All: true, Ack: true, Lease: json.RawMessage("true")})
				if string(raw) != tc.want {
					t.Fatalf("wire=%s want=%s", raw, tc.want)
				}
			}
			rows, err := b.Queue.PeekTracked(queueRouteKey(routes[0]), -1)
			if err != nil || len(rows) != 1 || rows[0].RecordID != row.RecordID || len(b.attempts.snapshot(time.Now())) != 0 {
				t.Fatalf("refused fetch changed queue/reservations: %v", err)
			}
		})
	}
}

func TestReadyPrefixStopsAtFirstNotReadyWire(t *testing.T) {
	for _, tc := range []struct {
		name, reason                        string
		noSource, frozen, pending, reserved bool
	}{
		{"pending", "stt_pending", false, false, true, false},
		{"legacy", "no_source", true, false, false, false},
		{"drained", "frozen", false, true, false, false},
		{"reserved", "reserved", false, false, false, true},
		{"reserved_precedes_all", "reserved", true, true, true, true},
		{"no_source_precedes_frozen_pending", "no_source", true, true, true, false},
		{"frozen_precedes_pending", "frozen", false, true, true, false},
	} {
		for _, ack := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ack=%v", tc.name, ack), func(t *testing.T) {
				clearFetchTestEnvironment(t)
				b, s, routes := readyPrefixFixture(t)
				ready := appendReadyPrefixRow(t, b, routes[0], false, false, false)
				blocked := appendReadyPrefixRow(t, b, routes[0], tc.noSource, tc.frozen, tc.pending)
				appendReadyPrefixRow(t, b, routes[0], false, false, false)
				appendReadyPrefixRow(t, b, routes[1], false, false, false)
				if tc.reserved {
					reserveReadyPrefixRow(t, b, s, routes[0], blocked)
				}
				raw := intakeFetchWire(t, b, s, ipc.FetchQueueReq{ID: "q", Mode: "ready_prefix", All: true, Ack: ack, Lease: json.RawMessage("true")})
				resp := assertReadyPrefixWire(t, raw, []string{readyPrefixMemberGolden()}, []queue.TrackedInbound{ready}, 3, blocked.RecordID, tc.reason, ack)
				wantAttempts := 0
				if tc.reserved {
					wantAttempts++
				}
				if ack {
					wantAttempts++
					attempts := b.attempts.lookup(resp.LeaseToken, time.Now())
					if len(attempts) != 1 || len(attempts[0].Members) != 1 || attempts[0].Members[0].ID != ready.RecordID {
						t.Fatal("receipt did not reserve exactly the returned prefix")
					}
					// The next real fetch must see the reservation at the physical head.
					next := intakeFetchWire(t, b, s, ipc.FetchQueueReq{ID: "q", Mode: "ready_prefix", All: true})
					assertReadyPrefixWire(t, next, nil, nil, 4, ready.RecordID, "reserved", false)
				}
				if len(b.attempts.snapshot(time.Now())) != wantAttempts || b.Queue.StatusFor(queueRouteKey(routes[0])).Pending != 3 || b.Queue.StatusFor(queueRouteKey(routes[1])).Pending != 1 {
					t.Fatal("fetch consumed rows or opened an unexpected receipt group")
				}
			})
		}
	}
}

func TestReadyPrefixZeroReadyWire(t *testing.T) {
	for _, reserved := range []bool{false, true} {
		for _, ack := range []bool{false, true} {
			t.Run(fmt.Sprintf("reserved=%v/ack=%v", reserved, ack), func(t *testing.T) {
				clearFetchTestEnvironment(t)
				b, s, routes := readyPrefixFixture(t)
				row := appendReadyPrefixRow(t, b, routes[0], false, false, true)
				appendReadyPrefixRow(t, b, routes[0], false, false, false)
				appendReadyPrefixRow(t, b, routes[1], false, false, false)
				reason, wantAttempts := "stt_pending", 0
				if reserved {
					reserveReadyPrefixRow(t, b, s, routes[0], row)
					reason, wantAttempts = "reserved", 1
				}
				raw := intakeFetchWire(t, b, s, ipc.FetchQueueReq{ID: "q", Mode: "ready_prefix", All: true, Ack: ack, Lease: json.RawMessage("true")})
				assertReadyPrefixWire(t, raw, nil, nil, 3, row.RecordID, reason, ack)
				if !reserved {
					want := `{"op":"fetch_queue_result","id":"q","messages":[],"remaining":3,"blocked_on":{"record_id":"` + row.RecordID + `","reason":"stt_pending"}}`
					if string(raw) != want {
						t.Fatalf("zero-ready wire=%s want=%s", raw, want)
					}
				}
				if len(b.attempts.snapshot(time.Now())) != wantAttempts {
					t.Fatal("zero-ready fetch opened a receipt group")
				}
			})
		}
	}
}

func TestReadyPrefixLimitAndRouteOrderWire(t *testing.T) {
	for _, tc := range []struct {
		name    string
		limit   int
		reverse bool
	}{
		{"zero", 0, false},
		{"ready_unreturned", 1, false},
		{"at_blocker", 2, false},
		{"output_first", 2, true},
	} {
		for _, ack := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ack=%v", tc.name, ack), func(t *testing.T) {
				clearFetchTestEnvironment(t)
				b, s, routes := readyPrefixFixture(t)
				first := appendReadyPrefixRow(t, b, routes[0], false, false, false)
				second := appendReadyPrefixRow(t, b, routes[0], false, false, false)
				blocked := appendReadyPrefixRow(t, b, routes[0], false, false, true)
				other := appendReadyPrefixRow(t, b, routes[1], false, false, false)
				wantRows := []queue.TrackedInbound{first, second}[:tc.limit]
				blockedID, reason := "", ""
				if tc.limit == 2 {
					blockedID, reason = blocked.RecordID, "stt_pending"
				}
				if tc.reverse {
					if !s.SetOutputRoute(routes[1]) {
						t.Fatal("output route not held")
					}
					wantRows = []queue.TrackedInbound{other, first}
					blockedID, reason = "", ""
				}
				var messages []string
				for range wantRows {
					messages = append(messages, readyPrefixMemberGolden())
				}
				raw := intakeFetchWire(t, b, s, ipc.FetchQueueReq{ID: "q", Mode: "ready_prefix", Limit: tc.limit, Ack: ack, Lease: json.RawMessage("true")})
				resp := assertReadyPrefixWire(t, raw, messages, wantRows, 4-len(wantRows), blockedID, reason, ack)
				if ack {
					reserved := 0
					for _, a := range b.attempts.lookup(resp.LeaseToken, time.Now()) {
						reserved += len(a.Members)
					}
					if reserved != len(wantRows) {
						t.Fatal("reservation count differs from wire prefix")
					}
				}
			})
		}
	}
}

func TestReadyPrefixFrameStopsLaterRoutesWire(t *testing.T) {
	for _, ack := range []bool{false, true} {
		t.Run(fmt.Sprint(ack), func(t *testing.T) {
			clearFetchTestEnvironment(t)
			b, s, routes := readyPrefixFixture(t)
			source := intakeFetchSource()
			source.Text = strings.Repeat("s", 900000)
			for i := 0; i < 5; i++ {
				if _, err := b.Queue.AppendTrackedIntake(queueRouteKey(routes[0]), &c3types.Inbound{Text: "hello"}, source,
					intake.AttachmentsState{{Index: 0, STT: intake.STTDone, Transcript: "spoken"}}); err != nil {
					t.Fatal(err)
				}
			}
			appendReadyPrefixRow(t, b, routes[0], true, false, false)
			appendReadyPrefixRow(t, b, routes[1], false, false, false)
			rows, err := b.Queue.PeekTracked(queueRouteKey(routes[0]), -1)
			if err != nil || len(rows) != 6 {
				t.Fatalf("rows=%d err=%v", len(rows), err)
			}
			message := strings.Replace(readyPrefixMemberGolden(), `"text":"raw"`, `"text":"`+source.Text+`"`, 1)
			raw := intakeFetchWire(t, b, s, ipc.FetchQueueReq{ID: "q", Mode: "ready_prefix", All: true, Ack: ack, Lease: json.RawMessage("true")})
			assertReadyPrefixWire(t, raw, []string{message, message, message, message}, rows[:4], 3, "", "", ack)
			if len(raw)+1 > ipc.MaxFrameSize {
				t.Fatal("oversize response")
			}
		})
	}
}

func TestReadyPrefixDefaultModeUnchangedWire(t *testing.T) {
	for _, active := range []bool{false, true} {
		for _, ack := range []bool{false, true} {
			t.Run(fmt.Sprintf("active=%v/ack=%v", active, ack), func(t *testing.T) {
				clearFetchTestEnvironment(t)
				b, s, routes := readyPrefixFixture(t)
				s.intakeActive.Store(active)
				row := appendReadyPrefixRow(t, b, routes[0], false, false, false)
				raw := intakeFetchWire(t, b, s, ipc.FetchQueueReq{ID: "q", All: true, Ack: ack, Lease: json.RawMessage("true")})
				message := strings.TrimSuffix(intakeInboundGolden, "}") + `,"V":1}`
				if active {
					message = readyPrefixMemberGolden()
				}
				assertReadyPrefixWire(t, raw, []string{message}, []queue.TrackedInbound{row}, 0, "", "", ack)
			})
		}
	}
}

func TestReadyPrefixLaterBlockerFrameBudgetWire(t *testing.T) {
	clearFetchTestEnvironment(t)
	b, s, routes := readyPrefixFixture(t)
	empty := strings.Replace(readyPrefixMemberGolden(), `"text":"raw"`, `"text":""`, 1)
	base := `{"op":"fetch_queue_result","id":"q","messages":[` + strings.Join([]string{empty, empty, empty, empty, empty}, ",") + `],"remaining":5}`
	// Five members fit alone, but leave too little room for a later blocker.
	size := (ipc.MaxFrameSize - 1 - 32 - len(base)) / 5
	source := intakeFetchSource()
	source.Text = strings.Repeat("s", size)
	for i := 0; i < 5; i++ {
		if _, err := b.Queue.AppendTrackedIntake(queueRouteKey(routes[0]), &c3types.Inbound{Text: "hello"}, source,
			intake.AttachmentsState{{Index: 0, STT: intake.STTDone, Transcript: "spoken"}}); err != nil {
			t.Fatal(err)
		}
	}
	appendReadyPrefixRow(t, b, routes[1], false, false, true)
	message := strings.Replace(empty, `"text":""`, `"text":"`+source.Text+`"`, 1)
	raw := intakeFetchWire(t, b, s, ipc.FetchQueueReq{ID: "q", Mode: "ready_prefix", All: true})
	assertReadyPrefixWire(t, raw, []string{message, message, message, message}, nil, 2, "", "", false)
	if len(raw)+1 > ipc.MaxFrameSize {
		t.Fatal("later blocker exceeded frame size")
	}
}

func TestReadyPrefixRequestWire(t *testing.T) {
	for _, mode := range []string{"", "ready_prefix"} {
		t.Run(mode, func(t *testing.T) {
			peer, conn := newConnPair(t)
			written := make(chan error, 1)
			go func() {
				written <- conn.WriteJSON(ipc.FetchQueueReq{Op: ipc.OpFetchQueue, ID: "q", Mode: mode, Ack: true, Lease: json.RawMessage("true")})
			}()
			raw := nextWireOp(t, peer, ipc.OpFetchQueue)
			if err := <-written; err != nil {
				t.Fatal(err)
			}
			want := `{"op":"fetch_queue","id":"q","ack":true,"lease":true}`
			if mode != "" {
				want = `{"op":"fetch_queue","id":"q","mode":"ready_prefix","ack":true,"lease":true}`
			}
			if string(raw) != want {
				t.Fatalf("request wire=%s want=%s", raw, want)
			}
		})
	}
}
