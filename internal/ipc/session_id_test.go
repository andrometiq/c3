package ipc

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRecoverSessionReq_RoundTrip(t *testing.T) {
	b, _ := json.Marshal(RecoverSessionReq{Op: OpRecoverSession, StableSessionID: "sess-1", CWD: "/x"})
	if !strings.Contains(string(b), `"stable_session_id":"sess-1"`) {
		t.Fatalf("missing stable_session_id on wire: %s", b)
	}
	var m RecoverSessionReq
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m.StableSessionID != "sess-1" || m.CWD != "/x" || m.Op != OpRecoverSession {
		t.Fatalf("round-trip mismatch: %+v", m)
	}
	// omitempty: empty cwd absent from the wire.
	b2, _ := json.Marshal(RecoverSessionReq{Op: OpRecoverSession, StableSessionID: "sess-1"})
	if strings.Contains(string(b2), `"cwd"`) {
		t.Fatalf("empty cwd must be omitted: %s", b2)
	}
}

func TestRecoverSessionResp_RoundTrip(t *testing.T) {
	tid := int64(281)
	telegram := RouteRef{Channel: "telegram", ChatID: -100, TopicID: &tid, Name: "c3", Group: "main"}
	web := RouteRef{Channel: "web", ChatID: 42, Name: "web"}
	b, _ := json.Marshal(RecoverSessionResp{
		Op: OpRecoverSessionResult, Recovered: true,
		Channel: "telegram", ChatID: -100, TopicID: &tid,
		Name: "c3", Group: "main", QueuedCount: 3,
		Routes: []RouteRef{telegram, web}, Output: &web,
	})
	var m RecoverSessionResp
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !m.Recovered || m.Name != "c3" || m.TopicID == nil || *m.TopicID != 281 || m.QueuedCount != 3 || len(m.Routes) != 2 || m.Output == nil || m.Output.Channel != "web" {
		t.Fatalf("round-trip mismatch: %+v", m)
	}
	// A not-recovered response stays compact (no topic / count).
	b2, _ := json.Marshal(RecoverSessionResp{Op: OpRecoverSessionResult})
	if strings.Contains(string(b2), `"queued_count"`) || strings.Contains(string(b2), `"topic_id"`) || strings.Contains(string(b2), `"routes"`) || strings.Contains(string(b2), `"output"`) {
		t.Fatalf("empty fields must be omitted: %s", b2)
	}
}
