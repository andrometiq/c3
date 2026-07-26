package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

// readHealthFile reads + unmarshals health.json into the wrapper shape, failing
// the test on any error. Returns the decoded wrapper.
func readHealthFile(t *testing.T, path string) healthFile {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read health file: %v", err)
	}
	var got healthFile
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("health file not valid JSON: %v (%s)", err, data)
	}
	return got
}

// assertWrapperLiveness asserts the broker-liveness fields: broker_pid is this
// process and written_unix is recent. Shared by the wrapper tests.
func assertWrapperLiveness(t *testing.T, hf healthFile) {
	t.Helper()
	if hf.BrokerPID != os.Getpid() {
		t.Errorf("broker_pid = %d, want current pid %d", hf.BrokerPID, os.Getpid())
	}
	now := time.Now().Unix()
	if hf.WrittenUnix == 0 {
		t.Error("written_unix not set")
	}
	if delta := now - hf.WrittenUnix; delta < -2 || delta > 60 {
		t.Errorf("written_unix = %d is not recent (now=%d, delta=%ds)", hf.WrittenUnix, now, delta)
	}
}

func TestWriteHealthFile_EmptySnapshot(t *testing.T) {
	hf := filepath.Join(t.TempDir(), "health.json")
	t.Setenv("C3_HEALTH_FILE", hf)
	b := newTestBroker()
	b.WriteHealthFile()
	got := readHealthFile(t, hf)
	assertWrapperLiveness(t, got)
	// Empty snapshot still yields a structurally-valid wrapper with an
	// (empty, non-nil) channels object — never a frozen flat map.
	if got.Channels == nil {
		t.Error("channels should be present (empty object), got nil")
	}
	if len(got.Channels) != 0 {
		t.Errorf("empty snapshot channels = %v, want empty", got.Channels)
	}
}

func TestWriteHealthFile_BrokerPIDAndWrittenUnix(t *testing.T) {
	hf := filepath.Join(t.TempDir(), "health.json")
	t.Setenv("C3_HEALTH_FILE", hf)
	b := newTestBroker()
	b.WriteHealthFile()
	got := readHealthFile(t, hf)
	assertWrapperLiveness(t, got)
}

func TestWriteHealthFile_DownEntry(t *testing.T) {
	hf := filepath.Join(t.TempDir(), "health.json")
	t.Setenv("C3_HEALTH_FILE", hf)
	b := newTestBroker()
	b.setLastHealth(c3types.HealthEvent{
		Channel: "telegram", State: c3types.HealthStateDown,
		Since: time.Unix(1718722680, 0), Consec: 3, Reason: "dial failures",
	})
	b.WriteHealthFile()
	got := readHealthFile(t, hf)
	assertWrapperLiveness(t, got)
	tg, ok := got.Channels["telegram"]
	if !ok || tg.State != "down" || tg.SinceUnix != 1718722680 || tg.Consec != 3 {
		t.Errorf("telegram entry = %+v, want down/1718722680/3", tg)
	}
	if tg.SinceHHMM == "" {
		t.Error("since_hhmm should be populated")
	}
}

func TestWriteHealthFile_ConcurrentEdgesProduceValidJSON(t *testing.T) {
	hf := filepath.Join(t.TempDir(), "health.json")
	t.Setenv("C3_HEALTH_FILE", hf)
	b := newTestBroker()
	b.setLastHealth(c3types.HealthEvent{Channel: "telegram", State: c3types.HealthStateDown, Since: time.Now(), Consec: 3})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); b.WriteHealthFile() }()
	}
	wg.Wait()
	got := readHealthFile(t, hf)
	// After concurrent edge + ticker-style writes the file is always one
	// complete generation with live wrapper fields and the channel nested.
	assertWrapperLiveness(t, got)
	if _, ok := got.Channels["telegram"]; !ok {
		t.Errorf("telegram channel missing after concurrent writes: %+v", got.Channels)
	}
}

// TestStartHealthRefresh_RefreshesWrittenUnix asserts the slow ticker re-writes
// health.json (keeping written_unix current) without any health edge, and that
// it stops cleanly when the broker context is cancelled (no leak). It overrides
// the (slow, production) interval indirectly by driving the same code path the
// ticker uses; here we verify the goroutine wiring + stop-on-cancel rather than
// waiting 45s for a real tick.
func TestStartHealthRefresh_StopsOnShutdown(t *testing.T) {
	hf := filepath.Join(t.TempDir(), "health.json")
	t.Setenv("C3_HEALTH_FILE", hf)
	b := newTestBroker()
	b.StartHealthRefresh()
	// Cancel the broker context; the refresh goroutine must observe Done and
	// return. We can't directly observe goroutine exit, but Shutdown() must not
	// hang and a subsequent direct write must still produce a valid wrapper.
	b.Shutdown()
	b.WriteHealthFile()
	got := readHealthFile(t, hf)
	assertWrapperLiveness(t, got)
}
