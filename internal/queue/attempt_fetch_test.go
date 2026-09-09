package queue

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

func TestEvictOverCapReturnsAgeAndCountSplit(t *testing.T) {
	s, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rk := RouteKey{Channel: "fixture", ChatID: 1}
	var raw []byte
	for i := 0; i < MaxMessages+5; i++ {
		stamp := time.Now()
		if i < 2 {
			stamp = stamp.Add(-MaxAge - time.Hour)
		}
		line, _ := json.Marshal(c3types.Inbound{Channel: "fixture", MessageID: int64(i + 1), Timestamp: stamp})
		raw = append(raw, line...)
		raw = append(raw, '\n')
	}
	if err := os.WriteFile(s.jsonlPath(rk), raw, 0600); err != nil {
		t.Fatal(err)
	}
	aged, overCount, err := s.EvictOverCap(rk)
	if err != nil || aged != 2 || overCount != 3 {
		t.Fatalf("aged=%d overCount=%d err=%v", aged, overCount, err)
	}
	if n, _ := s.Pending(rk); n != MaxMessages {
		t.Fatal(n)
	}
}
