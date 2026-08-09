package broker

import (
	"testing"
	"time"
)

func waitForVoiceQueueText(t *testing.T, b *Broker, key RouteKey, messageID int64, ready func(string) bool) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := b.Queue.Peek(queueRouteKey(key), -1)
		if err == nil {
			for _, row := range rows {
				if row.MessageID == messageID && ready(row.Text) {
					return row.Text
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	rows, _ := b.Queue.Peek(queueRouteKey(key), -1)
	t.Fatalf("timed out waiting for resolved voice text for msg=%d; queue=%+v", messageID, rows)
	return ""
}

func waitForVoiceCondition(t *testing.T, description string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
