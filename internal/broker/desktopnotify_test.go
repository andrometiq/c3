package broker

import (
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

func TestDesktopNotifier_NoBinaryReturnsNotDelivered(t *testing.T) {
	dn := &desktopNotifier{} // bin == "" — no notifier resolved
	got := dn.Notify(c3types.HealthEvent{Channel: "telegram", State: c3types.HealthStateDown, Since: time.Now()})
	if got {
		t.Error("Notify with no binary should report not delivered")
	}
}
