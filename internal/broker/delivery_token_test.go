package broker

import "testing"

func TestDeliveryTokenIncludesBrokerLifetimePrefix(t *testing.T) {
	t.Setenv("C3_QUEUE_DIR", t.TempDir())
	b1 := New(mfWithTelegram())
	defer b1.Shutdown()
	b2 := New(mfWithTelegram())
	defer b2.Shutdown()

	first := b1.mintDeliveryToken()
	second := b1.mintDeliveryToken()
	restarted := b2.mintDeliveryToken()
	if first == second {
		t.Fatalf("two pushes in one broker lifetime shared token %q: the atomic sequence is missing", first)
	}
	if first == restarted {
		t.Fatalf("a restarted broker re-minted stale token %q: the per-lifetime prefix is missing, so an old ack can collide with a fresh push", first)
	}
}
