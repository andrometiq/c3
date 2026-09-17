package swarm

import "testing"

func TestAccept(t *testing.T) {
	if Accept(false, false) {
		t.Fatal("deaf + unaddressed must drop")
	}
	if !Accept(false, true) {
		t.Fatal("mention of a deaf bot must deliver (and the caller arms)")
	}
	if !Accept(true, false) {
		t.Fatal("sticky-armed bot must receive untagged follow-ups")
	}
	if !Accept(true, true) {
		t.Fatal("mention while armed must still deliver")
	}
}

func TestStoreArmDisarm(t *testing.T) {
	s := NewStore()
	k := Key{ChatID: -100, TopicID: 8}
	if s.Armed(k) {
		t.Fatal("fresh store is deaf")
	}
	s.Arm(k)
	if !s.Armed(k) {
		t.Fatal("Arm did not stick")
	}
	s.Disarm(k)
	if s.Armed(k) {
		t.Fatal("Disarm left the conversation armed")
	}
	other := Key{ChatID: -100, TopicID: 9}
	s.Arm(k)
	if s.Armed(other) {
		t.Fatal("arming one topic must not arm another")
	}
}
