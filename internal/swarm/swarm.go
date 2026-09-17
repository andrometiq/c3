// Package swarm is the mention-and-sticky gate for C3 Swarm mode: several
// Telegram bots share one forum topic, and a bot only receives traffic that
// addresses it (a mention / reply-to-me) or that arrives while it is armed.
package swarm

import "sync"

// Key is one (chat, topic) conversation as seen by a single bot.
type Key struct {
	ChatID  int64
	TopicID int64 // 0 = DM / no topic
}

// Store holds per-conversation sticky arming for one bot. In-memory: a
// broker restart leaves every conversation deaf until tagged again.
type Store struct {
	mu    sync.Mutex
	armed map[Key]bool
}

func NewStore() *Store {
	return &Store{armed: map[Key]bool{}}
}

func (s *Store) Armed(k Key) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.armed[k]
}

func (s *Store) Arm(k Key) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.armed[k] = true
	s.mu.Unlock()
}

func (s *Store) Disarm(k Key) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.armed, k)
	s.mu.Unlock()
}

// Accept reports whether this bot should deliver the inbound. addressed is
// true when the message mentions this bot or replies to it. A delivered
// addressed message also arms sticky listening; /mute is Disarm, not Accept.
func Accept(armed, addressed bool) bool {
	return addressed || armed
}
