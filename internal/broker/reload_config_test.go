package broker

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/mappings"
)

// TestBroker_SetMappings_AtomicSwap asserts the SIGHUP-driven config reload
// path: SetMappings replaces the pointer, and subsequent reads through
// Broker.Mappings see the new file. The promise is "no torn reads" — a
// goroutine that loads Broker.Mappings between SetMappings calls will see
// either the old pointer or the new pointer, never a half-updated struct.
func TestBroker_SetMappings_AtomicSwap(t *testing.T) {
	old := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {BotToken: "old-token", DefaultGroup: "main"},
		},
	}
	b := New(old)
	defer b.Shutdown()

	if got := b.Mappings().Channels["telegram"].BotToken; got != "old-token" {
		t.Fatalf("initial token: got %q, want old-token", got)
	}

	fresh := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {BotToken: "new-token", DefaultGroup: "main"},
		},
		Mappings: map[string]mappings.Mapping{
			"/home/u/proj": {Channel: "telegram", ChatID: -100, TopicID: 914, Name: "proj"},
		},
	}
	b.SetMappings(fresh)

	if got := b.Mappings().Channels["telegram"].BotToken; got != "new-token" {
		t.Errorf("after reload: token %q, want new-token", got)
	}
	if _, ok := b.Mappings().LookupByCwd("/home/u/proj"); !ok {
		t.Error("after reload: new mapping not visible via LookupByCwd")
	}
}

// TestBroker_Mappings_ConcurrentAccess_NoRace is the regression test for
// the 2026-05-15 BLOCKER: Broker.Mappings was a public field mutated by
// per-connection goroutines via UpsertTopic/UpsertMapping while other
// goroutines iterated the inner maps. `go test -race` would catch
// concurrent map read/write; the production daemon would eventually fatal
// with "concurrent map read and map write" under enough load.
//
// The fix routes all mutations through copy-on-write under
// `mutationMu`; readers go through `Mappings()` which atomically loads
// the current immutable snapshot. With the fix, this test passes
// cleanly under `-race`; without it, `-race` reports a race.
//
// `go test -race` must be enabled for this to be meaningful; the test
// also exercises the path under normal conditions, so it never gives a
// false negative on a quiet day.
func TestBroker_Mappings_ConcurrentAccess_NoRace(t *testing.T) {
	mf := &mappings.MappingsFile{
		SchemaVersion: 1,
		Channels: map[string]mappings.ChannelConfig{
			"telegram": {DefaultGroup: "main"},
		},
		Mappings: map[string]mappings.Mapping{},
	}
	b := New(mf)
	defer b.Shutdown()

	const (
		mutators = 4
		readers  = 4
		duration = 150 * time.Millisecond
	)
	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup

	// Mutator goroutines: hammer UpsertTopic and UpsertMapping.
	for i := 0; i < mutators; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var n int
			for time.Now().Before(deadline) {
				b.mutateMappings(func(mf *mappings.MappingsFile) {
					mf.UpsertTopic("telegram", mappings.Topic{
						ChatID: int64(-1000 - id), TopicID: int64(n),
						Name: fmt.Sprintf("g%d-t%d", id, n),
					})
					mf.UpsertMapping(fmt.Sprintf("/home/u/p%d-%d", id, n), mappings.Mapping{
						Channel: "telegram", ChatID: int64(-1000 - id), TopicID: int64(n),
					})
				})
				n++
			}
		}(i)
	}

	// Reader goroutines: iterate Channels (which iterates inner Topics
	// slice and Groups map), iterate Mappings.
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				snap := b.Mappings()
				for _, cc := range snap.Channels {
					_ = cc.BotToken
					for _, tp := range cc.Topics {
						_ = tp.Name
					}
				}
				for cwd := range snap.Mappings {
					_ = cwd
				}
			}
		}()
	}

	// A SIGHUP-style swap while mutations are in flight.
	wg.Add(1)
	go func() {
		defer wg.Done()
		fresh := &mappings.MappingsFile{
			SchemaVersion: 1,
			Channels:      map[string]mappings.ChannelConfig{"telegram": {DefaultGroup: "main"}},
			Mappings:      map[string]mappings.Mapping{},
		}
		for time.Now().Before(deadline) {
			b.SetMappings(fresh.Clone())
			time.Sleep(5 * time.Millisecond)
		}
	}()

	wg.Wait()
	// Test passes iff `go test -race` reports no race. Functional
	// correctness (counts, content) isn't asserted here — the BLOCKER
	// was about race safety, not behavior.
}
