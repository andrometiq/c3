package broker

import (
	"log"
	"time"

	"github.com/Andrometiq/c3/internal/channel"
)

// enqueuePresenceChange is the route table's fast ownership-change sink. It
// preserves the table's mutation order without calling channel code on a claim
// or release path.
func (b *Broker) enqueuePresenceChange(change routePresenceChange) {
	b.chMu.RLock()
	registered := b.channels[change.key.Channel]
	interested := false
	if registered != nil {
		_, interested = registered.Channel.(channel.PresenceNotifier)
	}
	b.chMu.RUnlock()
	if !interested {
		return
	}
	b.presenceMu.Lock()
	b.presencePending = append(b.presencePending, change)
	b.presenceMu.Unlock()
	select {
	case b.presenceWake <- struct{}{}:
	default:
	}
}

// enqueueOutputRoleChange re-emits presence for held routes whose roles changed
// without a route-table mutation. Duplicate old/new keys are collapsed.
func (b *Broker) enqueueOutputRoleChange(stub *Stub, oldOutput, newOutput *RouteKey) {
	if stub == nil {
		return
	}
	seen := make(map[RouteKey]bool, 2)
	sinceByKey := make(map[RouteKey]time.Time, 2)
	for _, entry := range b.Routes.Snapshot() {
		sinceByKey[entry.Key] = entry.Since
	}
	for _, key := range []*RouteKey{oldOutput, newOutput} {
		if key == nil || seen[*key] || !stub.HasRoute(*key) {
			continue
		}
		seen[*key] = true
		b.enqueuePresenceChange(routePresenceChange{key: *key, stub: stub, since: sinceByKey[*key]})
	}
}

func (b *Broker) dispatchPresenceChanges() {
	for {
		select {
		case <-b.presenceWake:
			for {
				b.presenceMu.Lock()
				if len(b.presencePending) == 0 {
					b.presenceMu.Unlock()
					break
				}
				change := b.presencePending[0]
				b.presencePending[0] = routePresenceChange{}
				b.presencePending = b.presencePending[1:]
				b.presenceMu.Unlock()
				b.notifyRouteHolder(change)
			}
		case <-b.ctx.Done():
			return
		}
	}
}

// notifyRouteHolder is the sole channel call site for route presence. A broken
// optional notifier loses only its notification; it cannot unwind into the
// dispatcher or the broker's ownership paths.
func (b *Broker) notifyRouteHolder(change routePresenceChange) {
	registered, err := b.Channel(change.key.Channel)
	if err != nil {
		return
	}
	notifier, ok := registered.(channel.PresenceNotifier)
	if !ok {
		return
	}
	var topicID *int64
	if change.key.HasTopic {
		topic := change.key.TopicID
		topicID = &topic
	}
	var holder *channel.RouteHolder
	if change.stub != nil {
		since := change.since
		if since.IsZero() {
			since = time.Now().UTC()
		}
		role := "input"
		if output := change.stub.OutputRoute(); output != nil && *output == change.key {
			role = "output"
		}
		holder = &channel.RouteHolder{
			CLI:       change.stub.CLI,
			PID:       change.stub.PID,
			CWD:       change.stub.CWD,
			SessionID: change.stub.StableSessionIDValue(),
			Since:     since,
			Role:      role,
		}
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("presence notifier PANIC channel=%s route=%s: %v", change.key.Channel, routeKeyStr(change.key), recovered)
		}
	}()
	notifier.SetRouteHolder(change.key.ChatID, topicID, holder)
}

// replayRouteHolders seeds a newly registered notifier with every current
// holder on that channel. The channel has already been inserted in the broker
// registry when this is called.
func (b *Broker) replayRouteHolders(registered channel.Channel) {
	if _, ok := registered.(channel.PresenceNotifier); !ok {
		return
	}
	for _, entry := range b.Routes.Snapshot() {
		if entry.Key.Channel != registered.Name() {
			continue
		}
		b.enqueuePresenceChange(routePresenceChange{key: entry.Key, stub: entry.Stub, since: entry.Since})
	}
}
