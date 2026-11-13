package broker

import (
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// Guests are non-operator Telegram users who may only talk, from their own DM,
// to the one configured private-route holder. They get no broker commands,
// notices or permission taps (DECISIONS: guests are never operators).

const privateChannel = "telegram"

// isGuestDM reports a guest writing in their own one-to-one chat.
func isGuestDM(mf *mappings.MappingsFile, in *c3types.Inbound) bool {
	if in == nil || (in.Channel != "" && in.Channel != privateChannel) || !isPrivateChat(in) {
		return false
	}
	return in.TopicID == nil && in.Sender.UserID > 0 && in.ChatID == in.Sender.UserID && mf.IsGuest(in.Sender.UserID)
}

// isPrivateNonOperatorChat reports a Telegram private chat that is not the
// operator DM. It is keyed on the chat shape, not on current guest membership,
// so a removed guest's chat stays closed to broker text.
func (b *Broker) isPrivateNonOperatorChat(channelName string, chatID int64, hasTopic bool) bool {
	if channelName != privateChannel || hasTopic || chatID <= 0 {
		return false
	}
	mf := b.Mappings()
	if cc, ok := mf.Channels[channelName]; ok && cc.DMChatID == chatID {
		return false
	}
	return !mf.IsUserAllowed(chatID)
}

func (b *Broker) isPrivateNonOperatorRoute(key RouteKey) bool {
	return b.isPrivateNonOperatorChat(key.Channel, key.ChatID, key.HasTopic)
}

// AllowBrokerSend is the single choke point for broker-originated text: the
// channel asks it before every send that is not a holder's own tool call.
func (h *BrokerHost) AllowBrokerSend(chatID int64, topicID *int64) bool {
	if h.broker.isPrivateNonOperatorChat(h.channel, chatID, topicID != nil) {
		log.Printf("private: broker-originated send to private chat %d dropped", chatID)
		return false
	}
	if h.broker.isHeldByPrivateHolderCLI(MakeRouteKey(h.channel, chatID, topicID)) {
		log.Printf("private: broker-originated send to %s dropped (held by the private holder cli)", routeKeyStr(MakeRouteKey(h.channel, chatID, topicID)))
		return false
	}
	return true
}

// isHeldByPrivateHolderCLI reports a route held by the configured private
// holder's cli on any connection (for example its developer topic). The holder
// posts its own messages there, so broker text would only be noise.
func (b *Broker) isHeldByPrivateHolderCLI(key RouteKey) bool {
	cfg := b.Mappings().Channels[key.Channel].PrivateHolder
	if cfg == nil || cfg.CLI == "" {
		return false
	}
	holder, held := b.Routes.Holder(key)
	return held && holder.CLI == cfg.CLI
}

// privateHelloEligible reports a hello that can never render channel messages
// as session input: queue-only and no eligible live transport.
func privateHelloEligible(hello ipc.HelloMsg) bool {
	if !hello.CannotRenderChannels || hello.RenderState != ipc.RenderQueueOnly {
		return false
	}
	if len(hello.Delivery) == 0 {
		return true
	}
	offer := ipc.ParseDeliveryOffer(hello.Delivery)
	return offer == nil || (!offer.Live.Channel.Eligible && !offer.Live.Inbox.Eligible)
}

// privateAttachError returns why stub may not hold private routes, or "".
func (b *Broker) privateAttachError(stub *Stub, chanName string) string {
	if chanName != privateChannel {
		return "attach private: private routes exist only on the telegram channel"
	}
	cfg := b.Mappings().Channels[chanName].PrivateHolder
	if cfg == nil || cfg.CLI == "" {
		return "attach private: refused; channels.telegram.private_holder is not configured"
	}
	if stub.CLI != cfg.CLI {
		return "attach private: refused; private routes are reserved for the configured holder"
	}
	if !stub.privateEligible {
		return "attach private: refused; the holder must be queue-only with no live transport"
	}
	return ""
}

func (b *Broker) otherPrivateHolder(stub *Stub) *Stub {
	for _, s := range b.Stubs.Snapshot() {
		if s != stub && s.private.Load() && s.IsAlive() && !sameLogicalSession(s, stub) {
			return s
		}
	}
	return nil
}

// attachPrivate hands every guest DM route to the configured holder. It never
// persists a mapping or session attachment, sends no welcome, and leaves the
// holder without an output route.
func (b *Broker) attachPrivate(conn *ipc.Conn, stub *Stub, chanName string) {
	refuse := func(reason string) {
		_ = conn.WriteJSON(ipc.AttachedMsg{Op: ipc.OpAttached, OK: false, Err: reason})
	}
	if reason := b.privateAttachError(stub, chanName); reason != "" {
		refuse(reason)
		return
	}
	if len(stub.Routes()) > 0 && !stub.private.Load() {
		refuse("attach private: detach every other route first; private routes need their own connection")
		return
	}
	if other := b.otherPrivateHolder(stub); other != nil {
		refuse(fmt.Sprintf("attach private: private routes are held by %s pid %d", other.CLI, other.PID))
		return
	}
	stub.private.Store(true)
	stub.SetExplicitlyDetached(false)
	b.syncPrivateRoutes(stub)
	routes, _ := b.routeSetRefs(stub)
	log.Printf("private: holder cli=%s pid=%d conn=%d holds %d guest route(s)", stub.CLI, stub.PID, stub.ConnID, len(routes))
	_ = conn.WriteJSON(ipc.AttachedMsg{
		Op: ipc.OpAttached, OK: true, Status: ipc.AttachStatusOK,
		Channel: chanName, Name: "private", Routes: routes,
		Capabilities: b.capsForChannel(chanName),
	})
}

// syncPrivateRoutes makes the holder's held set equal the guest list. A holder
// the config no longer names loses every route.
func (b *Broker) syncPrivateRoutes(stub *Stub) {
	allowed := b.privateAttachError(stub, privateChannel) == ""
	want := map[RouteKey]bool{}
	var order []RouteKey
	if allowed {
		for _, id := range b.Mappings().AllowlistOrEmpty().Guests {
			key := MakeRouteKey(privateChannel, id, nil)
			if !want[key] {
				want[key] = true
				order = append(order, key)
			}
		}
	}
	for _, held := range stub.Routes() {
		if !want[held] {
			b.Routes.Release(held, stub.ConnID)
			stub.RemoveRoute(held)
		}
	}
	for _, key := range order {
		if stub.HasRoute(key) {
			continue
		}
		if holder, ok := b.Routes.Claim(key, stub); !ok {
			log.Printf("private: guest route %s held by cli=%s pid=%d; not claimed", routeKeyStr(key), holder.CLI, holder.PID)
			continue
		}
		stub.AddRoute(key)
		stub.MarkRouteConfirmed(key)
	}
	stub.clearOutputRoute()
	if !allowed {
		stub.private.Store(false)
		log.Printf("private: holder cli=%s pid=%d no longer matches private_holder; released", stub.CLI, stub.PID)
	}
	for _, key := range stub.Routes() {
		b.wakeDelivery(key)
	}
}

// carryPrivateHolder keeps the private role across a reconnect of the same
// session, or releases the guest routes when the new hello is not eligible.
func (b *Broker) carryPrivateHolder(existing, stub *Stub) {
	if !existing.private.Load() {
		return
	}
	if b.privateAttachError(stub, privateChannel) == "" {
		stub.private.Store(true)
		stub.clearOutputRoute()
		return
	}
	for _, key := range stub.Routes() {
		if b.isPrivateNonOperatorRoute(key) {
			b.Routes.Release(key, stub.ConnID)
			stub.RemoveRoute(key)
		}
	}
	log.Printf("private: reconnect of cli=%s pid=%d is not eligible; guest routes released", stub.CLI, stub.PID)
}

// applyGuestChange runs after a reload: it logs the guest diff, resyncs the
// private holder, and drops the queued rows of removed guests.
func (b *Broker) applyGuestChange(previous, next *mappings.MappingsFile) {
	before := previous.AllowlistOrEmpty().Guests
	after := next.AllowlistOrEmpty().Guests
	var added, removed []string
	for _, id := range after {
		if !slices.Contains(before, id) {
			added = append(added, strconv.FormatInt(id, 10))
		}
	}
	var dropped []RouteKey
	for _, id := range before {
		if !slices.Contains(after, id) {
			removed = append(removed, strconv.FormatInt(id, 10))
			dropped = append(dropped, MakeRouteKey(privateChannel, id, nil))
		}
	}
	log.Printf("guests: %d configured; added=[%s] removed=[%s]", len(after), strings.Join(added, ","), strings.Join(removed, ","))
	for _, s := range b.Stubs.Snapshot() {
		if s.private.Load() {
			b.syncPrivateRoutes(s)
		}
	}
	for _, key := range dropped {
		go b.purgeGuestQueue(key)
	}
}

// purgeGuestQueue drops a removed guest's queued rows on the route's own
// worker. Nothing is sent to the chat.
func (b *Broker) purgeGuestQueue(key RouteKey) {
	defer recoverGoroutine("broker.purgeGuestQueue")
	if b.Queue == nil || b.Workers == nil {
		return
	}
	peekCh := make(chan DrainPeekResult, 1)
	if !b.Workers.Submit(key, Job{Kind: JobDrainPeek, DrainPeek: &DrainPeekJob{ResultCh: peekCh}}) {
		log.Printf("guests: purge %s skipped: worker unavailable", routeKeyStr(key))
		return
	}
	peek := <-peekCh
	if peek.Err != nil {
		log.Printf("guests: purge %s peek failed: %v", routeKeyStr(key), peek.Err)
		return
	}
	ids := make([]string, 0, len(peek.Tracked))
	for _, row := range peek.Tracked {
		ids = append(ids, row.RecordID)
	}
	if len(ids) == 0 {
		return
	}
	removeCh := make(chan DrainRemoveResult, 1)
	if !b.Workers.Submit(key, Job{Kind: JobDrainRemove, DrainRemove: &DrainRemoveJob{RecordIDs: ids, ResultCh: removeCh}}) {
		log.Printf("guests: purge %s skipped: worker unavailable", routeKeyStr(key))
		return
	}
	result := <-removeCh
	if result.Err != nil {
		log.Printf("guests: purge %s failed: %v", routeKeyStr(key), result.Err)
		return
	}
	log.Printf("guests: removed guest %d; dropped %d queued message(s)", key.ChatID, len(result.Removed))
}

// resolveGuestSelector resolves `dm:<chat_id>` to a guest route the private
// holder owns. The id must be a canonical positive integer.
func (b *Broker) resolveGuestSelector(stub *Stub, selector, raw string) (RouteKey, error) {
	chatID, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || chatID <= 0 || strconv.FormatInt(chatID, 10) != raw {
		return RouteKey{}, fmt.Errorf("route selector %q is not dm:<positive chat id>", selector)
	}
	key := MakeRouteKey(privateChannel, chatID, nil)
	if stub == nil || !stub.private.Load() || !b.Mappings().IsGuest(chatID) || !stub.HasRoute(key) {
		return RouteKey{}, fmt.Errorf("route %q is not held by this session", selector)
	}
	return key, nil
}
