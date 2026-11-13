package broker

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

// Event pushes need current host rendering facts, not attempt eligibility or
// historical receipts (which may confirm inbox delivery). Missing facts fail closed.
// Legacy defaults are Capable; Probing, QueueOnly, CrossSession and unknown
// states cannot prove rendering. Reading the state leaves the probe unconsumed.
func eventPushRenders(holder *Stub) bool {
	if holder.negotiated() {
		holder.delivery.mu.Lock()
		defer holder.delivery.mu.Unlock()
		channel := holder.acceptedLive().Channel
		return channel.Eligible && channel.EventPushRenders
	}
	// An offered contract must be confirmed before direct events are safe.
	// Invalid or refused offers also convert; only absent offers are legacy.
	if holder.deliveryOffered || holder.delivery != nil || holder.deliveryRefused.Load() {
		return false
	}
	return holder.RenderRoute().State == ipc.RenderCapable
}

// eventMessage preserves channel context but uses a fresh queue identity: the
// same keyboard, poll or message may produce several distinct events.
func eventMessage(ev *c3types.Inbound) *c3types.Inbound {
	if ev.Event == nil {
		return nil
	}
	messageID := ev.MessageID
	var text, callbackID, callbackData string
	switch ev.Kind {
	case c3types.InboundCallback:
		cb := ev.Event.Callback
		if cb == nil {
			return nil
		}
		messageID = cb.MessageID
		label := ""
		if cb.Label != "" {
			label = fmt.Sprintf(" %q", cb.Label)
		}
		text = fmt.Sprintf("[button tap] %s pressed%s (data=%q) on message %d", eventActor(cb.Actor), label, cb.Data, messageID)
		callbackID, callbackData = cb.CallbackID, cb.Data
	case c3types.InboundReaction:
		reaction := ev.Event.Reaction
		if reaction == nil {
			return nil
		}
		messageID = reaction.MessageID
		text = fmt.Sprintf("[reaction] %s added %q removed %q on message %d", eventActor(reaction.Actor), reaction.Added, reaction.Removed, messageID)
	case c3types.InboundPollResult:
		poll := ev.Event.PollResult
		if poll == nil {
			return nil
		}
		options := make([]string, len(poll.Options))
		for i, option := range poll.Options {
			options[i] = fmt.Sprintf("%q=%d", option.Text, option.VoterCount)
		}
		text = fmt.Sprintf("[poll result] poll=%q question=%q on message %d: options=[%s] total voters=%d closed=%t", poll.PollID, poll.Question, messageID, strings.Join(options, ", "), poll.TotalVoters, poll.IsClosed)
	default:
		return nil
	}
	return &c3types.Inbound{Channel: ev.Channel, ChatID: ev.ChatID, TopicID: ev.TopicID, Sender: ev.Sender, Timestamp: ev.Timestamp, ConvKind: ev.ConvKind, TestInjected: ev.TestInjected, Text: text, ReplyTo: &c3types.ReplyContext{MessageID: messageID},
		ConvertedFrom: ev.Kind, ConvertedCallbackID: callbackID, ConvertedCallbackData: callbackData}
}

func eventActor(actor c3types.Sender) string {
	if label := senderLabel(actor); label != "" {
		return strings.NewReplacer("\n", "\\n", "\r", "\\r").Replace(label)
	}
	return "unknown actor"
}

func (w *RouteWorker) convertEvent(ctx context.Context, ev *c3types.Inbound) bool {
	if ev.Kind == c3types.InboundReaction && w.broker.isPrivateNonOperatorRoute(w.key) {
		log.Printf("event dropped chan=%s chat=%d kind=reaction reason=private_guest_route", w.key.Channel, w.key.ChatID)
		return true
	}
	converted := eventMessage(ev)
	if converted == nil {
		return false
	}
	log.Printf("event converted chan=%s topic=%s kind=%s reason=direct_push_unavailable", w.key.Channel, TopicKeyStr(w.key), ev.Kind)
	w.flushMessageOccurrences(ctx, unsourcedInbounds([]*c3types.Inbound{converted}), true)
	return true
}

func (w *RouteWorker) noteEventStorageFailure(err error) {
	log.Printf("event persist FAIL chan=%s topic=%s: %v — delivery is unconfirmed; durable fallback failed; automatic redelivery is not guaranteed", w.key.Channel, TopicKeyStr(w.key), err)
	w.noteStorageFailure("⚠️ Channel event delivery is unconfirmed; durable fallback failed (storage error). It may not be redelivered automatically. Check the broker host's disk and resend the choice or update as a message.")
}
