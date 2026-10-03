package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/Andrometiq/c3/internal/c3types"
)

// autoCardSender is the channel capability approval cards need. Both sends take
// the request's context, so its deadline bounds rate-limit queueing, upload and
// HTTP (§5.4). The overflow attachment is uploaded from memory: the tool input
// never touches disk.
type autoCardSender interface {
	// SendApprovalCard sends args.Text as pre-escaped HTML with args.Buttons.
	// With args.ReplyTo set, the send fails unless that message exists.
	SendApprovalCard(ctx context.Context, args c3types.ReplyArgs) (int64, error)
	// SendApprovalDocument uploads content as a plain-text document named name,
	// with args.Text as its caption.
	SendApprovalDocument(ctx context.Context, args c3types.ReplyArgs, name string, content []byte) (int64, error)
}

// Tap answers. Telegram caps them at 200 characters; they never echo input.
const (
	autoAnswerInvalidText       = "⚠️ This approval button is not valid — tap not processed."
	autoAnswerNotAuthorizedText = "⛔ Not authorized — only a DM-paired C3 operator can approve. Tap ignored."
	autoAnswerGoneText          = "⌛ This approval is no longer active — expired, cancelled or already used."
	autoAnswerDecidedText       = "This approval was already answered."
	autoAnswerWrongCardText     = "⚠️ This button does not belong to this approval's card — tap not processed."
	autoAnswerAllowedText       = "✅ Allowed — arming one retry"
	autoAnswerDeniedText        = "❌ Denied"
)

// autoCardStatus is the line a card is edited to carry in each state after the
// tap. Best effort: a broker restart loses it. None claims the call ran.
var autoCardStatus = map[autoState]string{
	autoArmed:     "✅ Allowed — retry armed",
	autoConsumed:  "✅ Retry authorized (execution unconfirmed)",
	autoExpired:   "⌛ Allowed, not used (expired)",
	autoDenied:    "❌ Denied",
	autoTimedOut:  "⌛ Timed out",
	autoCancelled: "🚫 Cancelled (session ended / hook gone)",
	autoVetoed:    "⚠️ Allowed, but Claude Code still blocked the retry; it did not run",
}

// sendAutoCard renders and posts r's card. When the input overflows one
// message, the escaped input goes first as a document and the card replies to
// it, so interleaved cards can't be matched to the wrong file. Any failure
// sends no card and cancels the request (fail closed).
func (b *Broker) sendAutoCard(r *autoRequest, toolInput json.RawMessage, reason string) {
	defer recoverGoroutine("autoCard")
	card, err := renderAutoCard(autoCardFields{
		requestID: r.id,
		inputHash: r.key.inputHash,
		toolName:  r.key.ToolName,
		cwd:       r.key.CWD,
		reason:    reason,
		toolInput: toolInput,
		grantTTL:  r.grantTTL,
	})
	if err != nil {
		b.failAutoCard(r, "render", err)
		return
	}
	ch, err := b.Channel(r.route.Channel)
	if err != nil {
		b.failAutoCard(r, "channel", err)
		return
	}
	sender, ok := ch.(autoCardSender)
	if !ok {
		b.failAutoCard(r, "channel", errors.New("cannot send approval cards"))
		return
	}
	args := c3types.ReplyArgs{Channel: r.route.Channel, ChatID: r.route.ChatID, TopicID: routeTopicID(r.route)}
	if card.overflow != nil {
		document := args
		document.Text = "Approval " + autoShortID(r.id) + " · input " + r.key.inputHash[:12]
		id, err := sender.SendApprovalDocument(r.ctx, document, autoOverflowName(r.id), card.overflow)
		if err != nil {
			b.failAutoCard(r, "attachment", err)
			return
		}
		// auditAuto reads only fields fixed at creation, so it needs no lock.
		b.auditAuto(r, "attachment-sent", 0, fmt.Sprintf("msg=%d", id))
		args.ReplyTo = &id
	}
	b.auto.mu.Lock()
	isLive := b.refreshAutoLocked(r)
	b.auto.mu.Unlock()
	if !isLive {
		if args.ReplyTo != nil {
			// The input is in the topic with no card; record where.
			b.auditAuto(r, "attachment-orphaned", 0, fmt.Sprintf("msg=%d", *args.ReplyTo))
		}
		return
	}
	args.Text = card.text
	args.Buttons = [][]c3types.Button{{
		{Text: "Allow once", Data: autoCallbackPrefix + "allow:" + r.id},
		{Text: "Deny", Data: autoCallbackPrefix + "deny:" + r.id},
	}}
	id, err := sender.SendApprovalCard(r.ctx, args)
	if err != nil {
		b.failAutoCard(r, "card", err)
		return
	}
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	r.messageID, r.cardText = id, card.text
	b.auditAuto(r, "card-sent", 0, fmt.Sprintf("msg=%d state=%s", id, r.state))
	if !r.state.isLive() {
		// The request ended while the card was in flight: show how.
		go b.editAutoCard(r)
	}
}

// failAutoCard audits a send that failed, with the channel's error (never
// input), and cancels the request. A send aborted because the request ended
// may still have reached Telegram, so the audit doesn't claim it didn't.
func (b *Broker) failAutoCard(r *autoRequest, step string, err error) {
	detail := step + ": " + err.Error()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		detail = step + ": send aborted, delivery unknown: " + err.Error()
	}
	b.auto.mu.Lock()
	defer b.auto.mu.Unlock()
	b.auditAuto(r, "card-failed", 0, detail)
	if r.state.isLive() {
		b.setAutoStateLocked(r, autoCancelled, "card not sent")
	}
}

// resolveAutoTap handles a tap on an approval card. Only an allowlisted
// operator's tap on the exact card (chat, topic and message recorded at send)
// of a still-pending request changes anything; every other tap is answered and
// audited with no state change. The channel deferred the tap's ack, so every
// path answers it.
func (b *Broker) resolveAutoTap(route RouteKey, cb *c3types.CallbackEvent) {
	verb, id, ok := parseAutoCallback(cb.Data)
	if !ok {
		log.Printf("auto-approval tap-refused route=%s actor=%d cause=malformed", routeKeyStr(route), cb.Actor.UserID)
		b.answerPermCallback(route, cb.CallbackID, autoAnswerInvalidText, false)
		return
	}
	if !b.Mappings().IsUserAllowed(cb.Actor.UserID) {
		// Looked up for the audit only: no refresh, no state change.
		b.auto.mu.Lock()
		if r := b.auto.byID[id]; r != nil {
			b.auditAuto(r, "tap-"+verb+"-refused", cb.Actor.UserID, "not an operator")
		} else {
			log.Printf("auto-approval tap-%s-refused id=%q route=%s actor=%d cause=not-operator", verb, id, routeKeyStr(route), cb.Actor.UserID)
		}
		b.auto.mu.Unlock()
		b.answerPermCallback(route, cb.CallbackID, autoAnswerNotAuthorizedText, true)
		return
	}
	b.auto.mu.Lock()
	r := b.auto.byID[id]
	answer, cause := "", ""
	switch {
	case r == nil || !b.refreshAutoLocked(r):
		answer, cause = autoAnswerGoneText, "not active"
	case r.state != autoPending:
		answer, cause = autoAnswerDecidedText, "already decided"
	case r.messageID == 0:
		answer, cause = autoAnswerWrongCardText, "card not recorded yet"
	case r.route != route || r.messageID != cb.MessageID:
		answer, cause = autoAnswerWrongCardText, "wrong chat, topic or message"
	case verb == "allow":
		b.setAutoStateLocked(r, autoApproved, "")
		answer = autoAnswerAllowedText
	default:
		b.setAutoStateLocked(r, autoDenied, "")
		answer = autoAnswerDeniedText
	}
	if r == nil {
		log.Printf("auto-approval tap-%s-refused id=%q route=%s actor=%d cause=unknown", verb, id, routeKeyStr(route), cb.Actor.UserID)
	} else if cause != "" {
		b.auditAuto(r, "tap-"+verb+"-refused", cb.Actor.UserID, cause)
	} else {
		b.auditAuto(r, "tap-"+verb, cb.Actor.UserID, "")
	}
	b.auto.mu.Unlock()
	b.answerPermCallback(route, cb.CallbackID, answer, false)
}

// parseAutoCallback splits "c3:auto:<allow|deny>:<request id>".
func parseAutoCallback(data string) (verb, id string, ok bool) {
	verb, id, found := strings.Cut(strings.TrimPrefix(data, autoCallbackPrefix), ":")
	if !strings.HasPrefix(data, autoCallbackPrefix) || !found || id == "" || (verb != "allow" && verb != "deny") {
		return "", "", false
	}
	return verb, id, true
}

// editAutoCard shows r's current state on its card and clears the keyboard.
// Edits run asynchronously and can finish out of order, so each one renders
// the state at the time it runs.
func (b *Broker) editAutoCard(r *autoRequest) {
	defer recoverGoroutine("autoCardEdit")
	r.editMu.Lock()
	defer r.editMu.Unlock()
	b.auto.mu.Lock()
	status, messageID, text := autoCardStatus[r.state], r.messageID, r.cardText
	b.auto.mu.Unlock()
	if status == "" || messageID == 0 {
		return
	}
	text += "\n\n<b>" + status + "</b>"
	if _, err := b.editKeyboardMessage(r.route, messageID, text, [][]c3types.Button{}, c3types.MarkupNative); err != nil {
		log.Printf("auto-approval card-edit-failed id=%s route=%s msg=%d: %v", r.id, routeKeyStr(r.route), messageID, err)
	}
}

// autoShortID is the request id prefix shown on a card and its attachment.
func autoShortID(id string) string { return id[:8] }

func autoOverflowName(id string) string { return "c3-approval-" + autoShortID(id) + ".txt" }
