package telegram

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"
	"golang.org/x/time/rate"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// approvalTransport runs the real gotgbot request path without a network.
type approvalTransport func(*http.Request) (*http.Response, error)

func (transport approvalTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func approvalChannel(transport approvalTransport) *Channel {
	client := &gotgbot.BaseBotClient{Client: http.Client{Transport: transport}}
	return &Channel{bot: &gotgbot.Bot{Token: "test-token", BotClient: client}, ctx: context.Background(), rate: newRateLimiter()}
}

func telegramResponse(body string) *http.Response {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

const telegramMessageOK = `{"ok":true,"result":{"message_id":123,"date":1,"chat":{"id":42,"type":"supergroup"}}}`

// An approval tap's single answer belongs to the broker's resolver, and its
// keyboard must never be collapsed like a generic button's.
func TestApprovalCallbackDeferredAndNeverCollapsed(t *testing.T) {
	for _, decision := range []channel.GateInboundDecision{channel.GateInboundAllow, channel.GateInboundDrop} {
		host := &fakeHost{decision: decision}
		client := &recordingBotClient{}
		c := makeChannelWithBot(host, client)
		query := permTapQuery("approval", autoTapDataPrefix+"allow:opaque", 7)
		query.Message.(*gotgbot.Message).ReplyMarkup = &gotgbot.InlineKeyboardMarkup{
			InlineKeyboard: [][]gotgbot.InlineKeyboardButton{{{Text: "Allow once", CallbackData: query.Data}}},
		}
		c.dispatchCallback(1, query)
		if len(client.callsFor("editMessageReplyMarkup")) != 0 {
			t.Fatal("approval keyboard collapsed by the channel")
		}
		answers := client.callsFor("answerCallbackQuery")
		if decision == channel.GateInboundAllow {
			if len(answers) != 0 || host.emitCount() != 1 {
				t.Fatalf("allowed tap: %d answers, %d emitted; want the broker to answer it", len(answers), host.emitCount())
			}
			continue
		}
		if len(answers) != 1 || host.emitCount() != 0 {
			t.Fatalf("refused tap: %d answers, %d emitted", len(answers), host.emitCount())
		}
	}
}

// The card is bound to its attachment: it is sent as a reply that Telegram
// must not detach, as HTML, with the keyboard.
func TestSendApprovalCardWireShape(t *testing.T) {
	client := &captureBotClient{}
	c := &Channel{bot: &gotgbot.Bot{Token: "test", BotClient: client}, ctx: context.Background(), rate: newRateLimiter()}
	topic, replyTo := int64(7), int64(456)
	buttons := [][]c3types.Button{{{Text: "Allow once", Data: "c3:auto:allow:x"}, {Text: "Deny", Data: "c3:auto:deny:x"}}}
	id, err := c.SendApprovalCard(context.Background(), c3types.ReplyArgs{ChatID: 42, TopicID: &topic, ReplyTo: &replyTo, Text: "<b>card</b>", Buttons: buttons})
	if err != nil || id != 123 {
		t.Fatalf("send: id=%d err=%v", id, err)
	}
	reply, ok := client.params["reply_parameters"].(*gotgbot.ReplyParameters)
	if !ok || reply.MessageId != replyTo || reply.AllowSendingWithoutReply {
		t.Fatalf("reply binding = %#v", client.params["reply_parameters"])
	}
	if client.params["parse_mode"] != "HTML" || client.params["text"] != "<b>card</b>" || client.params["message_thread_id"] != topic {
		t.Fatalf("unexpected wire params: %#v", client.params)
	}
	if _, ok := client.params["reply_markup"].(*gotgbot.InlineKeyboardMarkup); !ok {
		t.Fatal("keyboard missing")
	}
}

// A missing attachment fails the card rather than posting it detached, and the
// returned error carries Telegram's description (never the bot token).
func TestSendApprovalCardFailureKeepsDescription(t *testing.T) {
	c := approvalChannel(func(*http.Request) (*http.Response, error) {
		return telegramResponse(`{"ok":false,"error_code":400,"description":"Bad Request: message to be replied not found"}`), nil
	})
	c.authBrk = newAuthBreaker(auth401Threshold)
	c.host = &fakeHost{}
	replyTo := int64(456)
	id, err := c.SendApprovalCard(context.Background(), c3types.ReplyArgs{ChatID: 42, ReplyTo: &replyTo, Text: "card"})
	if err == nil || id != 0 {
		t.Fatalf("missing attachment: id=%d err=%v", id, err)
	}
	if !strings.Contains(err.Error(), "message to be replied not found") || strings.Contains(err.Error(), "test-token") {
		t.Fatalf("error = %v", err)
	}
}

// The overflow input is uploaded from memory, under its name and caption.
func TestSendApprovalDocumentFromMemory(t *testing.T) {
	var name, content, caption string
	c := approvalChannel(func(request *http.Request) (*http.Response, error) {
		if err := request.ParseMultipartForm(1 << 20); err != nil {
			return nil, err
		}
		file, header, err := request.FormFile("document")
		if err != nil {
			return nil, err
		}
		data, _ := io.ReadAll(file)
		name, content, caption = header.Filename, string(data), request.FormValue("caption")
		return telegramResponse(telegramMessageOK), nil
	})
	id, err := c.SendApprovalDocument(context.Background(), c3types.ReplyArgs{ChatID: 42, Text: "Approval 01234567"}, "c3-approval-01234567.txt", []byte("full input"))
	if err != nil || id != 123 {
		t.Fatalf("send: id=%d err=%v", id, err)
	}
	if name != "c3-approval-01234567.txt" || content != "full input" || caption != "Approval 01234567" {
		t.Fatalf("uploaded %q %q %q", name, content, caption)
	}
}

// The request deadline bounds the rate-limit queue: nothing is sent after it.
func TestSendApprovalDeadlineBoundsRateQueue(t *testing.T) {
	client := &captureBotClient{}
	c := &Channel{bot: &gotgbot.Bot{Token: "test", BotClient: client}, ctx: context.Background(), rate: newRateLimiter()}
	c.rate.global = rate.NewLimiter(rate.Every(time.Second), 1)
	c.rate.global.Allow()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.SendApprovalCard(ctx, c3types.ReplyArgs{ChatID: 42, Text: "card"}); err == nil {
		t.Fatal("send passed the deadline")
	}
	if _, err := c.SendApprovalDocument(ctx, c3types.ReplyArgs{ChatID: 42}, "a.txt", []byte("x")); err == nil {
		t.Fatal("upload passed the deadline")
	}
	if time.Since(start) > 500*time.Millisecond || client.method != "" {
		t.Fatal("rate wait ignored the deadline or sent after it")
	}
}

// Cancelling the request aborts an in-flight HTTP send.
func TestSendApprovalCancellationAbortsHTTP(t *testing.T) {
	for _, isDocument := range []bool{false, true} {
		reached := make(chan struct{})
		aborted := make(chan struct{})
		c := approvalChannel(func(request *http.Request) (*http.Response, error) {
			_, _ = io.Copy(io.Discard, request.Body)
			close(reached)
			<-request.Context().Done()
			close(aborted)
			return nil, request.Context().Err()
		})
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			var err error
			if isDocument {
				_, err = c.SendApprovalDocument(ctx, c3types.ReplyArgs{ChatID: 42}, "a.txt", []byte("x"))
			} else {
				_, err = c.SendApprovalCard(ctx, c3types.ReplyArgs{ChatID: 42, Text: "card"})
			}
			result <- err
		}()
		select {
		case <-reached:
		case <-time.After(2 * time.Second):
			t.Fatal("send never reached HTTP")
		}
		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("document=%t: cancelled send returned %v", isDocument, err)
			}
		case <-time.After(time.Second):
			t.Fatal("send did not abort")
		}
		<-aborted
	}
}
