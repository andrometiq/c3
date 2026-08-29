package telegram

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
)

type captureBotClient struct {
	method string
	params map[string]any
}

func (c *captureBotClient) RequestWithContext(_ context.Context, _ string, method string, params map[string]any, _ *gotgbot.RequestOpts) (json.RawMessage, error) {
	c.method = method
	c.params = params
	return json.RawMessage(`{"message_id":123,"date":0,"chat":{"id":42,"type":"private"}}`), nil
}

func (*captureBotClient) GetAPIURL(*gotgbot.RequestOpts) string { return gotgbot.DefaultAPIURL }
func (*captureBotClient) FileURL(string, string, *gotgbot.RequestOpts) string {
	return ""
}

func TestSendReplyHonorsDisableLinkPreview(t *testing.T) {
	client := &captureBotClient{}
	channel := &Channel{
		bot:       &gotgbot.Bot{Token: "test", BotClient: client},
		ctx:       context.Background(),
		rate:      newRateLimiter(),
		endpoints: []string{""},
	}
	if _, err := channel.SendReply(c3types.ReplyArgs{
		ChatID: 42, Text: "https://device.example/auth#token",
		Markup: c3types.MarkupNone, DisableLinkPreview: true,
	}); err != nil {
		t.Fatalf("SendReply: %v", err)
	}
	if client.method != "sendMessage" {
		t.Fatalf("method=%q, want sendMessage", client.method)
	}
	options, ok := client.params["link_preview_options"].(*gotgbot.LinkPreviewOptions)
	if !ok || options == nil || !options.IsDisabled {
		t.Fatalf("link_preview_options=%#v, want is_disabled=true", client.params["link_preview_options"])
	}
}
