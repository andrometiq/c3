package telegram

import (
	"testing"

	"github.com/PaulSonOfLars/gotgbot/v2"
)

func TestAtMentionsInText(t *testing.T) {
	got := atMentionsInText("hey @Glm_bot run tests and ping @claude")
	if len(got) != 2 || got[0] != "Glm_bot" || got[1] != "claude" {
		t.Fatalf("got %#v", got)
	}
	if atMentionsInText("email me@example.com") != nil && len(atMentionsInText("email me@example.com")) != 0 {
		// me@example.com has a letter before @ so it must not count
		if len(atMentionsInText("email me@example.com")) != 0 {
			t.Fatalf("email local-part @ must not be a mention: %#v", atMentionsInText("email me@example.com"))
		}
	}
}

func TestIsMuteCommand(t *testing.T) {
	for _, s := range []string{"/mute", "/MUTE", " /mute@glm_bot ", "/mute now"} {
		if !isMuteCommand(s) {
			t.Errorf("%q should be mute", s)
		}
	}
	if isMuteCommand("/muted") || isMuteCommand("please /mute") {
		t.Fatal("non-commands must not match")
	}
}

func TestAddressedToMe_TextMention(t *testing.T) {
	c := &Channel{}
	c.botUsername.Store("glm_bot")
	msg := &gotgbot.Message{Text: "please @glm_bot do this"}
	if !c.addressedToMe(msg, msg.Text) {
		t.Fatal("plaintext @glm_bot must address the bot")
	}
	if c.addressedToMe(msg, "no tags here") {
		t.Fatal("untagged text must not address")
	}
}

func TestAddressedToMe_ReplyToBot(t *testing.T) {
	c := &Channel{}
	c.botUsername.Store("glm_bot")
	msg := &gotgbot.Message{
		Text: "continue",
		ReplyToMessage: &gotgbot.Message{
			From: &gotgbot.User{Username: "glm_bot", IsBot: true},
		},
	}
	if !c.addressedToMe(msg, msg.Text) {
		t.Fatal("reply-to this bot must address it")
	}
}
