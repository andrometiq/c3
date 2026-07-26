package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
)

const fakeToken = "1234567890:AAHfake-Token-Value-For-Tests-Only-xyz"

func TestRedactToken(t *testing.T) {
	if got := redactToken("a "+fakeToken+" b", fakeToken); strings.Contains(got, fakeToken) {
		t.Errorf("token survived redaction: %q", got)
	}
	// An unconfigured channel must not turn every string into <redacted>.
	if got := redactToken("nothing to hide", ""); got != "nothing to hide" {
		t.Errorf("empty token should be a no-op; got %q", got)
	}
}

// The leak this closes: net/http reports failures as *url.Error, whose Error()
// prints the full request URL — and Bot-API URLs carry the token in the path.
func TestScrubToken_RemovesTokenFromURLError(t *testing.T) {
	c := &Channel{cfg: Config{BotToken: fakeToken}}

	raw := &url.Error{
		Op:  "Get",
		URL: "https://api.telegram.org/file/bot" + fakeToken + "/voice/file_3.oga",
		Err: errors.New("dial tcp: lookup api.telegram.org: no such host"),
	}
	wrapped := fmt.Errorf("telegram: download %q: %w", "voice/file_3.oga", raw)

	if !strings.Contains(wrapped.Error(), fakeToken) {
		t.Fatal("precondition: the unscrubbed error should contain the token")
	}

	got := c.scrubToken(wrapped)
	if strings.Contains(got.Error(), fakeToken) {
		t.Errorf("BOT TOKEN LEAKED into an agent-visible error: %q", got.Error())
	}
	if !strings.Contains(got.Error(), "no such host") {
		t.Errorf("scrubbing must keep the diagnostic detail; got %q", got.Error())
	}

	// Must not remain reachable by unwrapping — that would defeat the point.
	if unwrapped := errors.Unwrap(got); unwrapped != nil && strings.Contains(unwrapped.Error(), fakeToken) {
		t.Errorf("token still reachable via errors.Unwrap: %q", unwrapped.Error())
	}
}

func TestScrubToken_PassesThroughCleanErrors(t *testing.T) {
	c := &Channel{cfg: Config{BotToken: fakeToken}}
	in := errors.New("telegram: download \"x\": HTTP 404")
	if got := c.scrubToken(in); got != in {
		t.Errorf("an error without the token should be returned unchanged")
	}
	if c.scrubToken(nil) != nil {
		t.Error("nil must stay nil")
	}
}

// End-to-end on the real path: a download against an unroutable endpoint must
// not hand the token back to the agent.
func TestDownloadAttachment_ErrorNeverCarriesToken(t *testing.T) {
	// A reserved-for-documentation address that cannot be routed, with a short
	// timeout so the test stays fast and hermetic (no DNS, no real network).
	c := &Channel{
		cfg:        Config{BotToken: fakeToken},
		endpoints:  []string{"http://192.0.2.1:1"},
		ctx:        context.Background(),
		httpClient: &http.Client{Timeout: 250 * time.Millisecond},
	}

	dlURL := c.fileDownloadURL("voice/file_3.oga")
	if !strings.Contains(dlURL, fakeToken) {
		t.Fatalf("precondition: the download URL should embed the token; got %q", dlURL)
	}

	req, err := http.NewRequestWithContext(c.ctx, http.MethodGet, dlURL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	_, doErr := c.httpClient.Do(req)
	if doErr == nil {
		t.Skip("the unroutable endpoint unexpectedly connected; nothing to assert")
	}

	// This mirrors exactly what DownloadAttachment returns on a transport error.
	got := c.scrubTokenf("telegram: download %q: %w", "voice/file_3.oga", doErr)
	if strings.Contains(got.Error(), fakeToken) {
		t.Errorf("BOT TOKEN LEAKED to the agent: %q", got.Error())
	}
}

func TestGotgbotErrorPathsNeverExposeToken(t *testing.T) {
	raw := &url.Error{
		Op:  "Post",
		URL: "https://api.telegram.org/bot" + fakeToken + "/sendMessage",
		Err: errors.New("dial tcp: network unreachable"),
	}
	client := &funcBotClient{fn: func(int) (json.RawMessage, error) {
		return nil, raw
	}}
	host := &fakeHost{}
	c := &Channel{
		cfg:     Config{BotToken: fakeToken},
		host:    host,
		bot:     &gotgbot.Bot{Token: fakeToken, BotClient: client},
		rate:    newRateLimiter(),
		ctx:     context.Background(),
		authBrk: newAuthBreaker(auth401Threshold),
	}
	const chatID = int64(-100)

	poll := &c3types.PollSpec{Question: "Pick", Options: []string{"A", "B"}}
	calls := []struct {
		name string
		call func() error
	}{
		{"send-message", func() error {
			_, err := c.SendReply(c3types.ReplyArgs{ChatID: chatID, Text: "hello", Markup: c3types.MarkupNone})
			return err
		}},
		{"send-media", func() error {
			_, err := c.SendReply(c3types.ReplyArgs{ChatID: chatID, Media: []c3types.MediaItem{{Kind: c3types.MediaPhoto, URL: "https://example.invalid/a.jpg"}}})
			return err
		}},
		{"send-poll", func() error {
			_, err := c.SendReply(c3types.ReplyArgs{ChatID: chatID, Poll: poll})
			return err
		}},
		{"typing", func() error { return c.SendTyping(chatID, nil) }},
		{"edit", func() error {
			_, err := c.EditMessage(c3types.EditArgs{ChatID: chatID, MessageID: 1, Text: "edit", Markup: c3types.MarkupNone})
			return err
		}},
		{"reaction", func() error {
			return c.React(c3types.ReactArgs{ChatID: chatID, MessageID: 1, Emoji: "👍"})
		}},
		{"get-file", func() error {
			_, err := c.DownloadAttachment("file-id")
			return err
		}},
		{"create-topic", func() error {
			_, err := c.CreateTopic(chatID, "topic")
			return err
		}},
		{"stop-poll", func() error {
			_, err := c.StopPoll(chatID, 1)
			return err
		}},
		{"answer-callback", func() error {
			return c.AnswerCallback("callback-id", "done", false)
		}},
	}

	for _, tc := range calls {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			if err == nil {
				t.Fatal("precondition: scripted gotgbot client must fail")
			}
			if strings.Contains(err.Error(), fakeToken) {
				t.Fatalf("BOT TOKEN LEAKED from %s: %q", tc.name, err)
			}
			if !strings.Contains(err.Error(), "<redacted>") {
				t.Fatalf("%s error lost the redaction marker: %q", tc.name, err)
			}
		})
	}

	// Internal poll/heartbeat/readback paths log gotgbot errors instead of
	// returning them. The shared logger must scrub those arguments too.
	c.logf("telegram: scripted gotgbot failure: %v", raw)
	host.mu.Lock()
	defer host.mu.Unlock()
	for _, line := range host.logs {
		if strings.Contains(line, fakeToken) {
			t.Fatalf("BOT TOKEN LEAKED into log: %q", line)
		}
	}
}
