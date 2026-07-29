package telegram

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/channel"
)

// 2026-07-27 incident (local-notes/INCIDENT-2026-07-27-voice-limit-ordering.md,
// Ask #1): a 21,226,288-byte voice note was refused by Telegram with
// `Bad Request: file is too big`, and C3 reported a generic transcription
// failure that promised the audio was "saved and recoverable". Masking the real
// cause was the whole defect.
//
// The rule (maintainer, 2026-07-29): C3 holds NO size limit and compares nothing
// against one — 20 MiB is api.telegram.org's current number, not the Bot API's,
// a self-hosted server has none ("Download files without a size limit",
// https://core.telegram.org/bots/api#using-a-local-bot-api-server), and either
// can change. The bot server is asked, and whatever it answers is reported.

// getFileChannel builds a Channel wired to a fake Bot API that answers with
// body, over a REAL *gotgbot.Bot so errors travel the production path.
func getFileChannel(t *testing.T, body string) *Channel {
	t.Helper()
	return getFileChannelHandler(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	})
}

func getFileChannelHandler(t *testing.T, h http.HandlerFunc) *Channel {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	c := &Channel{
		host:       &fakeHost{},
		cfg:        Config{},
		authBrk:    newAuthBreaker(auth401Threshold),
		health:     newFetchHealth(),
		reach:      newReachability(),
		endpoints:  []string{srv.URL},
		httpClient: &http.Client{}, // body downloads bypass gotgbot
		bot: &gotgbot.Bot{
			Token:     "test",
			BotClient: &gotgbot.BaseBotClient{Client: http.Client{}},
		},
	}
	c.activeEndpoint.Store(0)
	c.ctx, c.cancel = context.WithCancel(context.Background())
	t.Cleanup(c.cancel)
	return c
}

const tooBigGetFileBody = `{"ok":false,"error_code":400,"description":"Bad Request: file is too big"}`

// The server's size refusal must come back tagged with ErrAttachmentTooLarge AND
// quoting the server. Without the tag the broker cannot tell a permanent refusal
// from a transient failure, and goes on offering retranscribe / download_attachment
// as recovery for a file that can never be fetched.
func TestAttachmentSize_TooBigCarriesSentinelAndTheServersWords(t *testing.T) {
	c := getFileChannel(t, tooBigGetFileBody)

	_, err := c.AttachmentSize("F-BIG")
	if err == nil {
		t.Fatal("getFile refused the file as too big; AttachmentSize returned no error")
	}
	if !errors.Is(err, channel.ErrAttachmentTooLarge) {
		t.Fatalf("a size refusal must be tagged ErrAttachmentTooLarge so callers stop offering a retry that cannot work; got %v", err)
	}
	if !strings.Contains(err.Error(), "file is too big") {
		t.Fatalf("the refusal must quote the SERVER's own description, not a cause C3 decided; got %q", err.Error())
	}
}

// C3 must not state a limit of its own — not even in the message. The number is
// the server's to know, and inventing one is exactly the assumption that breaks
// on a self-hosted server or the day Telegram moves its ceiling.
func TestAttachmentSize_RefusalStatesNoLimitOfOurOwn(t *testing.T) {
	c := getFileChannel(t, tooBigGetFileBody)

	_, err := c.AttachmentSize("F-BIG")
	if msg := err.Error(); strings.Contains(msg, "20.0 MB") || strings.Contains(msg, "20 MB") || strings.Contains(msg, "20971520") {
		t.Fatalf("the refusal quotes a ceiling C3 made up; only the server knows its limit. got %q", msg)
	}
}

// The classifier must key on the SIZE description alone. A different 400 (or a
// transport failure) is not permanent, and mislabelling it would tell the human
// to re-share a message that would have transcribed fine on the next attempt.
func TestAttachmentSize_OtherFailureIsNotTaggedAsSize(t *testing.T) {
	c := getFileChannel(t, `{"ok":false,"error_code":400,"description":"Bad Request: invalid file_id"}`)

	_, err := c.AttachmentSize("F-BOGUS")
	if err == nil {
		t.Fatal("getFile failed; AttachmentSize returned no error")
	}
	if errors.Is(err, channel.ErrAttachmentTooLarge) {
		t.Fatalf("a non-size getFile failure was tagged as too-large — the human would be told to re-share for a retryable error; got %v", err)
	}
}

// Wording drift degrades to VERBATIM PASSTHROUGH, not to a false classification
// and not to the generic transcription-failed text. Telegram owns this
// human-readable string and can reword it; when it does, the caller still shows
// the server's actual words, which satisfies the rule (transparent beats
// classified) even though the too-big-specific recovery advice is lost.
func TestAttachmentSize_RewordedRefusalDegradesToVerbatimPassthrough(t *testing.T) {
	c := getFileChannel(t, `{"ok":false,"error_code":400,"description":"Bad Request: file is too large"}`)

	_, err := c.AttachmentSize("F-BIG")
	if err == nil {
		t.Fatal("a getFile failure must still be an error")
	}
	if errors.Is(err, channel.ErrAttachmentTooLarge) {
		t.Fatal("reworded text must not be silently classified — this test makes the string match's blast radius visible if Telegram rewords it")
	}
	if !strings.Contains(err.Error(), "file is too large") {
		t.Fatalf("an unclassified refusal must carry the server's words through verbatim; got %q", err.Error())
	}
}

// A file the API is willing to describe answers with its size and no error, so
// the ask stays a cheap question and not a second failure path.
func TestAttachmentSize_ReportsSizeWithoutDownloading(t *testing.T) {
	c := getFileChannel(t, `{"ok":true,"result":{"file_id":"F-OK","file_unique_id":"U","file_size":1234,"file_path":"voice/file_1.oga"}}`)

	size, err := c.AttachmentSize("F-OK")
	if err != nil {
		t.Fatalf("AttachmentSize on a fetchable file: %v", err)
	}
	if size != 1234 {
		t.Fatalf("AttachmentSize = %d, want the file_size the API reported (1234)", size)
	}
}

// DownloadAttachment must not second-guess a server that already agreed to serve
// the file. getFile IS the size check; a local re-judgement could only ever
// refuse a download that was about to work — on a self-hosted Bot API server,
// every file over 20 MiB.
func TestDownloadAttachment_ServesWhateverTheServerAgreedTo(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	fetched := make(chan struct{}, 1)
	c := getFileChannelHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/file/") {
			select {
			case fetched <- struct{}{}:
			default:
			}
			_, _ = w.Write([]byte("OGGDATA"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"file_id":"F-BIG","file_unique_id":"U","file_size":41943040,"file_path":"voice/file_1.oga"}}`))
	})

	if _, err := c.DownloadAttachment("F-BIG"); err != nil {
		t.Fatalf("a 40 MB file the server handed a file_path for must be fetched, not refused by a ceiling of C3's own; got %v", err)
	}
	select {
	case <-fetched:
	default:
		t.Fatal("DownloadAttachment refused before requesting the body — it applied a size limit the server never claimed")
	}
}

// …and a server that refuses still surfaces as the tagged, quoted cause.
func TestDownloadAttachment_ServerRefusal_CarriesSentinel(t *testing.T) {
	c := getFileChannel(t, tooBigGetFileBody)

	_, err := c.DownloadAttachment("F-BIG")
	if !errors.Is(err, channel.ErrAttachmentTooLarge) {
		t.Fatalf("download_attachment must report a size refusal as the tagged size cause, not a generic fetch error; got %v", err)
	}
	if !strings.Contains(err.Error(), "file is too big") {
		t.Fatalf("download_attachment must quote the server; got %q", err.Error())
	}
}
