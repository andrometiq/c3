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
// Ask #1): a 21,226,288-byte voice note is past Telegram's hard getFile ceiling
// ("For the moment, bots can download files of up to 20MB in size" —
// https://core.telegram.org/bots/api#getfile), so the bot gets
// `Bad Request: file is too big` and no bytes, permanently. These tests pin that
// SIZE is reported as its own, recognizable cause — not as one more fetch error
// the caller might sensibly retry.

// getFileChannel builds a Channel wired to a fake Bot API that answers getFile
// with body, over a REAL *gotgbot.Bot so the error travels the same path it does
// in production.
func getFileChannel(t *testing.T, body string) *Channel {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)

	c := &Channel{
		host:      &fakeHost{},
		cfg:       Config{},
		authBrk:   newAuthBreaker(auth401Threshold),
		health:    newFetchHealth(),
		reach:     newReachability(),
		endpoints: []string{srv.URL},
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

// getFile's own "file is too big" refusal must come back tagged with
// ErrAttachmentTooLarge. Without the tag the broker cannot tell a permanent size
// refusal from a transient fetch failure, and it goes on offering retranscribe /
// download_attachment as recovery for a file that can never be fetched.
func TestAttachmentSize_TooBigCarriesSizeSentinel(t *testing.T) {
	c := getFileChannel(t, tooBigGetFileBody)

	_, err := c.AttachmentSize("F-BIG")
	if err == nil {
		t.Fatal("getFile refused the file as too big; AttachmentSize returned no error")
	}
	if !errors.Is(err, channel.ErrAttachmentTooLarge) {
		t.Fatalf("a 'file is too big' refusal must be tagged ErrAttachmentTooLarge so callers stop offering a retry that cannot work; got %v", err)
	}
	if !strings.Contains(err.Error(), "20.0 MB") {
		t.Fatalf("the size refusal must name the ceiling it measured against; got %q", err.Error())
	}
}

// The classifier must key on the SIZE description alone. A different 400 (or a
// transport failure) is not permanent, and mislabelling it would tell the human
// to re-record a message that would have transcribed fine on the next attempt.
func TestAttachmentSize_OtherFailureIsNotTaggedAsSize(t *testing.T) {
	c := getFileChannel(t, `{"ok":false,"error_code":400,"description":"Bad Request: invalid file_id"}`)

	_, err := c.AttachmentSize("F-BOGUS")
	if err == nil {
		t.Fatal("getFile failed; AttachmentSize returned no error")
	}
	if errors.Is(err, channel.ErrAttachmentTooLarge) {
		t.Fatalf("a non-size getFile failure was tagged as too-large — the human would be told to re-record for a retryable error; got %v", err)
	}
}

// A file the API is willing to describe answers with its size and no error, so
// the probe stays a cheap question and not a second failure path.
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

// DownloadAttachment's own pre-check must report a size the reader can act on.
// It used to divide bytes by 1 MiB as INTEGERS, so 21,226,288 bytes against the
// 20 MiB ceiling printed "20 MB > 20 MB limit" — a message that proves nothing.
func TestDownloadAttachment_OverLimitNamesDistinguishableSizes(t *testing.T) {
	c := getFileChannel(t, `{"ok":true,"result":{"file_id":"F-BIG","file_unique_id":"U","file_size":21226288,"file_path":"voice/file_1.oga"}}`)

	_, err := c.DownloadAttachment("F-BIG")
	if err == nil {
		t.Fatal("a 21,226,288-byte file is over the 20 MiB ceiling; DownloadAttachment returned no error")
	}
	if !errors.Is(err, channel.ErrAttachmentTooLarge) {
		t.Fatalf("the download size pre-check must carry ErrAttachmentTooLarge like the getFile refusal does; got %v", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "20.2 MB") || !strings.Contains(msg, "20.0 MB") {
		t.Fatalf("size and limit must print as DIFFERENT numbers (want 20.2 MB > 20.0 MB); got %q", msg)
	}
}
