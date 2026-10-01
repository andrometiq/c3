package telegram

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/channel"
)

const okGetFileBody = `{"ok":true,"result":{"file_id":"F","file_unique_id":"U","file_size":7,"file_path":"voice/file_1.oga"}}`

// voiceServerChannel is a channel whose fake Bot API answers getFile with a
// servable file and hands every /file/ download to body.
func voiceServerChannel(t *testing.T, body http.HandlerFunc) *Channel {
	t.Helper()
	return getFileChannelHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/file/") {
			body(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(okGetFileBody))
	})
}

// cutBody promises 1000 bytes, sends a few, and drops the connection.
func cutBody(w http.ResponseWriter, _ *http.Request) {
	conn, buf, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\npartial")
	_ = buf.Flush()
	_ = conn.Close()
}

// stallUntilDone reads the request and then never answers, until the client
// gives up or the test ends. Reading the body lets the server notice the
// client hanging up.
func stallUntilDone(t *testing.T) func(*http.Request) {
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	return func(r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-done:
		}
	}
}

func shortenVoiceFetchBudget(t *testing.T, d time.Duration) {
	previous := voiceFetchBudget
	voiceFetchBudget = d
	t.Cleanup(func() { voiceFetchBudget = previous })
}

func requireTransient(t *testing.T, err error) *channel.AttachmentTransientError {
	t.Helper()
	var transient *channel.AttachmentTransientError
	if !errors.As(err, &transient) {
		t.Fatalf("err = %v, want *channel.AttachmentTransientError", err)
	}
	return transient
}

func requireNoFiles(t *testing.T, dir string) {
	t.Helper()
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		t.Errorf("file left behind in %s: %s", dir, entry.Name())
	}
}

func TestFetchVoice_CutBodyIsTransientAndLeavesNoFile(t *testing.T) {
	attachments, _ := fetchVoiceEnv(t)
	c := voiceServerChannel(t, cutBody)

	_, _, err := c.FetchVoice(context.Background(), "F")
	requireTransient(t, err)
	requireNoFiles(t, attachments)
}

// R7-7: a getFile the server reads and never answers is a deadline, and a
// deadline is a network condition: retry, never a terminal failure.
func TestFetchVoice_StalledGetFileIsTransient(t *testing.T) {
	fetchVoiceEnv(t)
	shortenVoiceFetchBudget(t, 300*time.Millisecond)
	stall := stallUntilDone(t)
	c := getFileChannelHandler(t, func(_ http.ResponseWriter, r *http.Request) { stall(r) })

	start := time.Now()
	_, _, err := c.FetchVoice(context.Background(), "F")
	requireTransient(t, err)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("stalled getFile took %v; the fetch budget must bound it", elapsed)
	}
}

func TestFetchVoice_ClassifiesServerAnswers(t *testing.T) {
	cases := []struct {
		name        string
		getFile     string // getFile JSON; "" ⇒ servable file
		bodyStatus  int
		retryHeader string
		isTransient bool
		after       time.Duration
	}{
		{name: "telegram 429 carries retry_after", getFile: `{"ok":false,"error_code":429,"description":"Too Many Requests: retry after 120","parameters":{"retry_after":120}}`, isTransient: true, after: 120 * time.Second},
		{name: "telegram 502", getFile: `{"ok":false,"error_code":502,"description":"Bad Gateway"}`, isTransient: true},
		{name: "telegram 403", getFile: `{"ok":false,"error_code":403,"description":"Forbidden"}`},
		{name: "undecodable getFile body", getFile: `<html>proxy error</html>`, isTransient: true},
		{name: "body 503", bodyStatus: http.StatusServiceUnavailable, isTransient: true},
		{name: "body 429 with Retry-After", bodyStatus: http.StatusTooManyRequests, retryHeader: "7", isTransient: true, after: 7 * time.Second},
		{name: "body 404", bodyStatus: http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			attachments, _ := fetchVoiceEnv(t)
			c := getFileChannelHandler(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.Contains(r.URL.Path, "/file/") {
					if tc.retryHeader != "" {
						w.Header().Set("Retry-After", tc.retryHeader)
					}
					w.WriteHeader(tc.bodyStatus)
					return
				}
				body := tc.getFile
				if body == "" {
					body = okGetFileBody
				}
				_, _ = w.Write([]byte(body))
			})

			_, _, err := c.FetchVoice(context.Background(), "F")
			if err == nil {
				t.Fatal("FetchVoice succeeded; want a failure")
			}
			var transient *channel.AttachmentTransientError
			if got := errors.As(err, &transient); got != tc.isTransient {
				t.Fatalf("transient = %v, want %v (err %v)", got, tc.isTransient, err)
			}
			if tc.isTransient && transient.After != tc.after {
				t.Fatalf("After = %v, want %v", transient.After, tc.after)
			}
			requireNoFiles(t, attachments)
		})
	}
}

// The 60 s download-client timeout must not apply: a transfer that keeps
// progressing past it but inside the fetch budget succeeds.
func TestFetchVoice_SlowBodyOutlivesTheClientTimeout(t *testing.T) {
	fetchVoiceEnv(t)
	c := voiceServerChannel(t, func(w http.ResponseWriter, _ *http.Request) {
		for range 5 {
			_, _ = w.Write([]byte("chunk"))
			w.(http.Flusher).Flush()
			time.Sleep(80 * time.Millisecond)
		}
	})
	c.httpClient.Timeout = 100 * time.Millisecond

	path, _, err := c.FetchVoice(context.Background(), "F")
	if err != nil {
		t.Fatalf("a progressing body inside the budget failed: %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != strings.Repeat("chunk", 5) {
		t.Fatalf("attempt file = %q", got)
	}
}

func TestFetchVoice_BudgetExpiryIsTransient(t *testing.T) {
	attachments, _ := fetchVoiceEnv(t)
	shortenVoiceFetchBudget(t, 300*time.Millisecond)
	stall := stallUntilDone(t)
	c := voiceServerChannel(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("start"))
		w.(http.Flusher).Flush()
		stall(r)
	})

	_, _, err := c.FetchVoice(context.Background(), "F")
	requireTransient(t, err)
	requireNoFiles(t, attachments)
}

// R7-8: no size limit of C3's own; a custom Bot API server may serve more than
// 20 MB.
func TestFetchVoice_LargeFileFromCustomServer(t *testing.T) {
	fetchVoiceEnv(t)
	payload := bytes.Repeat([]byte{0xA5}, 25<<20)
	c := voiceServerChannel(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(payload) })

	path, size, err := c.FetchVoice(context.Background(), "F")
	if err != nil {
		t.Fatalf("25 MB fetch: %v", err)
	}
	if info, err := os.Stat(path); err != nil || info.Size() != int64(len(payload)) || size != int64(len(payload)) {
		t.Fatalf("attempt file size = %v/%d (err %v), want %d", info, size, err, len(payload))
	}
}

func TestFetchVoice_ConcurrentFetchesOwnDistinctFiles(t *testing.T) {
	fetchVoiceEnv(t)
	c := voiceServerChannel(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("same-audio")) })

	var wg sync.WaitGroup
	paths := make([]string, 2)
	errs := make([]error, 2)
	for i := range paths {
		wg.Add(1)
		go func() {
			defer wg.Done()
			paths[i], _, errs[i] = c.FetchVoice(context.Background(), "F")
		}()
	}
	wg.Wait()
	for i, path := range paths {
		if errs[i] != nil {
			t.Fatalf("fetch %d: %v", i, errs[i])
		}
		if got, _ := os.ReadFile(path); string(got) != "same-audio" {
			t.Fatalf("fetch %d file = %q", i, got)
		}
	}
	if paths[0] == paths[1] {
		t.Fatalf("concurrent attempts share %s; each must own its file", paths[0])
	}
}

// R7-6/R9-1: a cache hit is a fresh COPY with its own new mtime; the retained
// inbox file's bytes and mtime are untouched, and no network call is made. An
// hours-old source must not make the attempt file look stale to the sweep.
func TestFetchVoice_CacheHitIsAFreshCopyTheSweepKeeps(t *testing.T) {
	attachments, inbox := fetchVoiceEnv(t)
	var network atomic.Int64
	c := getFileChannelHandler(t, func(http.ResponseWriter, *http.Request) { network.Add(1) })
	cached := filepath.Join(inbox, "1700000000000-F-CACHED.oga")
	if err := os.WriteFile(cached, []byte("retained-audio"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-3 * time.Hour).Truncate(time.Second)
	if err := os.Chtimes(cached, old, old); err != nil {
		t.Fatal(err)
	}

	path, size, err := c.FetchVoice(context.Background(), "F-CACHED")
	if err != nil {
		t.Fatalf("cache hit: %v", err)
	}
	if network.Load() != 0 {
		t.Fatalf("cache hit made %d network call(s)", network.Load())
	}
	if path == cached || filepath.Dir(path) != attachments {
		t.Fatalf("cache hit returned %q; want a fresh attempt file in %s, never the inbox file", path, attachments)
	}
	if got, _ := os.ReadFile(path); string(got) != "retained-audio" || size != int64(len("retained-audio")) {
		t.Fatalf("attempt copy = %q (size %d)", got, size)
	}
	if info, _ := os.Stat(path); time.Since(info.ModTime()) > time.Minute {
		t.Fatalf("attempt copy mtime %v carries the source's age", info.ModTime())
	}
	if info, _ := os.Stat(cached); !info.ModTime().Equal(old) {
		t.Fatalf("inbox file mtime changed to %v; inbox retention order must not move", info.ModTime())
	}
	if got, _ := os.ReadFile(cached); string(got) != "retained-audio" {
		t.Fatalf("inbox file bytes changed: %q", got)
	}

	channel.SweepStaleTemps(attachments)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the sweep removed a live attempt file: %v", err)
	}
}

// R7-6: a cache entry that vanishes between lookup and copy falls back to the
// network fetch instead of failing.
func TestFetchVoice_CacheVanishedFallsBackToNetwork(t *testing.T) {
	_, inbox := fetchVoiceEnv(t)
	var getFiles atomic.Int64
	c := getFileChannelHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/file/") {
			_, _ = w.Write([]byte("network-audio"))
			return
		}
		getFiles.Add(1)
		_, _ = w.Write([]byte(okGetFileBody))
	})
	cached := filepath.Join(inbox, "1700000000000-F-GONE.oga")
	if err := os.WriteFile(cached, []byte("about to vanish"), 0o600); err != nil {
		t.Fatal(err)
	}
	previous := copyCachedVoice
	copyCachedVoice = func(src, dir, pattern string) (string, int64, error) {
		_ = os.Remove(src) // a concurrent handler prune wins the race
		return previous(src, dir, pattern)
	}
	t.Cleanup(func() { copyCachedVoice = previous })

	path, _, err := c.FetchVoice(context.Background(), "F-GONE")
	if err != nil {
		t.Fatalf("vanished cache entry: %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != "network-audio" || getFiles.Load() != 1 {
		t.Fatalf("attempt file = %q after %d getFile call(s); want the network copy after one", got, getFiles.Load())
	}
}

func TestDownloadAttachment_CutBodyLeavesNoFinalFile(t *testing.T) {
	attachments, _ := fetchVoiceEnv(t)
	c := voiceServerChannel(t, cutBody)

	if _, err := c.DownloadAttachment("F"); err == nil {
		t.Fatal("a cut body must fail the download")
	}
	requireNoFiles(t, attachments)
	if _, err := c.DownloadAttachment("F"); err == nil {
		t.Fatal("a truncated file was served as cached on the second call")
	}
}

// R12-3: the sweep removes only stale reserved-prefix temporaries. A completed
// attachment, even one whose upstream name ends in .part, is never touched.
func TestDownloadAttachment_SweepsOnlyStaleReservedTemps(t *testing.T) {
	attachments, _ := fetchVoiceEnv(t)
	if err := os.MkdirAll(attachments, 0o700); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-5 * time.Hour)
	write := func(name string, mtime time.Time) string {
		path := filepath.Join(attachments, name)
		if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, mtime, mtime); err != nil {
			t.Fatal(err)
		}
		return path
	}
	stale := write(channel.TempPrefix+"stale.part", old)
	fresh := write(channel.TempPrefix+"fresh.part", time.Now())
	completed := write("AgADcompleted_example.part", old)
	c := voiceServerChannel(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("OGGDATA")) })

	path, err := c.DownloadAttachment("F")
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != "OGGDATA" || filepath.Base(path) != "U_file_1.oga" {
		t.Fatalf("final file %s = %q", path, got)
	}
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale reserved temp survived the sweep: %v", err)
	}
	for _, kept := range []string{fresh, completed} {
		if _, err := os.Stat(kept); err != nil {
			t.Fatalf("the sweep removed %s: %v", filepath.Base(kept), err)
		}
	}
}

// The download URL carries the bot token; a transient body failure is
// classified first and still reaches the broker redacted.
func TestFetchVoice_TransientErrorNeverCarriesToken(t *testing.T) {
	fetchVoiceEnv(t)
	c := voiceServerChannel(t, func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close() // no response at all: the client error quotes the URL
		}
	})
	c.cfg.BotToken = fakeToken

	_, _, err := c.FetchVoice(context.Background(), "F")
	requireTransient(t, err)
	if strings.Contains(err.Error(), fakeToken) {
		t.Fatalf("BOT TOKEN LEAKED to the broker: %q", err.Error())
	}
}
