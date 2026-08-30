package web

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

func writeDocumentSource(t *testing.T, directory, name, contents string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func storedFileEntries(t *testing.T, c *Channel) []os.DirEntry {
	t.Helper()
	directory, err := c.filesDirectoryPath()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func TestFilesEndpointRequiresCookie(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	c, _, cookie := newHandlerChannel()
	source := writeDocumentSource(t, t.TempDir(), "report.html", "<!doctype html><title>report</title>")
	token, _, err := c.storeDocument(source, filepath.Base(source))
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name   string
		cookie *http.Cookie
		want   int
	}{
		{name: "missing", want: http.StatusUnauthorized},
		{name: "invalid", cookie: &http.Cookie{Name: sessionCookieName, Value: "invalid"}, want: http.StatusUnauthorized},
		{name: "valid", cookie: cookie, want: http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			c.routes().ServeHTTP(response, request(http.MethodGet, "/files/"+token, "", test.cookie, false))
			if response.Code != test.want {
				t.Fatalf("GET /files token status=%d, want %d", response.Code, test.want)
			}
		})
	}
}

func TestFilesEndpointHeaders(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	c, _, cookie := newHandlerChannel()
	source := writeDocumentSource(t, t.TempDir(), "headers.htm", "<!doctype html><title>headers</title>")
	if _, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: source}}}); err != nil {
		t.Fatal(err)
	}
	token := strings.TrimPrefix(c.replay[0].payload.Attachment.URL, "/files/")
	const documentCSP = "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data:; font-src data:; media-src data:; base-uri 'none'; form-action 'none'; frame-ancestors 'self'; sandbox allow-scripts"
	responses := make(map[string]*httptest.ResponseRecorder, 2)
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		response := httptest.NewRecorder()
		c.routes().ServeHTTP(response, request(method, "/files/"+token, "", cookie, false))
		if response.Code != http.StatusOK {
			t.Fatalf("%s /files status=%d body=%q", method, response.Code, response.Body.String())
		}
		for header, want := range map[string]string{
			"Cache-Control":           "no-store",
			"Content-Disposition":     `inline; filename="headers.htm"`,
			"Content-Security-Policy": documentCSP,
			"Content-Type":            "text/html; charset=utf-8",
			"Referrer-Policy":         "no-referrer",
			"X-Content-Type-Options":  "nosniff",
		} {
			if got := response.Header().Get(header); got != want {
				t.Fatalf("%s /files %s=%q, want %q", method, header, got, want)
			}
		}
		if got := response.Header().Get("X-Frame-Options"); got != "" {
			t.Fatalf("%s /files X-Frame-Options=%q, want absent", method, got)
		}
		responses[method] = response
	}
	if got := responses[http.MethodHead].Body.Len(); got != 0 {
		t.Fatalf("HEAD /files body is %d bytes, want none", got)
	}
	if getHeaders, headHeaders := responses[http.MethodGet].Header(), responses[http.MethodHead].Header(); !reflect.DeepEqual(getHeaders, headHeaders) {
		t.Fatalf("HEAD /files headers differ from GET:\nGET  %v\nHEAD %v", getHeaders, headHeaders)
	}
}

type growingRegularFileReader struct {
	file  *os.File
	path  string
	grew  bool
	error error
}

func (r *growingRegularFileReader) Read(buffer []byte) (int, error) {
	read, err := r.file.Read(buffer)
	if read > 0 && !r.grew {
		r.grew = true
		r.error = os.Truncate(r.path, maxDocumentBytes+1)
		if r.error != nil {
			return read, r.error
		}
	}
	return read, err
}

func TestStoreDocumentRejectsFileThatGrowsDuringCopy(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	c, _, _ := newHandlerChannel()
	stream, _, _ := c.connectStream("test-session", "")
	path := writeDocumentSource(t, t.TempDir(), "growing.html", "first bytes")
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	reader := &growingRegularFileReader{file: file, path: path}
	token, bytes, err := c.retainDocument(reader, ".html")
	if err == nil || !strings.Contains(err.Error(), "grew beyond") {
		t.Fatalf("retain growing document token/bytes/error=%q/%d/%v", token, bytes, err)
	}
	if !reader.grew || reader.error != nil {
		t.Fatalf("source growth happened/error=%v/%v", reader.grew, reader.error)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() >= maxDocumentBytes || after.Size() != maxDocumentBytes+1 {
		t.Fatalf("source size before/after=%d/%d", before.Size(), after.Size())
	}
	if entries := storedFileEntries(t, c); len(entries) != 0 {
		t.Fatalf("rejected growing document left %d destination entries: %v", len(entries), entries)
	}
	if len(c.replay) != 0 || len(stream.events) != 0 {
		t.Fatalf("rejected growing document published replay/live events=%d/%d", len(c.replay), len(stream.events))
	}
}

func TestFilesTokenPathSafety(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	c, _, cookie := newHandlerChannel()
	directory, err := c.ensureFilesDirectory()
	if err != nil {
		t.Fatal(err)
	}
	outside := writeDocumentSource(t, t.TempDir(), "outside.html", "outside sentinel")
	symlinkToken := strings.Repeat("a", 32)
	if err := os.Symlink(outside, filepath.Join(directory, symlinkToken+".html")); err != nil {
		t.Fatal(err)
	}
	directoryToken := strings.Repeat("b", 32)
	if err := os.Mkdir(filepath.Join(directory, directoryToken+".html"), 0o700); err != nil {
		t.Fatal(err)
	}

	for _, token := range []string{"../", "not-hex", strings.Repeat("c", 30), symlinkToken, directoryToken} {
		response := httptest.NewRecorder()
		c.routes().ServeHTTP(response, request(http.MethodGet, "/files/"+token, "", cookie, false))
		if response.Code != http.StatusNotFound {
			t.Errorf("token %q status=%d body=%q, want 404", token, response.Code, response.Body.String())
		}
		if strings.Contains(response.Body.String(), "outside sentinel") {
			t.Fatalf("token %q opened a file outside the files directory", token)
		}
	}
}

func TestWebRejectsUnsupportedMedia(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	c, _, _ := newHandlerChannel()
	stream, _, _ := c.connectStream("test-session", "")
	sources := t.TempDir()
	executable := writeDocumentSource(t, sources, "payload.exe", "not html")
	noExtension := writeDocumentSource(t, sources, "payload", "not html")
	directory := filepath.Join(sources, "directory.html")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(sources, "pipe.html")
	haveFIFO := makeTestFIFO(t, fifo)
	oversize := filepath.Join(sources, "large.html")
	large, err := os.OpenFile(oversize, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := large.Truncate(maxDocumentBytes + 1); err != nil {
		_ = large.Close()
		t.Fatal(err)
	}
	if err := large.Close(); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name  string
		media []c3types.MediaItem
	}{
		{name: "URL only", media: []c3types.MediaItem{{Kind: c3types.MediaFile, URL: "https://example.invalid/report.html"}}},
		{name: "photo", media: []c3types.MediaItem{{Kind: c3types.MediaPhoto, Path: executable}}},
		{name: "two items", media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: executable}, {Kind: c3types.MediaFile, Path: executable}}},
		{name: "disallowed extension", media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: executable}}},
		{name: "no extension", media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: noExtension}}},
		{name: "directory", media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: directory}}},
		{name: "oversize", media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: oversize}}},
	}
	if haveFIFO {
		tests = append(tests, struct {
			name  string
			media []c3types.MediaItem
		}{name: "FIFO", media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: fifo}}})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Media: test.media}); err == nil {
				t.Fatal("SendReply succeeded; want rejection")
			}
			if entries := storedFileEntries(t, c); len(entries) != 0 {
				t.Fatalf("rejected send stored %d entries: %v", len(entries), entries)
			}
			if len(c.replay) != 0 || len(stream.events) != 0 {
				t.Fatalf("rejected send published replay/live events=%d/%d", len(c.replay), len(stream.events))
			}
		})
	}
}

func TestWebStoresDocument(t *testing.T) {
	t.Run("store and publish", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		c, _, cookie := newHandlerChannel()
		source := writeDocumentSource(t, t.TempDir(), "Q3 <report>.html", "<!doctype html><h1>Q3</h1>")
		recorder, cancel, done := startSSE(c, cookie, "", "")
		defer func() {
			cancel()
			<-done
		}()
		waitFor(t, func() bool { return streamCount(c) == 1 })
		id, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Media: []c3types.MediaItem{{
			Kind: c3types.MediaFile, Path: source, Caption: "Quarterly report",
		}}})
		if err != nil || id <= 0 {
			t.Fatalf("SendReply=%d/%v", id, err)
		}
		if len(c.replay) != 1 || c.replay[0].kind != "message" {
			t.Fatalf("replay=%+v", c.replay)
		}
		payload := c.replay[0].payload
		if payload.Text != "Quarterly report" || payload.Attachment == nil {
			t.Fatalf("payload=%+v", payload)
		}
		attachment := payload.Attachment
		if attachment.Kind != "html" || attachment.Name != filepath.Base(source) || attachment.Bytes != len("<!doctype html><h1>Q3</h1>") || !strings.HasPrefix(attachment.URL, "/files/") {
			t.Fatalf("attachment=%+v", attachment)
		}
		token := strings.TrimPrefix(attachment.URL, "/files/")
		if len(token) != 32 || strings.ToLower(token) != token {
			t.Fatalf("token=%q, want 32 lowercase hex", token)
		}
		path, err := c.localFilePath(token)
		if err != nil {
			t.Fatal(err)
		}
		assertMode(t, filepath.Dir(path), 0o700)
		assertMode(t, path, 0o600)
		waitFor(t, func() bool {
			_, body := recorder.snapshot()
			return strings.Contains(body, `"text":"Quarterly report"`) &&
				strings.Contains(body, `"attachment":{"kind":"html"`)
		})
	})

	t.Run("upper-case extension", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		c, _, cookie := newHandlerChannel()
		const contents = "<!doctype html><title>upper case</title>"
		source := writeDocumentSource(t, t.TempDir(), "Report.HTML", contents)
		if _, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Media: []c3types.MediaItem{{Kind: c3types.MediaFile, Path: source}}}); err != nil {
			t.Fatal(err)
		}
		attachment := c.replay[0].payload.Attachment
		if attachment == nil || attachment.Name != "Report.HTML" {
			t.Fatalf("attachment=%+v", attachment)
		}
		token := strings.TrimPrefix(attachment.URL, "/files/")
		if _, err := c.localFilePath(token); err != nil {
			t.Fatalf("stored Report.HTML: %v", err)
		}
		response := httptest.NewRecorder()
		c.routes().ServeHTTP(response, request(http.MethodGet, attachment.URL, "", cookie, false))
		if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/html; charset=utf-8" || response.Body.String() != contents {
			t.Fatalf("GET Report.HTML status/content-type/body=%d/%q/%q", response.Code, response.Header().Get("Content-Type"), response.Body.String())
		}
	})

	t.Run("prune newest 200", func(t *testing.T) {
		t.Setenv("XDG_STATE_HOME", t.TempDir())
		c := New()
		directory, err := c.ensureFilesDirectory()
		if err != nil {
			t.Fatal(err)
		}
		base := time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC)
		for index := 0; index < filesRetention+1; index++ {
			name := fmt.Sprintf("%032x.html", index+1)
			path := filepath.Join(directory, name)
			if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
				t.Fatal(err)
			}
			modified := base.Add(time.Duration(index) * time.Second)
			if err := os.Chtimes(path, modified, modified); err != nil {
				t.Fatal(err)
			}
		}
		c.pruneFiles()
		entries := storedFileEntries(t, c)
		if len(entries) != filesRetention {
			t.Fatalf("retained files=%d, want %d", len(entries), filesRetention)
		}
		if _, err := os.Stat(filepath.Join(directory, fmt.Sprintf("%032x.html", 1))); !os.IsNotExist(err) {
			t.Fatalf("oldest file survived prune: %v", err)
		}
		if _, err := os.Stat(filepath.Join(directory, fmt.Sprintf("%032x.html", filesRetention+1))); err != nil {
			t.Fatalf("newest file was pruned: %v", err)
		}
	})
}
