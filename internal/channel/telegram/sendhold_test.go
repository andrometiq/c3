package telegram

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// Phase U: an uncertain send is held, never resent or reformatted. These tests
// drive the real SendReply / SendReadback call paths against a Bot API server.

const richTableText = "| a | b |\n|---|---|\n| 1 | 2 |"

type sendServer struct {
	mu       sync.Mutex
	requests []string // method names, in arrival order
}

func (s *sendServer) methods() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

// respond is a scripted reply; n is the 0-based index of the request overall.
type respond func(w http.ResponseWriter, r *http.Request, method string, n int)

func newSendChannel(t *testing.T, reply respond) (*Channel, *sendServer, *http.Transport) {
	t.Helper()
	ss := &sendServer{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body) // the whole request reached the server
		method := path.Base(r.URL.Path)
		ss.mu.Lock()
		n := len(ss.requests)
		ss.requests = append(ss.requests, method)
		ss.mu.Unlock()
		reply(w, r, method, n)
	}))
	t.Cleanup(srv.Close)
	tr := newBotTransport(func(string) string { return "" }, newSetupDialer(nil))
	tr.Proxy = nil
	t.Cleanup(tr.CloseIdleConnections)
	c := &Channel{
		host:      &fakeHost{},
		cfg:       Config{BotToken: "123:test"},
		ctx:       context.Background(),
		rate:      newRateLimiter(),
		authBrk:   newAuthBreaker(auth401Threshold),
		reach:     newReachability(),
		endpoints: []string{srv.URL},
	}
	bot, err := c.newBot(tr)
	if err != nil {
		t.Fatal(err)
	}
	c.bot = bot
	return c, ss, tr
}

func replyOK(w http.ResponseWriter) {
	fmt.Fprint(w, `{"ok":true,"result":{"message_id":7,"date":0,"chat":{"id":42,"type":"private"}}}`)
}

func replyTyped(w http.ResponseWriter, code int, description string) {
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"ok":false,"error_code":%d,"description":%q}`, code, description)
}

// uncertainReplies answer after the request was received, in ways that do not
// prove whether the message was created.
var uncertainReplies = map[string]func(w http.ResponseWriter, r *http.Request){
	"dropped connection": func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	},
	"undecodable 200": func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "<html>gateway</html>")
	},
	"307 redirect": func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://127.0.0.1:1/elsewhere", http.StatusTemporaryRedirect)
	},
}

func TestSendReplyRichUncertainIsNotResent(t *testing.T) {
	for name, uncertain := range uncertainReplies {
		t.Run(name, func(t *testing.T) {
			c, ss, _ := newSendChannel(t, func(w http.ResponseWriter, r *http.Request, _ string, _ int) { uncertain(w, r) })
			if _, err := c.SendReply(c3types.ReplyArgs{ChatID: 42, Text: richTableText}); err == nil {
				t.Fatal("want the uncertain error returned")
			}
			if got := ss.methods(); len(got) != 1 || got[0] != "sendRichMessage" {
				t.Fatalf("requests = %v, want exactly one sendRichMessage and no fallback", got)
			}
		})
	}
}

func TestSendReadbackUncertainIsHeld(t *testing.T) {
	long, _ := rbSentenceDoc(5000, 40)
	for name, uncertain := range uncertainReplies {
		for _, transcript := range []string{"short note", long} {
			t.Run(fmt.Sprintf("%s/%d", name, len(transcript)), func(t *testing.T) {
				c, ss, _ := newSendChannel(t, func(w http.ResponseWriter, r *http.Request, _ string, _ int) { uncertain(w, r) })
				if _, err := c.SendReadback(c3types.ReadbackArgs{ChatID: 42, Transcript: transcript}); err == nil {
					t.Fatal("want the uncertain error returned")
				}
				if got := ss.methods(); len(got) != 1 {
					t.Fatalf("requests = %v, want exactly one (held, no retry, no format step-down)", got)
				}
			})
		}
	}
}

func TestSendReplyRichFallbackOnlyOnTypedRejection(t *testing.T) {
	cases := []struct {
		code        int
		description string
		fallback    bool
	}{
		{400, "Bad Request: can't parse rich message", true},
		{404, "Not Found: method not found", true},
		{401, "Unauthorized", false},
		{403, "Forbidden: bot was blocked by the user", false},
		{502, "Bad Gateway", false},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.code), func(t *testing.T) {
			c, ss, _ := newSendChannel(t, func(w http.ResponseWriter, _ *http.Request, method string, _ int) {
				if method == "sendRichMessage" {
					replyTyped(w, tc.code, tc.description)
					return
				}
				replyOK(w)
			})
			_, err := c.SendReply(c3types.ReplyArgs{ChatID: 42, Text: richTableText})
			got := ss.methods()
			if tc.fallback {
				if err != nil || len(got) != 2 || got[1] != "sendMessage" {
					t.Fatalf("err=%v requests=%v, want exactly one plain fallback that succeeds", err, got)
				}
				return
			}
			if err == nil || len(got) != 1 {
				t.Fatalf("err=%v requests=%v, want the error and no fallback", err, got)
			}
			// Only a transient failure is outbound-health evidence (feedOutboundFailure).
			wantConsec := 0
			if tc.code >= 500 {
				wantConsec = 1
			}
			if _, consec, _, _, _ := c.reach.out.snapshot(); consec != wantConsec {
				t.Fatalf("outbound failure events = %d, want %d", consec, wantConsec)
			}
		})
	}
}

// wrapDial routes the channel's connections through wrap, which sees the
// 1-based dial count and the real connection (or its error).
func wrapDial(t *testing.T, tr *http.Transport, wrap func(n int, conn net.Conn, err error) (net.Conn, error)) {
	t.Helper()
	base := tr.DialContext
	var mu sync.Mutex
	dials := 0
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		mu.Lock()
		dials++
		n := dials
		mu.Unlock()
		conn, err := base(ctx, network, addr)
		return wrap(n, conn, err)
	}
}

func TestSendReadbackSetupFailureIsRetried(t *testing.T) {
	c, ss, tr := newSendChannel(t, func(w http.ResponseWriter, _ *http.Request, _ string, _ int) { replyOK(w) })
	wrapDial(t, tr, func(n int, conn net.Conn, err error) (net.Conn, error) {
		if n == 1 && err == nil {
			conn.Close()
			return nil, &setupError{msg: "connection setup to api.example:443 failed: ipv6 [2001:db8::1]: tls: timed out after 2s"}
		}
		return conn, err
	})
	if _, err := c.SendReadback(c3types.ReadbackArgs{ChatID: 42, Transcript: "hi"}); err != nil {
		t.Fatalf("readback after a setup failure should be retried and succeed: %v", err)
	}
	if got := ss.methods(); len(got) != 1 {
		t.Fatalf("requests = %v, want exactly one (the setup failure sent nothing)", got)
	}
}

// failingReadConn lets the request reach the server, then, once the response
// arrives, fails the read with an error whose text looks like a parse-entities
// rejection.
type failingReadConn struct{ net.Conn }

func (c failingReadConn) Read(p []byte) (int, error) {
	if _, err := c.Conn.Read(p); err != nil {
		return 0, err
	}
	return 0, errors.New("Bad Request: can't parse entities")
}

func TestParseEntityFallbackNeedsTypedRejection(t *testing.T) {
	type sender func(c *Channel) error
	senders := map[string]sender{
		"SendReply": func(c *Channel) error {
			_, err := c.SendReply(c3types.ReplyArgs{ChatID: 42, Text: "**hi**"})
			return err
		},
		"SendReadback": func(c *Channel) error {
			_, err := c.SendReadback(c3types.ReadbackArgs{ChatID: 42, Transcript: "hi"})
			return err
		},
	}
	for name, send := range senders {
		t.Run(name+"/typed 502", func(t *testing.T) {
			c, ss, _ := newSendChannel(t, func(w http.ResponseWriter, _ *http.Request, _ string, _ int) {
				replyTyped(w, 502, "Bad Request: can't parse entities")
			})
			if err := send(c); err == nil {
				t.Fatal("want the 502 returned")
			}
			if got := ss.methods(); len(got) != 1 {
				t.Fatalf("requests = %v, want one send and no plain resend", got)
			}
		})
		t.Run(name+"/untyped", func(t *testing.T) {
			c, ss, tr := newSendChannel(t, func(w http.ResponseWriter, _ *http.Request, _ string, _ int) { replyOK(w) })
			wrapDial(t, tr, func(_ int, conn net.Conn, err error) (net.Conn, error) {
				if err != nil {
					return nil, err
				}
				return failingReadConn{conn}, nil
			})
			err := send(c)
			if err == nil || !strings.Contains(err.Error(), "can't parse entities") {
				t.Fatalf("err = %v, want the untyped error carrying the parse text", err)
			}
			if got := ss.methods(); len(got) != 1 {
				t.Fatalf("requests = %v, want one send and no plain resend", got)
			}
		})
		t.Run(name+"/typed 400", func(t *testing.T) {
			c, ss, _ := newSendChannel(t, func(w http.ResponseWriter, _ *http.Request, _ string, n int) {
				if n == 0 {
					replyTyped(w, 400, "Bad Request: can't parse entities: unexpected end tag")
					return
				}
				replyOK(w)
			})
			if err := send(c); err != nil {
				t.Fatalf("plain resend should succeed: %v", err)
			}
			if got := ss.methods(); len(got) != 2 {
				t.Fatalf("requests = %v, want exactly one plain resend", got)
			}
		})
	}
}
