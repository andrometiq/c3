package web

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

type injectedRenderHost struct {
	*fakeHost
	injected bool
}

func (h *injectedRenderHost) GateInbound(in *c3types.Inbound) channel.GateInboundDecision {
	in.TestInjected = h.injected
	return channel.GateInboundAllow
}

func TestInjectedMarkerWebOwnRendering(t *testing.T) {
	// The test channel never routes to web. Exercise the web renderer directly
	// through its host boundary to keep provenance if it ever sees such input.
	for _, injected := range []bool{false, true} {
		c, host, cookie := newHandlerChannel()
		c.host = &injectedRenderHost{fakeHost: host, injected: injected}
		response := sendRequest(c, cookie, `{"text":"sample","client_id":"marker"}`)
		if response.Code != http.StatusAccepted {
			t.Fatal(response.Body.String())
		}
		if len(c.replay) != 1 {
			t.Fatalf("replay=%+v", c.replay)
		}
		if got := c.replay[0].payload.Text; strings.Contains(got, c3types.TestInjectedMarker) != injected {
			t.Fatalf("injected=%v web text=%q", injected, got)
		}
	}
}

func TestInjectedMarkerWebVoiceAndReadbackRendering(t *testing.T) {
	for _, injected := range []bool{false, true} {
		c, host, _, _ := voiceTestChannel(t)
		c.host = &injectedRenderHost{fakeHost: host, injected: injected}
		// Cached audio avoids codec/provider work: this test exercises rendering.
		c.clientMessages[clientMessageKey{sessionID: "matrix", clientID: "voice"}] = &clientMessage{
			messageID: 1, kind: "voice", textHash: sha256.Sum256(nil), created: c.now(),
			voiceFileID: "00000000000000000000000000000001", voiceSize: 1, voiceDuration: 1,
		}
		id, status, err := c.acceptVoiceInbound(context.Background(), "matrix", session{userID: 42}, "voice", "audio/ogg", nil)
		if err != nil || status != http.StatusAccepted {
			t.Fatalf("status=%d err=%v", status, err)
		}
		if len(c.replay) != 1 || strings.Contains(c.replay[0].payload.Text, c3types.TestInjectedMarker) != injected {
			t.Fatal(c.replay)
		}
		in := &c3types.Inbound{TestInjected: injected}
		if _, err := c.SendReadback(c3types.ReadbackArgs{ChatID: 42, ReplyTo: &id, Transcript: c3types.WithTestInjectionMarker(in, "sample transcript")}); err != nil {
			t.Fatal(err)
		}
		if len(c.replay) != 2 || strings.Contains(c.replay[1].payload.Text, c3types.TestInjectedMarker) != injected {
			t.Fatal(c.replay)
		}
	}
}
