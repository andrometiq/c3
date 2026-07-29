package stt

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// Codex review 2, finding 1. The handler performs its OWN getFile, independent
// of the broker's preflight ask. Two things followed from that and both are
// fixed here: the two calls could address DIFFERENT Bot-API hosts, and a failure
// of the second one was reduced to "[STT FAILED: error]", which the broker then
// rendered as the generic "transcription failed / the audio is saved and
// recoverable" — false on every clause when nothing was ever downloaded.

// endpointChannel is a channel double that reports a live Bot-API base, the way
// the telegram channel does once failover and C3_TELEGRAM_API_URL are folded in.
type endpointChannel struct {
	channel.Channel // never called; only APIBaseURL is
	base            string
}

func (e *endpointChannel) APIBaseURL() string { return e.base }

// plainChannel cannot report an endpoint — the fallback case.
type plainChannel struct{ channel.Channel }

func hostWithChannel(ch channel.Channel, mappingsBase string) *fakeHost {
	return &fakeHost{
		channelCfg: map[string]any{
			"telegram": map[string]string{"bot_token": "tok", "api_base_url": mappingsBase},
		},
		channel: ch,
	}
}

// The LIVE endpoint wins. mappings.json holds only the primary api_base_url: it
// cannot know about api_base_urls failover, and it cannot know the poll loop has
// already advanced. Reading it meant the preflight could succeed on one host
// while the handler fetched from another.
func TestReadTelegramConn_PrefersTheChannelsLiveEndpoint(t *testing.T) {
	h := hostWithChannel(&endpointChannel{base: "https://failed-over.example"}, "https://stale-primary.example")

	_, base, _, err := readTelegramConn(h)
	if err != nil {
		t.Fatalf("readTelegramConn: %v", err)
	}
	if base != "https://failed-over.example" {
		t.Fatalf("endpoint = %q, want the channel's LIVE base — the handler must fetch from the same host the preflight asked, or a successful probe is followed by a fetch against an unreachable endpoint", base)
	}
}

// "" is a real answer, not a missing one: it means api.telegram.org. Falling
// back to a stale mappings value there would re-introduce the split.
func TestReadTelegramConn_LiveOfficialEndpointIsNotTreatedAsUnset(t *testing.T) {
	h := hostWithChannel(&endpointChannel{base: ""}, "https://stale-primary.example")

	_, base, _, err := readTelegramConn(h)
	if err != nil {
		t.Fatalf("readTelegramConn: %v", err)
	}
	if base != "" {
		t.Fatalf("endpoint = %q, want \"\" (api.telegram.org) — the channel answered, and its answer stands", base)
	}
}

// A channel that cannot answer leaves the old env/mappings resolution in place.
func TestReadTelegramConn_FallsBackWhenTheChannelCannotAnswer(t *testing.T) {
	h := hostWithChannel(&plainChannel{}, "https://configured.example")

	_, base, _, err := readTelegramConn(h)
	if err != nil {
		t.Fatalf("readTelegramConn: %v", err)
	}
	if base != "https://configured.example" {
		t.Fatalf("endpoint = %q, want the mappings value — a transport with no live accessor must still work", base)
	}
}

// The handler emits ONE structured line; that line is the only place the
// server's own words survive.
func TestFetchFailureDetail_ExtractsTheHandlersCause(t *testing.T) {
	stderr := "some warmup noise\n" + fetchErrorMarker + `"getFile failed (error_code=400): Bad Request: file is too big"` +
		"\nTraceback (most recent call last):\n  File \"x\"\n"

	got := fetchFailureDetail(stderr)
	if got != "getFile failed (error_code=400): Bad Request: file is too big" {
		t.Fatalf("fetchFailureDetail = %q, want the server's own words — a traceback after the line must not swallow them", got)
	}
}

// A transcription failure is NOT a fetch failure and must not be relabelled.
func TestFetchFailureDetail_NonFetchFailureReportsNothing(t *testing.T) {
	if got := fetchFailureDetail("[stt-handler] provider chain exhausted\nTraceback\n"); got != "" {
		t.Fatalf("fetchFailureDetail = %q, want empty — a provider failure really is a transcription failure and its own recovery text is accurate", got)
	}
}

// The payload is a SERVER description: text C3 does not author. It must not be
// able to make the parse land inside itself, which is what an unanchored search
// for a human-readable delimiter allowed — the extracted suffix was whitespace,
// so a specific refusal silently became the generic "[STT FAILED: error]".
func TestFetchFailureDetail_ServerDescriptionCannotDerailTheParse(t *testing.T) {
	hostile := `Bad Request: rejected ` + fetchErrorMarker + `"" and more`
	stderr := "\n" + fetchErrorMarker + mustJSON(hostile) + "\n"

	if got := fetchFailureDetail(stderr); got != hostile {
		t.Fatalf("fetchFailureDetail = %q, want the description verbatim (%q) — a server that quotes the marker must not be able to blank out its own cause", got, hostile)
	}
}

// …nor truncate itself with an embedded newline, which is what "verbatim" means.
func TestFetchFailureDetail_EmbeddedNewlineSurvivesWhole(t *testing.T) {
	multi := "Bad Request: file is too big\nretry-after: never"
	stderr := "\n" + fetchErrorMarker + mustJSON(multi) + "\n"

	if got := fetchFailureDetail(stderr); got != multi {
		t.Fatalf("fetchFailureDetail = %q, want all %q — a newline in the description truncated the cause the agent needs", got, multi)
	}
}

// A marker appearing MID-LINE is not a report. Only a line that begins with it
// is, or any diagnostic quoting the marker could masquerade as a fetch failure.
func TestFetchFailureDetail_MidLineMarkerIsNotAReport(t *testing.T) {
	stderr := "provider chain exhausted; note: " + fetchErrorMarker + `"not a real report"` + "\n"

	if got := fetchFailureDetail(stderr); got != "" {
		t.Fatalf("fetchFailureDetail = %q, want empty — a mid-line marker made a transcription failure impersonate a fetch failure", got)
	}
}

func mustJSON(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// The channel's answer is FORCED onto the subprocess. An inherited
// C3_TELEGRAM_API_URL pointing at a proxy must not survive an authoritative
// answer of "" (api.telegram.org, after failover) — Python reads its default
// only when the variable is ABSENT, so "" means remove it, never set it empty.
func TestHandlerEnv_AuthoritativeEmptyAnswerRemovesTheInheritedOverride(t *testing.T) {
	t.Setenv(apiURLEnvVar, "https://proxy-a.example")

	env := handlerEnv("", true, 500)

	for _, kv := range env {
		if strings.HasPrefix(kv, apiURLEnvVar+"=") {
			t.Fatalf("child env still carries %q — the channel authoritatively said api.telegram.org, and the handler would fetch from the stale proxy instead", kv)
		}
	}
}

// A non-empty answer replaces the inherited value rather than joining it.
func TestHandlerEnv_AuthoritativeAnswerReplacesTheInheritedOverride(t *testing.T) {
	t.Setenv(apiURLEnvVar, "https://proxy-a.example")

	env := handlerEnv("https://proxy-b.example", true, 500)

	var seen []string
	for _, kv := range env {
		if strings.HasPrefix(kv, apiURLEnvVar+"=") {
			seen = append(seen, kv)
		}
	}
	if len(seen) != 1 || seen[0] != apiURLEnvVar+"=https://proxy-b.example" {
		t.Fatalf("child env %v, want exactly the live answer — a stale entry left beside it is decided by libc, not by us", seen)
	}
}

// No answer ⇒ inherit untouched, which is the behavior for a transport that
// cannot report its endpoint.
func TestHandlerEnv_NoAnswerInheritsTheEnvironment(t *testing.T) {
	t.Setenv(apiURLEnvVar, "https://proxy-a.example")

	env := handlerEnv("", false, 500)

	found := false
	for _, kv := range env {
		if kv == apiURLEnvVar+"=https://proxy-a.example" {
			found = true
		}
	}
	if !found {
		t.Fatal("an unanswerable channel must leave the inherited environment alone — stripping it would break every transport without the live accessor")
	}
}

// End to end through the real subprocess path: a handler whose fetch fails must
// hand the server's words upward, not a generic marker.
func TestRunHandler_FetchFailure_CarriesTheServersWordsUpward(t *testing.T) {
	tmp := t.TempDir()
	handler := filepath.Join(tmp, "stt-handler.py")
	// Mirrors stt-handler.py emit_fetch_error: one marker-anchored line whose
	// payload is the cause, JSON-encoded.
	const script = `#!/usr/bin/env python3
import json, sys
sys.stderr.write('\n' + 'C3-STT-FETCH-ERROR-v1 ' + json.dumps('getFile failed (error_code=400): Bad Request: file is too big') + '\n')
sys.exit(1)
`
	if err := os.WriteFile(handler, []byte(script), 0o755); err != nil {
		t.Fatalf("write handler: %v", err)
	}
	h := &fakeHost{
		cfg:        Config{Enabled: true, HandlerPath: handler, Timeout: 20},
		channelCfg: map[string]any{"telegram": map[string]string{"bot_token": "tok"}},
	}
	if err := Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, err := h.voiceCallback(context.Background(), c3types.VoicePayload{MessageID: 1, FileID: "V1"})
	if err != nil {
		t.Fatalf("callback error: %v", err)
	}
	if !strings.HasPrefix(got, FetchFailedPrefix) {
		t.Fatalf("marker = %q, want the %q form — labelling a fetch failure as a transcription failure is what produced the generic 'audio is saved and recoverable' text for audio that was never fetched", got, FetchFailedPrefix)
	}
	if !strings.Contains(got, "Bad Request: file is too big") {
		t.Fatalf("marker = %q, want the server's actual error carried verbatim", got)
	}
}

// A handler that fails for a NON-fetch reason keeps the transcription-failure
// marker, whose recovery advice is accurate there: the audio really was fetched.
func TestRunHandler_NonFetchFailure_KeepsTheTranscriptionMarker(t *testing.T) {
	tmp := t.TempDir()
	handler := filepath.Join(tmp, "stt-handler.py")
	const script = `#!/usr/bin/env python3
import sys
print('[stt-handler] provider chain exhausted', file=sys.stderr)
sys.exit(1)
`
	if err := os.WriteFile(handler, []byte(script), 0o755); err != nil {
		t.Fatalf("write handler: %v", err)
	}
	h := &fakeHost{
		cfg:        Config{Enabled: true, HandlerPath: handler, Timeout: 20},
		channelCfg: map[string]any{"telegram": map[string]string{"bot_token": "tok"}},
	}
	if err := Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, _ := h.voiceCallback(context.Background(), c3types.VoicePayload{MessageID: 1, FileID: "V1"})
	if strings.HasPrefix(got, FetchFailedPrefix) {
		t.Fatalf("marker = %q, want the [STT FAILED: …] form — a provider failure is not a fetch failure and must not borrow its surfaces", got)
	}
}

// The fetch-error protocol has two ends in two languages. Nothing else in the
// build would notice if one side drifted — the Go parser would simply stop
// matching, and every specific server refusal would quietly become the generic
// "[STT FAILED: error]" this finding is about. So the marker and the encoding
// are pinned against the REAL handler source.
func TestFetchErrorProtocol_HandlerAndShimAgree(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "plugins", "c3", "stt", "stt-handler.py"))
	if err != nil {
		t.Fatalf("read stt-handler.py: %v", err)
	}
	handler := string(src)

	want := "FETCH_ERROR_MARKER = '" + fetchErrorMarker + "'"
	if !strings.Contains(handler, want) {
		t.Fatalf("stt-handler.py does not define %s — the shim would stop recognizing fetch failures and report them as transcription failures", want)
	}
	if !strings.Contains(handler, "FETCH_ERROR_MARKER + json.dumps(") {
		t.Fatal("stt-handler.py no longer JSON-encodes the cause — an embedded newline would truncate it and a server description could fabricate a marker line")
	}
	// The readable form belongs in the handler's own log, never on stderr: an
	// unencoded copy there is exactly the injection vector the encoding closes.
	if strings.Contains(handler, "download failed: {e}', file=sys.stderr") {
		t.Fatal("stt-handler.py prints the raw cause to stderr again — an unencoded copy re-opens the fabricated-line path")
	}
}
