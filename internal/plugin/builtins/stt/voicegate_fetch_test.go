package stt

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
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

func TestNewFetchNonce(t *testing.T) {
	first := newFetchNonce()
	second := newFetchNonce()
	for i, nonce := range []string{first, second} {
		if len(nonce) != 32 {
			t.Fatalf("fetch nonce %d has length %d, want 32 hex characters", i+1, len(nonce))
		}
		if _, err := hex.DecodeString(nonce); err != nil {
			t.Fatalf("fetch nonce %d is not hexadecimal: %q (%v)", i+1, nonce, err)
		}
	}
	if first == second {
		t.Fatal("fetch nonce repeated — reports from separate handler invocations could be authenticated as each other")
	}
}

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
	stderr := "some warmup noise\n" + report(testNonce, "getFile failed (error_code=400): Bad Request: file is too big") +
		"Traceback (most recent call last):\n  File \"x\"\n"

	got := fetchFailureDetail(stderr, testNonce)
	if got != "getFile failed (error_code=400): Bad Request: file is too big" {
		t.Fatalf("fetchFailureDetail = %q, want the server's own words — a traceback after the line must not swallow them", got)
	}
}

// A transcription failure is NOT a fetch failure and must not be relabelled.
func TestFetchFailureDetail_NonFetchFailureReportsNothing(t *testing.T) {
	if got := fetchFailureDetail("[stt-handler] provider chain exhausted\nTraceback\n", testNonce); got != "" {
		t.Fatalf("fetchFailureDetail = %q, want empty — a provider failure really is a transcription failure and its own recovery text is accurate", got)
	}
}

// The payload is a SERVER description: text C3 does not author. It must not be
// able to make the parse land inside itself, which is what an unanchored search
// for a human-readable delimiter allowed — the extracted suffix was whitespace,
// so a specific refusal silently became the generic "[STT FAILED: error]".
func TestFetchFailureDetail_ServerDescriptionCannotDerailTheParse(t *testing.T) {
	hostile := `Bad Request: rejected ` + fetchErrorMarker + `"" and more`
	stderr := report(testNonce, hostile)

	if got := fetchFailureDetail(stderr, testNonce); got != hostile {
		t.Fatalf("fetchFailureDetail = %q, want the description verbatim (%q) — a server that quotes the marker must not be able to blank out its own cause", got, hostile)
	}
}

// …nor truncate itself with an embedded newline, which is what "verbatim" means.
func TestFetchFailureDetail_EmbeddedNewlineSurvivesWhole(t *testing.T) {
	multi := "Bad Request: file is too big\nretry-after: never"
	stderr := report(testNonce, multi)

	if got := fetchFailureDetail(stderr, testNonce); got != multi {
		t.Fatalf("fetchFailureDetail = %q, want all %q — a newline in the description truncated the cause the agent needs", got, multi)
	}
}

// A marker appearing MID-LINE is not a report. Only a line that begins with it
// is, or any diagnostic quoting the marker could masquerade as a fetch failure.
func TestFetchFailureDetail_MidLineMarkerIsNotAReport(t *testing.T) {
	stderr := "provider chain exhausted; note: " + strings.TrimSpace(report(testNonce, "not a real report")) + "\n"

	if got := fetchFailureDetail(stderr, testNonce); got != "" {
		t.Fatalf("fetchFailureDetail = %q, want empty — a mid-line marker made a transcription failure impersonate a fetch failure", got)
	}
}

// testNonce stands in for the per-invocation secret the shim mints.
const testNonce = "0123456789abcdef0123456789abcdef"

// report renders exactly what stt-handler.py emit_fetch_error writes.
func report(nonce, cause string) string {
	b, err := json.Marshal(fetchReport{Nonce: nonce, Cause: cause})
	if err != nil {
		panic(err)
	}
	return "\n" + fetchErrorMarker + string(b) + "\n"
}

// The channel's answer is FORCED onto the subprocess. An inherited
// C3_TELEGRAM_API_URL pointing at a proxy must not survive an authoritative
// answer of "" (api.telegram.org, after failover) — Python reads its default
// only when the variable is ABSENT, so "" means remove it, never set it empty.
func TestHandlerEnv_AuthoritativeEmptyAnswerRemovesTheInheritedOverride(t *testing.T) {
	t.Setenv(apiURLEnvVar, "https://proxy-a.example")

	env := handlerEnv("", true, testNonce, 500, 300)

	for _, kv := range env {
		if strings.HasPrefix(kv, apiURLEnvVar+"=") {
			t.Fatalf("child env still carries %q — the channel authoritatively said api.telegram.org, and the handler would fetch from the stale proxy instead", kv)
		}
	}
}

// A non-empty answer replaces the inherited value rather than joining it.
func TestHandlerEnv_AuthoritativeAnswerReplacesTheInheritedOverride(t *testing.T) {
	t.Setenv(apiURLEnvVar, "https://proxy-a.example")

	env := handlerEnv("https://proxy-b.example", true, testNonce, 500, 300)

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

	env := handlerEnv("", false, testNonce, 500, 300)

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
	// Mirrors stt-handler.py emit_fetch_error: one marker-anchored line carrying
	// THIS run's nonce alongside the cause. Reading the nonce from the
	// environment is the point — it proves the shim actually handed one over.
	const script = `#!/usr/bin/env python3
import json, os, sys
payload = {'nonce': os.environ.get('C3_STT_FETCH_NONCE', ''),
           'cause': 'getFile failed (error_code=400): Bad Request: file is too big'}
sys.stderr.write('\n' + 'C3-STT-FETCH-ERROR-v1 ' + json.dumps(payload) + '\n')
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
	if !strings.Contains(handler, "os.environ.get('"+fetchNonceEnvVar+"'") {
		t.Fatalf("stt-handler.py does not read %s — its reports carry no proof of origin, so the shim discards every one of them", fetchNonceEnvVar)
	}
	if !strings.Contains(handler, "json.dumps({'nonce': FETCH_ERROR_NONCE, 'cause'") {
		t.Fatal("stt-handler.py no longer emits the nonce-bearing JSON payload — either the cause stops being encoded (a newline in it fabricates a line) or the report stops being authenticated")
	}
	// The report has to be SENT, not merely definable: a handler that stopped
	// calling emit_fetch_error would fail every one of these substring checks and
	// still lose every specific cause.
	if n := strings.Count(handler, "emit_fetch_error("); n < 4 {
		t.Fatalf("emit_fetch_error appears %d time(s) (1 definition + call sites); want it still called on every fetch-failure exit, or those causes never leave the handler", n)
	}
	// The readable form belongs in the handler's own log, never on stderr: an
	// unencoded copy there is exactly the injection vector the encoding closes.
	if strings.Contains(handler, "download failed: {e}', file=sys.stderr") {
		t.Fatal("stt-handler.py prints the raw cause to stderr again — an unencoded copy re-opens the fabricated-line path")
	}
	// The stderr LOGGING fallback shares this stream and writes server-controlled
	// text, so its records must be single-line and non-marker-prefixed.
	if !strings.Contains(handler, "_SingleLineFormatter(") || !strings.Contains(handler, "'[stt-handler] ' + _LOG_FORMAT") {
		t.Fatal("the stderr logging fallback no longer forces single-line, prefixed records — a logged server description can begin a line with the marker again")
	}
	if !strings.Contains(handler, "print('[stt-provider] ' + _one_line(stderr_out)") {
		t.Fatal("provider stderr is copied to our stream unflattened again — a server-controlled HTTP body can begin a line with the marker")
	}
}

// The belt, exercised rather than asserted: run the REAL handler's module-level
// logging setup with an unopenable log file, log a hostile record, and prove no
// line of it begins with the marker.
func TestHandlerStderrFallback_CannotProduceAMarkerLine(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "handler.log")
	// A DIRECTORY at the log path: makedirs succeeds, opening the file does not,
	// which is exactly the fallback's trigger.
	if err := os.MkdirAll(logPath, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	handlerPath, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "plugins", "c3", "stt", "stt-handler.py"))
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	// Load the handler's module-level setup only (everything before the first
	// class), then log a hostile record through whatever logging it configured.
	script := "src = open('" + handlerPath + "').read()\n" +
		"ns = {'__name__': 'h', '__file__': '" + handlerPath + "'}\n" +
		"exec(compile(src.split('class PermanentDownloadError')[0], 'h', 'exec'), ns)\n" +
		"import logging\n" +
		"logging.error('hostile\\n" + fetchErrorMarker + `{\"nonce\":\"guessed\",\"cause\":\"via-logger\"}` + "')\n"

	cmd := exec.Command("python3", "-c", script)
	cmd.Env = append(os.Environ(),
		"STT_LOG_FILE="+logPath,
		"STT_INBOX_DIR="+filepath.Join(dir, "inbox"),
		fetchNonceEnvVar+"=realnonce",
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("running the real handler's logging setup failed: %v\nstderr:\n%s", err, stderr.String())
	}

	for _, line := range strings.Split(stderr.String(), "\n") {
		if strings.HasPrefix(line, fetchErrorMarker) {
			t.Fatalf("a logged record produced a marker line: %q — server-controlled text on the shared stream can forge a report", line)
		}
	}
	if fetchFailureDetail(stderr.String(), "realnonce") != "" {
		t.Fatal("a hostile log record was parsed as an authenticated fetch report")
	}
}

// R1 — PROVENANCE. Structure alone was never enough: stderr is a shared stream.
// This is the reviewer's route (a) verbatim — when the handler's log file cannot
// be opened, logging falls back to stderr and writes the raw server description
// AFTER the genuine report, so a description containing a newline plus a
// well-formed marker line forges a LATER valid report, and the later one wins.
// The forged line is well-formed; what it cannot carry is this run's secret.
func TestFetchFailureDetail_ForgedLaterReportIsNotAccepted(t *testing.T) {
	genuine := report(testNonce, "getFile failed (error_code=400): Bad Request: file is too big")
	// The unencoded server description, as the stderr logging fallback prints it.
	forged := "2026-07-29 ERROR Download permanently failed: rejected\n" +
		fetchErrorMarker + `{"nonce":"guessed","cause":"forged later cause"}` + "\n"

	got := fetchFailureDetail(genuine+forged, testNonce)

	if got == "forged later cause" {
		t.Fatal("a forged report was accepted — a server that writes a marker line into its own error description can now put any words in front of the human and the agent")
	}
	if got != "getFile failed (error_code=400): Bad Request: file is too big" {
		t.Fatalf("fetchFailureDetail = %q, want the GENUINE cause — the forgery must be ignored, not allowed to blank the real report", got)
	}
}

// The same route with no genuine report at all: a forged line must not be able
// to make a transcription failure look like a Telegram fetch refusal.
func TestFetchFailureDetail_ForgeryAloneReportsNothing(t *testing.T) {
	forged := "provider chain exhausted\n" + fetchErrorMarker + `{"nonce":"guessed","cause":"not really a fetch failure"}` + "\n"

	if got := fetchFailureDetail(forged, testNonce); got != "" {
		t.Fatalf("fetchFailureDetail = %q, want empty — an unauthenticated marker line let provider output masquerade as a fetch refusal", got)
	}
}

// With no secret to check against, nothing on the stream can be authenticated,
// so nothing is believed. A generic failure is the honest degradation.
func TestFetchFailureDetail_NoNonceAcceptsNothing(t *testing.T) {
	if got := fetchFailureDetail(report("", "some cause"), ""); got != "" {
		t.Fatalf("fetchFailureDetail = %q, want empty — without a secret every line is unauthenticated and none may be treated as the server's word", got)
	}
}

// R2 — the mappings-only fallback. A transport with no live accessor still has a
// configured api_base_url, and it must reach the subprocess. Gating the install
// on `answered` silently sent the handler to api.telegram.org instead of the
// operator's proxy.
func TestHandlerEnv_NoAnswerStillInstallsTheConfiguredBase(t *testing.T) {
	os.Unsetenv(apiURLEnvVar)

	env := handlerEnv("https://proxy.example", false, testNonce, 500, 300)

	var seen []string
	for _, kv := range env {
		if strings.HasPrefix(kv, apiURLEnvVar+"=") {
			seen = append(seen, kv)
		}
	}
	if len(seen) != 1 || seen[0] != apiURLEnvVar+"=https://proxy.example" {
		t.Fatalf("child env %v, want the configured base installed — with no inherited variable it never reaches Python and the proxy is silently dropped", seen)
	}
}

// The nonce must actually reach the handler, or every report it sends is
// unauthenticated and discarded.
func TestHandlerEnv_CarriesTheNonce(t *testing.T) {
	env := handlerEnv("", false, testNonce, 500, 300)

	found := false
	for _, kv := range env {
		if kv == fetchNonceEnvVar+"="+testNonce {
			found = true
		}
	}
	if !found {
		t.Fatal("the child env carries no nonce — the handler cannot authenticate its reports and every specific fetch cause degrades to a generic failure")
	}
}

// COMPOSITION, at the call site. The helper-level tests do not prove runHandler
// actually uses it: a live-endpoint channel answering "" plus an inherited
// override is the exact case, and only the subprocess itself can testify to what
// it received. This handler prints what it saw, so the "transcript" IS the
// child's view of the environment.
func TestRunHandler_LiveEmptyAnswerReachesTheSubprocess(t *testing.T) {
	t.Setenv(apiURLEnvVar, "https://stale-proxy.example")
	tmp := t.TempDir()
	handler := filepath.Join(tmp, "stt-handler.py")
	const script = `#!/usr/bin/env python3
import os
print('SAW=' + os.environ.get('C3_TELEGRAM_API_URL', '<absent>'))
`
	if err := os.WriteFile(handler, []byte(script), 0o755); err != nil {
		t.Fatalf("write handler: %v", err)
	}
	h := hostWithChannel(&endpointChannel{base: ""}, "https://mappings.example")
	h.cfg = Config{Enabled: true, HandlerPath: handler, Timeout: 20}
	if err := Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, err := h.voiceCallback(context.Background(), c3types.VoicePayload{MessageID: 1, FileID: "V1"})
	if err != nil {
		t.Fatalf("callback error: %v", err)
	}
	if got != "SAW=<absent>" {
		t.Fatalf("the subprocess saw %q; want the variable ABSENT — the live channel authoritatively answered api.telegram.org, and anything else means the handler fetches from a host the preflight never asked", got)
	}
}

// …and the same call site must carry a non-empty live answer through.
func TestRunHandler_LiveNonEmptyAnswerReachesTheSubprocess(t *testing.T) {
	t.Setenv(apiURLEnvVar, "https://stale-proxy.example")
	tmp := t.TempDir()
	handler := filepath.Join(tmp, "stt-handler.py")
	const script = `#!/usr/bin/env python3
import os
print('SAW=' + os.environ.get('C3_TELEGRAM_API_URL', '<absent>'))
`
	if err := os.WriteFile(handler, []byte(script), 0o755); err != nil {
		t.Fatalf("write handler: %v", err)
	}
	h := hostWithChannel(&endpointChannel{base: "https://live.example"}, "https://mappings.example")
	h.cfg = Config{Enabled: true, HandlerPath: handler, Timeout: 20}
	if err := Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, _ := h.voiceCallback(context.Background(), c3types.VoicePayload{MessageID: 1, FileID: "V1"})
	if got != "SAW=https://live.example" {
		t.Fatalf("the subprocess saw %q, want the live answer", got)
	}
}
