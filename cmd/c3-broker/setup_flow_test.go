package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/karthikeyan5/c3/internal/mappings"
)

// TestStartBackgroundInstall_DelegatesToRunFn confirms the goroutine
// runs the package-level installRunFn and the result reaches the
// channel. Foundational sanity check for everything else in this file.
func TestStartBackgroundInstall_DelegatesToRunFn(t *testing.T) {
	prev := installRunFn
	t.Cleanup(func() { installRunFn = prev })

	want := installResult{
		err:      errors.New("fake"),
		output:   []byte("fake stderr"),
		duration: 42 * time.Millisecond,
	}
	installRunFn = func(ctx context.Context) installResult { return want }

	ch := startBackgroundInstall(context.Background())
	select {
	case got := <-ch:
		if got.err == nil || got.err.Error() != "fake" {
			t.Errorf("err = %v, want fake", got.err)
		}
		if string(got.output) != "fake stderr" {
			t.Errorf("output = %q, want %q", got.output, "fake stderr")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("startBackgroundInstall channel never produced a result")
	}
}

// TestStartBackgroundInstall_StartsBeforeJoin_ConcurrentExecution
// verifies the install actually runs CONCURRENTLY — i.e. the
// goroutine progresses while the caller is doing other work. This is
// the whole point of items #4+#5: user reads the bot+group walk
// WHILE go install runs.
//
// We use a fake installRunFn that blocks on a channel until released,
// signals progress on another channel. The test starts the install,
// asserts the install has begun (signal received), then releases it.
// The releaser-as-caller mimics the caller doing other work during
// the bot+group walk.
func TestStartBackgroundInstall_StartsBeforeJoin_ConcurrentExecution(t *testing.T) {
	prev := installRunFn
	t.Cleanup(func() { installRunFn = prev })

	started := make(chan struct{})
	release := make(chan struct{})
	installRunFn = func(ctx context.Context) installResult {
		close(started)
		<-release // block until test releases
		return installResult{}
	}

	ch := startBackgroundInstall(context.Background())

	// The install MUST have started by now, even though we haven't
	// joined the channel. This is the concurrency guarantee.
	select {
	case <-started:
		// good — install began without us joining
	case <-time.After(2 * time.Second):
		t.Fatal("install goroutine never started; startBackgroundInstall is not actually concurrent")
	}

	// Now release the fake install and drain.
	close(release)
	select {
	case <-ch:
		// done
	case <-time.After(2 * time.Second):
		t.Fatal("install never produced a result after release")
	}
}

// TestJoinBackgroundInstall_SuccessQuiet — successful build returns
// nil error, no panic. (Side effect: prints "✓ binaries built" to
// stdout; we don't assert on that — maintainer copy, not load-bearing.)
func TestJoinBackgroundInstall_SuccessQuiet(t *testing.T) {
	ch := make(chan installResult, 1)
	ch <- installResult{duration: 5 * time.Millisecond}
	close(ch)
	if err := joinBackgroundInstall(ch); err != nil {
		t.Errorf("join on success returned err=%v, want nil", err)
	}
}

// TestJoinBackgroundInstall_SkippedNotError — skipped install is NOT
// an error; setup continues. Critical: source-dir-not-found mustn't
// abort setup.
func TestJoinBackgroundInstall_SkippedNotError(t *testing.T) {
	ch := make(chan installResult, 1)
	ch <- installResult{skipped: true}
	close(ch)
	if err := joinBackgroundInstall(ch); err != nil {
		t.Errorf("join on skipped returned err=%v, want nil (skip is not an error)", err)
	}
}

// TestJoinBackgroundInstall_ErrorPropagated — failed install
// surfaces the error so runSetup can warn but continue.
func TestJoinBackgroundInstall_ErrorPropagated(t *testing.T) {
	want := errors.New("simulated: exit status 1")
	ch := make(chan installResult, 1)
	ch <- installResult{err: want, output: []byte("go: undefined: foo")}
	close(ch)
	got := joinBackgroundInstall(ch)
	if !errors.Is(got, want) {
		t.Errorf("join on error returned err=%v, want %v", got, want)
	}
}

// TestDiscoverSourceDir_OverrideHit — $C3_SRC_DIR pointing at a real
// C3 source tree resolves. Uses the test binary's own go.mod walk.
func TestDiscoverSourceDir_OverrideHit(t *testing.T) {
	// Find the C3 root from the test binary's location.
	c3Root := findThisRepoRoot(t)
	t.Setenv("C3_SRC_DIR", c3Root)
	dir, ok := discoverSourceDir()
	if !ok {
		t.Fatalf("discoverSourceDir() = ok=false, want true (C3_SRC_DIR=%s)", c3Root)
	}
	if dir != c3Root {
		t.Errorf("discoverSourceDir() = %q, want %q (override should be authoritative)", dir, c3Root)
	}
}

// TestDiscoverSourceDir_OverrideMissesIsFalse — a bogus
// $C3_SRC_DIR returns (..., false). Setup will then skip the
// background build with a warning rather than abort.
func TestDiscoverSourceDir_OverrideMissesIsFalse(t *testing.T) {
	t.Setenv("C3_SRC_DIR", "/definitely/does/not/exist/path")
	if _, ok := discoverSourceDir(); ok {
		t.Error("discoverSourceDir(bogus override) = ok=true, want false")
	}
}

// TestDiscoverSourceDir_AutoWalkUp — without $C3_SRC_DIR set,
// discoverSourceDir walks up from the test binary's location and
// finds the repo root (since `go test` runs the binary from a
// tempdir inside the repo).
func TestDiscoverSourceDir_AutoWalkUp(t *testing.T) {
	// Save and clear env so the walk-up path is exercised.
	t.Setenv("C3_SRC_DIR", "")
	os.Unsetenv("C3_SRC_DIR")

	dir, ok := discoverSourceDir()
	if !ok {
		// Could legitimately fail in unusual test envs (e.g. running
		// the test binary from outside the source tree, no ~/src/c3).
		// Tolerate that — the production fallback to skip is what
		// matters, not the auto-walk hitting on every system.
		t.Skip("auto-walk didn't find a C3 source tree; non-fatal — production code skips background install in that case")
	}
	if !isC3SourceDir(dir) {
		t.Errorf("discoverSourceDir() returned %q which doesn't pass isC3SourceDir", dir)
	}
}

// TestIsC3SourceDir_RecognizesC3 — the repo root must pass.
func TestIsC3SourceDir_RecognizesC3(t *testing.T) {
	root := findThisRepoRoot(t)
	if !isC3SourceDir(root) {
		t.Errorf("isC3SourceDir(%q) = false; want true", root)
	}
}

// TestIsC3SourceDir_RejectsNonC3 — random tempdir must not pass.
func TestIsC3SourceDir_RejectsNonC3(t *testing.T) {
	dir := t.TempDir()
	if isC3SourceDir(dir) {
		t.Error("isC3SourceDir(tempdir) = true; want false")
	}
	// Now write a go.mod with a different module name.
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/other\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if isC3SourceDir(dir) {
		t.Error("isC3SourceDir(other module) = true; want false")
	}
}

// TestDefaultInstallRun_SkippedWhenNoSourceDir — set
// $C3_SRC_DIR to a bogus path and confirm defaultInstallRun returns
// skipped=true without touching `go install`. Belt-and-braces against
// the production code accidentally running `go install` from a
// wrong-cwd (which would either fail or, worse, install garbage).
func TestDefaultInstallRun_SkippedWhenNoSourceDir(t *testing.T) {
	t.Setenv("C3_SRC_DIR", "/definitely/does/not/exist/path")
	t.Setenv("HOME", t.TempDir()) // so ~/src/c3 doesn't accidentally exist
	result := defaultInstallRun(context.Background())
	if !result.skipped {
		t.Errorf("defaultInstallRun returned skipped=false, want true (source dir unfindable)")
	}
	if result.err != nil {
		t.Errorf("skipped result has err=%v, want nil", result.err)
	}
}

func TestDefaultInstallPackages_ExcludeOptInCodexLauncher(t *testing.T) {
	var hasBroker, hasCodexAdapter bool
	for _, pkg := range defaultInstallPackages {
		if pkg == "./cmd/codex" {
			t.Fatal("default setup/build package set must not install the PATH-shadowing Codex launcher")
		}
		hasBroker = hasBroker || pkg == "./cmd/c3-broker"
		hasCodexAdapter = hasCodexAdapter || pkg == "./cmd/c3-codex-adapter"
	}
	if !hasBroker || !hasCodexAdapter {
		t.Fatalf("core package set lost required binaries: broker=%v codex-adapter=%v", hasBroker, hasCodexAdapter)
	}
}

// TestInstallRunFn_ConcurrentSafety — multiple concurrent
// startBackgroundInstall calls don't race on installRunFn. Belt-
// and-braces — production only calls it once per setup, but TDD
// rule of "test under -race" demands no shared-state races.
func TestInstallRunFn_ConcurrentSafety(t *testing.T) {
	prev := installRunFn
	t.Cleanup(func() { installRunFn = prev })

	var calls atomic.Int32
	installRunFn = func(ctx context.Context) installResult {
		calls.Add(1)
		return installResult{}
	}

	var wg sync.WaitGroup
	chs := make([]<-chan installResult, 10)
	for i := range chs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			chs[i] = startBackgroundInstall(context.Background())
		}(i)
	}
	wg.Wait()
	for _, ch := range chs {
		<-ch
	}
	if got := calls.Load(); got != 10 {
		t.Errorf("installRunFn called %d times, want 10", got)
	}
}

// findThisRepoRoot walks up from runtime/debug.ReadBuildInfo's
// reported package main path (or fallback CWD) to locate the C3
// repo root. Helper for tests that need a known-good C3 source dir.
func findThisRepoRoot(t *testing.T) string {
	t.Helper()
	// Prefer the executable's location since tests usually run from
	// the package source dir.
	if exe, err := os.Executable(); err == nil {
		if dir := walkUpForC3GoMod(filepath.Dir(exe)); dir != "" {
			return dir
		}
	}
	// Fallback: CWD is the package source dir under `go test`.
	if cwd, err := os.Getwd(); err == nil {
		if dir := walkUpForC3GoMod(cwd); dir != "" {
			return dir
		}
	}
	t.Fatalf("could not locate C3 repo root from test binary; tests need a discoverable go.mod")
	return ""
}

// ---------------------------------------------------------------------------
// Setup phase dispatch
// ---------------------------------------------------------------------------

// TestRunSetupWithArgs_UnknownPhaseErrors — a typo'd phase must fail loudly
// with the phase list, not fall through into the interactive flow (which
// would hang a non-interactive agent driver on stdin).
func TestRunSetupWithArgs_UnknownPhaseErrors(t *testing.T) {
	err := runSetupWithArgs([]string{"bogus-phase"})
	if err == nil {
		t.Fatal("runSetupWithArgs(bogus-phase) = nil, want error")
	}
	if !strings.Contains(err.Error(), "unknown setup phase") {
		t.Errorf("error %q does not name the unknown phase", err)
	}
}

// TestSetupUsage_MentionsAllPhases — the usage text is what an agent (or
// human) sees on a bad invocation; every phase must be discoverable there.
func TestSetupUsage_MentionsAllPhases(t *testing.T) {
	for _, phase := range []string{"setup token", "pair dm", "pair group", "setup stt", "setup finish"} {
		if !strings.Contains(setupUsage, phase) {
			t.Errorf("setupUsage missing phase %q", phase)
		}
	}
}

// TestRunSetupPair_RequiresTarget — bare `setup pair` is a usage error.
func TestRunSetupPair_RequiresTarget(t *testing.T) {
	if err := runSetupPair(nil); err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Errorf("runSetupPair(nil) = %v, want usage error", err)
	}
	if err := runSetupPair([]string{"channel"}); err == nil || !strings.Contains(err.Error(), "usage:") {
		t.Errorf("runSetupPair(channel) = %v, want usage error", err)
	}
}

// ---------------------------------------------------------------------------
// Pair codes
// ---------------------------------------------------------------------------

// TestGeneratePairCode_FourDigits — the code must always be 4 ASCII digits
// (zero-padded), matching the shape the broker's own pairing gate accepts.
func TestGeneratePairCode_FourDigits(t *testing.T) {
	for i := 0; i < 64; i++ {
		code, err := generatePairCode()
		if err != nil {
			t.Fatalf("generatePairCode: %v", err)
		}
		if !isPairCode(code) {
			t.Fatalf("generatePairCode() = %q; want 4 ASCII digits", code)
		}
	}
}

// TestIsPairCode — strict 4-digit match, same contract as the broker gate's
// codeBody (internal/broker/pairing.go).
func TestIsPairCode(t *testing.T) {
	for code, want := range map[string]bool{
		"0000":  true,
		"1234":  true,
		"9999":  true,
		"123":   false,
		"12345": false,
		"12a4":  false,
		" 1234": false,
		"":      false,
	} {
		if got := isPairCode(code); got != want {
			t.Errorf("isPairCode(%q) = %v, want %v", code, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// pairPoller against a scripted fake Bot API
// ---------------------------------------------------------------------------

// fakeBotAPI serves scripted getUpdates JSON bodies in order (the last one
// repeats once the script is exhausted) and records the offset query param
// of every call so tests can assert cursor progression.
type fakeBotAPI struct {
	t         *testing.T
	mu        sync.Mutex
	responses []string
	calls     int
	offsets   []string
}

func (f *fakeBotAPI) handler(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !strings.HasSuffix(r.URL.Path, "/getUpdates") {
		f.t.Errorf("unexpected path %q", r.URL.Path)
	}
	f.offsets = append(f.offsets, r.URL.Query().Get("offset"))
	idx := f.calls
	if idx >= len(f.responses) {
		idx = len(f.responses) - 1
	}
	f.calls++
	_, _ = w.Write([]byte(f.responses[idx]))
}

// newTestPoller returns a pairPoller aimed at the fake server with
// test-friendly timings (no long poll, near-instant transient retry).
func newTestPoller(srvURL, code string, target pairTarget) *pairPoller {
	return &pairPoller{
		base:        srvURL,
		token:       "TESTTOKEN",
		code:        code,
		target:      target,
		pollTimeout: 0,
		retryDelay:  time.Millisecond,
		client:      &http.Client{Timeout: 2 * time.Second},
	}
}

// TestPairPoller_DMCaptureIgnoresNoiseAndGroups — the DM poller must skip
// /start noise, wrong codes, and a matching code sent in a GROUP (that's
// the group flow's business), then capture the DM sender's user id. Also
// pins the loss-freedom contract: the poller NEVER advances the getUpdates
// offset — not between polls (the second poll re-reads the first batch; the
// in-memory dedupe absorbs the repeats) and not after a successful match
// (no commit call) — so no unrelated update is ever confirmed server-side.
func TestPairPoller_DMCaptureIgnoresNoiseAndGroups(t *testing.T) {
	fake := &fakeBotAPI{t: t, responses: []string{
		`{"ok":true,"result":[
			{"update_id":10,"message":{"text":"/start","from":{"id":777},"chat":{"id":777}}},
			{"update_id":11,"message":{"text":"9999","from":{"id":777},"chat":{"id":777}}},
			{"update_id":12,"message":{"text":"1234","from":{"id":555},"chat":{"id":-100200,"title":"Ops"}}}
		]}`,
		`{"ok":true,"result":[
			{"update_id":10,"message":{"text":"/start","from":{"id":777},"chat":{"id":777}}},
			{"update_id":11,"message":{"text":"9999","from":{"id":777},"chat":{"id":777}}},
			{"update_id":12,"message":{"text":"1234","from":{"id":555},"chat":{"id":-100200,"title":"Ops"}}},
			{"update_id":13,"message":{"text":" 1234 ","from":{"id":777},"chat":{"id":777}}}
		]}`,
		`{"ok":true,"result":[]}`,
	}}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	p := newTestPoller(srv.URL, "1234", pairTargetDM)
	capture, err := p.wait(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if capture.UserID != 777 {
		t.Errorf("capture.UserID = %d, want 777", capture.UserID)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.offsets) != 2 {
		t.Fatalf("expected exactly 2 getUpdates calls (poll, poll — and NO post-match commit), got %d: %v", len(fake.offsets), fake.offsets)
	}
	for i, off := range fake.offsets {
		if off != "0" {
			t.Errorf("call %d used offset %s, want 0 — the poller must never confirm updates", i, off)
		}
	}
}

// TestPairPoller_NeverConfirmsOnFailure — an expired window over a stream of
// unrelated updates must leave the getUpdates cursor untouched: the broker's
// unconfirmed backlog (and any real messages that arrived mid-setup) must be
// re-fetchable by the restarted broker even when pairing FAILS.
func TestPairPoller_NeverConfirmsOnFailure(t *testing.T) {
	fake := &fakeBotAPI{t: t, responses: []string{
		`{"ok":true,"result":[
			{"update_id":50,"message":{"text":"an important real message","from":{"id":777},"chat":{"id":777}}},
			{"update_id":51,"message":{"text":"another one","from":{"id":777},"chat":{"id":777}}}
		]}`,
	}}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	p := newTestPoller(srv.URL, "1234", pairTargetDM)
	_, err := p.wait(time.Now().Add(150 * time.Millisecond))
	if err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("wait = %v, want expiry error", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.offsets) == 0 {
		t.Fatal("no getUpdates calls recorded")
	}
	for i, off := range fake.offsets {
		if off != "0" {
			t.Errorf("call %d used offset %s, want 0 — failure must not confirm/destroy unrelated updates", i, off)
		}
	}
}

// TestPairPoller_DedupedRepeatsThenMatch — because the poller re-polls with
// the same offset, every earlier update is re-delivered on every poll. The
// in-memory dedupe must absorb the repeats (here: a WRONG 4-digit code that
// would trip the fail-closed cap if re-counted each poll) and still capture
// the code when it finally arrives on a later poll.
func TestPairPoller_DedupedRepeatsThenMatch(t *testing.T) {
	repeated := `{"update_id":60,"message":{"text":"9999","from":{"id":777},"chat":{"id":777}}}`
	fake := &fakeBotAPI{t: t, responses: []string{
		`{"ok":true,"result":[` + repeated + `]}`,
		`{"ok":true,"result":[` + repeated + `]}`,
		`{"ok":true,"result":[` + repeated + `]}`,
		`{"ok":true,"result":[` + repeated + `]}`,
		`{"ok":true,"result":[` + repeated + `]}`,
		`{"ok":true,"result":[` + repeated + `]}`,
		`{"ok":true,"result":[` + repeated + `]}`,
		`{"ok":true,"result":[` + repeated + `]}`,
		`{"ok":true,"result":[` + repeated + `]}`,
		`{"ok":true,"result":[` + repeated + `]}`,
		`{"ok":true,"result":[` + repeated + `]}`,
		`{"ok":true,"result":[` + repeated + `,
			{"update_id":61,"message":{"text":"1234","from":{"id":777},"chat":{"id":777}}}]}`,
	}}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	p := newTestPoller(srv.URL, "1234", pairTargetDM)
	capture, err := p.wait(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatalf("wait: %v (a re-counted duplicate wrong code must not trip the fail-closed cap)", err)
	}
	if capture.UserID != 777 {
		t.Errorf("capture.UserID = %d, want 777", capture.UserID)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for i, off := range fake.offsets {
		if off != "0" {
			t.Errorf("call %d used offset %s, want 0", i, off)
		}
	}
}

// TestPairPoller_FullPageSkipLastResort — the ONLY case where the poller may
// advance the offset (confirming updates server-side): a FULL page
// (pairPageFull updates) with no code in it, which would otherwise hide every
// later update forever. It must warn loudly that those updates were skipped.
func TestPairPoller_FullPageSkipLastResort(t *testing.T) {
	var page strings.Builder
	page.WriteString(`{"ok":true,"result":[`)
	for i := 1; i <= pairPageFull; i++ {
		if i > 1 {
			page.WriteString(",")
		}
		fmt.Fprintf(&page, `{"update_id":%d,"message":{"text":"noise %d","from":{"id":777},"chat":{"id":777}}}`, i, i)
	}
	page.WriteString(`]}`)
	fake := &fakeBotAPI{t: t, responses: []string{
		page.String(),
		fmt.Sprintf(`{"ok":true,"result":[{"update_id":%d,"message":{"text":"1234","from":{"id":777},"chat":{"id":777}}}]}`, pairPageFull+1),
	}}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	var warnings bytes.Buffer
	p := newTestPoller(srv.URL, "1234", pairTargetDM)
	p.warn = &warnings
	capture, err := p.wait(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if capture.UserID != 777 {
		t.Errorf("capture.UserID = %d, want 777", capture.UserID)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.offsets) < 2 {
		t.Fatalf("expected ≥2 getUpdates calls, got %d", len(fake.offsets))
	}
	if fake.offsets[0] != "0" {
		t.Errorf("first call offset = %s, want 0", fake.offsets[0])
	}
	if want := fmt.Sprintf("%d", pairPageFull+1); fake.offsets[1] != want {
		t.Errorf("post-full-page offset = %s, want %s (advance past the full page)", fake.offsets[1], want)
	}
	if !strings.Contains(warnings.String(), "skipping past them") {
		t.Errorf("full-page skip produced no warning; warnings = %q", warnings.String())
	}
}

// TestPairPoller_TooManyWrongCodesFailsClosed — after pairMaxWrongCodes wrong
// 4-digit candidates in the watched chat type, the window must abort (fail
// closed) — even if the right code follows in the same batch — and must not
// confirm anything.
func TestPairPoller_TooManyWrongCodesFailsClosed(t *testing.T) {
	var page strings.Builder
	page.WriteString(`{"ok":true,"result":[`)
	for i := 0; i < pairMaxWrongCodes; i++ {
		if i > 0 {
			page.WriteString(",")
		}
		fmt.Fprintf(&page, `{"update_id":%d,"message":{"text":"%04d","from":{"id":555},"chat":{"id":555}}}`, 70+i, i)
	}
	// The right code, one update later — the window must already be closed.
	fmt.Fprintf(&page, `,{"update_id":%d,"message":{"text":"1234","from":{"id":777},"chat":{"id":777}}}`, 70+pairMaxWrongCodes)
	page.WriteString(`]}`)
	fake := &fakeBotAPI{t: t, responses: []string{page.String()}}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	p := newTestPoller(srv.URL, "1234", pairTargetDM)
	_, err := p.wait(time.Now().Add(5 * time.Second))
	if err == nil || !strings.Contains(err.Error(), "too many wrong codes") {
		t.Fatalf("wait = %v, want fail-closed too-many-wrong-codes error", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for i, off := range fake.offsets {
		if off != "0" {
			t.Errorf("call %d used offset %s, want 0 — fail-closed abort must not confirm updates", i, off)
		}
	}
}

// TestPairPoller_GroupSenderScoping — once an operator allowlist exists,
// only an allowlisted sender's code completes a GROUP pairing: the same code
// from a random member must be ignored. Any-sender remains only for the
// bootstrap case (senderAllowed == nil), covered by
// TestPairPoller_GroupCaptureIgnoresDMs.
func TestPairPoller_GroupSenderScoping(t *testing.T) {
	fake := &fakeBotAPI{t: t, responses: []string{
		`{"ok":true,"result":[
			{"update_id":80,"message":{"text":"4321","from":{"id":555},"chat":{"id":-100200,"title":"Ops"}}},
			{"update_id":81,"message":{"text":"4321","from":{"id":777},"chat":{"id":-100200,"title":"Ops"}}}
		]}`,
		`{"ok":true,"result":[]}`,
	}}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	p := newTestPoller(srv.URL, "4321", pairTargetGroup)
	p.senderAllowed = func(id int64) bool { return id == 777 }
	capture, err := p.wait(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if capture.UserID != 777 {
		t.Errorf("capture.UserID = %d, want 777 (the allowlisted sender, not the first sender)", capture.UserID)
	}
	if capture.ChatID != -100200 {
		t.Errorf("capture.ChatID = %d, want -100200", capture.ChatID)
	}
}

// TestPairPoller_GroupCaptureIgnoresDMs — the group poller must skip a
// matching code sent in a DM and capture the group's chat id + title from
// the in-group match. The sender is incidental (we trust the group).
func TestPairPoller_GroupCaptureIgnoresDMs(t *testing.T) {
	fake := &fakeBotAPI{t: t, responses: []string{
		`{"ok":true,"result":[
			{"update_id":20,"message":{"text":"4321","from":{"id":777},"chat":{"id":777}}},
			{"update_id":21,"message":{"text":"4321","from":{"id":555},"chat":{"id":-100987,"title":"Family Ops"}}}
		]}`,
		`{"ok":true,"result":[]}`,
	}}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	p := newTestPoller(srv.URL, "4321", pairTargetGroup)
	capture, err := p.wait(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if capture.ChatID != -100987 {
		t.Errorf("capture.ChatID = %d, want -100987", capture.ChatID)
	}
	if capture.ChatTitle != "Family Ops" {
		t.Errorf("capture.ChatTitle = %q, want \"Family Ops\"", capture.ChatTitle)
	}
}

// TestPairPoller_ExpiresWithoutMatch — an empty stream past the deadline is
// a clean expiry error, not a hang.
func TestPairPoller_ExpiresWithoutMatch(t *testing.T) {
	fake := &fakeBotAPI{t: t, responses: []string{`{"ok":true,"result":[]}`}}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	p := newTestPoller(srv.URL, "1234", pairTargetDM)
	_, err := p.wait(time.Now().Add(150 * time.Millisecond))
	if err == nil || !strings.Contains(err.Error(), "expired") {
		t.Errorf("wait on empty stream = %v, want expiry error", err)
	}
}

// TestPairPoller_FatalOnRejectedToken — a 401 from Telegram cannot be fixed
// by retrying; the poller must bail immediately with a token error instead
// of burning the whole window.
func TestPairPoller_FatalOnRejectedToken(t *testing.T) {
	fake := &fakeBotAPI{t: t, responses: []string{
		`{"ok":false,"error_code":401,"description":"Unauthorized"}`,
	}}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	p := newTestPoller(srv.URL, "1234", pairTargetDM)
	start := time.Now()
	_, err := p.wait(time.Now().Add(30 * time.Second))
	if err == nil || !strings.Contains(err.Error(), "token") {
		t.Errorf("wait on 401 = %v, want fatal token error", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Error("fatal 401 took longer than 5s — poller kept retrying a hopeless error")
	}
}

// TestPairPoller_TransientErrorsRetry — a transient Telegram error (rate
// limit, flood control) must be retried, and the match after recovery
// must still be captured.
func TestPairPoller_TransientErrorsRetry(t *testing.T) {
	fake := &fakeBotAPI{t: t, responses: []string{
		`{"ok":false,"error_code":429,"description":"Too Many Requests"}`,
		`{"ok":true,"result":[{"update_id":30,"message":{"text":"1234","from":{"id":42},"chat":{"id":42}}}]}`,
		`{"ok":true,"result":[]}`,
	}}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	p := newTestPoller(srv.URL, "1234", pairTargetDM)
	capture, err := p.wait(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if capture.UserID != 42 {
		t.Errorf("capture.UserID = %d, want 42", capture.UserID)
	}
}

// ---------------------------------------------------------------------------
// Config upserts
// ---------------------------------------------------------------------------

// TestApplyBotToken_PreservesOtherFields — the token upsert must not
// clobber unrelated channel fields (a re-run must never lose topics,
// groups, or ids the user already has).
func TestApplyBotToken_PreservesOtherFields(t *testing.T) {
	mf := skeletonMappings()
	mf.Channels[telegramChannelName] = mappings.ChannelConfig{
		BotToken:     "old-token",
		DefaultGroup: "main",
		Groups:       map[string]mappings.GroupConfig{"main": {ChatID: -100200}},
		DMChatID:     777,
		MasterUserID: 777,
	}
	applyBotToken(mf, "new-token")
	cc := mf.Channels[telegramChannelName]
	if cc.BotToken != "new-token" {
		t.Errorf("BotToken = %q, want new-token", cc.BotToken)
	}
	if cc.DefaultGroup != "main" || cc.DMChatID != 777 || cc.Groups["main"].ChatID != -100200 {
		t.Errorf("applyBotToken clobbered unrelated fields: %+v", cc)
	}
}

// TestApplyDMPair_SetsIdentityAndAllowlist — a DM pair must record all
// three: dm_chat_id, master_user_id, and the default-deny allowlist entry
// (the same mutation the broker's acceptDMPair performs). Idempotent.
func TestApplyDMPair_SetsIdentityAndAllowlist(t *testing.T) {
	mf := skeletonMappings()
	applyDMPair(mf, 777)
	applyDMPair(mf, 777) // idempotent
	cc := mf.Channels[telegramChannelName]
	if cc.DMChatID != 777 || cc.MasterUserID != 777 {
		t.Errorf("identity fields = dm:%d master:%d, want 777/777", cc.DMChatID, cc.MasterUserID)
	}
	if !mf.IsUserAllowed(777) {
		t.Error("user 777 not allowlisted after applyDMPair")
	}
	if got := len(mf.AllowlistOrEmpty().Users); got != 1 {
		t.Errorf("allowlist.users has %d entries, want 1 (idempotent)", got)
	}
}

// TestApplyGroupPair_RecordsGroupDefaultAndAllowlist — a group pair must
// record the groups entry (+title), claim the default when none is set,
// and allowlist the chat id (mirroring the broker's acceptGroupPair).
func TestApplyGroupPair_RecordsGroupDefaultAndAllowlist(t *testing.T) {
	mf := skeletonMappings()
	applyGroupPair(mf, "main", -100200, "My Group")
	cc := mf.Channels[telegramChannelName]
	if g := cc.Groups["main"]; g.ChatID != -100200 || g.Title != "My Group" {
		t.Errorf("groups[main] = %+v, want chat -100200 title \"My Group\"", g)
	}
	if cc.DefaultGroup != "main" {
		t.Errorf("DefaultGroup = %q, want main (claimed when unset)", cc.DefaultGroup)
	}
	if !mf.IsGroupAllowed(-100200) {
		t.Error("group -100200 not allowlisted after applyGroupPair")
	}
}

// TestApplyGroupPair_DoesNotStealExistingDefault — pairing a SECOND group
// must not silently re-point default_group at it.
func TestApplyGroupPair_DoesNotStealExistingDefault(t *testing.T) {
	mf := skeletonMappings()
	applyGroupPair(mf, "main", -100200, "")
	applyGroupPair(mf, "side", -100300, "Side")
	cc := mf.Channels[telegramChannelName]
	if cc.DefaultGroup != "main" {
		t.Errorf("DefaultGroup = %q after second pair, want main", cc.DefaultGroup)
	}
	if len(cc.Groups) != 2 {
		t.Errorf("groups = %d entries, want 2", len(cc.Groups))
	}
}

// ---------------------------------------------------------------------------
// Progress tracking (#4 — never re-show a completed step)
// ---------------------------------------------------------------------------

// TestDeriveProgress_EmptyAndFull — a skeleton reports nothing configured;
// a populated config reports every step done (preferring default_group).
func TestDeriveProgress_EmptyAndFull(t *testing.T) {
	empty := deriveProgress(skeletonMappings(), filepath.Join(t.TempDir(), "absent.env"))
	if empty.Token != "" || empty.DMChatID != 0 || empty.GroupChatID != 0 || empty.STTConfigured {
		t.Errorf("deriveProgress(skeleton) = %+v, want zero progress", empty)
	}

	sttPath := filepath.Join(t.TempDir(), "stt.env")
	if err := os.WriteFile(sttPath, []byte("OPENROUTER_API_KEY=x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mf := skeletonMappings()
	mf.Channels[telegramChannelName] = mappings.ChannelConfig{
		BotToken:     "tok",
		DefaultGroup: "main",
		Groups: map[string]mappings.GroupConfig{
			"side": {ChatID: -100300},
			"main": {ChatID: -100200},
		},
		DMChatID: 777,
	}
	full := deriveProgress(mf, sttPath)
	if full.Token != "tok" || full.DMChatID != 777 || !full.STTConfigured {
		t.Errorf("deriveProgress(full) = %+v, want token/dm/stt set", full)
	}
	if full.GroupName != "main" || full.GroupChatID != -100200 {
		t.Errorf("deriveProgress preferred %q/%d, want the default group main/-100200", full.GroupName, full.GroupChatID)
	}
}

// TestLoadOrInitMappings_MissingYieldsSkeleton — a missing file is a fresh
// start (not an error); a corrupt file is an error the caller must handle.
func TestLoadOrInitMappings_MissingYieldsSkeleton(t *testing.T) {
	mf, existed, err := loadOrInitMappings(filepath.Join(t.TempDir(), "absent.json"))
	if err != nil || existed {
		t.Fatalf("loadOrInitMappings(missing) = existed=%v err=%v, want false/nil", existed, err)
	}
	if mf == nil || mf.SchemaVersion != 1 || mf.Channels == nil {
		t.Errorf("skeleton = %+v, want schema 1 with maps allocated", mf)
	}
}

func TestLoadOrInitMappings_CorruptReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mappings.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, existed, err := loadOrInitMappings(path)
	if err == nil || !existed {
		t.Errorf("loadOrInitMappings(corrupt) = existed=%v err=%v, want true/error", existed, err)
	}
}

// ---------------------------------------------------------------------------
// Emitted copy guards (#4/#5/#6/#8/#9/#10)
// ---------------------------------------------------------------------------

// bannedSetupCopy are the manual-id-hunt breadcrumbs that must never appear
// in any setup-emitted text again: pairing replaced them (maintainer
// directive, 2026-06-30 install feedback #5/#6).
var bannedSetupCopy = []string{"userinfobot", "username_to_id_bot", "-100"}

// TestChecklist_NoBotCreationStepAndNoIDHunting — the checklist runs after
// the token is already validated, so it must not re-show bot creation (#4),
// and its copy must not send anyone hunting for ids (#5/#6).
func TestChecklist_NoBotCreationStepAndNoIDHunting(t *testing.T) {
	var all strings.Builder
	for _, s := range botGroupChecklistSteps() {
		all.WriteString(s.title)
		all.WriteString("\n")
		all.WriteString(strings.Join(s.body, "\n"))
		all.WriteString("\n")
		if strings.Contains(s.title, "Create the bot") {
			t.Errorf("checklist re-shows the completed bot-creation step: %q", s.title)
		}
	}
	text := all.String()
	for _, banned := range bannedSetupCopy {
		if strings.Contains(text, banned) {
			t.Errorf("checklist copy contains banned id-hunting breadcrumb %q", banned)
		}
	}
	if !strings.Contains(text, "privacy") && !strings.Contains(text, "setprivacy") {
		t.Error("checklist lost the privacy-mode step — group pairing depends on it")
	}
}

// TestPairIntros_ContainCodeAndNoIDHunting — the pairing banners must carry
// the code (and the bot username for DM) and stay free of id-hunt copy.
func TestPairIntros_ContainCodeAndNoIDHunting(t *testing.T) {
	dm := dmPairIntro("my_test_bot", "1234")
	if !strings.Contains(dm, "1234") || !strings.Contains(dm, "@my_test_bot") {
		t.Errorf("dmPairIntro missing code or bot username:\n%s", dm)
	}
	group := groupPairIntro("5678", false)
	if !strings.Contains(group, "5678") {
		t.Errorf("groupPairIntro missing code:\n%s", group)
	}
	restricted := groupPairIntro("5678", true)
	if !strings.Contains(restricted, "5678") || !strings.Contains(restricted, "paired") {
		t.Errorf("restricted groupPairIntro missing code or the paired-sender instruction:\n%s", restricted)
	}
	for _, text := range []string{dm, group, restricted, pairArmPrompt} {
		for _, banned := range bannedSetupCopy {
			if strings.Contains(text, banned) {
				t.Errorf("pairing copy contains banned id-hunting breadcrumb %q in:\n%s", banned, text)
			}
		}
	}
}

// TestPostSetupWhatNow_Claude_NoDefaultResume — the post-setup block must
// stand alone (#8): launch command, /c3:attach, send-from-phone. The launch
// line must NOT default to --resume (#9) — it may only be mentioned as an
// explicit opt-in — and the 30-second tour must be present (#10).
func TestPostSetupWhatNow_Claude_NoDefaultResume(t *testing.T) {
	for _, host := range []HostCLI{HostClaude, HostUnknown} {
		out := postSetupWhatNow(host)
		if !strings.Contains(out, "claude --dangerously-load-development-channels plugin:c3@c3") {
			t.Fatalf("host %v: missing the stand-alone launch command:\n%s", host, out)
		}
		for _, line := range strings.Split(out, "\n") {
			if strings.Contains(line, "--dangerously-load-development-channels") && strings.Contains(line, "--resume") {
				t.Errorf("host %v: launch line defaults to --resume: %q", host, line)
			}
		}
		if !strings.Contains(out, "--resume") {
			t.Errorf("host %v: --resume opt-in note missing entirely", host)
		}
		if !strings.Contains(out, "/c3:attach") {
			t.Errorf("host %v: missing the /c3:attach step", host)
		}
		if !strings.Contains(out, "phone") {
			t.Errorf("host %v: missing the send-from-phone step", host)
		}
		if !strings.Contains(out, "/c3:status") || !strings.Contains(out, "/c3:topics") {
			t.Errorf("host %v: missing the 30-second tour pointers", host)
		}
	}
}

// TestPostSetupWhatNow_Codex — the Codex block must not default the resume
// variant either, and must still cover attach + phone + tour.
func TestPostSetupWhatNow_Codex(t *testing.T) {
	out := postSetupWhatNow(HostCodex)
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "then run:") && strings.Contains(line, "resume") {
			t.Errorf("codex launch line defaults to resume: %q", line)
		}
	}
	if !strings.Contains(out, "resume --last") {
		t.Error("codex resume opt-in note missing")
	}
	if !strings.Contains(out, "attach") || !strings.Contains(out, "phone") {
		t.Errorf("codex block missing attach/phone steps:\n%s", out)
	}
}

// TestSetupCopy_NoIDHuntingAnywhere — belt and braces across every
// setup-emitted string constant/builder in one sweep.
func TestSetupCopy_NoIDHuntingAnywhere(t *testing.T) {
	texts := []string{
		dmPairIntro("bot", "0000"),
		groupPairIntro("0000", false),
		groupPairIntro("0000", true),
		pairArmPrompt,
		postSetupWhatNow(HostClaude),
		postSetupWhatNow(HostCodex),
		sttNotes,
		setupUsage,
	}
	for _, s := range botGroupChecklistSteps() {
		texts = append(texts, s.title, strings.Join(s.body, "\n"))
	}
	for _, text := range texts {
		for _, banned := range bannedSetupCopy {
			if strings.Contains(text, banned) {
				t.Errorf("setup copy contains banned id-hunting breadcrumb %q in:\n%s", banned, text)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Token validation + API base
// ---------------------------------------------------------------------------

// TestValidateBotTokenAt — getMe success returns the username; a Telegram
// error surfaces its description.
func TestValidateBotTokenAt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "goodtoken") {
			fmt.Fprint(w, `{"ok":true,"result":{"username":"my_test_bot"}}`)
			return
		}
		fmt.Fprint(w, `{"ok":false,"description":"Unauthorized"}`)
	}))
	defer srv.Close()

	username, err := validateBotTokenAt(srv.URL, "goodtoken")
	if err != nil || username != "my_test_bot" {
		t.Errorf("validateBotTokenAt(good) = %q, %v; want my_test_bot, nil", username, err)
	}
	if _, err := validateBotTokenAt(srv.URL, "badtoken"); err == nil || !strings.Contains(err.Error(), "Unauthorized") {
		t.Errorf("validateBotTokenAt(bad) = %v, want Unauthorized error", err)
	}
}

// TestTelegramAPIBase_EnvOverride — the env override wins (trailing slash
// trimmed); unset falls back to the public endpoint. Mirrors the telegram
// channel's env-beats-file precedence.
func TestTelegramAPIBase_EnvOverride(t *testing.T) {
	t.Setenv("C3_TELEGRAM_API_URL", "https://bot-api-proxy.example.com/")
	if got := telegramAPIBase(); got != "https://bot-api-proxy.example.com" {
		t.Errorf("telegramAPIBase(override) = %q", got)
	}
	t.Setenv("C3_TELEGRAM_API_URL", "")
	os.Unsetenv("C3_TELEGRAM_API_URL")
	if got := telegramAPIBase(); got != "https://api.telegram.org" {
		t.Errorf("telegramAPIBase(default) = %q", got)
	}
}

// TestFallbackGroupName — phased pair-group default: title when present,
// else "main".
func TestFallbackGroupName(t *testing.T) {
	if got := fallbackGroupName("Family Ops"); got != "Family Ops" {
		t.Errorf("fallbackGroupName(title) = %q", got)
	}
	if got := fallbackGroupName("  "); got != "main" {
		t.Errorf("fallbackGroupName(blank) = %q, want main", got)
	}
}
