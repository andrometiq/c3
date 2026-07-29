// Package stt is the v1 STT plugin for C3. It's a thin Go shim that
// subprocesses an external Python handler when a voice attachment arrives.
//
// Spec §6.2 (revised): the existing Python POC's stt-handler.py is mature
// (multi-provider chain with vocabulary support) and lives at the path the
// Python broker installs symlink to. The Go shim defaults to that same path
// and lets the user override via mappings.json:plugins.stt.handler_path.
//
// The handler's contract:
//
//	stdin (line 1):  <bot_token>\n
//	argv:            python3 stt-handler.py <chat_id> <reply_msg_id> <file_id> [<message_thread_id>]
//
// The bot token is supplied on stdin (not argv) so it doesn't appear in
// `ps`/`/proc/<pid>/cmdline`/audit logs. Addresses code-review-2026-05-15
// MAJOR #1 (cli.md §1.10).
//
// On success: prints transcript to stdout (and may also echo back to Telegram
// itself — that's POC-side behavior we don't override). On failure: empty
// stdout. The Go shim returns the trimmed stdout as the transcript.
package stt

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/plugin"
)

// Name is the plugin identifier and the mappings.json:plugins key.
const Name = "stt"

// Config is the plugin's slice of mappings.json:plugins.stt.
type Config struct {
	Enabled     bool   `json:"enabled"`
	Priority    int    `json:"priority"`
	HandlerPath string `json:"handler_path"`    // path to Python script
	Timeout     int    `json:"timeout_seconds"` // subprocess budget (default 300s — long voice notes
	//                                              need download + Gemini + Sarvam fallback time)
	// Python is the interpreter the handler runs under. Empty ⇒ bare "python3"
	// (PATH). STT needs only the standard library now (the Sarvam batch path was
	// ported to native urllib), so no venv is required — system python3 works.
	Python string `json:"python"`
	// AudioRetention is how many of the most-recent downloaded .oga voice files the
	// handler keeps in its inbox (a rolling window); older ones are pruned after
	// each transcription. Retains recent audio for retranscribe/testing while
	// bounding disk. 0/unset ⇒ defaultAudioRetention (500); negative ⇒ keep all.
	AudioRetention int `json:"audio_retention"`
}

// defaultTimeoutSeconds is the broker's hard deadline for the STT subprocess.
// 60s was too short — 2026-05-09: a 6m25s voice note (7.9 MB) ate
// 45s on download alone, leaving only 15s for the gemini/sarvam chain →
// SIGKILL'd before either provider returned. 300s gives room for slow
// downloads + a long-audio transcription cycle without surprising the user.
const defaultTimeoutSeconds = 300

// defaultAudioRetention is how many recent .oga voice files the handler keeps in
// its inbox when plugins.stt.audio_retention is unset (0). Recent audio stays
// available for retranscribe/testing; older files are pruned after each
// transcription. A negative config value keeps everything.
const defaultAudioRetention = 500

// Health is the EFFECTIVE state of the plugin, as opposed to the switch in
// config. Enabled only says the operator did not turn it off; a plugin can be
// enabled and still be unable to transcribe a single voice note, which is the
// state `c3-broker status` used to report as a plain "enabled=true".
type Health struct {
	// Enabled is the effective value of plugins.stt.enabled, WITH the default
	// applied. An absent key means true — matching Register.
	Enabled bool
	// HandlerPath is the configured plugins.stt.handler_path. Empty means the
	// broker runs the automatic plugin/source/release-bundle resolution chain,
	// whose process environment a separate status process cannot fully see.
	HandlerPath string
	// Detail is a one-line plain-language reading of the two fields above, or
	// "" when there is nothing a reader needs to know.
	Detail string
}

// Inspect reports Health for a raw plugins.stt config bag as read from
// mappings.json. It decodes exactly the way PluginHost.Config does — a JSON
// round-trip over defaults — so an absent or partial entry yields the same
// answer the broker itself computed at startup.
//
// It deliberately does NOT stat the handler or repeat automatic resolution.
// The relevant environment and executable path belong to the broker process,
// while `c3-broker status` is a different process: a status run from a plain
// shell could otherwise report "no handler" for a healthy broker.
// Replacing one wrong answer with another is not an improvement. The broker
// logs the authoritative resolved path once at startup, so the honest thing for
// status to do is say where the answer lives.
func Inspect(raw map[string]any) Health {
	cfg := Config{Enabled: true, Timeout: defaultTimeoutSeconds}
	if len(raw) > 0 {
		if data, err := json.Marshal(raw); err == nil {
			_ = json.Unmarshal(data, &cfg)
		}
	}
	h := Health{Enabled: cfg.Enabled, HandlerPath: cfg.HandlerPath}
	switch {
	case !cfg.Enabled:
		h.Detail = "off — plugins.stt.enabled=false; voice notes arrive untranscribed"
	case cfg.HandlerPath == "":
		h.Detail = "handler path not configured — the broker resolves Claude's plugin root, C3_SRC_DIR, " +
			"the release bundle beside its executable, and documented source checkouts in order. " +
			"broker.log records the resolved path at startup; set plugins.stt.handler_path to remove the ambiguity"
	}
	return h
}

// Register subscribes the plugin's OnVoiceReceived callback. Called once at
// broker startup if mappings.json:plugins.stt.enabled (default true).
func Register(host plugin.Host) error {
	cfg := Config{Enabled: true, Timeout: defaultTimeoutSeconds}
	if err := host.Config(Name, &cfg); err != nil {
		return fmt.Errorf("stt: read config: %w", err)
	}
	if !cfg.Enabled {
		host.Logf("stt: plugin disabled via mappings.json:plugins.stt.enabled=false")
		return nil
	}
	if cfg.HandlerPath == "" {
		cfg.HandlerPath = defaultHandlerPath()
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeoutSeconds
	}
	if cfg.AudioRetention == 0 {
		cfg.AudioRetention = defaultAudioRetention
	}
	// Handler existence is checked PER-CALL (inside the callback) rather than
	// once at startup. Two reasons:
	//   1. A missing handler at startup used to silently disable transcription,
	//      so voice messages reached the agent as a bare "(voice message)"
	//      placeholder with no indication anything had gone wrong.
	//   2. With the per-call check, if the user restores the script, the very
	//      next voice message transcribes — no broker restart required.
	// We still log once at startup so the operator knows the current state.
	if _, err := os.Stat(cfg.HandlerPath); err != nil {
		host.Logf("stt: handler %s missing at startup (%v); voice messages will surface [STT FAILED: handler_missing] until the handler is restored",
			cfg.HandlerPath, err)
	} else {
		host.Logf("stt: registered with handler=%s timeout=%ds", cfg.HandlerPath, cfg.Timeout)
	}

	// Log which interpreter we'll run the handler under. STT needs only the
	// standard library + ffmpeg/ffprobe — no Python packages, no venv.
	host.Logf("stt: python=%s", pythonExe(cfg))

	// TODO #12 (2026-05-16): belt-and-suspenders for the fresh-install STT
	// crash. The Python handler also mkdir's these on its own at import
	// time (see plugins/c3/stt/stt-handler.py), but doing it here too means
	// even if the user is running an older bundled handler, the broker
	// has already created the default dirs before the first voice arrives.
	// We only know the *default* paths; if the operator overrides via
	// STT_INBOX_DIR / STT_LOG_FILE in the handler's env, only the Python
	// side's mkdir covers those — that's fine, the Python side is
	// authoritative for handler-side paths.
	ensureSTTDefaultDirs(host)

	// Resolve telegram channel for the bot token. We need the token because
	// the POC handler shells out to Telegram's getFile API itself; the channel
	// owns the only authoritative copy of the token.
	host.OnVoiceReceived(func(ctx context.Context, p c3types.VoicePayload) (string, error) {
		if _, err := os.Stat(cfg.HandlerPath); err != nil {
			host.Logf("stt: msg=%d handler missing at %s (%v)", p.MessageID, cfg.HandlerPath, err)
			return sttFailureMarker("handler_missing"), nil
		}
		token, apiBaseURL, answered, err := readTelegramConn(host)
		if err != nil {
			host.Logf("stt: token read failed for msg=%d: %v", p.MessageID, err)
			return sttFailureMarker("token_unavailable"), nil
		}
		return runHandler(ctx, host, cfg, token, apiBaseURL, answered, p)
	})
	return nil
}

// sttFailureMarker is the stand-in transcript text the broker forwards when
// the STT chain fails. It replaces the previous silent (voice message)
// fallback so the receiver knows transcription didn't run and can resend.
// 2026-05-09: "if it's not delivered, you can log it"; equivalent
// principle for STT failure — surface, don't swallow.
//
// 2026-05-18 (#13): include a pointer to the broker log so a fresh-install
// user knows where the actual traceback lives. The marker keeps the
// machine-parseable "[STT FAILED: <reason>]" shape and appends a
// "(see <path>)" hint in human-readable form.
func sttFailureMarker(reason string) string {
	return "[STT FAILED: " + reason + " — see " + sttLogHintPath() + "]"
}

// FetchFailedPrefix marks a transcript stand-in that is NOT a transcription
// failure: the handler never got the audio, because the bot server refused or
// could not serve the fetch. The broker recognizes this prefix and reports the
// server's own words instead of the "audio is saved and recoverable" recovery
// text, which is false on every clause when nothing was ever downloaded (the
// 2026-07-27 incident, and Codex review 2 finding 1 — the same opacity returning
// through the handler's own getFile).
const FetchFailedPrefix = "[STT FETCH FAILED: "

// fetchErrorMarker is the handler's structured fetch-error line prefix. It must
// appear at the START of a line, and the rest of that line is the cause as a
// JSON string (see stt-handler.py emit_fetch_error).
//
// The protocol is structured, not prose, because the payload is a SERVER
// description — text C3 does not author and cannot constrain. An unanchored
// search for a human-readable delimiter could be made to match inside the
// description itself (parsing then lands on whitespace and the specific refusal
// silently becomes a generic "[STT FAILED: error]"), an embedded newline
// truncated the "verbatim" cause, and any non-fetch diagnostic containing the
// delimiter could masquerade as a fetch failure. JSON encoding removes all three
// at once: newlines become escapes, so a description can neither fabricate a
// line nor end one early. (Codex review 3, finding 1.)
const fetchErrorMarker = "C3-STT-FETCH-ERROR-v1 "

// sttFetchFailureMarker carries the handler's actual fetch error upward verbatim.
func sttFetchFailureMarker(detail string) string {
	return FetchFailedPrefix + detail + "]"
}

// fetchNonceEnvVar carries this invocation's shared secret to the handler.
const fetchNonceEnvVar = "C3_STT_FETCH_NONCE"

// fetchReport is the handler's structured fetch-error payload.
type fetchReport struct {
	Nonce string `json:"nonce"`
	Cause string `json:"cause"`
}

// newFetchNonce mints the per-invocation secret that gives a fetch report its
// PROVENANCE. Crypto-random and never reused, so it cannot be predicted by
// anything that produced its bytes before this run started.
func newFetchNonce() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// fetchFailureDetail returns the handler's reported fetch cause from its stderr,
// or "" when the failure was not an authenticated fetch failure. It scans the
// WHOLE stderr (not the logged tail), matches ONLY whole lines BEGINNING with
// the marker, and accepts a line only when its nonce is THIS run's.
//
// The nonce is the load-bearing check, and structure alone was not enough:
// stderr is a SHARED stream. On the handler's stderr logging fallback a raw
// server description is written unencoded, and provider stderr — which can embed
// a server-controlled HTTP body — is copied through it. Either can contain a
// newline followed by a perfectly well-formed marker line, and the later line
// would win. A server can author a marker; it cannot author a secret it has
// never seen (C3 review 4, R1).
//
// The last authenticated line wins: the handler emits at most one per run, and a
// later report is the more recent truth.
func fetchFailureDetail(stderr, nonce string) string {
	if nonce == "" {
		return "" // no secret to check against: nothing here can be authenticated
	}
	out := ""
	for _, line := range strings.Split(stderr, "\n") {
		rest, ok := strings.CutPrefix(line, fetchErrorMarker)
		if !ok {
			continue // not a marker line: anchored at line start, never mid-line
		}
		var rep fetchReport
		if err := json.Unmarshal([]byte(strings.TrimRight(rest, "\r")), &rep); err != nil {
			continue // not our encoding; refuse to guess at its meaning
		}
		if rep.Nonce != nonce {
			continue // well-formed but unauthenticated — someone else's words
		}
		if cause := strings.TrimSpace(rep.Cause); cause != "" {
			out = cause
		}
	}
	return out
}

// sttLogHintPath returns the broker log path to surface in the failure
// marker. Mirrors broker.LogPath() but lives here to avoid the
// plugin->broker import cycle. Falls back to the documented default if
// $HOME isn't readable (unlikely; if it happens we'd rather show *a*
// path than an empty string).
func sttLogHintPath() string {
	if env := os.Getenv("C3_LOG_FILE"); env != "" {
		return env
	}
	state := os.Getenv("XDG_STATE_HOME")
	if state == "" {
		home, err := os.UserHomeDir()
		if err != nil || home == "" {
			return "~/.local/state/c3/broker.log"
		}
		state = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(state, "c3", "broker.log")
}

func runHandler(ctx context.Context, host plugin.Host, cfg Config, token, apiBaseURL string, apiBaseAnswered bool, p c3types.VoicePayload) (string, error) {
	// argv: <chat_id> <msg_id> <file_id> [<thread_id>]
	// token is fed via stdin (see package doc).
	args := []string{
		cfg.HandlerPath,
		strconv.FormatInt(p.ChatID, 10),
		strconv.FormatInt(p.MessageID, 10),
		p.FileID,
	}
	if p.TopicID != nil {
		args = append(args, strconv.FormatInt(*p.TopicID, 10))
	} else {
		args = append(args, "")
	}

	timeout := time.Duration(cfg.Timeout) * time.Second
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	cmd := exec.CommandContext(tctx, pythonExe(cfg), args...)
	cmd.Stdin = strings.NewReader(token + "\n")
	// Route the handler's getFile + voice-file download through the same
	// Bot-API base the broker uses (the reverse proxy, when configured).
	// Direct api.telegram.org is IP-blocked in some networks (e.g. India),
	// which times out the download even with the proxy live. Empty =>
	// handler defaults to api.telegram.org.
	// Rolling-window audio retention: the handler keeps the newest N .oga in its
	// inbox and prunes older ones after each transcription (recent audio stays
	// available for retranscribe/testing). N is passed from config.
	// One fresh secret per invocation. If it cannot be minted we run WITHOUT one,
	// which makes every fetch report unauthenticated and therefore ignored: a
	// generic transcription failure is the honest degradation, never an
	// unauthenticated cause dressed up as the server's word.
	nonce, nerr := newFetchNonce()
	if nerr != nil {
		host.Logf("stt: msg=%d could not mint a fetch-report nonce (%v); fetch causes will not be authenticated this run", p.MessageID, nerr)
	}
	cmd.Env = handlerEnv(apiBaseURL, apiBaseAnswered, nonce, cfg.AudioRetention)

	// I-7: kill the whole process group on the ctx deadline, not just the direct
	// child. Setpgid makes stt-handler.py the leader of its OWN process group, so
	// the custom Cancel can SIGKILL the negative PID — reaping the Python
	// grandchild (subprocess.run stt.py), ffprobe, and any in-flight provider HTTP
	// together with the handler. Without this, exec.CommandContext's default
	// cancel (Process.Kill → SIGKILL to the direct child only) orphans those
	// grandchildren to init, wasting paid API spend after the deadline.
	// (Precedent: internal/spawn sets Setsid for an analogous detach reason.)
	cmd.SysProcAttr = sttSysProcAttr()
	cmd.Cancel = func() error {
		// exec only invokes Cancel after a successful Start, so Process is
		// non-nil here; guard anyway to match defensive conventions. On unix
		// sttKillTree signals the whole process group started by Setpgid; on
		// Windows it best-effort kills the direct child (see kill_*.go).
		return sttKillTree(cmd)
	}
	// I-7 backstop: the deadline only TRIGGERS the kill — it can't unblock a
	// cmd.Wait() that's stuck on an inherited stdout pipe held open by a surviving
	// child. WaitDelay bounds that: after Cancel fires, Wait force-closes the I/O
	// and returns within WaitDelay instead of hanging the serial worker. Today the
	// grandchild uses capture_output (no inherited pipe) so this is latent, but it
	// keeps the 300s/330s deadlines real if a future edit changes that.
	cmd.WaitDelay = 5 * time.Second

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	elapsed := time.Since(start)

	if err != nil {
		// Distinguish timeout from other errors — the caller (and the user)
		// benefit from knowing which.
		reason := "error"
		if tctx.Err() == context.DeadlineExceeded {
			reason = "timeout"
		} else if strings.Contains(err.Error(), "signal: killed") {
			reason = "killed"
		}
		// stderr tail (last 240 chars) helps diagnose without dumping the
		// full provider output. Audio bytes etc. never appear here.
		serr := bytes.TrimSpace(stderr.Bytes())
		if len(serr) > 240 {
			serr = serr[len(serr)-240:]
		}
		host.Logf("stt: msg=%d %s after %v (timeout=%v, file_size=%d): %v | stderr-tail=%q",
			p.MessageID, reason, elapsed.Round(time.Millisecond), timeout,
			p.Size, err, string(serr))
		// A FETCH failure is not a transcription failure. Reducing it to
		// "[STT FAILED: error]" is what let the server's specific answer reach
		// the agent as a generic "transcription failed / try again", so the
		// cause travels upward intact instead.
		if detail := fetchFailureDetail(stderr.String(), nonce); detail != "" {
			host.Logf("stt: msg=%d fetch failed before transcription: %s", p.MessageID, detail)
			return sttFetchFailureMarker(detail), nil
		}
		return sttFailureMarker(reason), nil
	}

	transcript := string(bytes.TrimSpace(stdout.Bytes()))
	if transcript == "" {
		host.Logf("stt: msg=%d empty transcript after %v (no provider returned text)",
			p.MessageID, elapsed.Round(time.Millisecond))
		return sttFailureMarker("empty"), nil
	}
	host.Logf("stt: msg=%d transcribed in %v (chars=%d)",
		p.MessageID, elapsed.Round(time.Millisecond), len(transcript))
	return transcript, nil
}

// pythonExe resolves the interpreter the STT handler runs under:
//  1. cfg.Python (mappings.json:plugins.stt.python), if set;
//  2. bare "python3" (PATH).
//
// STT needs only the standard library now (the Sarvam batch path was ported to
// native urllib), so the system python3 is sufficient — no venv required.
func pythonExe(cfg Config) string {
	if cfg.Python != "" {
		return cfg.Python
	}
	return "python3"
}

// apiBaseURLer is the OPTIONAL channel capability that reports which Bot-API
// host the channel is talking to RIGHT NOW.
type apiBaseURLer interface{ APIBaseURL() string }

// readTelegramConn pulls the bot token and the Bot-API base URL the handler must
// use. The plugin doesn't store its own copy.
//
// The base comes from the LIVE channel when it can answer, not from
// mappings.json. The handler performs its own getFile, and the broker has just
// performed one of its own during preflight; reading the static primary
// api_base_url made those two calls able to address different hosts — it ignores
// C3_TELEGRAM_API_URL, ignores the api_base_urls failover list, and ignores any
// failover advance the poll loop has already made. A preflight that succeeded on
// the active endpoint would then be followed by a fetch against a stale one, and
// the resulting failure looked like a transcription problem (Codex review 2,
// finding 1). The channel has already folded env + failover into its answer, so
// its answer is the only correct one; "" legitimately means api.telegram.org.
//
// The mappings/env path stays as the fallback for a channel that cannot answer
// (a transport without the accessor, or a unit harness with no channel wired).
// answered reports whether the LIVE channel gave an authoritative endpoint. It
// is separate from the value because "" is a real answer — api.telegram.org —
// and must not be mistaken for "nothing to say". Everything downstream has to
// honor that distinction or the answer dies at the first boundary that treats
// empty as unset (Codex review 3, finding 3).
func readTelegramConn(host plugin.Host) (token, apiBaseURL string, answered bool, err error) {
	var cc struct {
		BotToken   string `json:"bot_token"`
		APIBaseURL string `json:"api_base_url"`
	}
	if err := host.ChannelConfig("telegram", &cc); err != nil {
		return "", "", false, fmt.Errorf("stt: read telegram channel config: %w", err)
	}
	if cc.BotToken == "" {
		return "", "", false, fmt.Errorf("stt: bot_token is empty in mappings.json:channels.telegram")
	}
	if ch, cerr := host.Channel("telegram"); cerr == nil {
		if live, ok := ch.(apiBaseURLer); ok {
			return cc.BotToken, live.APIBaseURL(), true, nil
		}
	}
	base := os.Getenv("C3_TELEGRAM_API_URL")
	if base == "" {
		base = cc.APIBaseURL
	}
	return cc.BotToken, base, false, nil
}

// handlerEnv builds the subprocess environment.
//
// When the channel ANSWERED, its answer is FORCED onto the child: the inherited
// C3_TELEGRAM_API_URL is stripped first, and re-set only for a non-empty base.
// An answer of "" therefore means "use api.telegram.org" and the variable is
// absent, which is the only way Python reads a default — setting it to the empty
// string would build "/bot<token>/getFile" against no host at all. Without the
// strip, a broker started with C3_TELEGRAM_API_URL pointing at a proxy kept
// sending the handler there even after the channel had failed over to the
// official endpoint and the preflight had succeeded on it.
//
// When the channel could NOT answer, the environment is inherited untouched —
// that is the pre-existing behavior for a transport with no live accessor.
func handlerEnv(base string, answered bool, nonce string, retention int) []string {
	env := os.Environ()
	// A base is installed whenever we HAVE one — whether the live channel gave
	// it or readTelegramConn resolved it from env/mappings. Gating the install on
	// `answered` dropped the configured api_base_url for any transport without a
	// live accessor, silently sending the handler to api.telegram.org instead of
	// the operator's proxy (C3 review 4, R2).
	//
	// The strip is what makes an ANSWER of "" mean api.telegram.org: Python reads
	// its default only when the variable is ABSENT, and it also keeps a stale
	// inherited value from sitting beside the new one where libc, not us, picks
	// the winner. No base and no answer ⇒ inherit untouched.
	if answered || base != "" {
		kept := env[:0]
		for _, kv := range env {
			if !strings.HasPrefix(kv, apiURLEnvVar+"=") {
				kept = append(kept, kv)
			}
		}
		env = kept
		if base != "" {
			env = append(env, apiURLEnvVar+"="+base)
		}
	}
	return append(env,
		fetchNonceEnvVar+"="+nonce,
		"STT_AUDIO_RETENTION="+strconv.Itoa(retention))
}

// apiURLEnvVar is the variable stt-handler.py reads its Bot-API base from.
const apiURLEnvVar = "C3_TELEGRAM_API_URL"

// ensureSTTDefaultDirs creates the default handler-side log and inbox
// directories at broker startup. The Python handler also mkdir's these
// at import time; this is belt-and-suspenders so a fresh install can't
// surface the cryptic FileNotFoundError->[STT FAILED: error] failure
// mode if the user is somehow running an older handler bundle.
//
// Failures are logged and swallowed — broker startup must never be
// blocked by an inability to pre-create a plugin scratch dir.
func ensureSTTDefaultDirs(host plugin.Host) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		host.Logf("stt: skipping default-dir precreate (no home dir): %v", err)
		return
	}
	for _, d := range []string{
		filepath.Join(home, ".claude", "channels", "telegram", "inbox"),
		filepath.Join(home, ".claude", "channels", "telegram"), // for stt-handler.log
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			host.Logf("stt: failed to pre-create %s: %v (handler will retry at import)", d, err)
		}
	}
}

const sttHandlerRelativePath = "plugins/c3/stt/stt-handler.py"

var sttExecutablePath = os.Executable

// defaultHandlerPath finds the bundled handler when mappings.json did not
// explicitly name one. Claude Code's plugin root is authoritative when it
// contains the handler. Other hosts do not receive that environment, so source
// installs fall back to C3_SRC_DIR, the bundle next to a release binary, the
// documented local-marketplace checkout, and then ~/src/c3. install-desktop
// records the resolved path in mappings.json, which is preferred by Register
// and therefore remains stable for Desktop, Antigravity, Grok, and systemd
// brokers regardless of their working directory.
func defaultHandlerPath() string {
	if root := strings.TrimSpace(os.Getenv("CLAUDE_PLUGIN_ROOT")); root != "" {
		if path := existingHandlerPath(filepath.Join(root, "stt", "stt-handler.py")); path != "" {
			return path
		}
	}
	if root := strings.TrimSpace(os.Getenv("C3_SRC_DIR")); root != "" {
		if path := existingHandlerPath(filepath.Join(root, sttHandlerRelativePath)); path != "" {
			return path
		}
	}
	if exe, err := sttExecutablePath(); err == nil {
		if resolved, rerr := filepath.EvalSymlinks(exe); rerr == nil {
			exe = resolved
		}
		if path := existingHandlerPath(filepath.Join(filepath.Dir(exe), sttHandlerRelativePath)); path != "" {
			return path
		}
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		if path := existingHandlerPath(filepath.Join(home, ".local", "share", "c3", sttHandlerRelativePath)); path != "" {
			return path
		}
		return existingHandlerPath(filepath.Join(home, "src", "c3", sttHandlerRelativePath))
	}
	return ""
}

func existingHandlerPath(path string) string {
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		return path
	}
	return ""
}
