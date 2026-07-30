//go:build !windows

// codex is the C3 Codex launcher. It wraps interactive Codex sessions in a
// local app-server so the C3 MCP adapter can forward Telegram inbound messages
// into the visible Codex TUI. Unix-only; the Windows build gets a stub in
// main_windows.go (the launcher manages a /tmp app-server keyed on the unix uid).
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
)

const defaultWSURL = "ws://127.0.0.1:8766"

const tuiTerminationGrace = 2 * time.Second

var codexSubcommands = map[string]bool{
	"exec":        true,
	"e":           true,
	"review":      true,
	"login":       true,
	"logout":      true,
	"mcp":         true,
	"plugin":      true,
	"mcp-server":  true,
	"app-server":  true,
	"completion":  true,
	"update":      true,
	"sandbox":     true,
	"debug":       true,
	"apply":       true,
	"a":           true,
	"cloud":       true,
	"exec-server": true,
	"features":    true,
	"help":        true,
}

func main() {
	if err := run(os.Args[1:], os.Args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "c3 codex launcher: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, self string) error {
	realCodex, err := findRealCodex(self)
	if err != nil {
		return err
	}
	if shouldBypass(args) {
		return execReal(realCodex, args, os.Environ())
	}
	signals := make(chan os.Signal, 3)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(signals)

	adapterPath, err := findAdapter(self)
	if err != nil {
		return err
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	sharedRoot := os.Getenv("C3_CODEX_SHARED_ROOT")
	topic := inferTopicName(cwd, sharedRoot)
	if override, ok := os.LookupEnv("C3_ATTACH_NAME"); ok {
		topic = override
	}
	if override, ok := os.LookupEnv("C3_CODEX_TOPIC"); ok {
		topic = override
	}

	requestedWS := os.Getenv("C3_CODEX_APP_SERVER_WS")
	if requestedWS == "" {
		requestedWS = defaultWSURL
	}
	appServer, err := startAppServerTrackedWithSignals(
		realCodex, adapterPath, requestedWS, cwd, topic, signals,
	)
	if err != nil {
		return err
	}
	defer appServer.Stop()
	wsURL := appServer.URL

	argv := []string{realCodex}
	argv = append(argv, requiredFeatureArgs(args)...)
	argv = append(argv, mcpConfigArgs(adapterPath, wsURL, cwd, topic)...)
	argv = append(argv, "--remote", wsURL)
	if !hasCWDArg(args) {
		argv = append(argv, "-C", cwd)
	}
	argv = append(argv, args...)
	env := os.Environ()
	env = append(env, "C3_CODEX_APP_SERVER_WS="+wsURL, "C3_CODEX_REMOTE_BRIDGE=1", "C3_CODEX_CWD="+cwd)
	if topic != "" {
		env = append(env, "C3_ATTACH_NAME="+topic)
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = env
	return runTUI(cmd, signals)
}

func runTUI(cmd *exec.Cmd, signals <-chan os.Signal) error {
	if err := cmd.Start(); err != nil {
		return err
	}
	exited := make(chan error, 1)
	go func() {
		exited <- cmd.Wait()
	}()

	var terminationTimer *time.Timer
	var terminationDeadline <-chan time.Time
	defer func() {
		if terminationTimer != nil {
			terminationTimer.Stop()
		}
	}()

	for {
		select {
		case err := <-exited:
			return err
		case <-terminationDeadline:
			killErr := cmd.Process.Kill()
			waitErr := <-exited
			if killErr != nil &&
				!errors.Is(killErr, os.ErrProcessDone) &&
				!errors.Is(killErr, syscall.ESRCH) {
				return fmt.Errorf("kill unresponsive Codex TUI: %w", killErr)
			}
			return waitErr
		case sig := <-signals:
			if err := cmd.Process.Signal(sig); err != nil &&
				!errors.Is(err, os.ErrProcessDone) &&
				!errors.Is(err, syscall.ESRCH) {
				_ = cmd.Process.Kill()
				<-exited
				return fmt.Errorf("forward %s to Codex TUI: %w", sig, err)
			}
			if terminationTimer == nil &&
				(sig == syscall.SIGTERM || sig == syscall.SIGHUP) {
				terminationTimer = time.NewTimer(tuiTerminationGrace)
				terminationDeadline = terminationTimer.C
			}
		}
	}
}

func shouldBypass(args []string) bool {
	if os.Getenv("C3_CODEX_DISABLE") == "1" {
		return true
	}
	for _, arg := range args {
		switch arg {
		case "-h", "--help", "-V", "--version", "--remote":
			return true
		}
	}
	first := firstNonOption(args)
	return codexSubcommands[first]
}

func firstNonOption(args []string) string {
	optionsWithValues := map[string]bool{
		"-c": true, "--config": true, "--enable": true, "--disable": true,
		"--remote": true, "--remote-auth-token-env": true, "-i": true,
		"--image": true, "-m": true, "--model": true, "-p": true,
		"--profile": true, "-s": true, "--sandbox": true, "-C": true,
		"--cd": true, "--add-dir": true, "--ask-for-approval": true,
	}
	skip := false
	for _, arg := range args {
		if skip {
			skip = false
			continue
		}
		if arg == "--" {
			return ""
		}
		if optionsWithValues[arg] {
			skip = true
			continue
		}
		if strings.HasPrefix(arg, "-") {
			continue
		}
		return arg
	}
	return ""
}

func inferTopicName(cwd, sharedRoot string) string {
	cwdAbs, _ := filepath.Abs(cwd)
	sharedAbs, _ := filepath.Abs(sharedRoot)
	for dir := cwdAbs; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "CLAUDE.md")); err == nil {
			if samePath(dir, sharedAbs) {
				return ""
			}
			return filepath.Base(dir)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}
	if underPath(cwdAbs, sharedAbs) {
		return ""
	}
	return filepath.Base(cwdAbs)
}

func samePath(a, b string) bool {
	aa, _ := filepath.Abs(a)
	bb, _ := filepath.Abs(b)
	return aa == bb
}

func underPath(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func mcpConfigArgs(adapterPath, wsURL, cwd, topic string) []string {
	args := []string{
		"-c", "mcp_servers.c3_codex.command=" + tomlString(adapterPath),
		"-c", "mcp_servers.c3_codex.args=[]",
		"-c", "mcp_servers.c3_codex.env.C3_CODEX_APP_SERVER_WS=" + tomlString(wsURL),
		"-c", "mcp_servers.c3_codex.env.C3_CODEX_CWD=" + tomlString(cwd),
		"-c", `mcp_servers.c3_codex.env.C3_CODEX_REMOTE_BRIDGE="1"`,
		"-c", "mcp_servers.c3_codex.enabled=true",
	}
	if topic != "" {
		args = append(args, "-c", "mcp_servers.c3_codex.env.C3_ATTACH_NAME="+tomlString(topic))
	}
	return args
}

func tomlString(s string) string {
	data, _ := json.Marshal(s)
	return string(data)
}

// chooseAppServerURL picks a ws:// URL for this launch. The normal default is
// an OS-assigned ephemeral loopback port; an explicit override keeps its
// existing busy-port behavior. A busy port is never adopted — see ensureAppServer.
func chooseAppServerURL(requestedWSURL string) string {
	// The normal launcher path deliberately does not reuse its fixed default
	// port. Ask the kernel for an ephemeral port while the interprocess launch
	// lock is held instead. That removes the predictable collision point shared
	// by every Codex session. An explicit override remains an override.
	if requestedWSURL == defaultWSURL {
		if wsURL, err := ephemeralWSURL("127.0.0.1"); err == nil {
			return wsURL
		}
		return requestedWSURL
	}
	if requestedWSURL != defaultWSURL && !strings.HasPrefix(requestedWSURL, "ws://127.0.0.1:") {
		return requestedWSURL
	}
	host, port, err := parseWSURL(requestedWSURL)
	if err != nil {
		return requestedWSURL
	}
	if !tcpReachable(host, port, 500*time.Millisecond) {
		return requestedWSURL
	}
	for candidatePort := port + 1; candidatePort < port+50; candidatePort++ {
		if !tcpReachable(host, candidatePort, 500*time.Millisecond) {
			return "ws://" + host + ":" + strconv.Itoa(candidatePort)
		}
	}
	return requestedWSURL
}

// ephemeralWSURL asks the OS to allocate a free TCP port. The caller holds the
// launcher flock through child readiness, so another C3 launcher cannot claim
// the released port between this probe and the app-server's bind.
func ephemeralWSURL(host string) (string, error) {
	listener, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return "", err
	}
	defer listener.Close()
	_, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return "", err
	}
	return "ws://" + net.JoinHostPort(host, portText), nil
}

func parseWSURL(wsURL string) (string, int, error) {
	if !strings.HasPrefix(wsURL, "ws://") {
		return "", 0, fmt.Errorf("only ws:// URLs are supported: %s", wsURL)
	}
	hostPort := strings.TrimPrefix(wsURL, "ws://")
	if slash := strings.IndexByte(hostPort, '/'); slash >= 0 {
		hostPort = hostPort[:slash]
	}
	host, portText, err := net.SplitHostPort(hostPort)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portText)
	return host, port, err
}

func tcpReachable(host string, port int, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

type appServerHandle struct {
	URL  string
	PID  int
	done <-chan struct{}
}

func (h *appServerHandle) Stop() {
	if h == nil || h.PID <= 0 {
		return
	}
	_ = killAppServerProcessGroup(h.PID)
	if h.done != nil {
		<-h.done
	}
	_, port, err := parseWSURL(h.URL)
	if err == nil {
		_ = os.Remove(appServerMetaPath(port))
	}
}

func ensureAppServer(realCodex, adapterPath, wsURL, cwd, topic string) error {
	_, err := launchAppServer(realCodex, adapterPath, wsURL, cwd, topic)
	return err
}

func launchAppServer(realCodex, adapterPath, wsURL, cwd, topic string) (*appServerHandle, error) {
	return launchAppServerWithSignals(realCodex, adapterPath, wsURL, cwd, topic, nil)
}

func launchAppServerWithSignals(
	realCodex, adapterPath, wsURL, cwd, topic string,
	signals <-chan os.Signal,
) (*appServerHandle, error) {
	select {
	case sig := <-signals:
		return nil, fmt.Errorf("received %s while starting Codex app-server", sig)
	default:
	}

	host, port, err := parseWSURL(wsURL)
	if err != nil {
		return nil, err
	}
	if tcpReachable(host, port, 500*time.Millisecond) {
		// NEVER ADOPT. Something is listening, and reachability proves only that
		// a socket exists — not whose it is.
		//
		// This used to adopt on a matching (cwd, topic, adapter) record. That is
		// a DESCRIPTION, not an identity: two launches made the same way produce
		// the same description forever, so it can only ever answer "were these
		// started alike?", never "are these the same session?". Refusing when the
		// description was UNKNOWN (empty topic) stopped the launches from the
		// shared root that caused the two-topics-in-one-TUI incident, but two
		// launches from any project SUBDIRECTORY still both infer the project's
		// name, still match field for field, and still merged.
		//
		// The fix is not a better description. Adoption bought nothing worth
		// keeping: Codex threads live in their own rollout files and `codex
		// resume` reloads them onto any app-server, so an app-server holds no
		// state worth reclaiming. Session continuity belongs at the broker, keyed
		// on Codex's own thread id. So a busy port is now always refused, and the
		// record below is diagnostics only.
		//
		// chooseAppServerURL should already have moved us to a free port, so
		// reaching here means a race with another launcher or a foreign process.
		// A failed launch is recoverable; cross-talk is not.
		return nil, fmt.Errorf(
			"%s is already in use%s. Refusing to share an app-server — messages meant for "+
				"another session would surface in this one. Retry, or set "+
				"C3_CODEX_APP_SERVER_WS to a free ws://127.0.0.1:<port>",
			wsURL, appServerPortOwner(wsURL))
	}
	argv := []string{realCodex}
	argv = append(argv, requiredFeatureArgs(nil)...)
	argv = append(argv, mcpConfigArgs(adapterPath, wsURL, cwd, topic)...)
	argv = append(argv, "app-server", "--listen", wsURL)
	logFile, err := os.OpenFile(appServerLogPath(port), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil || logFile == nil {
		// Worth a line rather than a swallowed `_`: the fallback splices the
		// app-server's stdout+stderr into THIS terminal, which the Codex TUI is
		// about to take over. A garbled TUI with no explanation is a mystery
		// bug report.
		fmt.Fprintf(os.Stderr, "c3: app-server log unavailable (%v); its output will mix into this terminal\n", err)
		logFile = os.Stderr
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	// An app-server can itself start helpers. Give this launch its own process
	// group so a failed startup can reap the whole tree rather than leaving a
	// helper alive after its direct parent is gone.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdin = nil
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	// MY child, or nobody — the never-adopt rule applied to the startup window.
	//
	// chooseAppServerURL picks a free port by PROBING it, which reserves nothing.
	// Between that probe and this child binding, another launcher can probe the
	// same port and win the bind. Its listener is reachable from here, so
	// accepting "the port is reachable" as "my child is up" would put two
	// sessions on one app-server — the exact cross-talk the refusal above exists
	// to prevent, walked in through the startup window instead of the front door.
	//
	// A child that loses the bind race exits, so watching for its exit is what
	// distinguishes "my listener" from "someone else's". Residual window, stated
	// honestly rather than papered over: if the winner binds while my child is
	// alive but has not yet failed its own bind, the reachability check below can
	// still accept the winner's listener. Closing that completely needs either a
	// startup nonce in the app-server (upstream, not ours) or per-socket owner
	// lookup (not portable). Losing the race is what is handled here; the
	// sub-millisecond overlap is not, and the caller's retry does not know about it.
	exited := make(chan struct{})
	go func() { _ = cmd.Wait(); close(exited) }()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-exited:
			// The direct child may have spawned helpers into its process group
			// before exiting. Reap the whole group on both child-exit branches.
			_ = killAppServerProcessGroup(cmd.Process.Pid)
			// The child is gone. If the port is nonetheless live, someone else
			// owns it — do NOT adopt; tell the caller to pick another port.
			if tcpReachable(host, port, 500*time.Millisecond) {
				return nil, fmt.Errorf("%w: %s is held by another app-server%s",
					errAppServerLostPortRace, wsURL, appServerPortOwner(wsURL))
			}
			return nil, fmt.Errorf("codex app-server for %s exited during startup — see %s",
				wsURL, appServerLogPath(port))
		default:
		}
		if tcpReachable(host, port, 500*time.Millisecond) {
			writeAppServerMeta(wsURL, cwd, topic, adapterPath, cmd.Process.Pid)
			return &appServerHandle{URL: wsURL, PID: cmd.Process.Pid, done: exited}, nil
		}
		select {
		case sig := <-signals:
			_ = killAppServerProcessGroup(cmd.Process.Pid)
			<-exited
			return nil, fmt.Errorf("received %s while starting Codex app-server", sig)
		case <-time.After(200 * time.Millisecond):
		}
	}
	_ = killAppServerProcessGroup(cmd.Process.Pid)
	<-exited
	return nil, fmt.Errorf("codex app-server did not become reachable at %s", wsURL)
}

func killAppServerProcessGroup(pid int) error {
	if pid <= 0 {
		return nil
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
		return err
	}
	return nil
}

// errAppServerLostPortRace means another launcher won the port between our
// free-port probe and our child's bind. It is retryable — on the NEXT port, never
// by adopting the winner — and startAppServer is what retries it.
var errAppServerLostPortRace = errors.New("app-server port lost to a concurrent launcher")

// startAppServer picks a port and launches an app-server on it, retrying on a
// fresh port when it loses a startup race. Each retry re-runs chooseAppServerURL,
// which obtains another kernel-assigned port on the normal path (or walks past a
// busy explicit override), so a retry never adopts the winner. Returns the URL
// actually used, which is what the Codex TUI must be pointed at.
func startAppServer(realCodex, adapterPath, requestedWS, cwd, topic string) (string, error) {
	h, err := startAppServerTracked(realCodex, adapterPath, requestedWS, cwd, topic)
	if err != nil {
		return "", err
	}
	return h.URL, nil
}

func startAppServerTracked(realCodex, adapterPath, requestedWS, cwd, topic string) (*appServerHandle, error) {
	return startAppServerTrackedWithSignals(realCodex, adapterPath, requestedWS, cwd, topic, nil)
}

func startAppServerTrackedWithSignals(
	realCodex, adapterPath, requestedWS, cwd, topic string,
	signals <-chan os.Signal,
) (*appServerHandle, error) {
	lock, err := acquireAppServerLaunchLock()
	if err != nil {
		return nil, err
	}
	defer lock.Release()

	const attempts = 5
	for i := 0; i < attempts; i++ {
		wsURL := chooseAppServerURL(requestedWS)
		var handle *appServerHandle
		if handle, err = launchAppServerWithSignals(
			realCodex, adapterPath, wsURL, cwd, topic, signals,
		); err == nil {
			return handle, nil
		}
		if !errors.Is(err, errAppServerLostPortRace) {
			return nil, err
		}
		fmt.Fprintf(os.Stderr, "c3: %v — trying another port\n", err)
	}
	return nil, err
}

// appServerLaunchLock is the per-user interprocess guard for the window from
// port selection through child readiness. Its file shares the broker's
// validated runtime directory, rather than /tmp, so a second user cannot
// interfere with this user's launcher startup.
type appServerLaunchLock struct {
	file *os.File
}

var beforeAppServerLaunchFlock func()

func acquireAppServerLaunchLock() (*appServerLaunchLock, error) {
	pidFile, err := broker.PidFilePath()
	if err != nil {
		return nil, fmt.Errorf("resolve C3 runtime directory for Codex launcher lock: %w", err)
	}
	path := filepath.Join(filepath.Dir(pidFile), "c3-codex-launcher.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open Codex launcher lock %s: %w", path, err)
	}
	if beforeAppServerLaunchFlock != nil {
		beforeAppServerLaunchFlock()
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("flock Codex launcher lock %s: %w", path, err)
	}
	return &appServerLaunchLock{file: file}, nil
}

func (l *appServerLaunchLock) Release() {
	if l == nil || l.file == nil {
		return
	}
	_ = syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	_ = l.file.Close()
	l.file = nil
}

func requiredFeatureArgs(existing []string) []string {
	if hasFeatureArg(existing, "goals") {
		return nil
	}
	return []string{"--enable", "goals"}
}

func hasFeatureArg(args []string, feature string) bool {
	for i, arg := range args {
		if arg == "--enable" && i+1 < len(args) && args[i+1] == feature {
			return true
		}
		if arg == "--enable="+feature {
			return true
		}
	}
	return false
}

func hasCWDArg(args []string) bool {
	for _, arg := range args {
		if arg == "-C" || arg == "--cd" {
			return true
		}
	}
	return false
}

// appServerMetaPath is per-UID **and per-port**. It used to be one file per
// user, which meant a second app-server on a different port silently overwrote
// the first one's record — after which the first was unrecognisable and a third
// launch could not tell whose it was. One record per listener, or the record
// describes only whichever listener started most recently.
func appServerMetaPath(port int) string {
	return fmt.Sprintf("/tmp/c3-codex-app-server-%d-%d.json", os.Getuid(), port)
}

// appServerLogPath is per-UID and per-port for the same reason
// appServerMetaPath is: N app-servers run at once (chooseAppServerURL walks to
// the next free port and ensureAppServer refuses to share a busy one), and one
// shared log cannot say which of them wrote a line — the exact question a
// cross-talk incident needs answered. The UID component also stops user A's
// 0600 file from making user B's open fail, which would splice B's app-server
// output into B's terminal.
func appServerLogPath(port int) string {
	return fmt.Sprintf("/tmp/c3-codex-app-server-%d-%d.log", os.Getuid(), port)
}

// processAlive reports whether pid is a live process we own. Signal 0 performs
// the permission and existence checks without delivering anything.
func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

// appServerPortOwner describes, for a human reading a refusal, what we know
// about whoever holds wsURL's port. It returns a leading-space clause to splice
// into an error message, or "" when we know nothing useful.
//
// This is DIAGNOSTICS ONLY and must never gate a decision. The record it reads
// used to answer "may I adopt this app-server?" via a field-by-field comparison
// of (cwd, topic, adapter) — a description rather than an identity, which two
// sessions launched the same way share forever. That comparison put two Telegram
// topics into one Codex TUI. Nothing branches on this function's result; it only
// makes the refusal actionable by naming the process to look at.
func appServerPortOwner(wsURL string) string {
	_, port, err := parseWSURL(wsURL)
	if err != nil {
		return ""
	}
	data, err := os.ReadFile(appServerMetaPath(port))
	if err != nil {
		return ""
	}
	var meta struct {
		PID       int               `json:"pid"`
		Signature map[string]string `json:"signature"`
	}
	if err := json.Unmarshal(data, &meta); err != nil || meta.PID <= 0 {
		return ""
	}
	if !processAlive(meta.PID) {
		return fmt.Sprintf(" (a C3 app-server recorded on that port, pid %d, is no longer running —"+
			" something else has taken the port)", meta.PID)
	}
	if cwd := meta.Signature["cwd"]; cwd != "" {
		return fmt.Sprintf(" by a C3 app-server (pid %d, launched from %s)", meta.PID, cwd)
	}
	return fmt.Sprintf(" by a C3 app-server (pid %d)", meta.PID)
}

func writeAppServerMeta(wsURL, cwd, topic, adapterPath string, pid int) {
	_, port, err := parseWSURL(wsURL)
	if err != nil {
		return
	}
	data := map[string]any{
		"ws_url": wsURL,
		"pid":    pid,
		"signature": map[string]string{
			"cwd":     cwd,
			"topic":   topic,
			"adapter": adapterPath,
		},
	}
	encoded, _ := json.MarshalIndent(data, "", "  ")
	_ = os.WriteFile(appServerMetaPath(port), append(encoded, '\n'), 0o600)
}

func findRealCodex(self string) (string, error) {
	if explicit := os.Getenv("C3_CODEX_REAL"); explicit != "" {
		return explicit, nil
	}
	selfAbs, _ := filepath.EvalSymlinks(self)
	pathParts := filepath.SplitList(os.Getenv("PATH"))
	for _, dir := range pathParts {
		candidate := filepath.Join(dir, "codex")
		info, err := os.Stat(candidate)
		if err != nil || info.IsDir() || info.Mode()&0o111 == 0 {
			continue
		}
		resolved, _ := filepath.EvalSymlinks(candidate)
		if resolved == selfAbs {
			continue
		}
		return candidate, nil
	}
	home, _ := os.UserHomeDir()
	matches, _ := filepath.Glob(filepath.Join(home, ".nvm", "versions", "node", "*", "lib", "node_modules", "@openai", "codex", "bin", "codex.js"))
	for i := len(matches) - 1; i >= 0; i-- {
		return matches[i], nil
	}
	return "", fmt.Errorf("could not find real codex; set C3_CODEX_REAL")
}

func findAdapter(self string) (string, error) {
	if explicit := os.Getenv("C3_CODEX_ADAPTER"); explicit != "" {
		return explicit, nil
	}
	if found, err := exec.LookPath("c3-codex-adapter"); err == nil {
		return found, nil
	}
	selfAbs, _ := filepath.Abs(self)
	sibling := filepath.Join(filepath.Dir(selfAbs), "c3-codex-adapter")
	if _, err := os.Stat(sibling); err == nil {
		return sibling, nil
	}
	return "", fmt.Errorf("could not find c3-codex-adapter in PATH")
}

func execReal(realCodex string, args []string, env []string) error {
	cmd := exec.Command(realCodex, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = env
	return cmd.Run()
}
