//go:build !windows

// codex is the C3 Codex launcher. It wraps interactive Codex sessions in a
// local app-server so the C3 MCP adapter can forward Telegram inbound messages
// into the visible Codex TUI. Unix-only; the Windows build gets a stub in
// main_windows.go (the launcher manages a /tmp app-server keyed on the unix uid).
package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const defaultWSURL = "ws://127.0.0.1:8766"

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
	wsURL := chooseAppServerURL(requestedWS)
	if err := ensureAppServer(realCodex, adapterPath, wsURL, cwd, topic); err != nil {
		return err
	}

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
	return cmd.Run()
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

// chooseAppServerURL picks a ws:// URL for this launch. A busy port is never
// adopted — see the never-adopt rationale on ensureAppServer — so the only
// question here is which port is free.
func chooseAppServerURL(requestedWSURL string) string {
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

func ensureAppServer(realCodex, adapterPath, wsURL, cwd, topic string) error {
	host, port, err := parseWSURL(wsURL)
	if err != nil {
		return err
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
		return fmt.Errorf(
			"%s is already in use%s. Refusing to share an app-server — messages meant for "+
				"another session would surface in this one. Retry, or set "+
				"C3_CODEX_APP_SERVER_WS to a free ws://127.0.0.1:<port>",
			wsURL, appServerPortOwner(wsURL))
	}
	argv := []string{realCodex}
	argv = append(argv, requiredFeatureArgs(nil)...)
	argv = append(argv, mcpConfigArgs(adapterPath, wsURL, cwd, topic)...)
	argv = append(argv, "app-server", "--listen", wsURL)
	logFile, _ := os.OpenFile("/tmp/c3-codex-app-server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if logFile == nil {
		logFile = os.Stderr
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stdin = nil
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		return err
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if tcpReachable(host, port, 500*time.Millisecond) {
			writeAppServerMeta(wsURL, cwd, topic, adapterPath, cmd.Process.Pid)
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	_ = cmd.Process.Kill()
	return fmt.Errorf("codex app-server did not become reachable at %s", wsURL)
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
