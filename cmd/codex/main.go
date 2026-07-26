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
	wsURL := chooseAppServerURL(requestedWS, cwd, topic, func(candidate string) bool {
		return appServerMetaMatches(candidate, cwd, topic, adapterPath)
	})
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

func chooseAppServerURL(requestedWSURL, cwd, topic string, metaMatches func(string) bool) string {
	if requestedWSURL != defaultWSURL && !strings.HasPrefix(requestedWSURL, "ws://127.0.0.1:") {
		return requestedWSURL
	}
	host, port, err := parseWSURL(requestedWSURL)
	if err != nil {
		return requestedWSURL
	}
	if !tcpReachable(host, port, 500*time.Millisecond) || metaMatches(requestedWSURL) {
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
		// Something is listening. Adopt it ONLY if our own record vouches for it.
		// Reachability alone proves a socket exists, not whose it is — and this
		// early return used to be unconditional, so any process holding the port
		// was adopted as our app-server and every inbound message went to it.
		if appServerMetaMatches(wsURL, cwd, topic, adapterPath) {
			return nil
		}
		// chooseAppServerURL should already have moved us to a free port, so
		// reaching here means either a race with another launcher or a foreign
		// process on the port. Refuse loudly. Silently sharing is how two topics
		// ended up in one TUI; a failed launch is recoverable, cross-talk is not.
		return fmt.Errorf(
			"%s is already in use by an app-server this session cannot claim as its own "+
				"(cwd=%q topic=%q). Refusing to share it — messages for another session would "+
				"surface in this one. Retry, or set C3_CODEX_APP_SERVER_WS to a free ws://127.0.0.1:<port>",
			wsURL, cwd, topic)
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

// appServerMetaMatches reports whether the app-server recorded for wsURL is
// THIS session's own — i.e. whether it is safe to adopt instead of starting a
// fresh one.
//
// **An unknown identity must never match another unknown identity.** That single
// rule is the whole point of this function. inferTopicName deliberately returns
// "" when the launch directory is the shared root rather than a project, so two
// sessions started from that root both carry topic "". Comparing them field by
// field made "" == "" and concluded they were the same session — so the second
// adopted the first's app-server, and Telegram messages for two different topics
// were both posted into one TUI. The broker had routed each to the correct
// adapter; the adapters then merged them here.
//
// Failing to SEPARATE costs an extra process. Failing to SHARE correctly sends
// one person's messages to another. Those are not symmetrical, so an unknown
// identity refuses to match and takes a fresh port.
func appServerMetaMatches(wsURL, cwd, topic, adapterPath string) bool {
	if topic == "" {
		return false // unknown identity — never adopt someone else's app-server
	}
	_, port, err := parseWSURL(wsURL)
	if err != nil {
		return false
	}
	data, err := os.ReadFile(appServerMetaPath(port))
	if err != nil {
		return false
	}
	var meta struct {
		WSURL     string            `json:"ws_url"`
		PID       int               `json:"pid"`
		Signature map[string]string `json:"signature"`
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return false
	}
	// A stale record whose process is gone must not vouch for whatever else has
	// since taken the port.
	if !processAlive(meta.PID) {
		return false
	}
	return meta.WSURL == wsURL &&
		meta.Signature["cwd"] == cwd &&
		meta.Signature["topic"] == topic &&
		meta.Signature["adapter"] == adapterPath
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
