package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/ipc"
)

const crossSessionWindow = 2 * time.Second

// Credentials belong to this MCP child's owning session. Never discover another
// inbox, read another process's environment, or include credentials in errors.
type crossSessionTransport struct {
	runtimeDir string
	socketPath string
	token      string
	hostPID    int
}

func (*crossSessionTransport) String() string {
	return "cross-session transport (credentials redacted)"
}

// Called once by run, not by reconnect or per-message delivery. Runtime path
// resolution is shared with the broker; the only host credentials read are these
// two inherited variables. Tests supply all three paths/values explicitly.
func startupCrossSessionTransport() (*crossSessionTransport, string) {
	path := os.Getenv("CLAUDE_CODE_MESSAGING_SOCKET")
	token := os.Getenv("CLAUDE_CODE_MESSAGING_TOKEN")
	if path == "" || token == "" {
		return nil, "cross-session credentials unavailable"
	}
	socket, err := broker.SocketPath()
	if err != nil {
		return nil, "cross-session runtime directory unavailable"
	}
	tx := &crossSessionTransport{runtimeDir: filepath.Dir(socket), socketPath: path, token: token, hostPID: owningClaudePID(os.Getppid(), platformProcReaders())}
	if _, err := tx.validatedSocket(); err != nil {
		return tx, err.Error() // keep captured config for attach/reconnect revalidation
	}
	return tx, ""
}

// The direct parent is eligible, or the nearest positively identified Claude
// ancestor when a wrapper sits between host and adapter. Never skip a Claude
// host to reach another session farther up the tree. Read argv only, never env.
func owningClaudePID(parent int, r procReaders) int {
	if parent <= 1 || r.cmdline == nil || r.ppid == nil {
		return 0
	}
	pid := parent
	seen := map[int]bool{}
	for depth := 0; depth < 40 && pid > 1; depth++ {
		if seen[pid] {
			return 0
		}
		seen[pid] = true
		args, ok := r.cmdline(pid)
		if !ok {
			return 0
		}
		if isNode(args) {
			args, ok = nodeScript(args)
			if !ok {
				return 0
			}
		}
		if isClaudeHost(args) {
			return pid
		}
		pid, ok = r.ppid(pid)
		if !ok {
			return 0
		}
	}
	// No identified host: only the immediate parent can own the inherited inbox.
	if pid <= 1 {
		return parent
	}
	return 0
}

func (t *crossSessionTransport) validatedSocket() (string, error) {
	if !crossSessionPeerPIDAvailable {
		return "", errors.New("cross-session peer PID verification unavailable on this platform")
	}
	if t == nil || t.token == "" || !filepath.IsAbs(t.socketPath) || !filepath.IsAbs(t.runtimeDir) {
		return "", errors.New("cross-session socket configuration invalid")
	}
	if t.hostPID <= 1 || filepath.Base(t.socketPath) != strconv.Itoa(t.hostPID)+".sock" {
		return "", errors.New("cross-session socket does not name the owning host")
	}
	root, err := filepath.EvalSymlinks(t.runtimeDir)
	if err != nil {
		return "", errors.New("cross-session runtime directory unavailable")
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 || !crossSessionOwned(info) {
		return "", errors.New("cross-session runtime directory is not private to this user")
	}
	info, err = os.Lstat(t.socketPath)
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		return "", errors.New("cross-session path is not a socket")
	}
	path, err := filepath.EvalSymlinks(t.socketPath)
	if err != nil {
		return "", errors.New("cross-session socket unavailable")
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", errors.New("cross-session socket outside user runtime directory")
	}
	if rel != filepath.Join("cc-socks", strconv.Itoa(t.hostPID)+".sock") {
		return "", errors.New("cross-session socket is not the owning inbox")
	}
	info, err = os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSocket == 0 || !crossSessionOwned(info) {
		return "", errors.New("cross-session path is not a user-owned socket")
	}
	return path, nil
}

// Send proves transport completion only. A clean close can also mean host
// refusal, so the caller MUST still require the exact transcript receipt.
func (t *crossSessionTransport) Send(ctx context.Context, content string, sessionID string) error {
	path, err := t.validatedSocket() // revalidate before every credential write
	if err != nil {
		return err
	}
	if len(content) > ipc.MaxFrameSize || len(t.token) > ipc.MaxFrameSize {
		return fmt.Errorf("cross-session outbound frame exceeds IPC cap (%d bytes)", ipc.MaxFrameSize)
	}
	user := map[string]any{"type": "user", "from": "c3", "priority": "next",
		"message": map[string]any{"role": "user", "content": content}}
	if sessionID != "" {
		user["session_id"] = sessionID
	}
	// Marshal and bound BOTH complete frames before connecting or disclosing auth.
	frames := make([][]byte, 0, 2)
	for _, frame := range []any{map[string]any{"type": "auth", "token": t.token}, user} {
		data, err := json.Marshal(frame)
		if err != nil {
			return errors.New("cross-session frame encoding failed")
		}
		if len(data)+1 > ipc.MaxFrameSize {
			return fmt.Errorf("cross-session outbound frame exceeds IPC cap (%d bytes)", ipc.MaxFrameSize)
		}
		frames = append(frames, append(data, '\n'))
	}
	ctx, cancel := context.WithTimeout(ctx, crossSessionWindow)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return errors.New("cross-session connect failed")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if conn.SetDeadline(deadline) != nil {
		return errors.New("cross-session deadline failed")
	}
	if err := validateCrossSessionPeer(conn, t.hostPID); err != nil {
		return err
	}
	for _, data := range frames {
		if n, err := conn.Write(data); err != nil || n != len(data) {
			return errors.New("cross-session write failed")
		}
	}
	unix, ok := conn.(*net.UnixConn)
	if !ok || unix.CloseWrite() != nil {
		return errors.New("cross-session half-close failed")
	}
	var response [1]byte
	n, err := conn.Read(response[:])
	if n != 0 || err != io.EOF {
		return errors.New("cross-session peer did not close cleanly")
	}
	return nil
}

// Mirror the host's channel block: source plus every channel metadata attribute,
// followed by the unchanged plain-text body. Metadata is XML-escaped; the body
// is intentionally not XML (it may contain code or arbitrary user text).
func crossSessionChannelBlock(frame map[string]any) (string, error) {
	content, ok := frame["content"].(string)
	if !ok {
		return "", errors.New("channel content unavailable")
	}
	meta, ok := frame["meta"].(map[string]any)
	if !ok {
		return "", errors.New("channel metadata unavailable")
	}
	var b strings.Builder
	b.WriteString(`<channel source="plugin:c3:c3"`)
	keys := make([]string, 0, len(meta))
	for key := range meta {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value, ok := meta[key].(string)
		if !ok || key == "source" || strings.ContainsAny(key, " \t\r\n<>&\"'=/:") || key == "" {
			return "", errors.New("channel metadata invalid")
		}
		b.WriteString(" " + key + `="`)
		_ = xml.EscapeText(&b, []byte(value))
		b.WriteByte('"')
	}
	b.WriteString(">\n" + content + "\n</channel>")
	return b.String(), nil
}
