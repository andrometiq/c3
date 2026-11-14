package broker

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/autohook"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
)

// downloadChannel is the auto-mode fake Telegram channel whose
// DownloadAttachment returns path.
type downloadChannel struct {
	*autoTestChannel
	path string
}

func (c *downloadChannel) DownloadAttachment(string) (string, error) { return c.path, nil }

// newAttachmentFixture is newAutoFixture with its Telegram attachment cache in a
// fresh XDG_CACHE_HOME, reached through a symlink where the platform allows,
// and a channel whose downloads land there.
func newAttachmentFixture(t *testing.T) (*autoFixture, *downloadChannel, string) {
	t.Helper()
	f := newAutoFixture(t)
	cache := t.TempDir()
	if runtime.GOOS != "windows" {
		linked := filepath.Join(t.TempDir(), "linked-cache")
		if err := os.Symlink(cache, linked); err != nil {
			t.Fatal(err)
		}
		cache = linked
	}
	t.Setenv("XDG_CACHE_HOME", cache)
	dir, err := channel.AttachmentCacheDir("telegram")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	downloads := &downloadChannel{autoTestChannel: f.ch}
	f.b.chMu.Lock()
	f.b.channels["telegram"] = &channelRegistration{Channel: downloads}
	f.b.chMu.Unlock()
	return f, downloads, dir
}

// writeAttachment creates a file and returns its path as given and resolved:
// a channel returns the first, the hook names the second.
func writeAttachment(t *testing.T, dir, name string) (path, resolved string) {
	t.Helper()
	path = filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("jpeg"), 0o600); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, resolved
}

// download runs download_attachment for stub, the way its adapter does.
func download(t *testing.T, f *autoFixture, stub *Stub, downloads *downloadChannel, path string) {
	t.Helper()
	downloads.path = path
	if resp := toolCallForTest(t, f.b, stub, "download", "download_attachment", map[string]any{"file_id": "F"}); resp.Error != nil {
		t.Fatalf("download_attachment: %s", resp.Error.Message)
	}
}

// attachmentCheck sends handleGrantCheck one check of a call on path.
func attachmentCheck(t *testing.T, f *autoFixture, sessionID, tool, path string) ipc.GrantCheckResp {
	t.Helper()
	req := f.check()
	req.SessionID, req.ToolName, req.AttachmentPath = sessionID, tool, path
	raw, _ := json.Marshal(req)
	hook, brokerSide := newConnPair(t)
	go f.b.handleGrantCheck(brokerSide, "claude", raw)
	frame, err := hook.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var resp ipc.GrantCheckResp
	if err := ipc.DecodeStrict(frame, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func isAttachmentAllow(resp ipc.GrantCheckResp) bool { return resp.Allow && resp.Attachment }

// Through the real hook client and socket: a Read of an attachment is allowed
// for the session download_attachment returned it to, again and again and
// with auto-mode approval off, and for no other session or file.
func TestAttachmentReadAllowedOnlyForTheSessionThatDownloadedIt(t *testing.T) {
	f, downloads, dir := newAttachmentFixture(t)
	socketPath := listenAuto(t, f)
	photo, _ := writeAttachment(t, dir, "AQADunique_photo.jpg")
	other, _ := writeAttachment(t, dir, "AQADother_note.txt")
	read := func(sessionID, path string) string {
		input, _ := json.Marshal(map[string]string{"file_path": path})
		var out bytes.Buffer
		autohook.RunPreToolUse(strings.NewReader(`{"session_id":"`+sessionID+`","cwd":"/workspace","tool_name":"Read",`+
			`"tool_use_id":"toolu_r","tool_input":`+string(input)+`}`), &out, socketPath, dir)
		return out.String()
	}
	const allowed = "C3: Telegram attachment this session downloaded"

	if output := read("session-fixture", photo); output != "" {
		t.Fatalf("allowed before any download: %q", output)
	}
	download(t, f, f.owner, downloads, photo)
	for range 2 {
		if output := read("session-fixture", photo); !strings.Contains(output, allowed) {
			t.Fatalf("the downloading session's read printed %q", output)
		}
	}
	f.setEnabled(false)
	if output := read("session-fixture", photo); !strings.Contains(output, allowed) {
		t.Fatalf("with auto-mode approval off the read printed %q", output)
	}
	second := f.b.Stubs.Register("claude", 4343, "/elsewhere", struct{}{})
	second.SetStableSessionID("session-other")
	if output := read("session-other", photo); output != "" {
		t.Fatalf("another session's read of the same file printed %q", output)
	}
	if output := read("session-fixture", other); output != "" {
		t.Fatalf("a file this session never downloaded printed %q", output)
	}
}

// The broker allows only what its own record holds and the path still is: the
// hook's claim, another tool, a download outside the cache, a record outside
// the cache and a download swapped for a link, out of the cache or to another
// cached file, all get no attachment allow.
func TestAttachmentCheckAllowsOnlyWhatTheBrokerRecorded(t *testing.T) {
	f, downloads, dir := newAttachmentFixture(t)
	given, photo := writeAttachment(t, dir, "AQADunique_photo.jpg")
	givenNote, note := writeAttachment(t, dir, "AQADnote_note.txt")
	_, neverDownloaded := writeAttachment(t, dir, "AQADother_secret.txt")
	outsideDir := t.TempDir()
	_, outside := writeAttachment(t, outsideDir, "secret.txt")
	_, beside := writeAttachment(t, outsideDir, "AQADvoice.oga")
	download(t, f, f.owner, downloads, given)
	download(t, f, f.owner, downloads, givenNote)
	download(t, f, f.owner, downloads, beside) // like a voice inbox path

	if !isAttachmentAllow(attachmentCheck(t, f, "session-fixture", "Read", photo)) {
		t.Fatal("the recorded download was refused")
	}
	refused := map[string]ipc.GrantCheckResp{
		"a forged path outside the cache": attachmentCheck(t, f, "session-fixture", "Read", outside),
		"a download outside the cache":    attachmentCheck(t, f, "session-fixture", "Read", beside),
		"another tool on the download":    attachmentCheck(t, f, "session-fixture", "Bash", photo),
	}
	f.owner.stubMu.Lock()
	f.owner.attachments[stubAttachment{sessionID: "session-fixture", path: outside}] = "telegram"
	f.owner.stubMu.Unlock()
	refused["a record outside the cache"] = attachmentCheck(t, f, "session-fixture", "Read", outside)
	if runtime.GOOS != "windows" {
		if err := os.Remove(photo); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, photo); err != nil {
			t.Fatal(err)
		}
		refused["the download swapped for a link out"] = attachmentCheck(t, f, "session-fixture", "Read", photo)
		if err := os.Remove(note); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(neverDownloaded, note); err != nil {
			t.Fatal(err)
		}
		refused["the download swapped for a link to another cached file"] = attachmentCheck(t, f, "session-fixture", "Read", note)
	}
	for name, resp := range refused {
		if resp.Allow || resp.Attachment {
			t.Errorf("%s: %+v, want no allow", name, resp)
		}
	}
}

// The record lives and dies with the adapter connection, and keeps only the
// newest maxStubAttachments downloads.
func TestAttachmentRecordEndsWithTheConnection(t *testing.T) {
	f, downloads, dir := newAttachmentFixture(t)
	given, photo := writeAttachment(t, dir, "AQADunique_photo.jpg")
	download(t, f, f.owner, downloads, given)
	f.owner.MarkDisconnected()
	if resp := attachmentCheck(t, f, "session-fixture", "Read", photo); resp.Allow {
		t.Fatalf("allowed after the adapter disconnected: %+v", resp)
	}
	reconnected := f.b.Stubs.Register("claude", 4242, "/workspace", struct{}{})
	reconnected.SetStableSessionID("session-fixture")
	if resp := attachmentCheck(t, f, "session-fixture", "Read", photo); resp.Allow {
		t.Fatalf("a new connection inherited the old record: %+v", resp)
	}

	var resolved []string
	for i := range maxStubAttachments + 1 {
		given, path := writeAttachment(t, dir, fmt.Sprintf("AQAD%d_file.txt", i))
		rememberAttachment(reconnected, f.route, mcpText(given))
		resolved = append(resolved, path)
	}
	if isAttachmentAllow(attachmentCheck(t, f, "session-fixture", "Read", resolved[0])) {
		t.Fatal("the oldest record survived past the bound")
	}
	if !isAttachmentAllow(attachmentCheck(t, f, "session-fixture", "Read", resolved[1])) {
		t.Fatal("a record within the bound was lost")
	}
}

// The auto-mode grant path works as before with and without attachment_path.
// An armed grant for the very Read of a downloaded attachment is used first, so
// its card and veto bookkeeping hold; the next Read gets the attachment allow,
// which consumes nothing.
func TestAttachmentPathLeavesTheGrantPathAlone(t *testing.T) {
	for _, isNamed := range []bool{false, true} {
		f, _, dir := newAttachmentFixture(t)
		r := f.armed(t)
		path := ""
		if isNamed {
			_, path = writeAttachment(t, dir, "AQADnever_downloaded.jpg")
		}
		if resp := attachmentCheck(t, f, "session-fixture", f.req.ToolName, path); !resp.Allow || resp.Attachment ||
			f.state(r) != autoConsumed {
			t.Fatalf("attachment_path %q: %+v, state %v; want the grant consumed", path, resp, f.state(r))
		}
	}

	f, downloads, dir := newAttachmentFixture(t)
	given, photo := writeAttachment(t, dir, "AQADunique_photo.jpg")
	f.req.ToolName = "Read"
	f.req.ToolInput = json.RawMessage(`{"file_path":` + fmt.Sprintf("%q", photo) + `}`)
	r := f.armed(t)
	download(t, f, f.owner, downloads, given)
	if resp := attachmentCheck(t, f, "session-fixture", "Read", photo); !resp.Allow || resp.Attachment || f.state(r) != autoConsumed {
		t.Fatalf("%+v, state %v; want the armed grant consumed", resp, f.state(r))
	}
	if resp := attachmentCheck(t, f, "session-fixture", "Read", photo); !isAttachmentAllow(resp) {
		t.Fatalf("%+v; want the attachment allow once the grant is used", resp)
	}
}

// recoverAs switches stub's connection to the conversation stableID, the way
// the adapter does after /clear, /resume or /compact.
func recoverAs(t *testing.T, f *autoFixture, stub *Stub, stableID string) {
	t.Helper()
	adapter, brokerSide := newConnPair(t)
	go func() { _, _ = adapter.ReadFrame() }()
	raw, _ := json.Marshal(ipc.RecoverSessionReq{Op: ipc.OpRecoverSession, StableSessionID: stableID})
	f.b.handleRecoverSession(brokerSide, stub, raw, nil)
	if got := stub.StableSessionIDValue(); got != stableID {
		t.Fatalf("identity not switched: %q", got)
	}
}

// A download is allowed only for the conversation it was returned to, on the
// same connection: refused after a switch to another conversation (/clear, a
// /resume of another one), allowed again after switching back, and a download
// made after the switch is allowed for the new conversation only. A download
// made before the conversation was known is never allowed.
func TestAttachmentAllowedOnlyForTheConversationThatDownloadedIt(t *testing.T) {
	f, downloads, dir := newAttachmentFixture(t)
	before, beforeResolved := writeAttachment(t, dir, "AQADbefore_photo.jpg")
	after, afterResolved := writeAttachment(t, dir, "AQADafter_photo.jpg")
	download(t, f, f.owner, downloads, before)
	if !isAttachmentAllow(attachmentCheck(t, f, "session-fixture", "Read", beforeResolved)) {
		t.Fatal("the downloading conversation was refused")
	}

	recoverAs(t, f, f.owner, "other-conversation")
	if resp := attachmentCheck(t, f, "other-conversation", "Read", beforeResolved); resp.Allow {
		t.Fatalf("another conversation got the first one's download: %+v", resp)
	}
	claimConfirmed(t, f.b, f.owner, f.route) // the switch released the first conversation's topic
	download(t, f, f.owner, downloads, after)
	if !isAttachmentAllow(attachmentCheck(t, f, "other-conversation", "Read", afterResolved)) {
		t.Fatal("a download after the switch was refused for the new conversation")
	}

	recoverAs(t, f, f.owner, "session-fixture")
	if !isAttachmentAllow(attachmentCheck(t, f, "session-fixture", "Read", beforeResolved)) {
		t.Fatal("switching back lost the conversation's own download")
	}
	if resp := attachmentCheck(t, f, "session-fixture", "Read", afterResolved); resp.Allow {
		t.Fatalf("the first conversation got a download made after it switched away: %+v", resp)
	}

	early := f.b.Stubs.Register("claude", 4545, "/early", struct{}{})
	earlyRoute := RouteKey{Channel: "telegram", ChatID: -100, HasTopic: true, TopicID: 99}
	claimConfirmed(t, f.b, early, earlyRoute)
	unknown, unknownResolved := writeAttachment(t, dir, "AQADearly_photo.jpg")
	download(t, f, early, downloads, unknown)
	early.SetStableSessionID("late-id")
	if resp := attachmentCheck(t, f, "late-id", "Read", unknownResolved); resp.Allow {
		t.Fatalf("a download made before the conversation was known was allowed: %+v", resp)
	}
}

// The allow comes only from the session's one live adapter: with a second live
// adapter on the same session id (the conversation resumed in another window)
// neither gets it.
func TestAttachmentTwoLiveAdaptersOnOneSessionGetNoAllow(t *testing.T) {
	f, downloads, dir := newAttachmentFixture(t)
	given, photo := writeAttachment(t, dir, "AQADunique_photo.jpg")
	download(t, f, f.owner, downloads, given)
	if !isAttachmentAllow(attachmentCheck(t, f, "session-fixture", "Read", photo)) {
		t.Fatal("the downloading adapter was refused")
	}
	twin := f.b.Stubs.Register("claude", 4343, "/workspace", struct{}{})
	twin.SetStableSessionID("session-fixture")
	if resp := attachmentCheck(t, f, "session-fixture", "Read", photo); resp.Allow {
		t.Fatalf("allowed with two live adapters on one session id: %+v", resp)
	}
}

// The allow comes only from that adapter's own record: after the downloading
// connection switches to another conversation and a second window resumes the
// first one, the second window's adapter never downloaded the file and gets
// no allow through the first connection's record.
func TestAttachmentAllowComesOnlyFromTheAdaptersOwnRecord(t *testing.T) {
	f, downloads, dir := newAttachmentFixture(t)
	given, photo := writeAttachment(t, dir, "AQADunique_photo.jpg")
	download(t, f, f.owner, downloads, given)
	recoverAs(t, f, f.owner, "other-conversation")
	secondWindow := f.b.Stubs.Register("claude", 4343, "/workspace", struct{}{})
	secondWindow.SetStableSessionID("session-fixture")
	if resp := attachmentCheck(t, f, "session-fixture", "Read", photo); resp.Allow {
		t.Fatalf("allowed through another connection's record: %+v", resp)
	}
}
