package autohook

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

func writeTestFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func callPayload(t *testing.T, tool string, input any) string {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	return `{"session_id":"s","cwd":"/w","tool_name":"` + tool + `","tool_use_id":"toolu_9","tool_input":` + string(raw) + `}`
}

func readPayload(t *testing.T, path string) string {
	t.Helper()
	return callPayload(t, "Read", map[string]string{"file_path": path})
}

// attachmentBroker answers each grant check with reply, then passes on the
// next frame the hook sends (nil when none arrives within a second).
func attachmentBroker(t *testing.T, reply string) (*fakeBroker, <-chan []byte) {
	t.Helper()
	followUps := make(chan []byte, 8)
	var fake *fakeBroker
	fake = newFakeBroker(t, func(conn *ipc.Conn, raw net.Conn) {
		if !fake.helloAndOp(conn) {
			return
		}
		writeRaw(conn, reply)
		_ = raw.SetReadDeadline(time.Now().Add(time.Second))
		frame, _ := conn.ReadFrame()
		followUps <- frame
	})
	return fake, followUps
}

// sentAttachmentPath is the attachment_path of the grant check the hook sent,
// or "" when it sent none. The fake broker records the op before it replies,
// so after the hook returns the record is there or never will be.
func sentAttachmentPath(t *testing.T, fake *fakeBroker) string {
	t.Helper()
	select {
	case frame := <-fake.ops:
		var request ipc.GrantCheckReq
		if err := ipc.DecodeStrict(frame, &request); err != nil {
			t.Fatal(err)
		}
		return request.AttachmentPath
	default:
		return ""
	}
}

// The hook names an attachment to the broker only for a Read whose file_path
// is itself a regular file directly inside the attachment cache as configured
// (the form download_attachment returns), and names the resolved file. Links,
// other spellings of the key and every other call go out exactly as before.
func TestPreToolUseNamesOnlyAReadOfAFileInTheAttachmentCache(t *testing.T) {
	root := t.TempDir()
	cache := filepath.Join(root, "attachments")
	inside := filepath.Join(cache, "AQADunique_photo.jpg")
	outside := filepath.Join(root, "outside", "secret.txt")
	writeTestFile(t, inside)
	writeTestFile(t, outside)
	writeTestFile(t, filepath.Join(cache, "sub", "inner.txt"))
	resolvedInside, err := filepath.EvalSymlinks(inside)
	if err != nil {
		t.Fatal(err)
	}
	linkedCache := filepath.Join(root, "linked-cache")
	escapingLink := filepath.Join(cache, "AQADlink_note.txt")
	innerLink := filepath.Join(cache, "AQADinner_link.jpg")
	projectLink := filepath.Join(root, "project", "innocent.jpg")
	if runtime.GOOS != "windows" {
		for link, target := range map[string]string{linkedCache: cache, escapingLink: outside, innerLink: inside, projectLink: inside} {
			if err := os.MkdirAll(filepath.Dir(link), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, link); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Chdir(cache)

	cases := []struct {
		name      string
		stdin     string
		dir       string
		want      string
		isSymlink bool
	}{
		{name: "regular file inside", stdin: readPayload(t, inside), dir: cache, want: resolvedInside},
		{name: "cache dir configured through a symlink, path as configured", stdin: readPayload(t, filepath.Join(linkedCache, filepath.Base(inside))), dir: linkedCache, want: resolvedInside, isSymlink: true},
		{name: "path through a link to the cache dir", stdin: readPayload(t, filepath.Join(linkedCache, filepath.Base(inside))), dir: cache, isSymlink: true},
		{name: "real path while the cache dir is configured through a link", stdin: readPayload(t, inside), dir: linkedCache, isSymlink: true},
		{name: "link outside the cache to an attachment", stdin: readPayload(t, projectLink), dir: cache, isSymlink: true},
		{name: "link inside the cache to an attachment", stdin: readPayload(t, innerLink), dir: cache, isSymlink: true},
		{name: "file outside", stdin: readPayload(t, outside), dir: cache},
		{name: "dot-dot traversal out of the cache", stdin: readPayload(t, cache+"/../outside/secret.txt"), dir: cache},
		{name: "symlink inside pointing outside", stdin: readPayload(t, escapingLink), dir: cache, isSymlink: true},
		{name: "the cache dir itself", stdin: readPayload(t, cache), dir: cache},
		{name: "a subdirectory", stdin: readPayload(t, filepath.Join(cache, "sub")), dir: cache},
		{name: "a file in a subdirectory", stdin: readPayload(t, filepath.Join(cache, "sub", "inner.txt")), dir: cache},
		{name: "missing file", stdin: readPayload(t, filepath.Join(cache, "AQADgone_photo.jpg")), dir: cache},
		{name: "relative path that resolves inside", stdin: readPayload(t, filepath.Base(inside)), dir: cache},
		{name: "no cache dir", stdin: readPayload(t, inside), dir: ""},
		{name: "relative cache dir", stdin: readPayload(t, inside), dir: "."},
		{name: "another tool on an attachment", stdin: callPayload(t, "Edit", map[string]string{"file_path": inside}), dir: cache},
		{name: "file_path not a string", stdin: callPayload(t, "Read", map[string]int{"file_path": 7}), dir: cache},
		{name: "tool_input not an object", stdin: callPayload(t, "Read", inside), dir: cache},
		{name: "tool_input missing", stdin: `{"session_id":"s","cwd":"/w","tool_name":"Read"}`, dir: cache},
		{name: "a case variant of the key naming an attachment", dir: cache, // the variant comes last, so it would win a case-blind decode
			stdin: `{"session_id":"s","cwd":"/w","tool_name":"Read","tool_input":{"file_path":` +
				quoteJSON(t, outside) + `,"FILE_PATH":` + quoteJSON(t, inside) + `}}`},
		{name: "duplicate file_path keys", dir: cache,
			stdin: `{"session_id":"s","cwd":"/w","tool_name":"Read","tool_input":{"file_path":` +
				quoteJSON(t, outside) + `,"file_path":` + quoteJSON(t, inside) + `}}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.isSymlink && runtime.GOOS == "windows" {
				t.Skip("symlinks need privileges on Windows")
			}
			fake, _ := attachmentBroker(t, `{"op":"grant_check_result","allow":false}`)
			env := testEnv(time.Second)
			env.attachmentDir = c.dir
			if output, _ := runPreWith(t, c.stdin, fake.path, env); output != "" {
				t.Fatalf("printed %q with no allow", output)
			}
			if got := sentAttachmentPath(t, fake); got != c.want {
				t.Fatalf("attachment_path %q, want %q", got, c.want)
			}
		})
	}
}

func quoteJSON(t *testing.T, value string) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// The attachment allow is printed only when the broker gives it for a Read the
// hook named an attachment in, and is never confirmed (it consumed nothing). A
// grant allow keeps its own output and confirmation.
func TestPreToolUsePrintsTheAttachmentAllowOnlyForANamedAttachment(t *testing.T) {
	cache := t.TempDir()
	inside := filepath.Join(cache, "AQADunique_photo.jpg")
	writeTestFile(t, inside)
	attachmentAllow := `{"op":"grant_check_result","allow":true,"attachment":true}`
	cases := []struct {
		name, reply, stdin, want string
		isConfirmed              bool
	}{
		{name: "attachment allow for a named attachment", reply: attachmentAllow, stdin: readPayload(t, inside), want: attachmentReadOutput},
		{name: "attachment allow for a call that named none", reply: attachmentAllow, stdin: readPayload(t, filepath.Join(cache, "missing.jpg"))},
		{name: "attachment flag without allow", reply: `{"op":"grant_check_result","allow":false,"attachment":true}`, stdin: readPayload(t, inside)},
		{name: "grant allow on a named attachment", reply: `{"op":"grant_check_result","allow":true}`, stdin: readPayload(t, inside), want: allowOutput, isConfirmed: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fake, followUps := attachmentBroker(t, c.reply)
			env := testEnv(time.Second)
			env.attachmentDir = cache
			output, _ := runPreWith(t, c.stdin, fake.path, env)
			if output != c.want {
				t.Fatalf("printed %q, want %q", output, c.want)
			}
			if frame := receive(t, followUps, "follow-up read"); (frame != nil) != c.isConfirmed {
				t.Fatalf("follow-up frame %s, want a confirmation: %v", frame, c.isConfirmed)
			}
		})
	}
}
