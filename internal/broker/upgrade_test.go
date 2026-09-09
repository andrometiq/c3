package broker

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/buildidentity"
	"github.com/Andrometiq/c3/internal/ipc"
)

func TestUpgradeHelloHint(t *testing.T) {
	for _, kind := range []string{"same", "different", "unreadable", "prefeature", "contract", "disabled"} {
		t.Run(kind, func(t *testing.T) {
			b := &Broker{}
			b.upgrades.installed = func() (buildidentity.Installed, error) {
				if kind == "unreadable" {
					return buildidentity.Installed{}, errors.New("unreadable")
				}
				return buildidentity.Installed{Path: "installed-adapter", Build: "new", Contract: "contract"}, nil
			}
			hello := ipc.HelloMsg{CLI: "claude", Build: "old", ResumeContract: "contract"}
			switch kind {
			case "same":
				hello.Build = "new"
			case "prefeature":
				hello.Build = ""
			case "contract":
				hello.ResumeContract = "other"
			case "disabled":
				hello.UpgradeDisabled = true
			}
			var ack ipc.HelloAckMsg
			fallback := b.prepareUpgrade(hello, &Stub{ConnID: 41}, &ack)
			if ack.Build != buildidentity.Current() {
				t.Fatal(ack)
			}
			if (ack.Upgrade != nil) != (kind == "different") {
				t.Fatal(ack)
			}
			if ack.Upgrade != nil && (ack.Upgrade.Path != "installed-adapter" || ack.Upgrade.Build != "new") {
				t.Fatal(ack)
			}
			if (fallback != "") != (kind == "prefeature" || kind == "contract" || kind == "disabled") {
				t.Fatal(fallback)
			}
		})
	}
}
func TestUpgradeFallbackNoticeOnceAcrossReconnect(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	b := &Broker{}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	stub := &Stub{CLI: "claude", PID: 41, CWD: "/project", ConnID: 1, Conn: ipc.NewConn(left)}
	conn := ipc.NewConn(right)
	done := make(chan struct{})
	go func() { b.sendUpgradeNotice(stub, "new"); close(done) }()
	raw, err := conn.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var event ipc.InboundMsg
	if err = json.Unmarshal(raw, &event); err != nil {
		t.Fatal(err)
	}
	want := "C3 was updated to new. This session still runs the previous adapter: run /mcp and reconnect c3 (or restart the session) to switch."
	if event.Inbound.Event == nil || event.Inbound.Event.System.Message != want {
		t.Fatal(string(raw))
	}
	<-done
	b.sendUpgradeNotice(stub, "new") // would block if repeated
	stub.ConnID = 2
	b.sendUpgradeNotice(stub, "new") // same logical process, new connection
	restarted := &Broker{}
	restarted.sendUpgradeNotice(stub, "new") // persisted across the broker bounce too
	_ = right.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	if raw, err = conn.ReadFrame(); err == nil {
		t.Fatalf("duplicate: %s", raw)
	}
}
func TestUpgradeHelloWireIntegration(t *testing.T) {
	b := brokerWithChannel(t, mfWithTelegram(), &fakeChannel{})
	defer b.Shutdown()
	b.upgrades.installed = func() (buildidentity.Installed, error) {
		return buildidentity.Installed{Path: "adapter", Build: "next", Contract: "contract"}, nil
	}
	left, right := net.Pipe()
	defer right.Close()
	go b.HandleConn(left)
	conn := ipc.NewConn(right)
	if err := conn.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "claude", PID: 41, CWD: "/project", Build: "old", ResumeContract: "contract"}); err != nil {
		t.Fatal(err)
	}
	raw, err := conn.ReadFrame()
	if err != nil {
		t.Fatal(err)
	}
	var ack ipc.HelloAckMsg
	if json.Unmarshal(raw, &ack) != nil || ack.Upgrade == nil || ack.Upgrade.Build != "next" || !strings.Contains(string(raw), `"build"`) {
		t.Fatal(string(raw))
	}
}

func TestUpgradePrefeatureUnreadableInstalledBuild(t *testing.T) {
	b := &Broker{}
	b.upgrades.installed = func() (buildidentity.Installed, error) { return buildidentity.Installed{}, errors.New("unreadable") }
	var ack ipc.HelloAckMsg
	if notice := b.prepareUpgrade(ipc.HelloMsg{CLI: "claude"}, &Stub{ConnID: 1}, &ack); notice != buildidentity.Current() || ack.Upgrade != nil {
		t.Fatalf("prefeature fallback=%q hint=%+v", notice, ack.Upgrade)
	}
}

func TestUpgradeUnreadableFallbackNotice(t *testing.T) {
	for _, inspection := range []string{"missing", "invalid"} {
		for _, kind := range []string{"prefeature", "disabled", "enabled"} {
			t.Run(inspection+"/"+kind, func(t *testing.T) {
				t.Setenv("XDG_STATE_HOME", t.TempDir())
				path := filepath.Join(t.TempDir(), "adapter")
				if inspection == "invalid" {
					if err := os.WriteFile(path, []byte("C3_BUILD_ID_V1 invalid executable"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				b := &Broker{}
				b.upgrades.installed = func() (buildidentity.Installed, error) { return buildidentity.Read(path) }
				hello := ipc.HelloMsg{CLI: "claude", Build: "old", UpgradeDisabled: kind == "disabled"}
				if kind == "prefeature" {
					hello.Build = ""
				}
				left, right := net.Pipe()
				defer left.Close()
				defer right.Close()
				_ = right.SetReadDeadline(time.Now().Add(time.Second))
				stub := &Stub{CLI: "claude", PID: 41, CWD: "/project", ConnID: 1, Conn: ipc.NewConn(left)}
				var ack ipc.HelloAckMsg
				fallback := b.prepareUpgrade(hello, stub, &ack)
				if ack.Upgrade != nil {
					t.Fatal("inspection failure produced an exec hint")
				}
				if kind == "enabled" {
					if fallback != "" {
						t.Fatal("enabled adapter received a guessed stale notice")
					}
					return
				}
				if fallback != buildidentity.Current() {
					t.Fatalf("fallback=%q", fallback)
				}
				done := make(chan struct{})
				go func() {
					b.sendUpgradeNotice(stub, fallback)
					stub.ConnID++
					b.sendUpgradeNotice(stub, fallback)
					close(done)
				}()
				raw, err := ipc.NewConn(right).ReadFrame()
				if err != nil {
					t.Fatal(err)
				}
				var notice ipc.InboundMsg
				if err := json.Unmarshal(raw, &notice); err != nil {
					t.Fatal(err)
				}
				want := "C3 was updated to " + fallback + ". This session still runs the previous adapter: run /mcp and reconnect c3 (or restart the session) to switch."
				if notice.Inbound.Event == nil || notice.Inbound.Event.System == nil || notice.Inbound.Event.System.Message != want {
					t.Fatalf("notice=%s", raw)
				}
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("fallback repeated on reconnect")
				}
			})
		}
	}
}
