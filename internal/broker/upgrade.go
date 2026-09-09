package broker

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/Andrometiq/c3/internal/buildidentity"
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/ipc"
)

type upgradeRegistry struct {
	beforeAckRemoval func() // deterministic reconnect/ack regression seam
	mu               sync.Mutex
	notices          map[string]string
	installed        func() (buildidentity.Installed, error) // test seam
}

func (b *Broker) installedAdapter() (buildidentity.Installed, error) {
	if b.upgrades.installed != nil {
		return b.upgrades.installed()
	}
	return buildidentity.Claude()
}
func (b *Broker) prepareUpgrade(hello ipc.HelloMsg, stub *Stub, ack *ipc.HelloAckMsg) string {
	ack.Build = buildidentity.Current()
	if hello.CLI != "claude" {
		return ""
	}
	installed, err := b.installedAdapter()
	if hello.Build == "" {
		if err == nil {
			return installed.Build
		}
		return ack.Build
	}
	if err != nil || installed.Build == hello.Build {
		return ""
	}
	if hello.UpgradeDisabled || hello.ResumeContract == "" || hello.ResumeContract != installed.Contract {
		return installed.Build
	}
	ack.Upgrade = &ipc.UpgradeHint{Path: installed.Path, Build: installed.Build}
	log.Printf("upgrade hint sent conn=%d from=%s to=%s", stub.ConnID, hello.Build, installed.Build)
	return ""
}
func (b *Broker) sendUpgradeNotice(stub *Stub, build string) {
	key := fmt.Sprintf("%s:%d:%s", stub.CLI, stub.PID, stub.CWD)
	b.upgrades.mu.Lock()
	defer b.upgrades.mu.Unlock()
	b.upgrades.loadNotices()
	if b.upgrades.notices[key] == build {
		return
	}
	if b.sendSystemEventTo(stub, &c3types.SystemEvent{Source: "c3-broker", Title: "C3 adapter update", Message: fmt.Sprintf("C3 was updated to %s. This session still runs the previous adapter: run /mcp and reconnect c3 (or restart the session) to switch.", build)}) {
		if b.upgrades.notices == nil {
			b.upgrades.notices = map[string]string{}
		}
		b.upgrades.notices[key] = build
		if err := b.upgrades.saveNotices(); err != nil {
			log.Printf("upgrade notice persistence: %v", err)
		}
		log.Printf("upgrade fallback notice sent conn=%d", stub.ConnID)
	}
}
func (b *Broker) upgradeStale(s *Stub) bool {
	if s.CLI != "claude" {
		return false
	}
	installed, err := b.installedAdapter()
	return err == nil && installed.Build != s.Build
}

func upgradeNoticePath() string {
	return filepath.Join(filepath.Dir(HealthFilePath()), "upgrade-notices.json")
}

// Called under the registry mutex. Reuse the broker's state directory and an
// atomic rename so an ordinary broker bounce does not repeat an old notice.
func (u *upgradeRegistry) loadNotices() {
	if u.notices != nil {
		return
	}
	u.notices = map[string]string{}
	raw, err := os.ReadFile(upgradeNoticePath())
	if err == nil {
		_ = json.Unmarshal(raw, &u.notices)
	}
	if u.notices == nil {
		u.notices = map[string]string{}
	}
}
func (u *upgradeRegistry) saveNotices() error {
	path := upgradeNoticePath()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	data, err := json.Marshal(u.notices)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".upgrade-notices-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(data); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}
