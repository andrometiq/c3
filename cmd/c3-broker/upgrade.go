package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/Andrometiq/c3/internal/buildidentity"
	"github.com/Andrometiq/c3/internal/ipc"
)

// Reuse setup's singleton-aware stop/start path. No daemon is started in tests;
// both lifecycle functions already have injectable seams.
func bounceUpgradeBroker() error {
	stopped, note := stopBrokerFn()
	if !stopped && note != "" {
		return fmt.Errorf("broker bounce: %s", note)
	}
	ensureBrokerUpFn()
	return nil
}
func sourceBuildFlags(ctx context.Context, dir string) string {
	cmd := exec.CommandContext(ctx, "git", "describe", "--always", "--dirty")
	cmd.Dir = dir
	data, err := cmd.Output()
	build := "dev"
	if err == nil && strings.TrimSpace(string(data)) != "" {
		build = strings.TrimSpace(string(data))
	}
	return "-X " + buildidentity.Symbol + "=" + build
}
func upgradeBuildLabel(s ipc.SessionEntry) string {
	build := s.Build
	if build == "" {
		build = "unknown"
	}
	if s.Stale {
		build += " (stale)"
	}
	return build
}
func fetchUpgradeSessions() ([]ipc.SessionEntry, error) {
	conn, err := dialBroker()
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if err = conn.WriteJSON(ipc.ListSessionsReq{Op: ipc.OpListSessions}); err != nil {
		return nil, err
	}
	raw, err := conn.ReadFrame()
	if err != nil {
		return nil, err
	}
	var reply ipc.ListSessionsReplyMsg
	err = json.Unmarshal(raw, &reply)
	return reply.Sessions, err
}
