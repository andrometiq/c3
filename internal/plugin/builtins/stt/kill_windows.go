//go:build windows

package stt

import (
	"os/exec"
	"syscall"
)

// createNoWindow (CREATE_NO_WINDOW) suppresses the console window Windows would
// otherwise allocate for each console-subsystem child (python.exe) of a
// windowless broker — the visible flash on every transcription.
const createNoWindow = 0x08000000

// sttSysProcAttr puts the handler in its own process group (so it doesn't share
// the broker's console-signal fate) and runs it fully in the background — no
// console-window flash on each voice note.
func sttSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | createNoWindow,
		HideWindow:    true,
	}
}

// sttKillTree kills the direct child (best-effort). Tree-kill (grandchildren)
// via `taskkill /T` is a future refinement; on Windows the process-group SIGKILL
// trick isn't available.
func sttKillTree(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
