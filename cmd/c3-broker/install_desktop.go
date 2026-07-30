package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/plugin/builtins/stt"
	"github.com/Andrometiq/c3/internal/updater"
)

var (
	desktopExecutablePath    = os.Executable
	desktopWriteMappingsFile = writeMappingsFile
)

// runInstallDesktop configures Claude Desktop (Windows / macOS) to load the C3
// MCP adapter. It MERGES an `mcpServers.c3` entry into Claude Desktop's
// claude_desktop_config.json, preserving every other key and every other MCP
// server. The adapter is poll-only: Claude Desktop cannot push a Telegram
// message into a chat, so inbound sits in C3's durable queue and the user pulls
// it by asking Claude to call `fetch_queue`.
//
// Config path is per-OS (runtime.GOOS):
//   - windows: %APPDATA%\Claude\claude_desktop_config.json
//   - darwin:  ~/Library/Application Support/Claude/claude_desktop_config.json
//   - linux:   $XDG_CONFIG_HOME/Claude/claude_desktop_config.json
//     (default ~/.config/Claude/...) — the official Claude Desktop Linux beta
//     (2026-06) is an Electron app whose userData dir follows XDG.
//
// An explicit `--config <path>` / `--path <path>` override takes precedence on
// every OS.
func runInstallDesktop(args []string) error {
	override := parseDesktopConfigOverride(args)

	cfgPath, note, err := desktopConfigPath(override)
	if err != nil {
		return err
	}
	if note != "" {
		fmt.Print(note)
	}
	if cfgPath == "" {
		// Linux, no override — nothing to write. desktopConfigPath already
		// printed the explanatory note via the caller above.
		return nil
	}
	releaseInstallLock, err := acquireDesktopInstallLock()
	if err != nil {
		return err
	}
	defer releaseInstallLock()

	// Resolve the adapter path. Claude Desktop's docs require an ABSOLUTE
	// command path, so we prefer the resolved PATH entry (made absolute); if the
	// binary isn't built/installed yet we fall back to the bare command name and
	// warn the user to edit it.
	adapterBin := exeName("c3-desktop-adapter")
	adapterPath := "c3-desktop-adapter" // bare fallback per contract
	adapterResolved, lookErr := lookPath(adapterBin)
	if lookErr == nil {
		if abs, aerr := filepath.Abs(adapterResolved); aerr == nil {
			adapterResolved = abs
		}
		adapterPath = adapterResolved
	}

	// Load-or-create the config, then MERGE — never clobber.
	cfg := map[string]any{}
	raw, rerr := os.ReadFile(cfgPath)
	switch {
	case rerr == nil:
		if len(strings.TrimSpace(string(raw))) > 0 {
			if jerr := json.Unmarshal(raw, &cfg); jerr != nil {
				// Present but unparseable — protect the user's config; do NOT
				// overwrite it.
				return fmt.Errorf("existing Claude Desktop config is not valid JSON:\n  %s\n  (%v)\n"+
					"Refusing to overwrite it. Fix or remove the file, then re-run `c3-broker install-desktop`.", cfgPath, jerr)
			}
		}
	case os.IsNotExist(rerr):
		// Fresh install — create the parent dir; cfg stays an empty map.
		if mkErr := os.MkdirAll(filepath.Dir(cfgPath), 0o755); mkErr != nil {
			return fmt.Errorf("create config dir %s: %w", filepath.Dir(cfgPath), mkErr)
		}
	default:
		return fmt.Errorf("read %s: %w", cfgPath, rerr)
	}

	// Ensure an mcpServers object exists, preserving every other server. A null
	// mcpServers (valid JSON) is treated as "no servers" rather than an error.
	var servers map[string]any
	if existing, ok := cfg["mcpServers"]; ok && existing != nil {
		servers, ok = existing.(map[string]any)
		if !ok {
			return fmt.Errorf("existing %s has a non-object \"mcpServers\" value; refusing to modify it.\n"+
				"Fix the file, then re-run `c3-broker install-desktop`.", cfgPath)
		}
	} else {
		servers = map[string]any{}
	}
	servers["c3"] = map[string]any{"command": adapterPath}
	cfg["mcpServers"] = servers

	// json.MarshalIndent escapes backslashes in the (Windows) command path, so
	// the on-disk value is double-backslash-safe.
	out, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	out = append(out, '\n')
	sttInstall, err := prepareSTTHandlerInstall()
	if err != nil {
		return err
	}
	// Commit mappings first. An unused valid handler path is harmless if the
	// later Desktop config write fails; the inverse leaves Desktop configured
	// while the broker cannot find the voice runtime. The broker singleton held
	// by this command keeps live route/session mutations from racing this write.
	if err := sttInstall.commit(); err != nil {
		return err
	}
	// Preserve the existing file's mode (never widen a secrets-bearing 0600
	// config); default 0644 for a fresh file. Write to a temp sibling then rename
	// so a crash/disk-full mid-write can't truncate the config we work to protect.
	mode := os.FileMode(0o644)
	if fi, statErr := os.Stat(cfgPath); statErr == nil {
		mode = fi.Mode().Perm()
	}
	tmp := cfgPath + ".c3tmp"
	if err := os.WriteFile(tmp, out, mode); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, cfgPath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("write %s: %w", cfgPath, err)
	}
	if sttInstall.handler != "" {
		fmt.Printf("Recorded bundled STT handler:\n  %s\n\n", sttInstall.handler)
	}

	fmt.Printf("Wrote Claude Desktop MCP config:\n  %s\n\n", cfgPath)
	fmt.Printf("Merged entry:\n  mcpServers.c3 = {\"command\": %q}\n\n", adapterPath)

	if lookErr != nil {
		fmt.Fprintf(os.Stderr,
			"warning: %s not found on PATH — wrote the bare command %q.\n"+
				"         Claude Desktop requires an ABSOLUTE command path (per its docs), so\n"+
				"         build/install the adapter (make build && make install) and re-run this,\n"+
				"         or hand-edit the c3 entry's \"command\" to the adapter's full path.\n",
			adapterBin, adapterPath)
	}

	// Next steps.
	fmt.Println("Next steps:")
	fmt.Println("  1. Fully QUIT Claude Desktop (tray icon → Quit — closing the window is not")
	fmt.Println("     enough) and restart it so it re-reads the config and spawns the c3 server.")
	fmt.Println("  2. In a chat, tell Claude:  attach name=<topic>")
	fmt.Println("     then \"check my messages\" to pull anything waiting.")
	fmt.Println()
	fmt.Println("  Inbound is POLL-ONLY. Claude Desktop cannot surface a Telegram message on its")
	fmt.Println("  own — messages wait in C3's durable queue. Ask Claude to \"check messages\"")
	fmt.Println("  (it calls fetch_queue) to pull them; reply/react to send back.")
	fmt.Println()
	if runtime.GOOS == "windows" {
		fmt.Println("  Microsoft Store (MSIX) install? Edits to %APPDATA%\\Claude\\ are IGNORED —")
		fmt.Println("  the config that actually loads is under:")
		fmt.Println("    ...\\Packages\\Claude_*\\LocalCache\\Roaming\\Claude\\claude_desktop_config.json")
		fmt.Println("  Re-run with --config <that path>, or hand-edit it there.")
		fmt.Println()
	}

	// Verify adapter and broker on PATH (mirrors install-agy's tail).
	if lookErr != nil {
		fmt.Fprintf(os.Stderr, "warning: %s not on PATH — run: make build && make install\n", adapterBin)
	} else {
		fmt.Printf("adapter: %s\n", adapterResolved)
	}
	if p, err := lookPath(exeName("c3-broker")); err != nil {
		fmt.Fprintf(os.Stderr, "warning: c3-broker not on PATH — run: make build && make install\n")
	} else {
		fmt.Printf("broker:  %s\n", p)
	}

	return nil
}

func acquireDesktopInstallLock() (func(), error) {
	pidFile, err := broker.PidFilePath()
	if err != nil {
		return nil, fmt.Errorf("resolve broker lock before Desktop install: %w", err)
	}
	lock, err := broker.AcquireSingleton(pidFile)
	if err != nil {
		return nil, fmt.Errorf(
			"broker is running; quit active C3 CLI/Desktop sessions before install-desktop updates mappings, then retry: %w",
			err)
	}
	return lock.Release, nil
}

type sttHandlerInstall struct {
	handler      string
	mappingsPath string
	mappings     *mappings.MappingsFile
}

// prepareSTTHandlerInstall validates the mappings side before the Desktop
// config is changed. Cross-file atomicity is impossible, but a corrupt
// mappings file or missing runtime bundle must not produce a successful-looking
// Desktop entry whose voice path is already known to be dead.
func prepareSTTHandlerInstall() (sttHandlerInstall, error) {
	path, err := mappings.DefaultPath()
	if err != nil {
		return sttHandlerInstall{}, fmt.Errorf("resolve mappings path: %w", err)
	}
	mf, err := mappings.Read(path)
	switch {
	case err == nil:
	case os.IsNotExist(err):
		mf = &mappings.MappingsFile{
			SchemaVersion: 1,
			Channels:      map[string]mappings.ChannelConfig{},
			Mappings:      map[string]mappings.Mapping{},
		}
	default:
		return sttHandlerInstall{}, fmt.Errorf("read mappings before recording STT handler: %w", err)
	}
	if mf.Plugins == nil {
		mf.Plugins = map[string]map[string]any{}
	}
	cfg := mf.Plugins[stt.Name]
	if raw, exists := cfg["enabled"]; exists {
		enabled, ok := raw.(bool)
		if !ok {
			return sttHandlerInstall{}, fmt.Errorf("plugins.%s.enabled is not a boolean", stt.Name)
		}
		if !enabled {
			return sttHandlerInstall{}, nil
		}
	}
	if raw, exists := cfg["handler_path"]; exists {
		configured, ok := raw.(string)
		if !ok {
			return sttHandlerInstall{}, fmt.Errorf("plugins.%s.handler_path is not a string; refusing to overwrite it", stt.Name)
		}
		if strings.TrimSpace(configured) != "" {
			return sttHandlerInstall{}, nil
		}
	}
	handler := discoveredSTTHandlerPath()
	if handler == "" {
		return sttHandlerInstall{}, fmt.Errorf(
			"bundled STT handler not found; keep plugins/c3/stt beside the installed C3 binaries, run from a C3 source checkout, set C3_SRC_DIR, or configure plugins.%s.handler_path explicitly",
			stt.Name)
	}
	if cfg == nil {
		cfg = map[string]any{}
	}
	cfg["handler_path"] = handler
	mf.Plugins[stt.Name] = cfg
	return sttHandlerInstall{handler: handler, mappingsPath: path, mappings: mf}, nil
}

func (p sttHandlerInstall) commit() error {
	if p.mappings == nil {
		return nil
	}
	if err := desktopWriteMappingsFile(p.mappingsPath, p.mappings); err != nil {
		return fmt.Errorf("record bundled STT handler: %w", err)
	}
	return nil
}

func discoveredSTTHandlerPath() string {
	if root, ok := discoverSourceDir(); ok {
		path := filepath.Join(root, "plugins", "c3", "stt", "stt-handler.py")
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path
		}
	}
	if exe, err := desktopExecutablePath(); err == nil {
		if resolved, rerr := filepath.EvalSymlinks(exe); rerr == nil {
			exe = resolved
		}
		path := filepath.Join(filepath.Dir(exe), "plugins", "c3", "stt", "stt-handler.py")
		if info, err := os.Stat(path); err == nil && !info.IsDir() &&
			updater.ValidateSTTBundle(filepath.Dir(path)) == nil {
			return path
		}
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		path := filepath.Join(home, ".local", "share", "c3", "plugins", "c3", "stt", "stt-handler.py")
		if info, err := os.Stat(path); err == nil && !info.IsDir() &&
			updater.ValidateSTTBundle(filepath.Dir(path)) == nil {
			return path
		}
	}
	// A source checkout outside ~/src/c3 is common during development. The
	// installer is normally run from that checkout, so walk upward from CWD as
	// a second, explicit source-discovery route rather than recording a path
	// that does not exist.
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if isC3SourceDir(dir) {
			path := filepath.Join(dir, "plugins", "c3", "stt", "stt-handler.py")
			if info, err := os.Stat(path); err == nil && !info.IsDir() {
				return path
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// exeName appends the Windows executable suffix so PATH lookups match the real
// binary name on Windows (c3-desktop-adapter.exe) while staying bare elsewhere.
func exeName(base string) string {
	if runtime.GOOS == "windows" {
		return base + ".exe"
	}
	return base
}

// desktopConfigPath resolves Claude Desktop's config path for the current OS.
// An explicit override always wins. Windows/macOS/Linux each resolve a real
// default path; an unrecognized GOOS with no override returns a note asking for
// --config.
func desktopConfigPath(override string) (path string, note string, err error) {
	switch runtime.GOOS {
	case "windows":
		if override != "" {
			return override, "", nil
		}
		appData := os.Getenv("APPDATA")
		if appData == "" {
			return "", "", fmt.Errorf("APPDATA is not set; cannot locate Claude Desktop config. Pass --config <path>.")
		}
		return filepath.Join(appData, "Claude", "claude_desktop_config.json"), "", nil
	case "darwin":
		if override != "" {
			return override, "", nil
		}
		home, herr := os.UserHomeDir()
		if herr != nil {
			return "", "", herr
		}
		return filepath.Join(home, "Library", "Application Support", "Claude", "claude_desktop_config.json"), "", nil
	case "linux":
		if override != "" {
			return override, "", nil
		}
		// The official Claude Desktop Linux beta (2026-06) is an Electron app; its
		// userData dir follows XDG — $XDG_CONFIG_HOME/Claude, defaulting to
		// ~/.config/Claude. (Debian/Ubuntu official + the Arch AUR repackages all
		// land here.)
		cfgHome := os.Getenv("XDG_CONFIG_HOME")
		if cfgHome == "" {
			home, herr := os.UserHomeDir()
			if herr != nil {
				return "", "", herr
			}
			cfgHome = filepath.Join(home, ".config")
		}
		return filepath.Join(cfgHome, "Claude", "claude_desktop_config.json"), "", nil
	default:
		if override != "" {
			return override, "", nil
		}
		return "", "note: no default Claude Desktop config path is known for this OS.\n" +
			"      Pass --config <path> (or --path <path>) to point at the\n" +
			"      claude_desktop_config.json to write.\n", nil
	}
}

// parseDesktopConfigOverride pulls a `--config <path>` / `--path <path>` (or the
// `--config=<path>` / `--path=<path>` form) out of args. Empty if absent.
func parseDesktopConfigOverride(args []string) string {
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--config", "--path":
			if i+1 < len(args) {
				return args[i+1]
			}
		default:
			if v, ok := strings.CutPrefix(args[i], "--config="); ok {
				return v
			}
			if v, ok := strings.CutPrefix(args[i], "--path="); ok {
				return v
			}
		}
	}
	return ""
}
