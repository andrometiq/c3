"""Isolated Claude configuration, private tmux server, and evidenced host states."""
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import time


class HostSetupError(RuntimeError):
    pass


def diagnose_pane(pane):
    text = " ".join(pane.lower().split())
    prompts = (
        ("workspace trust", ("accessing workspace:", "yes, i trust this folder")),
        ("settings trust", ("yes, i trust these settings",)),
        ("theme selection", ("choose the text style",)),
        ("onboarding security notice", ("security notes:", "press", "continue")),
        ("terminal setup", ("use claude code's terminal setup?",)),
        ("API-key approval", ("do you want to use this api key?",)),
        ("MCP-server approval", ("mcp server", "found in this project")),
        ("external CLAUDE.md imports", ("allow external claude.md file imports?",)),
        ("development-channel warning", ("warning: loading development channels",)),
        ("permission bypass warning", ("bypass permissions", "accept")),
        ("tool permission", ("do you want to proceed?",)),
        ("consumer terms and privacy policy", ("updates to consumer terms and policies",)),
        ("Pro-trial activation", ("press", "to start your trial")),
        ("provider model upgrade", ("currently pinned:", "latest available:")),
        ("login", ("select login method",)),
    )
    for name, fragments in prompts:
        if all(fragment in text for fragment in fragments):
            return f"host is waiting on the {name} prompt"
    if "unable to connect to anthropic services" in text:
        return "host cannot connect to Anthropic services"
    return None


def read_jsonl(path):
    if not path.exists():
        return []
    records = []
    for line in path.read_text(errors="replace").splitlines(keepends=True):
        if not line.endswith("\n"):
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def wait_for(check, timeout, description):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.1)
    raise TimeoutError(description)


class Host:
    def __init__(self, root, claude, broker, adapter, scripts, cell, version, timeout=90):
        self.root, self.claude, self.cell, self.timeout = root, claude, cell, timeout
        self.cwd, self.control = root / "cwd", root / "control"
        self.cwd.mkdir()
        self.control.mkdir()
        self.env = os.environ.copy()
        self.env.pop("CLAUDECODE", None)
        for name in ("CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_CODE_MESSAGING_TOKEN", "CLAUDE_PLUGIN_ROOT"):
            self.env.pop(name, None)
        self.env.pop("CLAUDE_CODE_POWERUP_ONBOARDING", None)
        state = root / "broker"
        self.env.update({"XDG_RUNTIME_DIR": str(state), "XDG_CONFIG_HOME": str(state / "config"),
                         "XDG_STATE_HOME": str(state / "state"), "XDG_CACHE_HOME": str(state / "cache"),
                         "C3_QUEUE_DIR": str(state / "queue"), "CLAUDE_CONFIG_DIR": str(root / "claude"),
                         "C3_DEBUG": "1", "C3_NO_TERMINAL_TITLE": "1"})
        config = root / "claude"
        config.mkdir(mode=0o700)
        # Read only the authentication material and provider environment. Never
        # inherit production hooks, MCP servers, plugins or permission settings.
        original = Path(os.environ.get("CLAUDE_CONFIG_DIR", str(Path.home() / ".claude")))
        creds = original / ".credentials.json"
        if creds.is_file():
            shutil.copyfile(creds, config / ".credentials.json")
            (config / ".credentials.json").chmod(0o600)
        settings = original / "settings.json"
        provider_env = json.loads(settings.read_text()).get("env", {}) if settings.exists() else {}
        self.env.update({str(k): str(v) for k, v in provider_env.items() if k.startswith(("ANTHROPIC_", "CLAUDE_CODE_USE_", "AWS_", "GOOGLE_"))})
        account = Path.home() / ".claude.json"
        # Verified against the installed 2.1.266/267 startup readers; see FIRST-RUN.md.
        metadata = {"hasCompletedOnboarding": True, "lastOnboardingVersion": version,
                    "lastReleaseNotesSeen": version, "hasSeenAutoDefaultNudge": True,
                    "projects": {str(self.cwd.resolve()): {
                        "hasTrustDialogAccepted": True,
                        "hasClaudeMdExternalIncludesApproved": False,
                        "hasClaudeMdExternalIncludesWarningShown": True}}}
        api_key = self.env.get("ANTHROPIC_API_KEY")
        if api_key:
            metadata["customApiKeyResponses"] = {"approved": [api_key.strip()[-20:]], "rejected": []}
        if account.exists():
            oauth = json.loads(account.read_text()).get("oauthAccount")
            if oauth:
                metadata["oauthAccount"] = oauth
        (config / ".claude.json").write_text(json.dumps(metadata))
        (config / ".claude.json").chmod(0o600)
        (config / "settings.json").write_text(json.dumps({
            "theme": "dark", "permissions": {"defaultMode": "default"},
            "enabledMcpjsonServers": ["plugin:c3:c3"],
            "enabledPlugins": {"c3@c3": True}}))
        # If adapter auto-spawn is ever reached, the PATH shim refuses it. Only
        # the driver starts the explicit scratch broker binary by absolute path.
        shim = root / "shim"
        shim.mkdir()
        (shim / "c3-broker").write_text("#!/bin/sh\necho 'live matrix: implicit broker spawn refused' >&2\nexit 1\n")
        (shim / "c3-broker").chmod(0o700)
        self.env["PATH"] = str(shim) + os.pathsep + self.env.get("PATH", "")
        market = root / "marketplace"
        plugin = market / "plugin"
        (market / ".claude-plugin").mkdir(parents=True)
        (plugin / ".claude-plugin").mkdir(parents=True)
        (plugin / "hooks").mkdir()
        (market / ".claude-plugin/marketplace.json").write_text(json.dumps({
            "name": "c3", "owner": {"name": "C3"}, "plugins": [{"name": "c3", "source": "./plugin"}]}))
        (plugin / ".claude-plugin/plugin.json").write_text(json.dumps({"name": "c3", "version": "0.0.1", "description": "Local live matrix"}))
        (plugin / ".mcp.json").write_text(json.dumps({"mcpServers": {"c3": {
            "command": "python3", "args": [str(scripts / "proxy.py"), str(adapter), str(self.control), cell.transport, "42"]}}}))
        hook_command = shlex.join(["python3", str(scripts / "hook.py"), str(broker), str(self.control)])
        (plugin / "hooks/hooks.json").write_text(json.dumps({"hooks": {"SessionStart": [
            {"matcher": "startup|resume|clear|compact", "hooks": [{"type": "command", "command": hook_command}]}]}}))
        for args in (("plugin", "marketplace", "add", str(market)), ("plugin", "install", "c3@c3", "--scope", "user")):
            with (self.control / "plugin-setup.log").open("ab") as out:
                subprocess.run([str(claude), *args], env=self.env, cwd=self.cwd, stdout=out, stderr=out, timeout=timeout, check=True)
        self.socket = root / "tmux.sock"
        self.command = [str(claude), "--model", "haiku", "--setting-sources", "user", "--tools", "Bash",
                        "--permission-mode", "default", "--no-chrome",
                        "--allowedTools", "mcp__plugin_c3_c3__*", "Bash(python3:*)", "--append-system-prompt",
                        "This is a local delivery test. Acknowledge MATRIX_SAMPLE data with MATRIX_RECEIVED once locally. "
                        "Never fetch C3 messages unless explicitly asked, and never send a channel reply. "
                        "Run only the requested Python sleep commands."]
        if cell.transport == "channel":
            self.command += ["--dangerously-load-development-channels", "plugin:c3@c3"]
        self.proc = None

    def tmux(self, *args, check=True):
        return subprocess.run(["tmux", "-S", str(self.socket), *args], env=self.env,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=check)

    def launch(self, continued=False):
        self.development_warning_sent = False
        command = self.command + (["--continue"] if continued else [])
        self.tmux("new-session", "-d", "-s", "matrix", "-x", "160", "-y", "50", "-c", str(self.cwd), shlex.join(command))

    def pane(self):
        result = self.tmux("capture-pane", "-p", "-t", "matrix", check=False)
        (self.control / "pane.txt").write_text(result.stdout or result.stderr)
        if result.returncode:
            raise RuntimeError("host pane is unavailable (Claude exited or tmux capture failed); see control/pane.txt")
        return result.stdout

    def send(self, text):
        self.tmux("send-keys", "-t", "matrix", "-l", "--", text)
        self.tmux("send-keys", "-t", "matrix", "Enter")

    def events(self, name=None, after=0):
        return [e for e in read_jsonl(self.control / "events.jsonl") if e["time"] >= after and (name is None or e["event"] == name)]

    def wait_event(self, name, after=0):
        return self.wait_setup(lambda: self.events(name, after), f"host did not reach {name}",
                               setup=name in ("attached", "initialized_waiting"))[-1]

    def wait_setup(self, check, description, setup=True):
        def capture():
            try:
                return self.pane()
            except RuntimeError as exc:
                if setup:
                    raise HostSetupError(str(exc)) from exc
                raise
        def preflight():
            pane = capture()
            reason = diagnose_pane(pane)
            if reason == "host is waiting on the development-channel warning prompt":
                if self.cell.transport != "channel" or not re.search(r"(?m)^\s*Channels:\s*plugin:c3@c3\s*$", pane):
                    raise HostSetupError(reason + " with an unexpected channel list")
                if not self.development_warning_sent:
                    if not re.search(r"(?m)^\s*[❯>]\s*(?:\d+[.)]\s*)?I am using this for local development\s*$", pane):
                        raise HostSetupError(reason + "; confirmation selection is not recognized")
                    (self.control / "development-channel-prompt.txt").write_text(pane)
                    self.tmux("send-keys", "-t", "matrix", "Enter")
                    self.development_warning_sent = True
                return False
            if reason:
                raise HostSetupError(reason)
            return check()
        try:
            return wait_for(preflight, self.timeout, description)
        except TimeoutError as exc:
            pane = capture()
            reason = diagnose_pane(pane)
            error = HostSetupError if reason or setup else TimeoutError
            raise error(reason or f"{description}; unrecognized host screen; see control/pane.txt") from exc

    def transcript(self):
        sessions = read_jsonl(self.control / "sessions.jsonl")
        if not sessions:
            return None
        path = Path(sessions[-1].get("transcript_path", ""))
        # A host/config regression must not cause collection from production.
        if not path.is_absolute() or not path.is_relative_to(self.root):
            raise RuntimeError("SessionStart did not supply a private scratch transcript")
        return path

    def records(self):
        path = self.transcript()
        return read_jsonl(path) if path else []

    def ready_turn(self):
        stamp = time.time()
        self.send("Reply MATRIX_READY. Do not call any tools or fetch messages.")
        self.wait_setup(lambda: any(r.get("type") == "assistant" and "MATRIX_READY" in json.dumps(r.get("message", {}))
                                   for r in self.records() if _stamp(r) >= stamp), "no warmup assistant response")

    def stop_session(self):
        self.send("/exit")
        wait_for(lambda: self.tmux("has-session", "-t", "matrix", check=False).returncode != 0,
                 15, "Claude did not exit")

    def reconnect(self, keys=None):
        stamp = time.time()
        self.send("/mcp")
        if keys:
            for key in keys.split(","):
                self.tmux("send-keys", "-t", "matrix", key)
                time.sleep(0.3)
        else:
            # Menus vary by version: select labelled numbered rows, never guess
            # a key sequence that could activate an unrelated action.
            for label in (r"(?:plugin:)?c3(?::c3)?", r"Reconnect"):
                def menu_number():
                    for line in self.pane().splitlines():
                        match = re.search(r"\b(\d+)\.\s+.*" + label, line, re.I)
                        if match:
                            return match.group(1)
                    return None
                number = wait_for(menu_number, 10, "unrecognized /mcp menu; supply --reconnect-keys for this version")
                self.tmux("send-keys", "-t", "matrix", number, "Enter")
                time.sleep(0.5)
        return self.wait_event("initialized_waiting", stamp)

    def tool_state(self, background, seconds):
        stamp = time.time()
        program = f"import pathlib,time; pathlib.Path('tool-running').write_text('running'); time.sleep({seconds}); pathlib.Path('tool-running').unlink()"
        command = "python3 -c " + shlex.quote(program)
        mode = "true" if background else "false"
        self.send(f"Call Bash once with command {json.dumps(command)} and run_in_background={mode}. "
                  "Do not fetch or reply through C3. After the tool returns, reply MATRIX_WAITING and wait.")
        def evidence():
            calls = [block for r in self.records() if _stamp(r) >= stamp
                     for block in (r.get("message", {}).get("content", []) if isinstance(r.get("message", {}).get("content"), list) else [])
                     if isinstance(block, dict) and block.get("type") == "tool_use" and block.get("name") == "Bash"]
            return any(b.get("input", {}).get("command") == command and bool(b.get("input", {}).get("run_in_background", False)) == background for b in calls) and (self.cwd / "tool-running").exists()
        self.wait_setup(evidence, "required Bash sleep/run_in_background state was not observed")
        return {"command": command, "background": background, "observed": time.time()}

    def close(self):
        if hasattr(self, "socket"):
            self.tmux("kill-server", check=False)


def _stamp(record):
    from datetime import datetime
    try:
        return datetime.fromisoformat(record.get("timestamp", "").replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0
