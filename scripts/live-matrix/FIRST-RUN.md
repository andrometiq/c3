# Isolated Claude first-run audit

Audited by reading the installed `~/.local/share/claude/versions/2.1.266`
and `2.1.267` binaries as data, including their embedded JavaScript defaults,
settings schema, startup step list and dialog implementations. No Claude session
was started. `~/.claude.json` confirmed the per-project trust/import fields;
`~/.claude/settings.json` confirmed the settings/metadata split. No credential
file was read during the audit. Byte offsets below are decimal offsets in those
two binaries, respectively; they identify the actual readers, not guessed keys.

The solution reuses `Host`'s JSON seed, pane capture and polling loop and the
collector's existing verdict. It needs only the Python standard library.

| Gate | Isolated configuration / action | Installed evidence (2.1.266 / 2.1.267) |
| --- | --- | --- |
| Workspace/folder trust, including project hooks and permission grants | `.claude.json`: `projects[resolved scratch cwd].hasTrustDialogAccepted = true`. No parent directory is trusted. | Trust reader at 180543211 / 181732551; TrustDialog in `chunk-30hyvtf9.js` / `chunk-bhzwp1js.js`. The newer dialog explicitly focuses **No, exit**. |
| First onboarding: theme, connectivity/login introduction, security notice, terminal setup, Powerup discovery | `.claude.json`: `hasCompletedOnboarding = true`, `lastOnboardingVersion = selected version`; `settings.json`: `theme = "dark"`. Remove inherited `CLAUDE_CODE_POWERUP_ONBOARDING`, which can force onboarding despite the completed bit. Existing authenticated account metadata is still copied. | Startup guard at 192079374 / 193284140; Onboarding in `chunk-vv452557.js` / `chunk-s3zbjkgv.js`. `lastOnboardingVersion` records completion; the boolean is the guard. `theme` is a settings key (2.1.267 schema at 179687158), not an onboarding acceptance by itself. |
| Release notes / first-use update summary | `.claude.json`: `lastReleaseNotesSeen = selected version`. | Summary reader at 202235070 / 203370023. This is a nonblocking banner, not another mandatory confirmation; no other changelog acceptance key was found. |
| MCP-server approval (including project plugin servers) | `settings.json`: `enabledMcpjsonServers = ["plugin:c3:c3"]`. | Approval reader at 186081235 / 185935310. The startup server enumeration includes plugin server names; 2.1.267 `Alt`/`xnn` at 192938057. `enableAllProjectMcpServers` is real but unnecessary: the harness approves only its own server. |
| Plugin enablement / marketplace installation | `settings.json`: `enabledPlugins["c3@c3"] = true`; reuse the existing local marketplace add and user-scope install. Project settings are excluded by `--setting-sources user`. | Both settings schemas contain `enabledPlugins`; the retained `control/plugin-setup.log` confirms both commands succeeded. No additional installed-user-plugin trust acceptance key was found. |
| External CLAUDE.md imports | `.claude.json`, same exact project: `hasClaudeMdExternalIncludesApproved = false`, `hasClaudeMdExternalIncludesWarningShown = true`. This records a decline, keeping external imports disabled. | Guard at 188016830 / 189259222; dialog in `chunk-wsjpdtjg.js` / `chunk-xt5n0p00.js` writes the approval choice plus the warning-shown bit. |
| First tool permission | Keep `--tools Bash` and the existing `--allowedTools mcp__plugin_c3_c3__* Bash(python3:*)`; set `settings.json` `permissions.defaultMode = "default"` and `--permission-mode default`. | Both binaries' CLI definitions and settings schemas contain these options. The existing allowlist already grants the actual harness calls; no broader permission mode is needed. |
| Auto-mode first-use nudge / opt-in and dangerous bypass disclaimer | `.claude.json`: `hasSeenAutoDefaultNudge = true`; explicitly select default permission mode. Do not enable auto or bypass modes. | Nudge guard at 200670306 / 200853634. Both schemas define `skipAutoPermissionPrompt` and `skipDangerousModePermissionPrompt`, but neither acceptance is needed in default mode. Legacy `bypassPermissionsModeAccepted` migrates to the latter settings key. |
| Chrome integration onboarding / auto-enable offer | `--no-chrome`. | CLI option at 192133512 / 193338284; startup gates in the same step list as onboarding. The real metadata keys are `hasCompletedClaudeInChromeOnboarding` and `claudeInChromeDefaultEnabled`; the native disable option avoids both optional flows. |
| Environment API-key approval | When `ANTHROPIC_API_KEY` is supplied, `.claude.json`: `customApiKeyResponses.approved` contains its trimmed last 20 characters, the host's own selector. Never store the full environment key. This metadata file is mode 0600 within private scratch, like the existing copied authentication material. | Selector at 180616295 / 181809126, plus `customApiKeyResponses` approval reader and ApproveApiKey dialog. The selector is authentication material and is never exported. |
| Development-channel warning | **No persisted acceptance key exists in these startup steps.** For channel cells only, capture the exact warning and `Channels: plugin:c3@c3`, then press Enter only when the pane visibly selects `I am using this for local development`. Send once per launch; retain the triggering pane in private control evidence. Unknown channels or selections fail. | Unconditional dialog after the channel/provider/policy checks at 192082615 / 193287381. DevChannelsDialog in `chunk-tr17m4zp.js` / `chunk-5rendrtq.js` only invokes `onAccept`; it writes no configuration. |

The [settings documentation](https://code.claude.com/docs/en/settings) also
distinguishes user settings from `.claude.json` project/account state. The
installed versions, rather than the changing online schema, are the authority
for this seed. Old `hasCompletedProjectOnboarding` and
`projectOnboardingSeenCount` are removed by the installed migration and are not
suppression keys for these versions.

## Account, provider and policy limits

The complete startup step lists also contain these conditional screens. They
are not safely replaced by invented local acceptance state:

- Consumer terms/privacy (Grove): `groveConfigCache` only decides whether to
  fetch/display the dialog. Its actual acceptance uses server-side
  `grove_enabled` and `grove_notice_viewed_at` through API calls. A cache entry
  would not be evidence of consent. Verified in `chunk-8ck8t3bd.js` /
  `chunk-k7mvhnee.js`; 2.1.267 gate at 192773872, dialog at 205256536.
- Pro-trial activation: `oauthAccount.ccOnboardingFlags.e10` and
  `oauthAccount.claudeCodeTrialEndsAt` reflect account state. Starting a trial
  calls `/api/oauth/organizations/:orgUUID/claude_code/pro_trial`. Do not forge
  these fields. Verified in `chunk-r2ndf8m0.js` / `chunk-nrry4zta.js`;
  2.1.267 state reader at 202520049.
- Bedrock/Vertex upgrades: `bedrockDeclinedUpgrades[tier]` and
  `vertexDeclinedUpgrades[tier]` suppress a **specific** `fromKey-to-toKey`
  proposal, not all proposals. Candidate discovery performs provider API
  probes. No provider-independent skip key was found. These do not apply to
  the first-party OAuth configuration in the retained run. Do not change the
  provider or manufacture a candidate map. Verified in both startup step
  lists, 2.1.267 candidate/decline handling at 193290206 and key encoding at
  205063426. Provider availability notices in that list auto-dismiss after
  1.5 or 4 seconds and are not interactive gates.
- Invalid/expired credentials, enforced organization settings, and newly
  introduced policy/settings trust screens remain prerequisites or errors.
  A completed local onboarding bit cannot repair authentication or override
  managed policy. No external account state or provider probes were checked.

These recognized screens produce specific setup failures. Unknown screens and
an exited host retain a bounded diagnostic failure with `control/pane.txt` as
private evidence. The harness does not claim a live run has validated this
audit. The maintainer must run both installed versions, including the channel
warning fallback, and handle any account-specific prerequisites.

## Failure reporting

Setup polling reads the visible pane, excluding scrollback so an old prompt
cannot trigger another acknowledgment. A recognized blocking prompt fails on
the first poll. On a timeout the pane is captured again; an unknown screen is
explicitly reported as unknown. The workspace trust dialog never receives keys.

A setup failure records one primary reason in `summary.json` and `REPORT.md`.
Every delivery assertion is listed under `not_evaluated` / `NOT EVALUATED`, and
the delivery verdict is not called. Collection failures remain in diagnostic
evidence without creating delivery failures. An ordinary post-setup run error
still evaluates the existing delivery assertions. Hermetic tests exercise the
real constructor with mocked plugin commands and the run/collection/report
failure path with mocked host and broker processes.
