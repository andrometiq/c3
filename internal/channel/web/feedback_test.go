package web

import (
	"encoding/json"
	"os/exec"
	"regexp"
	"testing"
)

func runFeedbackNode(t *testing.T, cases string, result any) {
	t.Helper()
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not on PATH; the executable Drive feedback test requires Node.js")
	}
	page, err := pages.ReadFile("page.html")
	if err != nil {
		t.Fatal(err)
	}
	helperPattern := regexp.MustCompile(`(?s)(const feedbackNotices = \{.*?\n  if \(globalThis\.C3FeedbackTestShim\) \{\n    globalThis\.c3FeedbackTest = \{[^\n]+\};\n  \})`)
	match := helperPattern.FindSubmatch(page)
	if len(match) != 2 {
		t.Fatal("could not extract pure Drive feedback helpers from page.html")
	}
	const constants = `
const DRIVE_LOCK_DISTANCE_PX = 60;
const DRIVE_CANCEL_ARM_PX = 96;
const DRIVE_CANCEL_CONFIRM_MS = 5000;
`
	command := exec.Command(nodePath, "-e", "globalThis.C3FeedbackTestShim = true;\n"+constants+string(match[1])+"\n"+cases)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("node Drive feedback execution failed: %v\n%s", err, output)
	}
	if err := json.Unmarshal(output, result); err != nil {
		t.Fatalf("decode node Drive feedback output: %v\n%s", err, output)
	}
}

func TestFeedbackLookupWithNode(t *testing.T) {
	const cases = `
const api = globalThis.c3FeedbackTest;
const expectedKinds = [
  'no-session', 'session-returned', 'permission-held', 'permission-returned',
  'connected', 'disconnected', 'reconnected', 'live-on', 'live-off', 'muted',
  'unmuted', 'hold-start', 'locked', 'sending', 'sent', 'reply-ready',
  'speaking-start', 'speaking-end', 'listening-start', 'speech-end', 'barge-in',
  'reply-timeout', 'document', 'cancel-armed', 'send-cancelled',
  'recording-cancelled', 'draft-saved', 'draft-found', 'send-refused', 'error'
];
const kinds = api.feedbackKinds();
const routine = [
  'disconnected', 'reconnected', 'hold-start', 'locked', 'sending', 'sent',
  'reply-ready', 'speaking-start', 'speaking-end', 'listening-start',
  'speech-end', 'barge-in', 'cancel-armed', 'error'
];
const safety = [
  'send-cancelled', 'recording-cancelled', 'no-session', 'session-returned',
  'permission-held', 'permission-returned', 'send-refused', 'muted', 'unmuted',
  'draft-saved', 'draft-found', 'reply-timeout'
];
const stateWords = ['connected', 'live-on', 'live-off', 'document'];
const feltRepresentatives = [
  'hold-start', 'speaking-start', 'muted', 'locked', 'connected', 'sent',
  'reply-ready', 'no-session', 'document', 'send-cancelled', 'send-refused', 'barge-in'
];
const notices = kinds.map(kind => ({kind, notice: api.feedbackFor(kind)}));
const feltPatterns = feltRepresentatives.map(kind => api.feedbackFor(kind).vibe);
const feltPatternSet = new Set(feltPatterns.map(pattern => JSON.stringify(pattern)));
process.stdout.write(JSON.stringify({
  missingKinds: expectedKinds.filter(kind => !kinds.includes(kind)),
  unexpectedKinds: kinds.filter(kind => !expectedKinds.includes(kind)),
  missing: notices.filter(({notice}) => {
    return !notice || typeof notice.earcon !== 'string' || !('vibe' in notice) ||
      typeof notice.speak !== 'string' || typeof notice.safety !== 'boolean';
  }).map(({kind}) => kind),
  safetyFlagMismatch: notices.filter(({kind, notice}) => {
    return notice && notice.safety !== safety.includes(kind);
  }).map(({kind}) => kind),
  safetyMuted: safety.filter(kind => {
    return !api.noticeShouldSpeak(api.feedbackFor(kind), false, true);
  }),
  stateMuted: stateWords.filter(kind => {
    return api.noticeShouldSpeak(api.feedbackFor(kind), true, true);
  }),
  stateDisabled: stateWords.filter(kind => {
    return api.noticeShouldSpeak(api.feedbackFor(kind), false, false);
  }),
  stateEnabledSilent: stateWords.filter(kind => {
    return !api.noticeShouldSpeak(api.feedbackFor(kind), true, false);
  }),
  routineSpoken: routine.filter(kind => api.feedbackFor(kind).speak),
  safetySilent: safety.filter(kind => !api.feedbackFor(kind).speak),
  documentSpeak: api.feedbackFor('document').speak,
  unknownFelt: notices.filter(({notice}) => {
    return notice && !feltPatternSet.has(JSON.stringify(notice.vibe));
  }).map(({kind}) => kind),
  feltCount: feltPatterns.length,
  distinctFeltCount: new Set(feltPatterns.map(pattern => JSON.stringify(pattern))).size
}));
`
	var result struct {
		MissingKinds       []string `json:"missingKinds"`
		UnexpectedKinds    []string `json:"unexpectedKinds"`
		Missing            []string `json:"missing"`
		SafetyFlagMismatch []string `json:"safetyFlagMismatch"`
		SafetyMuted        []string `json:"safetyMuted"`
		StateMuted         []string `json:"stateMuted"`
		StateDisabled      []string `json:"stateDisabled"`
		StateEnabledSilent []string `json:"stateEnabledSilent"`
		RoutineSpoken      []string `json:"routineSpoken"`
		SafetySilent       []string `json:"safetySilent"`
		DocumentSpeak      string   `json:"documentSpeak"`
		UnknownFelt        []string `json:"unknownFelt"`
		FeltCount          int      `json:"feltCount"`
		DistinctFeltCount  int      `json:"distinctFeltCount"`
	}
	runFeedbackNode(t, cases, &result)
	if len(result.MissingKinds) != 0 || len(result.UnexpectedKinds) != 0 {
		t.Errorf("feedback kind set differs: missing=%v unexpected=%v", result.MissingKinds, result.UnexpectedKinds)
	}
	if len(result.Missing) != 0 {
		t.Errorf("feedback kinds missing or malformed: %v", result.Missing)
	}
	if len(result.SafetyFlagMismatch) != 0 {
		t.Errorf("feedback kinds have the wrong safety flag: %v", result.SafetyFlagMismatch)
	}
	if len(result.SafetyMuted) != 0 {
		t.Errorf("safety feedback was gated by mute: %v", result.SafetyMuted)
	}
	if len(result.StateMuted) != 0 || len(result.StateDisabled) != 0 {
		t.Errorf("state feedback spoke while gated: muted=%v disabled=%v", result.StateMuted, result.StateDisabled)
	}
	if len(result.StateEnabledSilent) != 0 {
		t.Errorf("state feedback did not speak while enabled: %v", result.StateEnabledSilent)
	}
	if len(result.RoutineSpoken) != 0 {
		t.Errorf("routine feedback unexpectedly speaks: %v", result.RoutineSpoken)
	}
	if len(result.SafetySilent) != 0 {
		t.Errorf("safety feedback is silent: %v", result.SafetySilent)
	}
	if result.DocumentSpeak != "Document received" {
		t.Errorf("document feedback speech=%q", result.DocumentSpeak)
	}
	if len(result.UnknownFelt) != 0 {
		t.Errorf("feedback kinds use an unclassified vibration pattern: %v", result.UnknownFelt)
	}
	if result.DistinctFeltCount != result.FeltCount {
		t.Errorf("felt vibration patterns are not distinct: %d/%d", result.DistinctFeltCount, result.FeltCount)
	}
}

func TestCancelDecisionWithNode(t *testing.T) {
	const cases = `
const decide = globalThis.c3FeedbackTest.cancelDecision;
process.stdout.write(JSON.stringify({
  sideways: decide({upward: 0, downward: 0, horizontal: 60, armed: false, released: false}),
  smallDown: decide({upward: -50, downward: 50, horizontal: 0, armed: false, released: false}),
  arm: decide({upward: -96, downward: 96, horizontal: 40, armed: false, released: false}),
  armEqual: decide({upward: -96, downward: 96, horizontal: 96, armed: false, released: false}),
  lock: decide({upward: 60, downward: -60, horizontal: 20, armed: false, released: false}),
  releaseInside: decide({upward: -110, downward: 110, horizontal: 10, armed: true, released: true, insideZone: true}),
  releaseOutside: decide({upward: -110, downward: 110, horizontal: 10, armed: true, released: true, insideZone: false}),
  unarmedInside: decide({upward: -110, downward: 110, horizontal: 10, armed: false, released: true, insideZone: true}),
  horizontalInside: decide({upward: -110, downward: 110, horizontal: 120, armed: true, released: true, insideZone: true}),
  moveBack: decide({upward: -50, downward: 50, horizontal: 10, armed: true, released: false}),
  sidewaysDisarm: decide({upward: -100, downward: 100, horizontal: 120, armed: true, released: false})
}));
`
	result := map[string]string{}
	runFeedbackNode(t, cases, &result)
	want := map[string]string{
		"sideways": "none", "smallDown": "none", "arm": "arm", "armEqual": "arm",
		"lock": "lock", "releaseInside": "cancel", "releaseOutside": "none", "unarmedInside": "none",
		"horizontalInside": "none", "moveBack": "disarm", "sidewaysDisarm": "disarm",
	}
	for name, expected := range want {
		if result[name] != expected {
			t.Errorf("%s decision=%q, want %q", name, result[name], expected)
		}
	}
}

func TestUnsentWorkWithNode(t *testing.T) {
	const cases = `
const has = globalThis.c3FeedbackTest.unsentWork;
const empty = {
  recording: false, recordingStarting: false, handsFree: false, liveSendBeat: false,
  pendingCount: 0, undurableDraft: false, draftWriteInFlight: false
};
process.stdout.write(JSON.stringify({
  recording: has({...empty, recording: true}),
  starting: has({...empty, recordingStarting: true}),
  handsFree: has({...empty, handsFree: true}),
  liveSendBeat: has({...empty, liveSendBeat: true}),
  retry: has({...empty, pendingCount: 1}),
  undurableDraft: has({...empty, undurableDraft: true}),
  draftWrite: has({...empty, draftWriteInFlight: true}),
  thinking: has({...empty, state: 'thinking'}),
  speaking: has({...empty, state: 'speaking'})
}));
`
	result := map[string]bool{}
	runFeedbackNode(t, cases, &result)
	for _, name := range []string{"recording", "starting", "handsFree", "liveSendBeat", "retry", "undurableDraft", "draftWrite"} {
		if !result[name] {
			t.Errorf("%s did not count as unsent work", name)
		}
	}
	for _, name := range []string{"thinking", "speaking"} {
		if result[name] {
			t.Errorf("%s without pending work triggered leave guard", name)
		}
	}
}

func TestLiveBeatSurvivesHideWithNode(t *testing.T) {
	const cases = `
const action = globalThis.c3FeedbackTest.hideVoiceAction;
process.stdout.write(JSON.stringify({
  beat: action({liveSendBeat: true, handsFree: false}),
  beatWins: action({liveSendBeat: true, handsFree: true}),
  capture: action({liveSendBeat: false, handsFree: true}),
  recording: action({liveSendBeat: false, handsFree: false})
}));
`
	result := map[string]string{}
	runFeedbackNode(t, cases, &result)
	want := map[string]string{
		"beat": "deliver-live-beat", "beatWins": "deliver-live-beat",
		"capture": "finish-hands-free", "recording": "preserve-recording",
	}
	for name, expected := range want {
		if result[name] != expected {
			t.Errorf("%s hide action=%q, want %q", name, result[name], expected)
		}
	}
}

func TestDraftDecisionWithNode(t *testing.T) {
	const cases = `
const decide = globalThis.c3FeedbackTest.draftDecision;
process.stdout.write(JSON.stringify({short: decide(4999), boundary: decide(5000), long: decide(12000)}));
`
	result := map[string]string{}
	runFeedbackNode(t, cases, &result)
	want := map[string]string{"short": "discard", "boundary": "draft", "long": "draft"}
	for name, expected := range want {
		if result[name] != expected {
			t.Errorf("%s draft decision=%q, want %q", name, result[name], expected)
		}
	}
}
