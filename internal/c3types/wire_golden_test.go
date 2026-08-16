package c3types

// wire_golden_test.go — ASYMMETRIC format tests.
//
// Why this file exists, in one paragraph. Every other test in this package (and
// in the 169 test files across the tree) is a SYMMETRIC round-trip: marshal a
// struct, unmarshal it back into the same struct, compare. That shape is
// invariant under a key rename BY CONSTRUCTION — rename ChatID's tag to
// "chat_id" and a symmetric test still passes, because both the writer and the
// reader moved together. Meanwhile the queue on disk did not move, and neither
// did the third-party adapters parsing the IPC frames. So the whole existing
// suite would bless the exact change that orphans users' queued messages.
//
// The tests below are deliberately one-directional, and they are the only kind
// that can fail on a rename:
//
//	forward  — struct  -> bytes, compared against key names written out as
//	           LITERALS in this source file. Nothing derives them from the
//	           structs, so the structs cannot vote on their own correctness.
//	backward — bytes   -> struct, from a byte-for-byte capture of a REAL queue
//	           line taken off this machine. Nothing regenerates it, so it cannot
//	           drift toward whatever the code happens to do today.
//
// If you are reading this because a test here failed: the failure is the point.
// Do not update the literals to match the new output. The literals ARE the
// contract; the code is what has to move back.

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 1. MARSHAL-FORWARD — frozen key sets
// ---------------------------------------------------------------------------

// wireGolden is one wire-bearing type, a FULLY-POPULATED instance of it, and the
// exact set of top-level JSON keys marshaling that instance must produce.
//
// "Fully populated" is load-bearing: every omitempty field is given a non-zero
// value so that the key set below is the COMPLETE set, not the subset that
// happens to survive omitempty. A field that lost its tag, gained a renamed one,
// or was deleted outright shows up as a diff against `keys`.
type wireGolden struct {
	// name is the Go type name; it must match the type name in the source, and
	// TestEveryExportedStructIsPinnedByAGolden enforces that this table has a
	// row for every exported struct in the package.
	name string
	// value is a fully-populated instance.
	value any
	// keys is the frozen literal expectation, written by hand. NEVER generate
	// this from the struct — that reintroduces the symmetry this file exists to
	// break.
	keys []string
	// omitEmpty is the frozen list of fields that are ALLOWED to disappear when
	// they hold their zero value, i.e. exactly the fields tagged `,omitempty`.
	// Every other key in `keys` must be present unconditionally.
	//
	// This exists because a fully-populated instance cannot detect omitempty
	// being ADDED to a field — the value is non-zero, so the key shows up either
	// way. Presence semantics are part of the format: a reader that has always
	// seen "Text" and suddenly does not is looking at a different format, even
	// though no key was renamed. Pinning the omitempty surface separately is the
	// only way to catch that.
	omitEmpty []string
}

// mustInt64 / mustInt exist only to take addresses of literals inline.
func ptrInt64(v int64) *int64 { return &v }
func ptrInt(v int) *int       { return &v }

// wireGoldens is the frozen surface. Order within `keys` is irrelevant (JSON
// objects are unordered); the comparison is set-based, so reordering struct
// fields is correctly NOT a failure while renaming one is.
func wireGoldens() []wireGolden {
	ts := time.Date(2026, 7, 25, 14, 44, 4, 0, time.UTC)

	return []wireGolden{
		// -- types.go ------------------------------------------------------
		{
			name: "Inbound",
			value: Inbound{
				Channel:       "telegram",
				ChatID:        -1001,
				TopicID:       ptrInt64(7),
				MessageID:     42,
				Sender:        Sender{UserID: 1, Username: "u"},
				Text:          "t",
				Attachments:   []Attachment{{Kind: "voice"}},
				ReplyTo:       &ReplyContext{MessageID: 41},
				Timestamp:     ts,
				MediaGroupID:  "album-7",
				ForwardOrigin: &ForwardOrigin{Kind: "user", Name: "Alice"},
				Merged:        []MergedSource{{MessageID: 42, Sender: Sender{UserID: 1}, Text: "t"}},
				Kind:          InboundPollResult,
				Event:         &InboundEvent{PollResult: &PollResult{PollID: "p"}},
				DrainedFrom:   "telegram__-1001__7",
				V:             InboundRecordVersion,
				ConvKind:      "group",
				Edited:        true,
			},
			keys: []string{
				"Channel", "ChatID", "TopicID", "MessageID", "Sender", "Text",
				"Attachments", "ReplyTo", "Timestamp", "MediaGroupID", "ForwardOrigin", "Merged", "Kind", "Event",
				"DrainedFrom", "V", "ConvKind", "Edited",
			},
			omitEmpty: []string{"MediaGroupID", "ForwardOrigin", "Merged", "Kind", "Event", "DrainedFrom", "V", "ConvKind", "Edited"},
		},
		{
			name: "InboundEvent",
			value: InboundEvent{
				PollResult: &PollResult{PollID: "p"},
				Reaction:   &ReactionEvent{MessageID: 1},
				Callback:   &CallbackEvent{CallbackID: "c"},
				System:     &SystemEvent{Source: "telegram"},
			},
			keys:      []string{"PollResult", "Reaction", "Callback", "System"},
			omitEmpty: []string{"PollResult", "Reaction", "Callback", "System"},
		},
		{
			name:      "SystemEvent",
			value:     SystemEvent{Source: "telegram", Level: "warn", Title: "T", Message: "M"},
			keys:      []string{"Source", "Level", "Title", "Message"},
			omitEmpty: nil,
		},
		{
			name: "HealthEvent",
			value: HealthEvent{
				Channel: "telegram", State: HealthStateDown, Since: ts,
				Consec: 3, Reason: "dial failures", DownFor: 90 * time.Second,
			},
			keys:      []string{"Channel", "State", "Since", "Consec", "Reason", "DownFor"},
			omitEmpty: nil,
		},
		{
			name: "PollResult",
			value: PollResult{
				PollID: "p", Question: "q", TotalVoters: 5, IsClosed: true,
				Options: []PollOptionTally{{Text: "a", VoterCount: 5}},
			},
			keys:      []string{"PollID", "Question", "TotalVoters", "IsClosed", "Options"},
			omitEmpty: nil,
		},
		{
			name:      "PollOptionTally",
			value:     PollOptionTally{Text: "a", VoterCount: 5},
			keys:      []string{"Text", "VoterCount"},
			omitEmpty: nil,
		},
		{
			name: "ReactionEvent",
			value: ReactionEvent{
				MessageID: 42, Actor: Sender{UserID: 1, Username: "u"},
				Added: []string{"👍"}, Removed: []string{"👎"},
			},
			keys:      []string{"MessageID", "Actor", "Added", "Removed"},
			omitEmpty: nil,
		},
		{
			name: "CallbackEvent",
			value: CallbackEvent{
				CallbackID: "cb", MessageID: 42,
				Actor: Sender{UserID: 1, Username: "u"}, Data: "approve",
			},
			keys:      []string{"CallbackID", "MessageID", "Actor", "Data"},
			omitEmpty: nil,
		},
		{
			name:      "Sender",
			value:     Sender{UserID: 85720317, Username: "u"},
			keys:      []string{"UserID", "Username"},
			omitEmpty: nil,
		},
		{
			name:      "ForwardOrigin",
			value:     ForwardOrigin{Kind: "user", Name: "Alice"},
			keys:      []string{"Kind", "Name"},
			omitEmpty: []string{"Name"},
		},
		{
			name: "MergedSource",
			value: MergedSource{
				MessageID: 42, Sender: Sender{UserID: 1, Username: "u"}, Text: "t",
				ForwardOrigin: &ForwardOrigin{Kind: "user", Name: "Alice"},
			},
			keys:      []string{"MessageID", "Sender", "Text", "ForwardOrigin"},
			omitEmpty: []string{"Text", "ForwardOrigin"},
		},
		{
			name: "Attachment",
			// MIME, not Mime and not mime. FileID, not file_id.
			value:     Attachment{Kind: "voice", FileID: "F", Size: 1024, MIME: "audio/ogg", Name: "n.ogg", SourceMessageID: 42},
			keys:      []string{"Kind", "FileID", "Size", "MIME", "Name", "SourceMessageID"},
			omitEmpty: []string{"SourceMessageID"},
		},
		{
			// ReplyContext is the type behind the "ReplyTo" key on Inbound. The
			// KEY is ReplyTo; the TYPE is ReplyContext. Both are pinned — the key
			// on the Inbound row above, the type's own fields here.
			name:      "ReplyContext",
			value:     ReplyContext{MessageID: 41, User: Sender{UserID: 2, Username: "v"}, Text: "prior"},
			keys:      []string{"MessageID", "User", "Text"},
			omitEmpty: nil,
		},
		{
			name: "Outbound",
			value: Outbound{
				Channel: "telegram", ChatID: -1001, TopicID: ptrInt64(7), Text: "t",
				Markup: MarkupMarkdown,
				Media:  []MediaItem{{Kind: MediaPhoto, Path: "/tmp/x.png"}},
				Poll:   &PollSpec{Question: "q"},
				// ReplyTo here is a MESSAGE ID, not a route and not a user. The
				// Go name does not say "MessageID", which is exactly why a
				// name-based audit misses it and why it is pinned explicitly.
				ReplyTo: ptrInt64(41),
				Buttons: [][]Button{{{Text: "ok", Data: "d"}}},
			},
			keys: []string{
				"Channel", "ChatID", "TopicID", "Text", "Markup", "Media",
				"Poll", "ReplyTo", "Buttons",
			},
			omitEmpty: []string{"Buttons"},
		},
		{
			// Data and URL are both omitempty and are mutually exclusive in real
			// use; both are set here purely so the full key set materializes.
			name:      "Button",
			value:     Button{Text: "ok", Data: "d", URL: "https://example.invalid"},
			keys:      []string{"Text", "Data", "URL"},
			omitEmpty: []string{"Data", "URL"},
		},
		{
			name: "EditArgs",
			value: EditArgs{
				Channel: "telegram", ChatID: -1001, MessageID: 42, Text: "t",
				Markup: MarkupMarkdown, Buttons: [][]Button{{{Text: "ok", Data: "d"}}},
			},
			keys:      []string{"Channel", "ChatID", "MessageID", "Text", "Markup", "Buttons"},
			omitEmpty: []string{"Buttons"},
		},
		{
			name:      "EditResult",
			value:     EditResult{MessageID: 42},
			keys:      []string{"MessageID"},
			omitEmpty: nil,
		},
		{
			name:      "ReactArgs",
			value:     ReactArgs{Channel: "telegram", ChatID: -1001, MessageID: 42, Emoji: "👍"},
			keys:      []string{"Channel", "ChatID", "MessageID", "Emoji"},
			omitEmpty: nil,
		},
		{
			// Same ReplyTo-is-a-message-id trap as Outbound.
			name:      "ReadbackArgs",
			value:     ReadbackArgs{ChatID: -1001, ReplyTo: ptrInt64(42), TopicID: ptrInt64(7), Transcript: "words"},
			keys:      []string{"ChatID", "ReplyTo", "TopicID", "Transcript"},
			omitEmpty: nil,
		},
		{
			name: "VoicePayload",
			value: VoicePayload{
				Channel: "telegram", ChatID: -1001, TopicID: ptrInt64(7),
				MessageID: 42, FileID: "F", MIME: "audio/ogg", Size: 1024,
			},
			keys:      []string{"Channel", "ChatID", "TopicID", "MessageID", "FileID", "MIME", "Size"},
			omitEmpty: nil,
		},

		// -- caps.go -------------------------------------------------------
		{
			name: "Capabilities",
			value: Capabilities{
				Channel: "telegram", RichText: true,
				MaxMessageRunes: 4096, MaxMessageRunesSource: 3500, MaxCaptionRunes: 1024,
				AutoChunks:      true,
				MediaKinds:      []MediaKind{MediaPhoto, MediaFile},
				CompressedPhoto: true, OriginalFile: true, Albums: true,
				MaxSendBytes: 50 << 20,
				Polls:        true, Reactions: true, ReactionsSingle: true,
				EditMessages: true, Threads: true, Typing: true,
				ExpandableQuotes: true, InlineKeyboards: true,
				RichMessages: true, RichTables: true,
				Inbound: InboundCaps{MaxDownloadBytes: 20 << 20},
				Stream:  StreamCaps{StreamViaEdit: false, MinEditInterval: time.Second},
			},
			keys: []string{
				"Channel", "RichText", "MaxMessageRunes", "MaxMessageRunesSource",
				"MaxCaptionRunes", "AutoChunks", "MediaKinds", "CompressedPhoto",
				"OriginalFile", "Albums", "MaxSendBytes", "Polls", "Reactions",
				"ReactionsSingle", "EditMessages", "Threads", "Typing",
				"ExpandableQuotes", "InlineKeyboards", "RichMessages", "RichTables",
				// "Inbound" is the FIELD name, not the type name (InboundCaps).
				"Inbound", "Stream",
			},
			omitEmpty: nil,
		},
		{
			name: "InboundCaps",
			value: InboundCaps{
				MaxDownloadBytes:     20 << 20,
				InboundKinds:         []MediaKind{MediaVoice},
				SupportsReplyContext: true, DeliversPollResults: true,
				DeliversReactions: true, DeliversCallbacks: true,
				DeliversRichMessages: true,
			},
			keys: []string{
				"MaxDownloadBytes", "InboundKinds", "SupportsReplyContext",
				"DeliversPollResults", "DeliversReactions", "DeliversCallbacks",
				"DeliversRichMessages",
			},
			omitEmpty: nil,
		},
		{
			name:      "StreamCaps",
			value:     StreamCaps{StreamViaEdit: true, MinEditInterval: time.Second},
			keys:      []string{"StreamViaEdit", "MinEditInterval"},
			omitEmpty: nil,
		},
		{
			name: "MediaItem",
			// URL, not Url.
			value:     MediaItem{Kind: MediaPhoto, Path: "/tmp/x.png", URL: "https://example.invalid/x.png", Caption: "c", Spoiler: true},
			keys:      []string{"Kind", "Path", "URL", "Caption", "Spoiler"},
			omitEmpty: nil,
		},
		{
			name: "PollSpec",
			// CloseDateUnix / OpenPeriodSec read like Telegram-isms; they are the
			// wire keys regardless and are pinned verbatim.
			value: PollSpec{
				Question: "q", Options: []string{"a", "b"},
				Anonymous: true, MultipleAnswers: true,
				Kind: PollQuiz, CorrectOption: ptrInt(0), Explanation: "because",
				OpenPeriodSec: 60, CloseDateUnix: 1_800_000_000,
			},
			keys: []string{
				"Question", "Options", "Anonymous", "MultipleAnswers", "Kind",
				"CorrectOption", "Explanation", "OpenPeriodSec", "CloseDateUnix",
			},
			omitEmpty: nil,
		},
		{
			name:      "Alteration",
			value:     Alteration{Kind: "text_split", Detail: "split into 2 messages"},
			keys:      []string{"Kind", "Detail"},
			omitEmpty: nil,
		},
	}
}

// TestMarshalForward_FrozenWireKeys marshals a fully-populated instance of every
// wire-bearing type and compares the resulting top-level key set against the
// literal list frozen in wireGoldens().
//
// WHAT IT CATCHES that a round-trip cannot:
//   - a json tag renamed (ChatID -> "chat_id"): reported as MISSING "ChatID" +
//     UNEXPECTED "chat_id", with an explicit RENAME line naming both.
//   - a tag deleted, so the Go field name silently becomes the key again and a
//     later Go-level rename would move the wire format.
//   - a field removed from the struct (a reader of an existing record loses it).
//   - omitempty added to a field that was previously always present, which
//     silently drops the key from records whose value is zero.
//
// A field REORDER is intentionally not a failure: JSON objects are unordered and
// no reader can depend on member order.
func TestMarshalForward_FrozenWireKeys(t *testing.T) {
	for _, g := range wireGoldens() {
		g := g
		t.Run(g.name, func(t *testing.T) {
			raw, err := json.Marshal(g.value)
			if err != nil {
				t.Fatalf("marshal %s: %v", g.name, err)
			}
			var got map[string]json.RawMessage
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("%s did not marshal to a JSON object: %v\ngot: %s", g.name, err, raw)
			}

			want := map[string]bool{}
			for _, k := range g.keys {
				want[k] = true
			}

			var missing, unexpected []string
			for k := range want {
				if _, ok := got[k]; !ok {
					missing = append(missing, k)
				}
			}
			for k := range got {
				if !want[k] {
					unexpected = append(unexpected, k)
				}
			}
			sort.Strings(missing)
			sort.Strings(unexpected)

			if len(missing) == 0 && len(unexpected) == 0 {
				return
			}

			t.Errorf("%s: the on-the-wire key set changed — this is a FORMAT BREAK, not a test that needs updating.", g.name)
			for _, k := range missing {
				t.Errorf("  MISSING   %q — records already written carry this key; dropping it means every existing record loses that field on read, and every new record is unreadable by shipped adapters that require it.", k)
			}
			for _, k := range unexpected {
				t.Errorf("  UNEXPECTED %q — nothing on disk or on the IPC wire has ever carried this key.", k)
			}
			if len(missing) > 0 && len(unexpected) > 0 {
				t.Errorf("  RENAME?   %v was replaced by %v. If this is a Go-level field rename, the FIX IS TO KEEP THE OLD json TAG: `json:%q` on the renamed field. The wire key is the contract; the Go identifier is not.",
					missing, unexpected, missing[0])
			}
			t.Errorf("  frozen expectation (%d keys): %v\n  actual marshal output: %s", len(g.keys), g.keys, raw)
		})
	}
}

// TestMarshalForward_FrozenOmitEmptySurface pins PRESENCE semantics, which the
// fully-populated test above structurally cannot: with every field set, a key
// appears whether or not it is tagged omitempty.
//
// It marshals the ZERO value of each type and asserts the key set equals
// keys − omitEmpty. That makes "which fields may vanish when empty" an explicit,
// frozen part of the contract.
//
// WHAT IT CATCHES:
//   - omitempty ADDED to an existing field. Nothing renames, nothing errors, and
//     every symmetric round-trip still passes — but a key that every shipped
//     record and every third-party parser has always seen now disappears
//     whenever the value is zero. Readers doing presence checks break silently.
//   - omitempty REMOVED. The mirror image: every newly written record gains a
//     key (e.g. "V":0) that no existing record carries, so this build's output
//     stops being byte-comparable with what is already on disk — an unannounced
//     format change, which is the exact failure Inbound.V's doc comment warns
//     about.
func TestMarshalForward_FrozenOmitEmptySurface(t *testing.T) {
	for _, g := range wireGoldens() {
		g := g
		t.Run(g.name, func(t *testing.T) {
			// A zero value of the same type as the populated instance.
			zero := reflect.New(reflect.TypeOf(g.value)).Elem().Interface()
			raw, err := json.Marshal(zero)
			if err != nil {
				t.Fatalf("marshal zero %s: %v", g.name, err)
			}
			var got map[string]json.RawMessage
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("zero %s did not marshal to a JSON object: %v\ngot: %s", g.name, err, raw)
			}

			mayVanish := map[string]bool{}
			for _, k := range g.omitEmpty {
				mayVanish[k] = true
			}
			// Guard the table itself: an omitEmpty entry that is not in keys is a
			// typo, and a typo here silently weakens the assertion.
			inKeys := map[string]bool{}
			for _, k := range g.keys {
				inKeys[k] = true
			}
			for _, k := range g.omitEmpty {
				if !inKeys[k] {
					t.Errorf("wireGoldens()[%s].omitEmpty lists %q, which is not in keys — fix the table, not the code", g.name, k)
				}
			}

			for _, k := range g.keys {
				_, present := got[k]
				switch {
				case mayVanish[k] && present:
					t.Errorf("%s.%s is pinned as omitempty but its key was WRITTEN on a zero value (%s). "+
						"Dropping omitempty means every record this build writes carries a key no existing record has — "+
						"an unannounced format change.", g.name, k, raw)
				case !mayVanish[k] && !present:
					t.Errorf("%s.%s is pinned as UNCONDITIONAL but its key vanished on a zero value (%s). "+
						"Adding omitempty renames nothing and errors nowhere, yet a key every shipped record and every "+
						"third-party parser has always seen now disappears whenever the value is zero.", g.name, k, raw)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 2. UNMARSHAL-BACKWARD — a real captured queue line
// ---------------------------------------------------------------------------

// liveQueueLineCaptured20260725 is a BYTE-FOR-BYTE capture of a single line from
// a live .jsonl inbound queue on the maintainer's machine, taken 2026-07-25.
// It was copied out of the queue file; it was NOT produced by marshaling these
// structs, and it must NEVER be regenerated that way.
//
// PROVENANCE MATTERS MORE THAN TIDINESS. This literal is the only artifact in
// the test suite that was written by a build that predates the explicit json
// tags — i.e. by the format that is actually sitting on users' disks right now.
// The moment someone "cleans it up", regenerates it, prettifies it, or swaps its
// values for round numbers, it stops being evidence and becomes another mirror
// of the current code, and this file loses the one test that can prove the old
// format still reads.
//
// DO NOT EDIT, REFORMAT, REGENERATE, OR REPLACE THIS CONSTANT. If a field must
// be added to Inbound, add a SECOND constant for the new-format sample and leave
// this one exactly as it is — old records do not get rewritten in place, so the
// old shape has to keep decoding forever.
//
// Note for reviewers/PII audit: this is real captured traffic (real chat id,
// user ids, handle, and message text) retained deliberately for provenance.
const liveQueueLineCaptured20260725 = `{"Channel":"telegram","ChatID":-1003990699908,"TopicID":3826,"MessageID":6675,"Sender":{"UserID":85720317,"Username":"skarthi"},"Text":"Need to steal as much ideas and conventions from this.","Attachments":null,"ReplyTo":{"MessageID":3826,"User":{"UserID":1205071350,"Username":"OCDWaterBot"},"Text":""},"Timestamp":"2026-07-25T14:44:04Z"}`

// TestUnmarshalBackward_LiveQueueLine decodes the captured line and asserts that
// EVERY field landed in the right place, including the nested Sender and the
// nested ReplyTo (which has a Sender of its own).
//
// WHAT IT CATCHES: any tag rename, at the point where it actually hurts. Rename
// ChatID's tag and this test reports `ChatID = 0, want -1003990699908` — the
// literal damage, in the literal units of the bug: a queued message whose route
// has been zeroed and can therefore never be delivered. A symmetric round-trip
// reports nothing at all.
//
// The assertions are individual `if`s, not a `switch`, so a rename that breaks
// six fields reports six lines rather than stopping at the first.
func TestUnmarshalBackward_LiveQueueLine(t *testing.T) {
	var in Inbound
	if err := json.Unmarshal([]byte(liveQueueLineCaptured20260725), &in); err != nil {
		t.Fatalf("a real, already-queued line no longer decodes at all: %v", err)
	}

	if in.Channel != "telegram" {
		t.Errorf("Channel = %q, want %q — the queued record cannot be routed to a channel", in.Channel, "telegram")
	}
	// Negative id = supergroup, per Telegram's sign convention. A zero here means
	// the key stopped matching and the message is undeliverable.
	if in.ChatID != -1003990699908 {
		t.Errorf("ChatID = %d, want -1003990699908 — a zeroed chat id means this queued message can never be delivered", in.ChatID)
	}
	if in.TopicID == nil {
		t.Errorf("TopicID = nil, want 3826 — a nil topic silently re-routes a forum-topic message to the DM/root")
	} else if *in.TopicID != 3826 {
		t.Errorf("TopicID = %d, want 3826", *in.TopicID)
	}
	if in.MessageID != 6675 {
		t.Errorf("MessageID = %d, want 6675 — dedup and reply-quoting both key on this", in.MessageID)
	}
	if in.Text != "Need to steal as much ideas and conventions from this." {
		t.Errorf("Text = %q, want the captured text — the message body itself was lost", in.Text)
	}
	if in.Attachments != nil {
		t.Errorf("Attachments = %v, want nil (the line carries an explicit null)", in.Attachments)
	}

	// Nested Sender.
	if in.Sender.UserID != 85720317 {
		t.Errorf("Sender.UserID = %d, want 85720317 — this feeds the allowlist trust boundary", in.Sender.UserID)
	}
	if in.Sender.Username != "skarthi" {
		t.Errorf("Sender.Username = %q, want %q", in.Sender.Username, "skarthi")
	}

	// Nested ReplyTo, including ITS nested Sender under the key "User".
	if in.ReplyTo == nil {
		t.Fatalf("ReplyTo = nil — the quote-reply context was dropped entirely; check that the key is still spelled %q", "ReplyTo")
	}
	if in.ReplyTo.MessageID != 3826 {
		t.Errorf("ReplyTo.MessageID = %d, want 3826", in.ReplyTo.MessageID)
	}
	if in.ReplyTo.User.UserID != 1205071350 {
		t.Errorf("ReplyTo.User.UserID = %d, want 1205071350 — note the key is %q, not %q", in.ReplyTo.User.UserID, "User", "Sender")
	}
	if in.ReplyTo.User.Username != "OCDWaterBot" {
		t.Errorf("ReplyTo.User.Username = %q, want %q", in.ReplyTo.User.Username, "OCDWaterBot")
	}
	if in.ReplyTo.Text != "" {
		t.Errorf("ReplyTo.Text = %q, want empty", in.ReplyTo.Text)
	}

	if want := time.Date(2026, 7, 25, 14, 44, 4, 0, time.UTC); !in.Timestamp.Equal(want) {
		t.Errorf("Timestamp = %v, want %v", in.Timestamp, want)
	}

	// Fields that did not exist when this line was written must read as their
	// zero values — not as errors, and not as garbage.
	if in.Kind != InboundMessage {
		t.Errorf("Kind = %q, want %q — a legacy line is an ordinary message", in.Kind, InboundMessage)
	}
	if in.Event != nil {
		t.Errorf("Event = %+v, want nil", in.Event)
	}
	if in.DrainedFrom != "" {
		t.Errorf("DrainedFrom = %q, want empty (organic, never drained)", in.DrainedFrom)
	}
	if in.ConvKind != "" {
		t.Errorf("ConvKind = %q, want empty — the channel that wrote this line predates the field", in.ConvKind)
	}
}

// TestUnmarshalBackward_ReEncodeKeepsEveryCapturedKey closes the write half of
// the loop: after decoding the captured line, re-encoding must still emit every
// key the captured line carried, spelled identically — at the top level AND
// inside Sender and ReplyTo.
//
// The expectation is derived from the CAPTURED BYTES, not from the structs, so
// it stays honest as fields are added. Extra keys are fine (additive change);
// a dropped or renamed key is not.
func TestUnmarshalBackward_ReEncodeKeepsEveryCapturedKey(t *testing.T) {
	var captured map[string]json.RawMessage
	if err := json.Unmarshal([]byte(liveQueueLineCaptured20260725), &captured); err != nil {
		t.Fatalf("captured line is not valid JSON: %v", err)
	}

	var in Inbound
	if err := json.Unmarshal([]byte(liveQueueLineCaptured20260725), &in); err != nil {
		t.Fatalf("decode captured line: %v", err)
	}
	out, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("re-encode: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("re-encoded record is not a JSON object: %v", err)
	}

	topLevel := make([]string, 0, len(captured))
	for k := range captured {
		topLevel = append(topLevel, k)
	}
	sort.Strings(topLevel)
	for _, k := range topLevel {
		if _, ok := got[k]; !ok {
			t.Errorf("re-encoding dropped top-level key %q that the captured line carried — a rewrite of the queue file would silently strip it from every record\n  captured: %s\n  re-encoded: %s",
				k, liveQueueLineCaptured20260725, out)
		}
	}

	// Nested objects: Sender and ReplyTo (and ReplyTo.User inside it).
	for _, nested := range []struct{ key string }{{"Sender"}, {"ReplyTo"}} {
		var wantKeys, gotKeys map[string]json.RawMessage
		if err := json.Unmarshal(captured[nested.key], &wantKeys); err != nil {
			t.Fatalf("captured %s is not an object: %v", nested.key, err)
		}
		if err := json.Unmarshal(got[nested.key], &gotKeys); err != nil {
			t.Fatalf("re-encoded %s is not an object: %v", nested.key, err)
		}
		for k := range wantKeys {
			if _, ok := gotKeys[k]; !ok {
				t.Errorf("re-encoding dropped %s.%q — nested keys are part of the same frozen contract\n  captured: %s\n  re-encoded: %s",
					nested.key, k, captured[nested.key], got[nested.key])
			}
		}
	}
}

// ---------------------------------------------------------------------------
// 3. VERSION SEMANTICS + FORWARD COMPATIBILITY
// ---------------------------------------------------------------------------

// effectiveRecordVersion states the "absent V means version 1" contract in one
// place, the way a real reader must implement it. If this helper ever needs a
// different rule, that IS the format change.
func effectiveRecordVersion(in Inbound) int {
	if in.V == 0 {
		return InboundRecordVersion
	}
	return in.V
}

// TestAbsentVersionReadsAsVersionOne pins the only workable reading of a format
// that shipped with no version marker at all: records already on disk have NO
// "V" key, there is no migration pass that could add one, so absent MUST mean
// version 1.
//
// WHAT IT CATCHES: someone bumping InboundRecordVersion to 2 without a migration
// story, or "fixing" V by dropping omitempty (which would stamp "V":0 onto every
// newly written record — itself an unannounced format change), or a reader that
// starts treating V==0 as "unknown version, reject".
func TestAbsentVersionReadsAsVersionOne(t *testing.T) {
	var legacy Inbound
	if err := json.Unmarshal([]byte(liveQueueLineCaptured20260725), &legacy); err != nil {
		t.Fatalf("decode captured line: %v", err)
	}
	if legacy.V != 0 {
		t.Errorf("V = %d on a line that has no \"V\" key at all, want 0", legacy.V)
	}
	if got := effectiveRecordVersion(legacy); got != 1 {
		t.Errorf("a record with no V key reads as version %d, want 1 — every record already on disk is version 1 and nothing will ever rewrite them", got)
	}

	// The absent-V record must not gain a "V" key on write either, or the next
	// build's output stops being byte-comparable with what shipped.
	out, err := json.Marshal(Inbound{Channel: "telegram", ChatID: -1001, MessageID: 1})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("unmarshal own output: %v", err)
	}
	if _, present := m["V"]; present {
		t.Errorf("an unset V was WRITTEN as a key (%s) — omitempty is required, otherwise every record this build writes differs from every record already on disk", out)
	}
}

// TestUnknownFutureKeyIsIgnored proves forward compatibility: a record written
// by a NEWER build — higher V, plus keys this build has never heard of, at the
// top level and nested — must decode best-effort rather than erroring.
//
// WHY IT MATTERS: a partially-updated install (new broker, old adapter, one
// shared queue directory) is a normal, expected state. If the decoder used
// DisallowUnknownFields, or rejected V > known, that cosmetic skew would become
// permanent message loss — the exact outcome the version marker exists to make
// survivable.
//
// WHAT IT CATCHES: anyone adding DisallowUnknownFields to a decoder path, or
// adding a strict `if in.V > InboundRecordVersion { return error }` guard.
func TestUnknownFutureKeyIsIgnored(t *testing.T) {
	const fromTheFuture = `{"Channel":"telegram","ChatID":-1003990699908,"TopicID":3826,"MessageID":9001,` +
		`"Sender":{"UserID":85720317,"Username":"u","FutureSenderField":"ignore me"},` +
		`"Text":"written by a newer build","Attachments":null,"ReplyTo":null,` +
		`"Timestamp":"2026-07-25T14:44:04Z","V":99,"ConvKind":"group",` +
		`"SomeFieldInventedLater":{"nested":[1,2,3]},"AnotherOne":true}`

	var in Inbound
	if err := json.Unmarshal([]byte(fromTheFuture), &in); err != nil {
		t.Fatalf("a record from a newer build was REJECTED: %v\n"+
			"A mixed-version install sharing one queue directory is normal. Rejecting unknown keys turns that into permanent message loss.", err)
	}

	if in.V != 99 {
		t.Errorf("V = %d, want 99 — a higher version must be preserved, not clamped", in.V)
	}
	if got := effectiveRecordVersion(in); got != 99 {
		t.Errorf("effective version = %d, want 99", got)
	}
	if in.Text != "written by a newer build" {
		t.Errorf("Text = %q — the known fields must survive alongside the unknown ones", in.Text)
	}
	if in.ChatID != -1003990699908 || in.MessageID != 9001 {
		t.Errorf("routing fields lost: ChatID=%d MessageID=%d", in.ChatID, in.MessageID)
	}
	if in.Sender.UserID != 85720317 {
		t.Errorf("Sender.UserID = %d — an unknown key INSIDE a nested object must not poison its known siblings", in.Sender.UserID)
	}
	if in.ConvKind != "group" {
		t.Errorf("ConvKind = %q, want %q", in.ConvKind, "group")
	}
}

// ---------------------------------------------------------------------------
// 4. REFLECTION GUARD — the project rule, enforced
// ---------------------------------------------------------------------------

// pkgPath is this package's import path; the tag walk only enforces the rule on
// types declared HERE (stdlib types like time.Time are not ours to police).
var pkgPath = reflect.TypeOf(Inbound{}).PkgPath()

// TestEveryExportedFieldTagEqualsGoFieldName is the project rule turned into a
// build failure: every exported field of every type in this package must carry
// an explicit json tag whose NAME is byte-identical to the Go field name.
//
// The rule exists because, before the tags, encoding/json fell back to the Go
// field name — so the Go identifiers WERE the on-disk queue format and the IPC
// wire format. The tags decouple them: the Go name is now free to change while
// the key stays put. That only holds while every field is tagged.
//
// WHAT IT CATCHES:
//   - a NEW field added with no tag (the Go name silently becomes the key again,
//     re-arming the original trap for the next rename).
//   - a tag "tidied" to snake_case or camelCase — the single most likely way
//     this format gets broken, because it looks like hygiene.
//   - a tag that followed a Go-level rename instead of staying behind.
func TestEveryExportedFieldTagEqualsGoFieldName(t *testing.T) {
	seen := map[reflect.Type]bool{}

	var walk func(rt reflect.Type, path string)
	walk = func(rt reflect.Type, path string) {
		for rt.Kind() == reflect.Ptr || rt.Kind() == reflect.Slice ||
			rt.Kind() == reflect.Array || rt.Kind() == reflect.Map {
			rt = rt.Elem()
		}
		if rt.Kind() != reflect.Struct || rt.PkgPath() != pkgPath || seen[rt] {
			return
		}
		seen[rt] = true

		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			if f.PkgPath != "" { // unexported — never serialized
				continue
			}
			tag, ok := f.Tag.Lookup("json")
			if !ok {
				t.Errorf("%s.%s has NO json tag. Without one, encoding/json uses the Go field name as the wire key, "+
					"which means the next rename of this identifier silently changes the on-disk queue format. Add `json:%q`.",
					rt.Name(), f.Name, f.Name)
				continue
			}
			name, opts := tag, ""
			if idx := strings.Index(tag, ","); idx >= 0 {
				name, opts = tag[:idx], tag[idx:]
			}
			if name == "" {
				t.Errorf("%s.%s is tagged json:%q — an options-only tag leaves the KEY defaulting to the Go field name. "+
					"Write the name explicitly: `json:%q`.", rt.Name(), f.Name, tag, f.Name+opts)
				continue
			}
			if name != f.Name {
				t.Errorf("%s.%s is tagged json:%q but the wire key MUST be %q (byte-identical to the Go field name). "+
					"If you renamed the Go field, the tag must NOT follow the rename — the old key is the contract, and every "+
					"record already queued on disk is spelled with it.", rt.Name(), f.Name, tag, f.Name)
			}
			walk(f.Type, path+"."+f.Name)
		}
	}

	for _, g := range wireGoldens() {
		walk(reflect.TypeOf(g.value), g.name)
	}

	if len(seen) == 0 {
		t.Fatal("the tag walk visited zero structs — the guard is not actually guarding anything")
	}
}

// TestEveryExportedStructIsPinnedByAGolden keeps the two tests above honest as
// the package grows. reflect cannot enumerate a package's types at runtime, so a
// hand-written golden table silently stops covering anything added after it was
// written. This parses the package's own source and fails if any exported struct
// declared in a non-test file has no row in wireGoldens().
//
// WHAT IT CATCHES: the realistic decay mode — someone adds a new wire-bearing
// struct, the frozen-key test keeps passing (it just never looks at the new
// type), and the format grows an unpinned corner. Here, adding a struct without
// adding a golden row is a failure with instructions attached.
func TestEveryExportedStructIsPinnedByAGolden(t *testing.T) {
	pinned := map[string]bool{}
	for _, g := range wireGoldens() {
		rt := reflect.TypeOf(g.value)
		for rt.Kind() == reflect.Ptr {
			rt = rt.Elem()
		}
		if rt.Name() != g.name {
			t.Errorf("wireGoldens() row %q holds a value of type %q — the name field must match the Go type", g.name, rt.Name())
		}
		pinned[g.name] = true
	}

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi fs.FileInfo) bool {
		return strings.HasSuffix(fi.Name(), ".go") && !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse package source: %v", err)
	}

	var declared []string
	for _, pkg := range pkgs {
		for fileName, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				ts, ok := n.(*ast.TypeSpec)
				if !ok {
					return true
				}
				if ts.Assign.IsValid() { // type alias (e.g. ReplyArgs = Outbound)
					return true
				}
				if _, isStruct := ts.Type.(*ast.StructType); !isStruct {
					return true
				}
				if !ts.Name.IsExported() {
					return true
				}
				declared = append(declared, ts.Name.Name)
				if !pinned[ts.Name.Name] {
					t.Errorf("exported struct %s (declared in %s) has NO row in wireGoldens(). "+
						"Every struct in this package is wire-bearing until proven otherwise, and an unpinned struct is a corner of the "+
						"format that a rename can move without any test noticing. Add a row: a fully-populated instance plus its exact "+
						"key set written out as literals.",
						ts.Name.Name, fileName)
				}
				return true
			})
		}
	}

	if len(declared) == 0 {
		t.Fatal("found no exported structs in the package source — the source scan is broken, so this guard is not guarding anything")
	}

	sort.Strings(declared)
	for name := range pinned {
		if sort.SearchStrings(declared, name) == len(declared) || declared[sort.SearchStrings(declared, name)] != name {
			t.Errorf("wireGoldens() pins %q, but no exported struct by that name is declared in the package source — "+
				"if the type was deleted, its wire shape is gone too; that is a format change, not a cleanup", name)
		}
	}
}

// TestEveryExportedFieldIsPinnedByAGolden closes the FIELD-level decay hole that
// the type-level guard above cannot see.
//
// TestEveryExportedStructIsPinnedByAGolden proves every wire-bearing TYPE has a
// golden row. It says nothing about whether that row covers every FIELD of the
// type, and TestMarshalForward_FrozenWireKeys cannot make up the difference: it
// compares the marshal output of the hand-written `value` against `keys`, so a
// field that is (a) tagged `,omitempty` and (b) left at its zero value in `value`
// emits no key, is absent from `keys`, and the comparison balances perfectly.
//
// That is not a hypothetical. Inbound.V and Inbound.ConvKind — the two fields
// added when this freeze was written — are both `,omitempty` fields appended to
// an existing struct, i.e. exactly this shape. Verified against the suite: adding
// a third one and pinning nothing leaves all eight tests green while a brand-new
// key rides out onto the wire with no golden covering it. Six months later it is
// indistinguishable from a field that was always frozen, and the next "tidy this
// tag" pass moves it for free.
//
// SCOPE, deliberately narrow — this test asserts COVERAGE ONLY:
//
//	it checks that each field's wire key APPEARS in `keys`; it does NOT check
//	what that key is spelled, and it does NOT check omitempty status.
//
// The narrowness is the design, not laziness. The value of a golden is that a
// human wrote the expectation down; any assertion derived from the structs that
// could be silenced by editing the table trades that away. So spelling stays with
// TestMarshalForward_FrozenWireKeys (output vs. hand-written literals) and
// presence semantics stay with TestMarshalForward_FrozenOmitEmptySurface (zero
// value vs. keys − omitEmpty). This test only refuses to let a field escape those
// two entirely. The three compose: coverage here, spelling there, semantics
// there — and a newly added field must be written into `keys` (this test), then
// into `omitEmpty` if it is optional (that test), before the suite goes green.
//
// Nested types need no recursion: every struct declared in this package has its
// own golden row (enforced above), so walking each row's own fields covers all of
// them exactly once.
func TestEveryExportedFieldIsPinnedByAGolden(t *testing.T) {
	checked := 0

	for _, g := range wireGoldens() {
		g := g
		t.Run(g.name, func(t *testing.T) {
			rt := reflect.TypeOf(g.value)
			for rt.Kind() == reflect.Ptr {
				rt = rt.Elem()
			}
			if rt.Kind() != reflect.Struct {
				t.Fatalf("wireGoldens() row %q does not hold a struct", g.name)
			}

			pinned := make(map[string]bool, len(g.keys))
			for _, k := range g.keys {
				pinned[k] = true
			}

			for i := 0; i < rt.NumField(); i++ {
				f := rt.Field(i)
				if f.PkgPath != "" { // unexported — never serialized
					continue
				}
				// An embedded field is FLATTENED by encoding/json: its inner keys
				// are promoted to this object, so this golden's flat `keys` list
				// stops describing the real wire shape. Nothing in this file models
				// that, so refuse it rather than pin a lie.
				if f.Anonymous {
					t.Errorf("%s.%s is EMBEDDED. encoding/json promotes an embedded struct's keys into the outer "+
						"object, so this type's flat golden no longer describes what goes on the wire. Give the field "+
						"a name (and a json tag), or teach wireGoldens() about flattening before shipping this.",
						g.name, f.Name)
					continue
				}

				// The key this field actually lands under. An absent or
				// options-only tag falls back to the Go name — that is a finding of
				// its own, reported by TestEveryExportedFieldTagEqualsGoFieldName;
				// here we just resolve the effective key so coverage stays accurate
				// either way.
				key := f.Name
				if tag, ok := f.Tag.Lookup("json"); ok {
					name := tag
					if idx := strings.Index(tag, ","); idx >= 0 {
						name = tag[:idx]
					}
					if name == "-" {
						continue // explicitly not serialized
					}
					if name != "" {
						key = name
					}
				}

				checked++
				if !pinned[key] {
					t.Errorf("%s.%s serializes as %q, but %q is NOT in this type's frozen `keys` list — "+
						"the field is on the wire and NO golden covers it.\n"+
						"  Why the rest of the suite is silent: a `,omitempty` field left at its zero value in the golden's "+
						"`value` emits no key, so the frozen-key comparison balances and every round-trip passes. The key "+
						"still ships.\n"+
						"  FIX: add %q to wireGoldens()[%s].keys, set the field to a non-zero value in `value`, and — if it "+
						"is tagged omitempty — add it to `omitEmpty` as well.",
						g.name, f.Name, key, key, key, g.name)
				}
			}
		})
	}

	if checked == 0 {
		t.Fatal("inspected zero fields — the guard is not actually guarding anything")
	}
}
