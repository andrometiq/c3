// Package mcptools is the single source of truth for the C3 MCP tool
// InputSchemas that are shared verbatim between the Claude and Codex adapters.
//
// the maintainer's standing principle (2026-05-18, TODO #20): anything duplicated
// between the Claude and Codex adapters must have ONE source of truth. P3 added
// per-adapter `replyMediaSchema()` / `pollToolSchema()` helpers that were
// byte-identical copies; P4 collapses them here so the two adapters stay in
// lockstep and the schemas can be derived from the capability manifest rather
// than hand-maintained enums (CMG spec §L5 — "ONE shared helper generates tool
// Descriptions/InputSchemas from the manifest").
//
// These builders return plain map[string]any so the MCP SDK can marshal them
// directly (the adapters pass raw JSON-schema maps, not struct-tag reflection).
package mcptools

import "github.com/Andrometiq/c3/internal/c3types"

// allMediaKinds is the fallback ordered media-kind enum used when a manifest
// reports no MediaKinds (e.g. a zero-value Capabilities). Matches the historical
// hardcoded enum the per-adapter helpers used pre-P4.
var allMediaKinds = []c3types.MediaKind{
	c3types.MediaPhoto,
	c3types.MediaFile,
	c3types.MediaVideo,
	c3types.MediaAudio,
	c3types.MediaVoice,
	c3types.MediaAnimation,
}

// mediaKindEnum returns the media-kind enum (as []string) for the reply tool's
// `media[].kind` property, sourced from the manifest's MediaKinds so the schema
// can never advertise a kind the channel cannot send. Falls back to the full
// set when the manifest lists none (zero-value caps / older broker).
func mediaKindEnum(caps c3types.Capabilities) []string {
	kinds := caps.MediaKinds
	if len(kinds) == 0 {
		kinds = allMediaKinds
	}
	out := make([]string, 0, len(kinds))
	for _, k := range kinds {
		out = append(out, string(k))
	}
	return out
}

// ReplyMediaSchema is the JSON-schema for the reply tool's `media` array arg
// (P3, manifest-driven in P4). Each item is one media object; the broker's gate
// splits a multi-item array into one message per item. The `kind` enum is
// derived from caps.MediaKinds.
func ReplyMediaSchema(caps c3types.Capabilities) map[string]any {
	return map[string]any{
		"type":        "array",
		"description": "Media to send, each item as its own message after the text.",
		"items": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"kind": map[string]any{
					"type": "string",
					"enum": mediaKindEnum(caps),
				},
				"path":    map[string]any{"type": "string", "description": "Local file path on the shared host."},
				"url":     map[string]any{"type": "string", "description": "Public URL Telegram fetches server-side."},
				"caption": map[string]any{"type": "string"},
				"spoiler": map[string]any{"type": "boolean"},
			},
			"required": []string{"kind"},
		},
	}
}

// ReplyButtonsSchema is the JSON-schema for the reply tool's `buttons` arg (P7):
// an optional inline keyboard, expressed as ROWS of buttons (a 2-D array). Each
// button has a `text` label plus EXACTLY ONE of `data` (a callback button — its
// tap comes back to the agent as a `<channel>` callback event) or `url` (a link
// button that just opens the URL). Channel-neutral: the broker gate drops the
// keyboard on a channel without inline-keyboard support, and the channel
// enforces its own limits (Telegram: callback data <=64 bytes).
func ReplyButtonsSchema() map[string]any {
	return map[string]any{
		"type":        "array",
		"description": "Optional inline keyboard: an array of ROWS, each row an array of buttons. A button is {text, data} (a callback button — its tap comes back to you as a callback event) OR {text, url} (opens a link). Set exactly one of data/url. Keep callback data short (<=64 bytes).",
		"items": map[string]any{
			"type": "array",
			"items": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"text": map[string]any{"type": "string", "description": "Button label."},
					"data": map[string]any{"type": "string", "description": "Callback payload; tapping returns it to you as a callback event. <=64 bytes."},
					"url":  map[string]any{"type": "string", "description": "Link the button opens; does NOT come back to you."},
				},
				"required": []string{"text"},
			},
		},
	}
}

// PollToolSchema is the JSON-schema for the `poll` tool (P3; full surface in
// P2). Channel-neutral — the gate hard-rejects on a channel that does not
// support polls and owns all shape validation, so the schema itself is the same
// regardless of manifest. Covers regular AND quiz polls plus an optional timer.
func PollToolSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"question": map[string]any{"type": "string"},
			"options": map[string]any{
				"type":  "array",
				"items": map[string]any{"type": "string"},
			},
			"anonymous": map[string]any{"type": "boolean", "description": "default true"},
			"multiple":  map[string]any{"type": "boolean", "description": "allow multiple answers; default false (ignored for a quiz)"},
			"type": map[string]any{
				"type":        "string",
				"enum":        []string{"regular", "quiz"},
				"description": `"regular" (default) or "quiz".`,
			},
			"correct_option": map[string]any{
				"type":        "integer",
				"description": "0-based index of the correct answer; REQUIRED when type=quiz.",
			},
			"explanation": map[string]any{
				"type":        "string",
				"description": "shown when a quiz answer is wrong (0-200 chars).",
			},
			"open_period": map[string]any{
				"type":        "integer",
				"description": "seconds the poll stays open before auto-closing; mutually exclusive with close_date.",
			},
			"close_date": map[string]any{
				"type":        "integer",
				"description": "Unix timestamp at which the poll auto-closes; mutually exclusive with open_period.",
			},
		},
		"required": []string{"question", "options"},
	}
}

// AskToolSchema is the JSON-schema for the blocking `ask` tool. The agent supplies
// a `question` and a non-empty `options` array; C3 renders the question on the
// channel with one inline-keyboard button per option and BLOCKS until the human
// answers — the answer is returned as the tool result.
//
//   - single-select (default): the chosen option string.
//   - multi (multi-select): each option toggles a ✓; a trailing "Done" button
//     resolves with the comma-joined list of selected options.
//   - allow_skip: adds a trailing "Skip" button that resolves with a skip notice.
//
// allow_other / free_text are accepted but NOT yet supported — the tool returns an
// error if either is true (free-text answers intercept the durable-queue text path
// and need a product decision). Mirrors PollToolSchema's shape.
func AskToolSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"question": map[string]any{
				"type":        "string",
				"description": "The question to ask the human.",
			},
			"options": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "The choices — rendered one inline-keyboard button each. Required and non-empty.",
			},
			"multi":       map[string]any{"type": "boolean", "description": "Multi-select: each option toggles a ✓ and a 'Done' button resolves with the selected list. Default false (single-select)."},
			"allow_skip":  map[string]any{"type": "boolean", "description": "Add a 'Skip' button that lets the human decline to answer. Default false."},
			"allow_other": map[string]any{"type": "boolean", "description": "NOT yet supported (free-text 'Other'); setting true returns an error."},
			"free_text":   map[string]any{"type": "boolean", "description": "NOT yet supported (free-text answers); setting true returns an error."},
		},
		"required": []string{"question", "options"},
	}
}

// StopPollToolSchema is the JSON-schema for the `stop_poll` tool (P4). It
// force-closes a bot-sent poll and returns its final aggregate tally — the
// deterministic read path, since the passive poll-result event only arrives when
// a poll closes. message_id is the id returned by the `poll` tool when the poll
// was sent. Channel-neutral: the broker gate hard-rejects on a channel without
// poll support and the channel owns the stopPoll wire call.
func StopPollToolSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"message_id": map[string]any{
				"type":        "integer",
				"description": "id of the poll message to close (the id returned when you sent the poll).",
			},
		},
		"required": []string{"message_id"},
	}
}
