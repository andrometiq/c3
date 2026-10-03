package broker

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"strings"
	"time"
	"unicode"
)

// autoCardLimit bounds a card in UTF-16 units, under Telegram's 4096 so the
// status line still fits when the card is edited.
const autoCardLimit = 3500

// In an overflow file every value line starts with autoValueLinePrefix, and a
// line longer than autoWrapRunes continues on lines starting with
// autoWrapLinePrefix. Untrusted text can't contain either (displayEscape
// escapes them), so input can't forge a field boundary or a line break.
const (
	autoValueLinePrefix = "│ "
	autoWrapLinePrefix  = "┆ "
	autoWrapRunes       = 120
)

// autoSpaceRunMin is the shortest run of spaces displayEscape collapses into
// one visible marker. Shorter runs (indentation) stay as they are.
const autoSpaceRunMin = 16

type autoCardFields struct {
	requestID, inputHash  string
	toolName, cwd, reason string
	toolInput             json.RawMessage
	grantTTL              time.Duration
}

// autoCard is a rendered card: HTML text, plus the full input as a plain-text
// file when it does not fit (overflow is nil when it does).
type autoCard struct {
	text     string
	overflow []byte
}

// autoField is one labelled, display-escaped value on a card.
type autoField struct {
	label, value string
}

// renderAutoCard renders the approval card from the raw tool_input (§5.6), not
// from any harness preview. Every field is shown in full; nothing is masked:
// the operator must see exactly what Allow lets run. Display escaping applies
// to what is shown only. The grant is keyed on the real input.
func renderAutoCard(fields autoCardFields) (autoCard, error) {
	inputFields, err := autoInputFields(fields.toolInput)
	if err != nil {
		return autoCard{}, err
	}
	// Metadata is text and is quoted like text values, so leading or trailing
	// spaces (a cwd of "/work " is not "/work") stay visible.
	metadata := []autoField{
		{"tool_name", quoted(fields.toolName, false)},
		{"cwd", quoted(fields.cwd, false)},
		{"reason", quoted(fields.reason, false)},
	}
	shortID, hashPrefix := autoShortID(fields.requestID), fields.inputHash[:12]
	header := fmt.Sprintf("<b>🛡 Auto mode blocked a tool call</b>\nRequest <code>%s</code> · input <code>%s</code>\n"+
		"Allow once arms one identical retry for %d s. It does not make the model retry, and C3 cannot confirm what ran.\n"+
		"Text values are in double quotes; ⟦U+XXXX⟧ marks an escaped character.",
		shortID, hashPrefix, int(fields.grantTTL.Seconds()))
	full := header + htmlFields(metadata) + htmlFields(inputFields)
	if utf16Len(full) <= autoCardLimit {
		return autoCard{text: full}, nil
	}
	card := header + htmlFields(metadata) + fmt.Sprintf(
		"\n\nThe tool input is too long for one message. It is in full in <code>%s</code>, the file this card replies to "+
			"(request <code>%s</code>, input <code>%s</code>).", autoOverflowName(fields.requestID), shortID, hashPrefix)
	if utf16Len(card) > autoCardLimit {
		return autoCard{}, errors.New("card metadata exceeds one message")
	}
	var file strings.Builder
	fmt.Fprintf(&file, "C3 auto-mode approval\nRequest %s · input %s\n", shortID, hashPrefix)
	fmt.Fprintf(&file, "Each value line starts with %q; a long line continues on lines starting with %q.\n",
		autoValueLinePrefix, autoWrapLinePrefix)
	fmt.Fprintf(&file, "Text values are in double quotes. Invisible, control and right-to-left characters show as "+
		"⟦U+XXXX⟧, and runs of %d or more spaces as ⟦U+0020×N⟧.\n", autoSpaceRunMin)
	for _, field := range append(metadata, inputFields...) {
		file.WriteString("\n" + field.label + "\n")
		for _, line := range strings.Split(field.value, "\n") {
			writeWrappedLine(&file, line)
		}
	}
	return autoCard{text: card, overflow: []byte(file.String())}, nil
}

// writeWrappedLine writes one value line of an overflow file, wrapped every
// autoWrapRunes runes so nothing sits far off to the right in a viewer that
// doesn't wrap. It never breaks inside a ⟦…⟧ escape.
func writeWrappedLine(file *strings.Builder, line string) {
	file.WriteString(autoValueLinePrefix)
	count, isInEscape := 0, false
	for _, r := range line {
		if count >= autoWrapRunes && !isInEscape {
			file.WriteString("\n" + autoWrapLinePrefix)
			count = 0
		}
		switch r {
		case '⟦':
			isInEscape = true
		case '⟧':
			isInEscape = false
		}
		file.WriteRune(r)
		count++
	}
	file.WriteString("\n")
}

// autoInputFields lists tool_input's top-level fields in their original order,
// labelled tool_input["<key>"]. A string shows as its text in double quotes; any
// other value as compact JSON, which never starts with a quote, so the string
// "false" and the boolean false can't look alike. A non-object input is one
// field, "tool_input".
func autoInputFields(raw json.RawMessage) ([]autoField, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		value, err := autoFieldValue(raw)
		return []autoField{{"tool_input", value}}, err
	}
	var fields []autoField
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, _ := token.(string)
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		// The key sits inside tool_input["…"], and any annotation goes after the
		// closing bracket, so no literal key can render as an annotated label:
		// a bare label always ends in "], an annotated one never does.
		label := "tool_input[" + quoted(key, false) + "]"
		if key == "description" {
			label += " (the model's own words)"
		}
		text, err := autoFieldValue(value)
		if err != nil {
			return nil, err
		}
		fields = append(fields, autoField{label, text})
	}
	return fields, nil
}

func autoFieldValue(raw json.RawMessage) (string, error) {
	raw = bytes.TrimSpace(raw)
	var text string
	// Only a JSON string shows as text: null must not render as an empty string.
	if len(raw) > 0 && raw[0] == '"' && json.Unmarshal(raw, &text) == nil {
		return quoted(text, true), nil
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return "", err
	}
	return displayEscape(compact.String(), true), nil
}

// quoted is displayEscape inside double quotes, the card's mark of a text
// value. The quotes sit at fixed positions, so no content can fake them.
func quoted(text string, keepLineBreaks bool) string {
	return `"` + displayEscape(text, keepLineBreaks) + `"`
}

// htmlFields renders each field as a bold label over a code block. The value is
// HTML-escaped, so input can't open or close a block.
func htmlFields(fields []autoField) string {
	var out strings.Builder
	for _, field := range fields {
		out.WriteString("\n\n<b>" + html.EscapeString(field.label) + "</b>\n<pre>" + html.EscapeString(field.value) + "</pre>")
	}
	return out.String()
}

// displayEscape makes untrusted text safe to show an approver (§5.6). Control,
// format (bidi, zero-width), unassigned, private-use, combining and non-ASCII
// space characters, letters that render as blank, and right-to-left letters
// (which a client may visually reorder) become ⟦U+XXXX⟧. So do ⟦, ⟧, │ and ┆,
// so input can't forge an escape or an overflow-file line. A run of
// autoSpaceRunMin or more spaces becomes ⟦U+0020×N⟧, so padding can't push text
// out of view sideways. With keepLineBreaks, a line feed renders as ⟦U+000A⟧
// followed by a real break: every break is visible, so a run of blank lines
// can't push text out of view either.
func displayEscape(text string, keepLineBreaks bool) string {
	var out strings.Builder
	runes := []rune(text)
	for index := 0; index < len(runes); index++ {
		r := runes[index]
		switch {
		case r == ' ':
			run := 1
			for index+run < len(runes) && runes[index+run] == ' ' {
				run++
			}
			if run >= autoSpaceRunMin {
				fmt.Fprintf(&out, "⟦U+0020×%d⟧", run)
			} else {
				out.WriteString(strings.Repeat(" ", run))
			}
			index += run - 1
		case r == '\n' && keepLineBreaks:
			out.WriteString("⟦U+000A⟧\n")
		case isDisplayVisible(r):
			out.WriteRune(r)
		default:
			fmt.Fprintf(&out, "⟦U+%04X⟧", r)
		}
	}
	return out.String()
}

func isDisplayVisible(r rune) bool {
	switch r {
	case '⟦', '⟧', '│', '┆': // C3's own escape and overflow-file markers
		return false
	case 0x115F, 0x1160, 0x3164, 0xFFA0, 0x2800: // Hangul fillers and the blank braille pattern render as nothing
		return false
	}
	if isRightToLeft(r) {
		return false
	}
	return unicode.IsGraphic(r) && !unicode.In(r, unicode.Mn, unicode.Me, unicode.Zs)
}

// isRightToLeft reports whether r is in a default right-to-left block, which
// holds every right-to-left (R, AL) and Arabic-number (AN) character.
func isRightToLeft(r rune) bool {
	return (r >= 0x0590 && r <= 0x08FF) ||
		(r >= 0xFB1D && r <= 0xFDFF) ||
		(r >= 0xFE70 && r <= 0xFEFE) ||
		(r >= 0x10800 && r <= 0x10FFF) ||
		(r >= 0x1E800 && r <= 0x1EFFF)
}
