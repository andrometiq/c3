package telegram

// richdecode.go — inbound Bot API 10.1 rich-message decoder. ALL 10.1 inbound
// wire knowledge (RichMessage / RichBlock / RichText shapes) lives in this file
// (the R7 no-leak rule). The decoder turns the structured tree Telegram sends in
// Message.rich_message into GFM markdown the agent reads, plus any embedded media
// as downloadable attachments. gotgbot rc.34 has no typed RichMessage, so the
// caller hands us the raw json.RawMessage captured in poll.go.

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/Andrometiq/c3/internal/c3types"
)

// richKind tags which of RichText's three JSON shapes a node holds.
type richKind int

const (
	richLeaf   richKind = iota // a bare JSON string
	richArray                  // an array of RichText
	richTagged                 // a {"type":..., "text":...} object
)

// richText is the parsed form of a RichText value. Exactly one shape per node.
type richText struct {
	kind  richKind
	text  string     // richLeaf
	items []richText // richArray
	typ   string     // richTagged: discriminator
	inner *richText  // richTagged: nested "text"
	url   string     // url
	name  string     // anchor/reference name
	alt   string     // custom_emoji alternative_text
	expr  string     // mathematical_expression
	user  int64      // text_mention user.id
}

// UnmarshalJSON handles RichText's three shapes: string | array | tagged object.
// Unknown/garbage shapes decode to an empty leaf (never an error) so the whole
// decode degrades gracefully rather than failing.
func (rt *richText) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || string(data) == "null" {
		rt.kind = richLeaf
		return nil
	}
	switch data[0] {
	case '"':
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		rt.kind = richLeaf
		rt.text = s
	case '[':
		var arr []richText
		if err := json.Unmarshal(data, &arr); err != nil {
			return err
		}
		rt.kind = richArray
		rt.items = arr
	case '{':
		var obj struct {
			Type            string    `json:"type"`
			Text            *richText `json:"text"`
			URL             string    `json:"url"`
			Name            string    `json:"name"`
			AnchorName      string    `json:"anchor_name"`
			ReferenceName   string    `json:"reference_name"`
			AlternativeText string    `json:"alternative_text"`
			Expression      string    `json:"expression"`
			User            *struct {
				ID int64 `json:"id"`
			} `json:"user"`
		}
		if err := json.Unmarshal(data, &obj); err != nil {
			return err
		}
		rt.kind = richTagged
		rt.typ = obj.Type
		rt.inner = obj.Text
		rt.url = obj.URL
		rt.name = firstNonEmpty(obj.Name, obj.AnchorName, obj.ReferenceName)
		rt.alt = obj.AlternativeText
		rt.expr = obj.Expression
		if obj.User != nil {
			rt.user = obj.User.ID
		}
	default:
		rt.kind = richLeaf
	}
	return nil
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

// inlineEscaper escapes the markdown-structural characters that would otherwise
// make literal text misrender (a deliberate LIGHT set — readability over perfect
// round-trip; see spec §5.4).
var inlineEscaper = strings.NewReplacer(
	`\`, `\\`,
	"`", "\\`",
	"*", `\*`,
	"_", `\_`,
	"[", `\[`,
	"]", `\]`,
	"|", `\|`,
	"~", `\~`,
)

func escapeInline(s string) string { return inlineEscaper.Replace(s) }

// maxDecodeDepth bounds recursion in the rich-message renderers as
// defense-in-depth on untrusted input. Telegram-rendered trees are shallow
// (articles cap at ≤500 blocks, typically a handful of nesting levels) and Go's
// json decoder already errors out past its own deep-nesting limit before any
// stack-overflow risk — so this cap should never fire on legitimate content. It
// is a belt-and-suspenders guarantee independent of those upstream limits: past
// the cap the renderer emits depthMarker instead of recursing further. (The
// top-level recover() in decodeRichMessage is the final backstop.)
const maxDecodeDepth = 256

// depthMarker is emitted in place of content cut off at maxDecodeDepth.
const depthMarker = "[nesting too deep]"

// renderRichText renders a RichText node to GFM markdown, escaping plain text.
func renderRichText(rt *richText) string { return renderRichTextAt(rt, 0) }

func renderRichTextAt(rt *richText, depth int) string {
	if rt == nil {
		return ""
	}
	if depth > maxDecodeDepth {
		return depthMarker
	}
	switch rt.kind {
	case richLeaf:
		return escapeInline(rt.text)
	case richArray:
		var b strings.Builder
		for i := range rt.items {
			b.WriteString(renderRichTextAt(&rt.items[i], depth+1))
		}
		return b.String()
	case richTagged:
		inner := renderRichTextAt(rt.inner, depth+1)
		switch rt.typ {
		case "bold":
			return "**" + inner + "**"
		case "italic":
			return "*" + inner + "*"
		case "underline":
			return "__" + inner + "__"
		case "strikethrough":
			return "~~" + inner + "~~"
		case "spoiler":
			return "||" + inner + "||"
		case "marked":
			return "==" + inner + "=="
		case "code":
			return "`" + plainTextAt(rt.inner, depth+1) + "`"
		case "url":
			return "[" + inner + "](" + rt.url + ")"
		case "text_mention":
			return "[" + inner + "](tg://user?id=" + strconv.FormatInt(rt.user, 10) + ")"
		case "custom_emoji":
			return escapeInline(rt.alt)
		case "mathematical_expression":
			return "$" + rt.expr + "$"
		case "anchor", "anchor_link", "reference", "reference_link":
			return "[" + inner + "](#" + rt.name + ")"
		default:
			// subscript/superscript/date_time and all auto-detected entities
			// (mention/hashtag/cashtag/bot_command/email/phone/bank_card) plus
			// any future inline type → render their text literally.
			return inner
		}
	}
	return ""
}

// plainText returns a node's text WITHOUT markdown escaping — used for inline
// code spans where the content is literal monospace.
func plainText(rt *richText) string { return plainTextAt(rt, 0) }

func plainTextAt(rt *richText, depth int) string {
	if rt == nil {
		return ""
	}
	if depth > maxDecodeDepth {
		return ""
	}
	switch rt.kind {
	case richLeaf:
		return rt.text
	case richArray:
		var b strings.Builder
		for i := range rt.items {
			b.WriteString(plainTextAt(&rt.items[i], depth+1))
		}
		return b.String()
	case richTagged:
		switch rt.typ {
		case "custom_emoji":
			return rt.alt
		case "mathematical_expression":
			return rt.expr
		default:
			return plainTextAt(rt.inner, depth+1)
		}
	}
	return ""
}

// richBlock is a single RichBlock. A union over all block types; only the fields
// for its Type are populated. Caption is raw because it has two shapes: RichText
// (table) vs RichBlockCaption (media) — decoded per-type by the handler.
type richBlock struct {
	Type       string          `json:"type"`
	Text       *richText       `json:"text"`
	Size       int             `json:"size"`       // heading: 1=largest..6
	Language   string          `json:"language"`   // pre
	Blocks     []richBlock     `json:"blocks"`     // blockquote/details/collage/slideshow
	Credit     *richText       `json:"credit"`     // blockquote/pullquote
	Summary    *richText       `json:"summary"`    // details
	Items      []richListItem  `json:"items"`      // list
	Cells      [][]richCell    `json:"cells"`      // table
	Expression string          `json:"expression"` // math
	Name       string          `json:"name"`       // anchor
	Caption    json.RawMessage `json:"caption"`    // table=RichText, media=RichBlockCaption
	Photo      []photoSize     `json:"photo"`      // media
	Video      *fileObj        `json:"video"`
	Animation  *fileObj        `json:"animation"`
	Audio      *fileObj        `json:"audio"`
	VoiceNote  *fileObj        `json:"voice_note"`
}

type richListItem struct {
	Label       string      `json:"label"`
	Blocks      []richBlock `json:"blocks"`
	HasCheckbox bool        `json:"has_checkbox"`
	IsChecked   bool        `json:"is_checked"`
	Value       int         `json:"value"`
	Type        string      `json:"type"` // "a"/"A"/"i"/"I"/"1"
}

type richCell struct {
	Text     *richText `json:"text"`
	IsHeader bool      `json:"is_header"`
	Colspan  int       `json:"colspan"`
	Rowspan  int       `json:"rowspan"`
	Align    string    `json:"align"`
	Valign   string    `json:"valign"`
}

type photoSize struct {
	FileID   string `json:"file_id"`
	FileSize int64  `json:"file_size"`
	Width    int64  `json:"width"`
	Height   int64  `json:"height"`
}

type fileObj struct {
	FileID   string `json:"file_id"`
	FileSize int64  `json:"file_size"`
	MimeType string `json:"mime_type"`
	FileName string `json:"file_name"`
}

type richBlockCaption struct {
	Text   *richText `json:"text"`
	Credit *richText `json:"credit"`
}

// renderBlock renders one block to markdown and any embedded media attachments.
func renderBlock(b *richBlock) (string, []c3types.Attachment) { return renderBlockAt(b, 0) }

func renderBlockAt(b *richBlock, depth int) (string, []c3types.Attachment) {
	if depth > maxDecodeDepth {
		return depthMarker, nil
	}
	switch b.Type {
	case "paragraph":
		return renderRichText(b.Text), nil
	case "heading":
		level := b.Size
		if level < 1 {
			level = 1
		}
		if level > 6 {
			level = 6
		}
		return strings.Repeat("#", level) + " " + renderRichText(b.Text), nil
	case "pre":
		return "```" + b.Language + "\n" + plainText(b.Text) + "\n```", nil
	case "divider":
		return "---", nil
	case "mathematical_expression":
		return "$$" + b.Expression + "$$", nil
	case "footer":
		return "---\n*" + renderRichText(b.Text) + "*", nil
	case "blockquote":
		body, atts := renderBlocksAt(b.Blocks, depth+1)
		out := prefixLines(body, "> ")
		if b.Credit != nil {
			out += "\n> — " + renderRichText(b.Credit)
		}
		return out, atts
	case "pullquote":
		out := prefixLines(renderRichText(b.Text), "> ")
		if b.Credit != nil {
			out += "\n> — " + renderRichText(b.Credit)
		}
		return out, nil
	case "details":
		body, atts := renderBlocksAt(b.Blocks, depth+1)
		return "**" + renderRichText(b.Summary) + "**\n\n" + body, atts
	case "list":
		return renderListAt(b.Items, depth+1)
	case "collage", "slideshow":
		return renderBlocksAt(b.Blocks, depth+1)
	case "anchor", "thinking":
		// anchor has no readable text; thinking is outbound-only (never inbound).
		return "", nil
	case "table":
		return renderTable(b), nil // Task 4
	case "map":
		return "[map]", nil
	case "photo", "video", "animation", "audio", "voice_note":
		return renderMedia(b) // Task 5
	default:
		// Graceful degradation for unknown/future block types: salvage any text
		// or child blocks; never silently empty.
		if b.Text != nil {
			return renderRichText(b.Text), nil
		}
		if len(b.Blocks) > 0 {
			return renderBlocksAt(b.Blocks, depth+1)
		}
		return "[unsupported block: " + b.Type + "]", nil
	}
}

// renderBlocks renders a slice of blocks, joining non-empty results with a blank
// line and aggregating their attachments in order.
func renderBlocks(blocks []richBlock) (string, []c3types.Attachment) {
	return renderBlocksAt(blocks, 0)
}

func renderBlocksAt(blocks []richBlock, depth int) (string, []c3types.Attachment) {
	if depth > maxDecodeDepth {
		return depthMarker, nil
	}
	var parts []string
	var atts []c3types.Attachment
	for i := range blocks {
		// Siblings share depth; renderBlockAt deepens when it descends into a
		// block's own children.
		md, a := renderBlockAt(&blocks[i], depth)
		if md != "" {
			parts = append(parts, md)
		}
		atts = append(atts, a...)
	}
	return strings.Join(parts, "\n\n"), atts
}

// renderList renders a RichBlockList's items as GFM list lines.
func renderList(items []richListItem) (string, []c3types.Attachment) {
	return renderListAt(items, 0)
}

func renderListAt(items []richListItem, depth int) (string, []c3types.Attachment) {
	if depth > maxDecodeDepth {
		return depthMarker, nil
	}
	var lines []string
	var atts []c3types.Attachment
	for i := range items {
		it := &items[i]
		marker := "-"
		switch {
		case it.HasCheckbox && it.IsChecked:
			marker = "- [x]"
		case it.HasCheckbox:
			marker = "- [ ]"
		case it.Type != "" || it.Value != 0:
			n := it.Value
			if n == 0 {
				n = i + 1
			}
			marker = strconv.Itoa(n) + "."
		}
		body, a := renderBlocksAt(it.Blocks, depth)
		atts = append(atts, a...)
		if body == "" {
			body = escapeInline(it.Label)
		}
		lines = append(lines, marker+" "+body)
	}
	return strings.Join(lines, "\n"), atts
}

// prefixLines prefixes every line of s with prefix (used for blockquotes).
func prefixLines(s, prefix string) string {
	if s == "" {
		return prefix
	}
	lines := strings.Split(s, "\n")
	for i, ln := range lines {
		lines[i] = prefix + ln
	}
	return strings.Join(lines, "\n")
}

// renderTable renders a RichBlockTable as a GFM pipe table. The header is the
// first row whose cells are is_header; if no row is a header, an empty header is
// synthesized (GFM requires one). colspan/rowspan cannot be expressed in GFM —
// each cell renders in its primary position and spans are left blank (documented
// lossy degradation, spec §5.3). align maps left→:--, center→:-:, right→--:.
func renderTable(b *richBlock) string {
	rows := b.Cells
	if len(rows) == 0 {
		return ""
	}
	// Column count = widest row.
	cols := 0
	for _, r := range rows {
		if len(r) > cols {
			cols = len(r)
		}
	}
	if cols == 0 {
		return ""
	}
	// Find the header row (first all/any is_header row); default to none.
	headerIdx := -1
	for i, r := range rows {
		for _, c := range r {
			if c.IsHeader {
				headerIdx = i
				break
			}
		}
		if headerIdx >= 0 {
			break
		}
	}

	cell := func(r []richCell, ci int) string {
		if ci < len(r) {
			return renderRichText(r[ci].Text)
		}
		return ""
	}
	align := func(r []richCell, ci int) string {
		a := ""
		if ci < len(r) {
			a = r[ci].Align
		}
		switch a {
		case "center":
			return ":-:"
		case "right":
			return "--:"
		case "left":
			return ":--"
		default:
			return "---"
		}
	}
	rowLine := func(r []richCell) string {
		fields := make([]string, cols)
		for ci := 0; ci < cols; ci++ {
			fields[ci] = cell(r, ci)
		}
		return "| " + strings.Join(fields, " | ") + " |"
	}

	var out []string
	var headerRow []richCell
	var bodyRows [][]richCell
	if headerIdx >= 0 {
		headerRow = rows[headerIdx]
		for i, r := range rows {
			if i != headerIdx {
				bodyRows = append(bodyRows, r)
			}
		}
	} else {
		headerRow = nil // synthesize empty header
		bodyRows = rows
	}

	out = append(out, rowLine(headerRow))
	// Delimiter row: use header's per-column align when present, else "---".
	delims := make([]string, cols)
	for ci := 0; ci < cols; ci++ {
		delims[ci] = align(headerRow, ci)
	}
	out = append(out, "| "+strings.Join(delims, " | ")+" |")
	for _, r := range bodyRows {
		out = append(out, rowLine(r))
	}
	return strings.Join(out, "\n")
}

// renderMedia maps a media block to a downloadable Attachment plus an inline
// text marker at its position. Mirrors convertInbound's media mapping so the
// existing download path + size caps apply unchanged.
func renderMedia(b *richBlock) (string, []c3types.Attachment) {
	var att c3types.Attachment
	switch b.Type {
	case "photo":
		if len(b.Photo) == 0 {
			return "[photo]", nil
		}
		best := b.Photo[0]
		bestArea := best.Width * best.Height
		for _, p := range b.Photo[1:] {
			if a := p.Width * p.Height; a > bestArea {
				best, bestArea = p, a
			}
		}
		att = c3types.Attachment{Kind: "photo", FileID: best.FileID, Size: best.FileSize}
	case "video":
		att = fileAttachment("video", b.Video)
	case "animation":
		att = fileAttachment("animation", b.Animation)
	case "audio":
		att = fileAttachment("audio", b.Audio)
	case "voice_note":
		att = fileAttachment("voice", b.VoiceNote)
	}
	marker := "[" + b.Type + "]"
	if caption := mediaCaption(b.Caption); caption != "" {
		marker = "[" + b.Type + ": " + caption + "]"
	}
	if att.FileID == "" {
		return marker, nil
	}
	return marker, []c3types.Attachment{att}
}

func fileAttachment(kind string, f *fileObj) c3types.Attachment {
	if f == nil {
		return c3types.Attachment{}
	}
	return c3types.Attachment{
		Kind:   kind,
		FileID: f.FileID,
		Size:   f.FileSize,
		MIME:   f.MimeType,
		Name:   f.FileName,
	}
}

// mediaCaption decodes a media block's caption (a RichBlockCaption object) into
// plain marker text. Returns "" if absent or undecodable.
func mediaCaption(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var c richBlockCaption
	if err := json.Unmarshal(raw, &c); err != nil {
		return ""
	}
	return strings.TrimSpace(plainText(c.Text))
}

// richMessage is the inbound RichMessage envelope: blocks + is_rtl.
type richMessage struct {
	Blocks []richBlock `json:"blocks"`
	IsRTL  bool        `json:"is_rtl"`
}

// decodeRichMessage parses a Bot API 10.1 rich_message payload into GFM markdown
// plus embedded media attachments. ok=false on malformed JSON (caller falls back
// to a marker). When the tree is present but yields no content, returns the
// "[rich message]" marker with ok=true — a present rich_message NEVER yields
// empty text. NEVER panics: a top-level recover turns any panic into ok=false.
func decodeRichMessage(raw json.RawMessage) (markdown string, atts []c3types.Attachment, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			markdown, atts, ok = "", nil, false
		}
	}()
	if len(raw) == 0 {
		return "", nil, false
	}
	var rm richMessage
	if err := json.Unmarshal(raw, &rm); err != nil {
		return "", nil, false
	}
	md, a := renderBlocks(rm.Blocks)
	md = strings.TrimSpace(md)
	if md == "" && len(a) == 0 {
		return "[rich message]", nil, true
	}
	return md, a, true
}
