package capability

import (
	"fmt"
	"strings"

	"github.com/Andrometiq/c3/internal/c3types"
)

// chunkMarkdown splits source markdown into parts each measuring <= limit in
// UTF-16 code units, on construct-safe boundaries. It NEVER splits inside a
// fenced code block (``` / ~~~), a [label](url) link, or a blockquote run. The
// preference order is: paragraph (blank-line) boundary → line boundary → word
// boundary. A single indivisible construct longer than limit is hard-split at a
// UTF-16-safe boundary and recorded as a "hard_split" Alteration.
//
// The caller guarantees limit > 0 and utf16Len(src) > limit (else a single part
// is emitted upstream without calling this).
func chunkMarkdown(src string, limit int) ([]string, []c3types.Alteration) {
	var alts []c3types.Alteration

	// 1. Tokenize into top-level blocks. A block is an atomic unit at the
	//    block level: a fenced code block or a blockquote run is one indivisible
	//    block (never split across parts); any other run of lines is an ordinary
	//    block whose lines/words MAY be split if it alone exceeds the limit.
	blocks := splitBlocks(src)

	// 2. Greedily pack blocks into parts, joined by the blank-line paragraph
	//    separator, never exceeding the limit. A block that itself exceeds the
	//    limit is split further (line→word→hard) by splitBlock.
	const sep = "\n\n"
	var parts []string
	var cur strings.Builder
	curLen := 0

	flush := func() {
		if cur.Len() > 0 {
			parts = append(parts, cur.String())
			cur.Reset()
			curLen = 0
		}
	}
	// appendPiece appends a piece that is already known to fit the limit on its
	// own, packing it onto the current part if it fits with a separator.
	appendPiece := func(piece string) {
		pl := utf16Len(piece)
		if cur.Len() == 0 {
			cur.WriteString(piece)
			curLen = pl
			return
		}
		if curLen+utf16Len(sep)+pl <= limit {
			cur.WriteString(sep)
			cur.WriteString(piece)
			curLen += utf16Len(sep) + pl
			return
		}
		flush()
		cur.WriteString(piece)
		curLen = pl
	}

	for _, blk := range blocks {
		if utf16Len(blk.text) <= limit {
			appendPiece(blk.text)
			continue
		}
		// Block too big on its own. Flush the current part first so the big
		// block's sub-pieces start a fresh part.
		flush()
		pieces, blkAlts := splitBlock(blk, limit)
		alts = append(alts, blkAlts...)
		for _, p := range pieces {
			// Each sub-piece is guaranteed <= limit by splitBlock; pack them
			// (they share a part only if they fit — line pieces of a long
			// paragraph repack onto each other).
			appendPiece(p)
		}
	}
	flush()

	if len(parts) == 0 {
		parts = []string{""}
	}
	return parts, alts
}

// block is one top-level markdown block. atomic=true marks a fenced code block
// or a blockquote run, which must never be split across parts.
type block struct {
	text   string
	atomic bool
}

// splitBlocks tokenizes src into top-level blocks. Fenced code blocks and
// blockquote runs become atomic blocks; everything else is grouped into ordinary
// blocks separated by blank lines (paragraph boundaries).
func splitBlocks(src string) []block {
	lines := strings.Split(src, "\n")
	var blocks []block
	var ordinary []string

	flushOrdinary := func() {
		if len(ordinary) > 0 {
			// Trim trailing blank lines that are pure separators; keep internal.
			blocks = append(blocks, block{text: strings.Join(ordinary, "\n")})
			ordinary = nil
		}
	}

	i := 0
	for i < len(lines) {
		line := lines[i]

		// Fenced code block opener.
		if fence, ok := fenceToken(line); ok {
			flushOrdinary()
			body := []string{line}
			j := i + 1
			closed := false
			for j < len(lines) {
				body = append(body, lines[j])
				if isFenceClose(lines[j], fence) {
					closed = true
					j++
					break
				}
				j++
			}
			_ = closed // an unclosed fence is still kept atomic to be safe
			blocks = append(blocks, block{text: strings.Join(body, "\n"), atomic: true})
			i = j
			continue
		}

		// Blockquote run: consecutive "> " / ">" lines.
		if isBlockquoteLine(line) {
			flushOrdinary()
			var quoted []string
			j := i
			for j < len(lines) && isBlockquoteLine(lines[j]) {
				quoted = append(quoted, lines[j])
				j++
			}
			blocks = append(blocks, block{text: strings.Join(quoted, "\n"), atomic: true})
			i = j
			continue
		}

		// GFM pipe-table run: a header line + a `|---|` delimiter row + body rows.
		// Kept atomic so a table is never bisected across messages (the rendered
		// monospace <pre> on the Telegram side relies on the whole table arriving
		// in one part). The shape test here is a PURE markdown-level mirror of the
		// telegram package's detectTable — this package must not import telegram.
		if end, ok := tableRunEnd(lines, i); ok {
			flushOrdinary()
			blocks = append(blocks, block{text: strings.Join(lines[i:end], "\n"), atomic: true})
			i = end
			continue
		}

		// Blank line = paragraph boundary: close the current ordinary block.
		if strings.TrimSpace(line) == "" {
			flushOrdinary()
			i++
			continue
		}

		ordinary = append(ordinary, line)
		i++
	}
	flushOrdinary()

	if len(blocks) == 0 {
		blocks = []block{{text: src}}
	}
	return blocks
}

// splitBlock splits a single over-limit block into <=limit pieces. An atomic
// block (fenced code / blockquote) cannot be subdivided safely, so it is
// hard-split with a recorded Alteration. An ordinary block is split on line
// boundaries, then (for an over-limit single line) on link-aware word
// boundaries, then hard-split as a last resort.
func splitBlock(blk block, limit int) ([]string, []c3types.Alteration) {
	if blk.atomic {
		pieces, n := hardSplit(blk.text, limit)
		kind := "fenced code block"
		if isBlockquoteLine(firstLine(blk.text)) {
			kind = "blockquote"
		} else if _, ok := tableRunEnd(strings.Split(blk.text, "\n"), 0); ok {
			kind = "table"
		}
		return pieces, []c3types.Alteration{{
			Kind:   "hard_split",
			Detail: fmt.Sprintf("a %s exceeded the message limit and was hard-split into %d pieces", kind, n),
		}}
	}

	var alts []c3types.Alteration
	var pieces []string
	for _, line := range strings.Split(blk.text, "\n") {
		if utf16Len(line) <= limit {
			pieces = append(pieces, line)
			continue
		}
		// Single over-limit line: split on link-aware word boundaries.
		wp, wAlts := splitLine(line, limit)
		pieces = append(pieces, wp...)
		alts = append(alts, wAlts...)
	}
	// Repack adjacent line pieces onto one another up to the limit, joined by
	// newlines (so a long paragraph re-coalesces into fewer parts).
	packed := repack(pieces, "\n", limit)
	return packed, alts
}

// splitLine splits a single line that exceeds limit on word boundaries, never
// bisecting a [label](url) link. A single word (or link) longer than limit is
// hard-split.
func splitLine(line string, limit int) ([]string, []c3types.Alteration) {
	var alts []c3types.Alteration
	tokens := tokenizeLine(line) // words + links, each indivisible at the word level
	var pieces []string
	var cur strings.Builder
	curLen := 0
	flush := func() {
		if cur.Len() > 0 {
			pieces = append(pieces, cur.String())
			cur.Reset()
			curLen = 0
		}
	}
	for _, tok := range tokens {
		tl := utf16Len(tok)
		if tl > limit {
			// A single token (word or whole link) longer than the limit must be
			// hard-split — there is no safe word boundary inside it.
			flush()
			hp, n := hardSplit(tok, limit)
			pieces = append(pieces, hp...)
			alts = append(alts, c3types.Alteration{
				Kind:   "hard_split",
				Detail: fmt.Sprintf("a single token exceeded the message limit and was hard-split into %d pieces", n),
			})
			continue
		}
		if cur.Len() == 0 {
			cur.WriteString(tok)
			curLen = tl
			continue
		}
		// Join tokens with a single space; words were split on whitespace runs.
		if curLen+1+tl <= limit {
			cur.WriteString(" ")
			cur.WriteString(tok)
			curLen += 1 + tl
			continue
		}
		flush()
		cur.WriteString(tok)
		curLen = tl
	}
	flush()
	return pieces, alts
}

// tokenizeLine splits a line into word tokens, but keeps a [label](url) link as
// a single token so a word-boundary split can never bisect it. Whitespace runs
// are the separators (collapsed to single spaces on rejoin).
func tokenizeLine(line string) []string {
	runes := []rune(line)
	var tokens []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			tokens = append(tokens, cur.String())
			cur.Reset()
		}
	}
	i := 0
	for i < len(runes) {
		r := runes[i]
		if r == ' ' || r == '\t' {
			flush()
			i++
			continue
		}
		// Link start: absorb the whole [label](url) into the current token so it
		// stays indivisible.
		if r == '[' {
			if end, ok := linkSpan(runes, i); ok {
				cur.WriteString(string(runes[i:end]))
				i = end
				continue
			}
		}
		cur.WriteRune(r)
		i++
	}
	flush()
	return tokens
}

// linkSpan reports the exclusive end index of a [label](url) link starting at
// the '[' at index i, or ok=false if i does not begin a well-formed link.
func linkSpan(runes []rune, i int) (end int, ok bool) {
	if i >= len(runes) || runes[i] != '[' {
		return 0, false
	}
	close := indexRune(runes, ']', i+1)
	if close < 0 || close+1 >= len(runes) || runes[close+1] != '(' {
		return 0, false
	}
	// Scan to the MATCHING close paren with depth tracking so a URL containing
	// balanced parens (e.g. https://en.wikipedia.org/wiki/Foo_(bar)) is treated
	// as one indivisible link and not truncated at the first ')'.
	paren := matchCloseParen(runes, close+1)
	if paren < 0 {
		return 0, false
	}
	return paren + 1, true
}

// matchCloseParen returns the index of the ')' that matches the '(' at index
// open, tracking nesting depth so balanced inner parens don't end the scan
// early. Returns -1 if runes[open] is not '(' or no matching close exists.
func matchCloseParen(runes []rune, open int) int {
	if open >= len(runes) || runes[open] != '(' {
		return -1
	}
	depth := 0
	for j := open; j < len(runes); j++ {
		switch runes[j] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return j
			}
		}
	}
	return -1
}

// repack greedily coalesces pieces (each already <= limit) into fewer pieces
// joined by sep, never exceeding limit.
func repack(pieces []string, sep string, limit int) []string {
	if len(pieces) == 0 {
		return pieces
	}
	var out []string
	var cur strings.Builder
	curLen := 0
	sepLen := utf16Len(sep)
	flush := func() {
		if cur.Len() > 0 {
			out = append(out, cur.String())
			cur.Reset()
			curLen = 0
		}
	}
	for _, p := range pieces {
		pl := utf16Len(p)
		if cur.Len() == 0 {
			cur.WriteString(p)
			curLen = pl
			continue
		}
		if curLen+sepLen+pl <= limit {
			cur.WriteString(sep)
			cur.WriteString(p)
			curLen += sepLen + pl
			continue
		}
		flush()
		cur.WriteString(p)
		curLen = pl
	}
	flush()
	return out
}

// hardSplit splits s into pieces each <= limit UTF-16 code units, breaking at
// rune boundaries that don't bisect a UTF-16 surrogate pair. Returns the pieces
// and their count. This is the last-resort splitter for an indivisible construct
// that alone exceeds the limit.
func hardSplit(s string, limit int) ([]string, int) {
	if limit <= 0 {
		return []string{s}, 1
	}
	runes := []rune(s)
	var pieces []string
	var cur []rune
	curUnits := 0
	for _, r := range runes {
		w := runeUTF16Width(r) // 1 for BMP, 2 for astral (surrogate pair)
		if curUnits+w > limit && len(cur) > 0 {
			pieces = append(pieces, string(cur))
			cur = cur[:0]
			curUnits = 0
		}
		// A single astral rune in a limit==1 channel can't fit without bisecting
		// a surrogate pair; we never split a rune, so we emit it whole (>limit by
		// 1 unit) — a degenerate channel; acceptable since real limits are large.
		cur = append(cur, r)
		curUnits += w
	}
	if len(cur) > 0 {
		pieces = append(pieces, string(cur))
	}
	if len(pieces) == 0 {
		pieces = []string{""}
	}
	return pieces, len(pieces)
}

// runeUTF16Width returns the number of UTF-16 code units a rune occupies: 1 for
// the Basic Multilingual Plane, 2 for astral (surrogate-pair) runes.
func runeUTF16Width(r rune) int {
	if r >= 0x10000 {
		return 2
	}
	return 1
}

// --- small line classifiers (kept local so this package stays pure) ---------

// fenceToken returns the fence token (``` or ~~~) if line opens a fenced code
// block.
func fenceToken(line string) (string, bool) {
	t := strings.TrimLeft(line, " ")
	for _, f := range []string{"```", "~~~"} {
		if strings.HasPrefix(t, f) {
			return f, true
		}
	}
	return "", false
}

// isFenceClose reports whether line closes a fence opened with token fence.
func isFenceClose(line, fence string) bool {
	t := strings.TrimSpace(line)
	return t == fence || (strings.HasPrefix(t, fence) && strings.TrimSpace(t[len(fence):]) == "")
}

// isBlockquoteLine reports whether line is part of a blockquote run.
func isBlockquoteLine(line string) bool {
	return strings.HasPrefix(line, "> ") || line == ">"
}

// tableRunEnd reports whether a GFM pipe-table run starts at lines[i] and, if so,
// the exclusive end index of the run. It is a PURE markdown-level mirror of the
// telegram package's detectTable shape test, kept here so chunk.go can treat a
// table as an atomic block without importing the telegram package (capability
// purity, archguard). Only the SHAPE is recognized — no rendering, no Telegram
// literals.
//
// Shape: a header line containing '|', immediately followed by a delimiter row
// (`|? :?-+:? (| :?-+:?)+ |?`), then zero or more body rows (lines containing
// '|') until a line with no '|' (or end of input).
func tableRunEnd(lines []string, i int) (end int, ok bool) {
	if i+1 >= len(lines) {
		return 0, false
	}
	if !strings.Contains(lines[i], "|") {
		return 0, false
	}
	if !isTableDelimiterLine(lines[i+1]) {
		return 0, false
	}
	// GFM requires the delimiter row to have the SAME number of cells as the
	// header. Without this check, prose containing a pipe followed by a bare
	// thematic break (`some prose | aside` then `---`) is mis-grouped as an
	// atomic table block. Split both rows the SAME way so the counts compare.
	// Kept byte-for-byte consistent with the telegram package's detectTable.
	headerCells := tableRowCellCount(lines[i])
	if headerCells == 0 || tableRowCellCount(lines[i+1]) != headerCells {
		return 0, false
	}
	j := i + 2
	for j < len(lines) {
		if !strings.Contains(lines[j], "|") {
			break
		}
		if isTableDelimiterLine(lines[j]) {
			break // a second delimiter row ends this run (start of another table)
		}
		j++
	}
	return j, true
}

// isTableDelimiterLine reports whether line is a GFM table delimiter row: a run
// of dash cells separated by pipes, each cell optionally carrying `:` alignment
// markers, with an optional leading/trailing pipe. This is the load-bearing
// signal that a `|`-containing line is a table rather than prose. Pure mirror of
// the telegram package's isTableDelimiter.
func isTableDelimiterLine(line string) bool {
	t := strings.TrimSpace(line)
	if t == "" {
		return false
	}
	t = strings.TrimPrefix(t, "|")
	t = strings.TrimSuffix(t, "|")
	cells := strings.Split(t, "|")
	if len(cells) == 0 {
		return false
	}
	for _, c := range cells {
		if !isTableDelimiterCell(c) {
			return false
		}
	}
	return true
}

// tableRowCellCount returns the number of cells a GFM table row splits into,
// stripping one optional leading/trailing pipe before splitting on `|`. It is a
// pure mirror of the telegram package's splitTableRow cell-count logic and is
// used to enforce the GFM rule that a delimiter row must have the same cell count
// as its header. Kept byte-for-byte consistent with that split so the two
// detectors agree on what is (and is not) a table.
func tableRowCellCount(line string) int {
	t := strings.TrimSpace(line)
	t = strings.TrimPrefix(t, "|")
	t = strings.TrimSuffix(t, "|")
	return len(strings.Split(t, "|"))
}

// isTableDelimiterCell reports whether c is a single GFM delimiter cell: optional
// whitespace, an optional leading `:`, one or more `-`, an optional trailing `:`.
func isTableDelimiterCell(c string) bool {
	c = strings.TrimSpace(c)
	c = strings.TrimPrefix(c, ":")
	c = strings.TrimSuffix(c, ":")
	if c == "" {
		return false
	}
	for _, r := range c {
		if r != '-' {
			return false
		}
	}
	return true
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

func indexRune(runes []rune, r rune, from int) int {
	for j := from; j < len(runes); j++ {
		if runes[j] == r {
			return j
		}
	}
	return -1
}
