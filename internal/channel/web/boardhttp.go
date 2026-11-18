package web

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

const (
	boardActBodyLimit  = 256 << 10
	boardClientIDBytes = 128
	boardEmojiMaxBytes = 32
	boardPageCSP       = documentContentSecurityPolicy + "; connect-src 'none'"
	// The shell holds the cookie and shows agent strings, so it runs only its
	// own script file: no inline script or event handlers.
	boardShellCSP       = "default-src 'self'; script-src 'self'; style-src 'unsafe-inline'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'"
	boardRawCSP         = "default-src 'none'; sandbox"
	boardRetryAfterBusy = "2"
	// A long recording comes as numbered parts of a few minutes each.
	boardVoiceMaxSegment = 999
)

// boardAct is the body of POST /board/<name>/act, decoded strictly: a viewer
// can name only these fields, never a sender.
type boardAct struct {
	ClientID string `json:"client_id"`
	Op       string `json:"op"`
	Part     string `json:"part"`
	Text     string `json:"text"`
	Quote    string `json:"quote"`
	Ref      int64  `json:"ref"`
	Note     string `json:"note"`
	Value    string `json:"value"`
	Emoji    string `json:"emoji"`
	// A voice act's place in a longer recording; never decoded from JSON.
	Segment int  `json:"-"`
	HasMore bool `json:"-"`
}

// boardOpFields lists, per op, the fields it needs and the ones it may add.
var boardOpFields = map[string]struct{ required, optional []string }{
	"comment": {required: []string{"part", "text"}, optional: []string{"quote"}},
	"approve": {required: []string{"ref"}, optional: []string{"part", "note"}},
	"tap":     {required: []string{"part", "value"}, optional: []string{"ref"}},
	"react":   {required: []string{"ref", "emoji"}},
}

var boardLintRules = []struct {
	pattern *regexp.Regexp
	warning string
}{
	{regexp.MustCompile(`localStorage|sessionStorage`), "uses browser storage, which throws in a board frame"},
	{regexp.MustCompile(`\bfetch\(|XMLHttpRequest`), "makes network requests, which a board frame blocks"},
	{regexp.MustCompile(`(?i)\b(?:src|href)\s*=\s*["']?\s*https?://`), "loads or links an outside URL, which a board frame blocks"},
	{regexp.MustCompile(`(?i)<form\b`), "has a form, which a board frame cannot submit"},
	{regexp.MustCompile(`(?i)\btarget\s*=\s*["']?_blank`), "opens new windows, which a board frame cannot"},
}

var (
	boardPagePrologue = regexp.MustCompile(`(?is)^\x{FEFF}?(?:\s|<!--.*?-->)*(?:<!doctype[^>]*>)?`)
	boardPageTitle    = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)
)

func (c *Channel) handleBoardList(w http.ResponseWriter, r *http.Request) {
	if _, status := c.boardViewer(r, false); status != 0 {
		refuseBoardPage(w, r, status)
		return
	}
	var list strings.Builder
	for _, board := range c.board.Boards() {
		state := ""
		if board.IsClosed {
			state = " (closed)"
		}
		fmt.Fprintf(&list, "<li><a href=\"/board/%s\">%s</a>%s</li>", board.Name, board.Name, state)
	}
	setPageHeaders(w)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, "<!doctype html><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>Boards</title><h1>Boards</h1><ul>%s</ul>", list.String())
}

// handleBoardShell serves C3's shell, whose frame may load only this board's
// page and which no opener can reach.
func (c *Channel) handleBoardShell(w http.ResponseWriter, r *http.Request) {
	if _, status := c.boardViewer(r, false); status != 0 {
		refuseBoardPage(w, r, status)
		return
	}
	name := r.PathValue("name")
	if _, ok := c.board.BoardByName(name); !ok {
		http.NotFound(w, r)
		return
	}
	shell, err := fs.ReadFile(c.board.assets, "board.html")
	if err != nil {
		http.Error(w, "page unavailable", http.StatusInternalServerError)
		return
	}
	setPageHeaders(w)
	w.Header().Set("Content-Security-Policy", boardShellCSP+"; frame-src "+c.requestOrigin(r)+"/board/"+name+"/page")
	w.Header().Set("Cross-Origin-Opener-Policy", "same-origin")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(shell)
}

func refuseBoardPage(w http.ResponseWriter, r *http.Request, status int) {
	if status == http.StatusUnauthorized {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	http.Error(w, "forbidden", status)
}

var boardAssetTypes = map[string]string{"board.js": "text/javascript"}

func (c *Channel) handleBoardAsset(w http.ResponseWriter, r *http.Request) {
	if _, status := c.boardViewer(r, false); status != 0 {
		http.Error(w, http.StatusText(status), status)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, channel.BoardAssetPathPrefix)
	contentType, ok := boardAssetTypes[name]
	asset, err := fs.ReadFile(c.board.assets, name)
	if !ok || err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write(asset)
}

// boardStateView is GET /board/<name>/state.
type boardStateView struct {
	Board   string            `json:"board"`
	Title   string            `json:"title"`
	Rev     int64             `json:"rev"`
	Cap     string            `json:"cap"`
	Viewer  map[string]string `json:"viewer"`
	Live    bool              `json:"live"`
	Holder  map[string]string `json:"holder"`
	Waiting int               `json:"waiting"`
	Closed  bool              `json:"closed"`
	Log     []boardLogEntry   `json:"log"`
}

func (c *Channel) handleBoardState(w http.ResponseWriter, r *http.Request) {
	viewer, ok := c.boardRequest(w, r, true)
	if !ok {
		return
	}
	name := r.PathValue("name")
	if board, ok := c.board.BoardByName(name); ok {
		c.board.refreshWaiting(board.Number)
	}
	view := boardStateView{Board: name, Cap: c.board.capability(name, viewer.id), Viewer: map[string]string{"name": viewer.sender.Username, "role": "owner"}}
	err := c.board.withBoardNamed(name, func(state *boardState) error {
		view.Title, view.Rev, view.Closed, view.Waiting = state.record.Title, state.record.Rev, state.record.Closed, state.waiting
		view.Live, view.Log = state.holder != nil, append([]boardLogEntry{}, state.log...)
		if state.holder != nil {
			view.Holder = map[string]string{"cli": state.holder.CLI, "cwd": state.holder.CWD}
		}
		return nil
	})
	if err != nil {
		writeBoardStoreError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(view)
}

// handleBoardPage needs the frame capability, never a cookie: a frame-started
// request may carry none. On the front the cap counts only on a request the
// front vouches for.
func (c *Channel) handleBoardPage(w http.ResponseWriter, r *http.Request) {
	name, query := r.PathValue("name"), r.URL.Query()
	if !c.board.isCapabilityValid(name, query.Get("cap"), c.capabilityViewers(r)) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	page, err := c.board.revision(name, query.Get("rev"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if query.Get("raw") == "1" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Content-Security-Policy", boardRawCSP)
		_, _ = w.Write(page)
		return
	}
	sdk, err := fs.ReadFile(c.board.assets, "board-sdk.js")
	if err == nil {
		page, err = inlineBoardSDK(page, sdk)
	}
	if err != nil {
		c.board.logf("board: serve %q page: %v", name, err)
		http.Error(w, "page unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Security-Policy", boardPageCSP)
	_, _ = w.Write(page)
}

// revision returns the stored text of rev ("" for the current one), or a
// placeholder before the first page; only the kept window is served.
func (b *boardChannel) revision(name, rawRev string) ([]byte, error) {
	var path, placeholder string
	err := b.withBoardNamed(name, func(board *boardState) error {
		rev := board.record.Rev
		if rawRev != "" {
			parsed, err := strconv.ParseInt(rawRev, 10, 64)
			if err != nil || parsed < 1 || parsed <= rev-channel.BoardRevisionsKept || parsed > rev {
				return errNoBoard
			}
			rev = parsed
		}
		if rev == 0 {
			placeholder = "<!doctype html><meta charset=\"utf-8\"><title>" + html.EscapeString(board.record.Title) + "</title><p>No page yet: the session has not sent one.</p>"
		}
		path = boardRevisionPath(filepath.Join(b.directory, board.name), rev)
		return nil
	})
	if err != nil || placeholder != "" {
		return []byte(placeholder), err
	}
	return os.ReadFile(path)
}

// inlineBoardSDK puts C3's SDK first: after any byte-order mark, comments and
// doctype, where the parser opens <head>, so it runs before the page's scripts.
func inlineBoardSDK(page, sdk []byte) ([]byte, error) {
	if bytes.Contains(bytes.ToLower(sdk), []byte("</script")) {
		return nil, errors.New("board SDK source contains </script")
	}
	offset := len(boardPagePrologue.Find(page))
	inlined := make([]byte, 0, len(page)+len(sdk)+len("<script></script>"))
	inlined = append(inlined, page[:offset]...)
	inlined = append(inlined, "<script>"...)
	inlined = append(inlined, sdk...)
	inlined = append(inlined, "</script>"...)
	return append(inlined, page[offset:]...), nil
}

// lintBoardPage names what will break in the sandboxed frame. It warns; the
// page is stored either way.
func lintBoardPage(page []byte) []string {
	var warnings []string
	for _, rule := range boardLintRules {
		if rule.pattern.Match(page) {
			warnings = append(warnings, rule.warning)
		}
	}
	return warnings
}

func pageTitle(page []byte, fallback string) string {
	match := boardPageTitle.FindSubmatch(page)
	if match == nil {
		return fallback
	}
	title := []rune(strings.TrimSpace(html.UnescapeString(string(match[1]))))
	if len(title) == 0 {
		return fallback
	}
	return string(title[:min(len(title), 200)])
}

func (c *Channel) handleBoardAct(w http.ResponseWriter, r *http.Request) {
	if isAudioRequest(r) {
		c.handleBoardVoice(w, r)
		return
	}
	c.postDeadline(w, r)
	if !c.sameOrigin(r) {
		writeBoardError(w, http.StatusForbidden, "forbidden")
		return
	}
	viewer, ok := c.boardRequest(w, r, true)
	if !ok {
		return
	}
	name := r.PathValue("name")
	var act boardAct
	if err := decodeJSONLimit(w, r, &act, boardActBodyLimit); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeBoardError(w, http.StatusRequestEntityTooLarge, "too_big")
		} else {
			writeBoardError(w, http.StatusBadRequest, "invalid")
		}
		return
	}
	if status, rule := act.check(); status != 0 {
		writeBoardError(w, status, rule)
		return
	}
	content, _ := json.Marshal(act)
	key := clientMessageKey{scope: name, sessionID: viewer.id, clientID: act.ClientID}
	host, _ := c.board.boardHost()
	admitted, number, retryAfter, err := c.board.admit(name, act, !c.hasClientMessage(c.boardClientMessages, key))
	if err == nil && host == nil {
		err = errBoardsStopped
	}
	if err != nil {
		if retryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		}
		writeBoardStoreError(w, err)
		return
	}
	messageID, status, err := c.acceptInbound(c.boardInbound(host, name, number, admitted), key, viewer.sender, string(content))
	c.board.refreshWaiting(number)
	switch {
	case errors.Is(err, errClientIDConflict):
		writeBoardError(w, status, "conflict")
	case status == http.StatusForbidden:
		writeBoardError(w, status, "forbidden")
	case status == http.StatusServiceUnavailable:
		w.Header().Set("Retry-After", boardRetryAfterBusy)
		writeBoardError(w, status, "offline")
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]int64{"id": messageID})
	}
}

func isAudioRequest(r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && strings.HasPrefix(strings.ToLower(mediaType), "audio/")
}

// handleBoardVoice takes a voice act: the recording as the body, its fields as
// headers, under an act's checks. The audio is transcoded and stored like a
// web chat voice note, and the broker transcribes it for the session.
func (c *Channel) handleBoardVoice(w http.ResponseWriter, r *http.Request) {
	extendVoiceDeadline(w)
	if !c.sameOrigin(r) {
		writeBoardError(w, http.StatusForbidden, "forbidden")
		return
	}
	viewer, ok := c.boardRequest(w, r, true)
	if !ok {
		return
	}
	act, isValid := voiceAct(r.Header)
	if !isValid {
		writeBoardError(w, http.StatusBadRequest, "invalid")
		return
	}
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	r.Body = http.MaxBytesReader(w, r.Body, voiceBodyLimit)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeBoardError(w, http.StatusRequestEntityTooLarge, "too_big")
		} else {
			writeBoardError(w, http.StatusBadRequest, "invalid")
		}
		return
	}
	name := r.PathValue("name")
	key := clientMessageKey{scope: name, sessionID: viewer.id, clientID: act.ClientID}
	host, _ := c.board.boardHost()
	admitted, number, retryAfter, err := c.board.admit(name, act, !c.hasClientMessage(c.boardClientMessages, key))
	if err == nil && host == nil {
		err = errBoardsStopped
	}
	if err != nil {
		if retryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		}
		writeBoardStoreError(w, err)
		return
	}
	route := c.boardInbound(host, name, number, admitted)
	messageID, status, err := c.acceptVoice(r.Context(), route, key, viewer.sender, mediaType, body, func(inbound *c3types.Inbound, _ float64) {
		entry := boardLogEntry{Dir: "in", ID: inbound.MessageID, Op: "voice", Part: act.Part, From: inbound.Sender.Username, Segment: act.Segment, HasMore: act.HasMore}
		if err := c.board.withBoard(number, func(board *boardState) error { return c.board.appendLocked(board, entry) }); err != nil {
			c.board.logf("board: log %q voice act %d: %v", name, inbound.MessageID, err)
		}
	})
	c.board.refreshWaiting(number)
	switch {
	case errors.Is(err, errClientIDConflict):
		writeBoardError(w, status, "conflict")
	case errors.Is(err, errVoiceDecode):
		writeBoardError(w, http.StatusUnprocessableEntity, "unreadable")
	case err != nil:
		c.board.logf("board: store %q voice act: %v", name, err)
		writeBoardError(w, http.StatusInternalServerError, "unavailable")
	case status == http.StatusForbidden:
		writeBoardError(w, status, "forbidden")
	case status == http.StatusServiceUnavailable:
		w.Header().Set("Retry-After", boardRetryAfterBusy)
		writeBoardError(w, status, "offline")
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]int64{"id": messageID})
	}
}

// voiceAct reads a voice act's fields from its headers: X-Client-Id,
// X-Board-Part (the part the shell recorded for) and, for a part of a longer
// recording, X-Voice-Segment (1, 2, …) with X-Voice-More: 1 on all but the last.
func voiceAct(header http.Header) (boardAct, bool) {
	act := boardAct{Op: "voice", ClientID: strings.TrimSpace(header.Get("X-Client-Id")), Part: header.Get("X-Board-Part")}
	if act.ClientID == "" || len(act.ClientID) > boardClientIDBytes || !channel.IsValidBoardPart(act.Part) {
		return boardAct{}, false
	}
	segment, more := header.Get("X-Voice-Segment"), header.Get("X-Voice-More")
	if segment != "" {
		number, err := strconv.Atoi(segment)
		if err != nil || number < 1 || number > boardVoiceMaxSegment || strconv.Itoa(number) != segment {
			return boardAct{}, false
		}
		act.Segment = number
	}
	switch {
	case more == "1" && act.Segment > 0:
		act.HasMore = true
	case more != "":
		return boardAct{}, false
	}
	return act, true
}

// check validates a decoded act: the op's fields and only those, each in its
// rule and size, and no line the session would read as C3's own.
func (act boardAct) check() (int, string) {
	fields, known := boardOpFields[act.Op]
	if !known || act.ClientID == "" || len(act.ClientID) > boardClientIDBytes {
		return http.StatusBadRequest, "invalid"
	}
	present := map[string]bool{
		"part": act.Part != "", "text": act.Text != "", "quote": act.Quote != "", "ref": act.Ref != 0,
		"note": act.Note != "", "value": act.Value != "", "emoji": act.Emoji != "",
	}
	for _, field := range fields.required {
		if !present[field] {
			return http.StatusBadRequest, "invalid"
		}
		delete(present, field)
	}
	for _, field := range fields.optional {
		delete(present, field)
	}
	for _, isPresent := range present {
		if isPresent {
			return http.StatusBadRequest, "invalid"
		}
	}
	if act.Part != "" && !channel.IsValidBoardPart(act.Part) || act.Ref < 0 || act.Emoji != "" && !isBoardEmoji(act.Emoji) ||
		act.Text != "" && strings.TrimSpace(act.Text) == "" || !utf8.ValidString(act.Text+act.Quote+act.Note+act.Value) {
		return http.StatusBadRequest, "invalid"
	}
	if utf8.RuneCountInString(act.Text) > channel.BoardTextMaxRunes || utf8.RuneCountInString(act.Note) > channel.BoardTextMaxRunes ||
		utf8.RuneCountInString(act.Quote) > channel.BoardQuoteMaxRunes || len(act.Value) > channel.BoardTapValueMaxBytes {
		return http.StatusRequestEntityTooLarge, "too_big"
	}
	if couldForgeBoardLine(act.Text) || couldForgeBoardLine(act.Quote) || couldForgeBoardLine(act.Note) {
		return http.StatusBadRequest, "invalid"
	}
	return 0, ""
}

// admit applies the board's own rules to an act: open, a ref naming one of
// the session's messages (whose part a missing part takes), and the rate,
// which charges a client_id once: its retries and replays are free.
func (b *boardChannel) admit(name string, act boardAct, isNewClientID bool) (boardAct, int64, int, error) {
	var number int64
	retryAfter := 0
	err := b.withBoardNamed(name, func(board *boardState) error {
		number = board.record.Number
		if board.record.Closed {
			return errBoardClosed
		}
		if act.Ref > 0 {
			answered, ok := board.entry(act.Ref)
			if !ok || answered.Dir != "out" {
				return errBoardRef
			}
			if act.Part == "" {
				act.Part = answered.Part
			}
		}
		if !isNewClientID {
			return nil
		}
		now := b.web.now()
		recent := board.acts[:0]
		for _, at := range board.acts {
			if now.Sub(at) < time.Minute {
				recent = append(recent, at)
			}
		}
		board.acts = recent
		if len(recent) >= channel.BoardActsPerMinute {
			retryAfter = int((time.Minute - now.Sub(recent[0]) + time.Second - 1) / time.Second)
			return errBoardRate
		}
		board.acts = append(board.acts, now)
		return nil
	})
	return act, number, retryAfter, err
}

var (
	errBoardRef  = errors.New("board: ref names no message of the session on this board")
	errBoardRate = errors.New("board: too many acts this minute")
)

// boardInbound is acceptInbound's route for one board: its own records (never
// persisted with web's), id counter, C3-drawn line and log.
func (c *Channel) boardInbound(host channel.Host, name string, number int64, act boardAct) inboundRoute {
	return inboundRoute{
		host: host, channel: channel.BoardChannelName, chatID: number,
		records: c.boardClientMessages, persist: func(string) {},
		nextID: func() int64 {
			var id int64
			_ = c.board.withBoard(number, func(board *boardState) error { id = board.nextID(); return nil })
			return id
		},
		line: func(inbound *c3types.Inbound) {
			inbound.Text = boardLine(name, act, inbound.MessageID)
			if act.Ref > 0 {
				inbound.ReplyTo = &c3types.ReplyContext{MessageID: act.Ref}
			}
		},
		echo: func(inbound *c3types.Inbound, _ string) {
			entry := boardLogEntry{Dir: "in", ID: inbound.MessageID, Op: act.Op, Part: act.Part, Text: act.logText(), Ref: act.Ref, From: inbound.Sender.Username}
			if err := c.board.withBoard(number, func(board *boardState) error { return c.board.appendLocked(board, entry) }); err != nil {
				c.board.logf("board: log %q action %d: %v", name, inbound.MessageID, err)
			}
		},
	}
}

// boardLine is what the session reads for one act: C3's header, then the
// viewer's checked fields.
func boardLine(name string, act boardAct, messageID int64) string {
	header := "[board " + name
	if act.Part != "" {
		header += " · " + act.Part
	}
	header += fmt.Sprintf(" · msg %d]", messageID)
	switch act.Op {
	case "voice":
		// The broker adds the transcript (or why there is none) on the next line.
		switch {
		case act.HasMore:
			return fmt.Sprintf("%s Voice note (part %d; more follows):", header, act.Segment)
		case act.Segment > 0:
			return fmt.Sprintf("%s Voice note (part %d, the last):", header, act.Segment)
		default:
			return header + " Voice note:"
		}
	case "comment":
		if act.Quote == "" {
			return header + " " + act.Text
		}
		return header + "\n" + act.logText()
	case "approve":
		line := fmt.Sprintf("%s Approved your message %d.", header, act.Ref)
		if act.Note != "" {
			line += "\n" + act.Note
		}
		return line
	case "tap":
		if act.Ref > 0 {
			return fmt.Sprintf("%s Tapped %q on your message %d.", header, act.Value, act.Ref)
		}
		return fmt.Sprintf("%s Tapped %q.", header, act.Value)
	default:
		return fmt.Sprintf("%s Reacted %s to your message %d.", header, act.Emoji, act.Ref)
	}
}

// logText is the viewer's own words for the log: a quote as "> " lines first.
func (act boardAct) logText() string {
	switch act.Op {
	case "comment":
		if act.Quote == "" {
			return act.Text
		}
		quoted := strings.FieldsFunc(act.Quote, c3types.IsLineBreak)
		return "> " + strings.Join(quoted, "\n> ") + "\n" + act.Text
	case "approve":
		return act.Note
	case "tap":
		return act.Value
	default:
		return act.Emoji
	}
}

// couldForgeBoardLine reports a line the session would read as C3's own
// "[board " line, as bodyCouldForgeLine does for the metadata line. Invisible
// runes are dropped first, so none can hide the prefix.
func couldForgeBoardLine(text string) bool {
	for _, line := range strings.FieldsFunc(text, c3types.IsLineBreak) {
		line = strings.ToLower(strings.TrimLeftFunc(strings.Map(c3types.DropInvisibleRune, line), unicode.IsSpace))
		if after, found := strings.CutPrefix(line, "[board"); found {
			if next, _ := utf8.DecodeRuneInString(after); after == "" || unicode.IsSpace(next) {
				return true
			}
		}
	}
	return false
}

func isBoardEmoji(emoji string) bool {
	return emoji != "" && len(emoji) <= boardEmojiMaxBytes && utf8.ValidString(emoji) &&
		!strings.ContainsFunc(emoji, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) })
}

// handleBoardClose marks the board closed in the store first, then has the
// broker release its route; repeating it retries the release.
func (c *Channel) handleBoardClose(w http.ResponseWriter, r *http.Request) {
	c.postDeadline(w, r)
	if !c.sameOrigin(r) {
		writeBoardError(w, http.StatusForbidden, "forbidden")
		return
	}
	if _, ok := c.boardRequest(w, r, true); !ok {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, postBodyLimit)
	if body, err := io.ReadAll(r.Body); err != nil || len(bytes.TrimSpace(body)) != 0 {
		writeBoardError(w, http.StatusBadRequest, "invalid")
		return
	}
	var number int64
	err := c.board.withBoardNamed(r.PathValue("name"), func(board *boardState) error {
		number = board.record.Number
		if !board.record.Closed {
			record := board.record
			record.Closed = true
			if err := saveBoardRecord(filepath.Join(c.board.directory, board.name), record); err != nil {
				return err
			}
			board.record = record
		}
		board.broadcast(board.statusEvent(false))
		return nil
	})
	if err != nil {
		writeBoardStoreError(w, err)
		return
	}
	if _, extension := c.board.boardHost(); extension == nil {
		writeBoardStoreError(w, errBoardsStopped)
	} else if err := extension.CloseBoardRoute(number); err != nil {
		c.board.logf("board: release board %d route: %v", number, err)
		writeBoardError(w, http.StatusInternalServerError, "close_failed")
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleBoardEvents streams one board's events. A Last-Event-ID replays the
// log after it; a fresh connect is live only (the shell has /state's log).
func (c *Channel) handleBoardEvents(w http.ResponseWriter, r *http.Request) {
	viewer, ok := c.boardRequest(w, r, true)
	if !ok {
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unavailable", http.StatusInternalServerError)
		return
	}
	stream := &boardStream{viewer: viewer.id, events: make(chan boardEvent, 64), done: make(chan struct{})}
	var replay []boardEvent
	name := r.PathValue("name")
	last, lastErr := strconv.ParseInt(r.Header.Get("Last-Event-ID"), 10, 64)
	err := c.board.withBoardNamed(name, func(board *boardState) error {
		replay = append(replay, board.statusEvent(false))
		for _, entry := range board.log {
			if lastErr == nil && entry.Seq > last {
				replay = append(replay, entryEvent(entry))
			}
		}
		board.streams[stream] = struct{}{}
		return nil
	})
	if err != nil {
		writeBoardStoreError(w, err)
		return
	}
	defer func() {
		_ = c.board.withBoardNamed(name, func(board *boardState) error { delete(board.streams, stream); return nil })
		stream.close()
	}()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	for _, event := range replay {
		if writeBoardSSE(w, event) != nil {
			return
		}
	}
	flusher.Flush()
	heartbeat := time.NewTicker(c.heartbeatInterval)
	defer heartbeat.Stop()
	for {
		var err error
		select {
		case event := <-stream.events:
			err = writeBoardSSE(w, event)
		case <-heartbeat.C:
			_, err = io.WriteString(w, ":hb\n\n")
		case <-stream.done:
			return
		case <-r.Context().Done():
			return
		}
		if err != nil {
			return
		}
		flusher.Flush()
	}
}

func writeBoardSSE(w io.Writer, event boardEvent) error {
	if event.seq > 0 {
		if _, err := fmt.Fprintf(w, "id: %d\n", event.seq); err != nil {
			return err
		}
	}
	_, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.kind, event.data)
	return err
}

// boardRequest admits a board API request (a cross-site Origin, or anyone
// but a signed-in viewer or the front's operator, is refused) and names its
// board 404 when the name breaks the rule.
func (c *Channel) boardRequest(w http.ResponseWriter, r *http.Request, touch bool) (boardViewer, bool) {
	if !c.eventsOriginAllowed(r) {
		writeBoardError(w, http.StatusForbidden, "forbidden")
		return boardViewer{}, false
	}
	viewer, status := c.boardViewer(r, touch)
	if status != 0 {
		writeBoardError(w, status, map[int]string{http.StatusUnauthorized: "unauthorized", http.StatusForbidden: "forbidden"}[status])
		return boardViewer{}, false
	}
	if !channel.IsValidBoardName(r.PathValue("name")) {
		writeBoardError(w, http.StatusNotFound, "not_found")
		return boardViewer{}, false
	}
	return viewer, true
}

func writeBoardStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errNoBoard):
		writeBoardError(w, http.StatusNotFound, "not_found")
	case errors.Is(err, errBoardClosed):
		writeBoardError(w, http.StatusConflict, "closed")
	case errors.Is(err, errBoardRef):
		writeBoardError(w, http.StatusBadRequest, "invalid")
	case errors.Is(err, errBoardRate):
		writeBoardError(w, http.StatusTooManyRequests, "rate")
	case errors.Is(err, errBoardsStopped):
		w.Header().Set("Retry-After", boardRetryAfterBusy)
		writeBoardError(w, http.StatusServiceUnavailable, "offline")
	default:
		writeBoardError(w, http.StatusInternalServerError, "unavailable")
	}
}

func writeBoardError(w http.ResponseWriter, status int, rule string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": rule})
}
