package web

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"net/url"
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
	"github.com/Andrometiq/c3/internal/intake"
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
	// Why a part of a recording never reached C3 (X-Voice-Unsent), also its
	// stand-in act's log status: the owner cancelled the recording, or C3
	// refused the part's upload.
	voiceUnsentCancelled = "cancelled"
	voiceUnsentRefused   = "refused"
	// A file from C3's chat: at most what a Telegram bot can fetch, so the agent
	// meets one ceiling on both routes. Its bytes live in boardFilesDirectory.
	boardFileLimit      = 20 << 20
	boardFileNameRunes  = 120
	boardFilesDirectory = "board-files"
	boardFileMIMEBytes  = 127 // a media type's longest name (RFC 6838)
	// A caption travels as a header, which a fronting proxy refuses when large.
	boardFileCaptionBytes = 6 << 10
	// The session's line for a reply quotes this much of the message it answers.
	boardReplySnippetRunes = 80
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
	// FromPage marks an act the page's script sent (c3.comment, c3.tap, …):
	// the shell sets it, so the session never reads it as typed by the owner.
	FromPage bool `json:"from_page"`
	// A voice act's place in a longer recording, the shell's id for that
	// recording, and for a part that never reached C3 why; not from JSON.
	Segment   int    `json:"-"`
	HasMore   bool   `json:"-"`
	Recording string `json:"-"`
	Unsent    string `json:"-"`
	// A file act's fields from its headers and its stored bytes' id; not from JSON.
	Name   string `json:"-"`
	MIME   string `json:"-"`
	Size   int64  `json:"-"`
	FileID string `json:"-"`
	// RefText is the start of the message a reply answers, as admit read it,
	// and IsRefFromPage tells that message came from the page's script.
	RefText       string `json:"-"`
	IsRefFromPage bool   `json:"-"`
	// Selection is a pencil act's (the Magic Pencil); RefSelection is the
	// selection of the pencil act a reply answers, as C3 logged it.
	Selection    *pencilSelection `json:"selection,omitempty"`
	RefSelection json.RawMessage  `json:"-"`
}

// boardFields are the fields an op needs and the ones it may add.
type boardFields struct{ required, optional []string }

// boardOpFields are the JSON ops'; a file comes only as its own request.
var boardOpFields = map[string]boardFields{
	"comment": {required: []string{"part", "text"}, optional: []string{"quote", "ref"}},
	"approve": {required: []string{"ref"}, optional: []string{"part", "note"}},
	"tap":     {required: []string{"part", "value"}, optional: []string{"ref"}},
	"react":   {required: []string{"ref", "emoji"}},
	"pencil":  {required: []string{"part", "selection"}},
}

var boardFileFields = boardFields{required: []string{"part"}, optional: []string{"text", "ref"}}

// boardReplyOps' ref is the message they answer: any on the board.
var boardReplyOps = map[string]bool{"comment": true, "voice": true, "file": true}

var boardLintRules = []struct {
	pattern *regexp.Regexp
	warning string
}{
	{regexp.MustCompile(`localStorage|sessionStorage`), "uses browser storage, which throws in a board frame"},
	{regexp.MustCompile(`\bfetch\(|XMLHttpRequest`), "makes network requests, which a board frame blocks"},
	{regexp.MustCompile(`(?i)\b(?:src|href)\s*=\s*["']?\s*https?://`), "loads or links an outside URL, which a board frame blocks"},
	{regexp.MustCompile(`(?i)<form\b`), "has a form, which a board frame cannot submit"},
	{regexp.MustCompile(`(?i)\btarget\s*=\s*["']?_blank`), "opens new windows, which a board frame cannot"},
	{regexp.MustCompile(`(?i)\bdata-c3-part\s*=\s*["']?page(?:["'\s/>]|$)`), `uses the part address "page", which C3 keeps for Magic Pencil selections outside parts`},
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
		writeBodyError(w, err)
		return
	}
	if status, rule := act.check(); status != 0 {
		writeBoardError(w, status, rule)
		return
	}
	if act.Op == "pencil" {
		c.handleBoardPencil(w, viewer, name, act)
		return
	}
	content, _ := json.Marshal(act)
	route, key, number, ok := c.admitAct(w, name, viewer, act)
	if !ok {
		return
	}
	messageID, status, err := c.acceptInbound(route, key, viewer.sender, string(content))
	c.board.refreshWaiting(number)
	c.writeActOutcome(w, name, messageID, status, err)
}

// admitAct runs admit on a checked act: its route, or the refusal written.
func (c *Channel) admitAct(w http.ResponseWriter, name string, viewer boardViewer, act boardAct) (inboundRoute, clientMessageKey, int64, bool) {
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
		return inboundRoute{}, key, number, false
	}
	return c.boardInbound(host, name, number, admitted), key, number, true
}

// writeActOutcome answers an admitted act: 202 {id}, or why C3 didn't take it.
func (c *Channel) writeActOutcome(w http.ResponseWriter, name string, messageID int64, status int, err error) {
	switch {
	case errors.Is(err, errClientIDConflict):
		writeBoardError(w, status, "conflict")
	case errors.Is(err, errBoardClosed), errors.Is(err, errNoBoard), errors.Is(err, errBoardsStopped):
		writeBoardStoreError(w, err) // made read-only (or gone) since it was admitted
	case errors.Is(err, errVoiceDecode):
		writeBoardError(w, http.StatusUnprocessableEntity, "unreadable")
	case err != nil:
		c.board.logf("board: store %q act: %v", name, err)
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

// writeBodyError refuses a body cut short: too_big past its limit, else invalid.
func writeBodyError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeBoardError(w, http.StatusRequestEntityTooLarge, "too_big")
	} else {
		writeBoardError(w, http.StatusBadRequest, "invalid")
	}
}

func isAudioRequest(r *http.Request) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return err == nil && strings.HasPrefix(strings.ToLower(mediaType), "audio/")
}

// handleBoardVoice takes a voice act: the recording as the body, its fields as
// headers, under an act's checks. The audio is transcoded and stored like a
// web chat voice note, and the broker transcribes it for the session. A part
// of a recording that never reached C3 comes with no body, as a line saying so,
// so the session still sees every part.
// voiceMeterPattern is the shell's X-Voice-Meter line: short key=value pairs only.
var voiceMeterPattern = regexp.MustCompile(`^[a-z]+=[A-Za-z0-9.-]{1,16}(;[a-z]+=[A-Za-z0-9.-]{1,16}){0,9}$`)

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
	if meter := r.Header.Get("X-Voice-Meter"); meter != "" && voiceMeterPattern.MatchString(meter) {
		c.board.logf("board: %q voice meter %s", r.PathValue("name"), meter) // what the shell's wave measured
	}
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	r.Body = http.MaxBytesReader(w, r.Body, voiceBodyLimit)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeBodyError(w, err)
		return
	}
	if act.Unsent != "" && len(body) != 0 {
		writeBoardError(w, http.StatusBadRequest, "invalid")
		return
	}
	name := r.PathValue("name")
	route, key, number, ok := c.admitAct(w, name, viewer, act)
	if !ok {
		return
	}
	var messageID int64
	var status int
	if act.Unsent != "" {
		messageID, status, err = c.acceptInbound(route, key, viewer.sender, act.voiceFields())
	} else {
		messageID, status, err = c.acceptVoice(r.Context(), route, key, viewer.sender, mediaType, act.voiceFields(), body,
			func(inbound *c3types.Inbound, _ float64) { route.echo(inbound, act.ClientID) })
	}
	c.board.refreshWaiting(number)
	c.writeActOutcome(w, name, messageID, status, err)
}

// handleBoardFile takes a file the owner picked in C3's chat under a voice act's
// checks. It is stored by an id from its contents (a retry stores it again in
// place) and reaches the session as an attachment of the act's line.
func (c *Channel) handleBoardFile(w http.ResponseWriter, r *http.Request) {
	extendVoiceDeadline(w)
	if !c.sameOrigin(r) {
		writeBoardError(w, http.StatusForbidden, "forbidden")
		return
	}
	viewer, ok := c.boardRequest(w, r, true)
	if !ok {
		return
	}
	act, status, rule := fileAct(r.Header)
	if status != 0 {
		writeBoardError(w, status, rule)
		return
	}
	name := r.PathValue("name")
	temporary, fileID, size, err := c.receiveBoardFile(http.MaxBytesReader(w, r.Body, boardFileLimit))
	if err != nil {
		if errors.As(err, new(*http.MaxBytesError)) {
			writeBodyError(w, err)
		} else {
			c.writeActOutcome(w, name, 0, 0, err)
		}
		return
	}
	defer os.Remove(temporary) // already gone once kept
	act.FileID, act.Size = fileID, size
	if c.isClientIDReused(clientMessageKey{scope: name, sessionID: viewer.id, clientID: act.ClientID}, act.fileFields()) {
		writeBoardError(w, http.StatusConflict, "conflict")
		return
	}
	route, key, number, ok := c.admitAct(w, name, viewer, act)
	if !ok {
		return
	}
	var messageID int64
	if err = c.keepBoardFile(temporary, fileID); err == nil {
		err = route.checkOpen()
	}
	if err == nil {
		messageID, status, err = c.acceptInbound(route, key, viewer.sender, act.fileFields())
	}
	c.board.refreshWaiting(number)
	c.writeActOutcome(w, name, messageID, status, err)
}

// fileAct reads a file act's headers: X-Client-Id, X-Board-Part, X-File-Name,
// X-Caption (both percent-encoded UTF-8; the caption checked as a comment's
// text), X-Reply-To and the browser's Content-Type, kept only when valid.
func fileAct(header http.Header) (boardAct, int, string) {
	name, nameErr := url.PathUnescape(header.Get("X-File-Name"))
	caption, captionErr := url.PathUnescape(header.Get("X-Caption"))
	ref, isRefValid := replyRef(header)
	mediaType, _, err := mime.ParseMediaType(header.Get("Content-Type"))
	if err != nil || len(mediaType) > boardFileMIMEBytes {
		mediaType = "application/octet-stream"
	}
	act := boardAct{Op: "file", ClientID: strings.TrimSpace(header.Get("X-Client-Id")), Part: header.Get("X-Board-Part"),
		Text: caption, Ref: ref, Name: safeFileName(name), MIME: mediaType}
	if len(header.Get("X-Caption")) > boardFileCaptionBytes {
		return boardAct{}, http.StatusRequestEntityTooLarge, "too_big"
	}
	if nameErr != nil || captionErr != nil || !isRefValid || act.Name == "" {
		return boardAct{}, http.StatusBadRequest, "invalid"
	}
	status, rule := act.checkFields(boardFileFields)
	return act, status, rule
}

// safeFileName is the browser's name for a file, shown as text: non-graphic
// runes and path separators replaced, clipped to boardFileNameRunes.
func safeFileName(name string) string {
	safe := []rune(strings.Map(func(r rune) rune {
		if !unicode.IsGraphic(r) || r == '/' || r == '\\' {
			return '_'
		}
		return r
	}, strings.ToValidUTF8(name, "\uFFFD")))
	return strings.TrimSpace(string(safe[:min(len(safe), boardFileNameRunes)]))
}

// fileFields are what a file act's client id retry must repeat.
func (act boardAct) fileFields() string {
	return fmt.Sprintf("file part=%s ref=%d name=%q mime=%q caption=%q id=%s", act.Part, act.Ref, act.Name, act.MIME, act.Text, act.FileID)
}

// isClientIDReused reports a client id the dedup window holds for other
// content, which acceptInbound would refuse, so nothing of it is kept.
func (c *Channel) isClientIDReused(key clientMessageKey, content string) bool {
	c.clientMu.Lock()
	record := c.boardClientMessages[key]
	c.clientMu.Unlock()
	if record == nil || c.now().Sub(record.created) >= clientMessageTTL {
		return false
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	return record.kind != "" || record.textHash != textHash(content)
}

// replyRef reads X-Reply-To, the message an upload answers: none or an id.
func replyRef(header http.Header) (int64, bool) {
	value := header.Get("X-Reply-To")
	if value == "" {
		return 0, true
	}
	ref, err := strconv.ParseInt(value, 10, 64)
	return ref, err == nil && ref > 0 && strconv.FormatInt(ref, 10) == value
}

// voiceAct reads a voice act's fields from its headers: X-Client-Id,
// X-Board-Part (the part the shell recorded for) and, for a part of a longer
// recording, X-Voice-Segment (1, 2, …) with X-Voice-More: 1 on all but the
// last, the optional X-Voice-Recording (one id on all of a recording's
// parts), X-Voice-Unsent (cancelled: the last part; refused: any) for a
// part whose audio never reached C3, and the optional X-Reply-To.
func voiceAct(header http.Header) (boardAct, bool) {
	ref, isRefValid := replyRef(header)
	act := boardAct{Op: "voice", ClientID: strings.TrimSpace(header.Get("X-Client-Id")), Part: header.Get("X-Board-Part"),
		Recording: strings.TrimSpace(header.Get("X-Voice-Recording")), Ref: ref}
	if !isRefValid || act.ClientID == "" || len(act.ClientID) > boardClientIDBytes || !channel.IsValidBoardPart(act.Part) ||
		len(act.Recording) > boardClientIDBytes || strings.ContainsFunc(act.Recording, func(r rune) bool { return r < '!' || r > '~' }) {
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
	act.Unsent = header.Get("X-Voice-Unsent")
	isUnsentValid := act.Unsent == "" || act.Segment > 0 &&
		(act.Unsent == voiceUnsentRefused || act.Unsent == voiceUnsentCancelled && !act.HasMore)
	if !isUnsentValid {
		return boardAct{}, false
	}
	return act, true
}

// voiceFields are a voice act's own fields, which a retry of its client id
// must repeat along with the audio.
func (act boardAct) voiceFields() string {
	return fmt.Sprintf("voice part=%s segment=%d more=%t unsent=%s recording=%s ref=%d", act.Part, act.Segment, act.HasMore, act.Unsent, act.Recording, act.Ref)
}

// check validates a decoded JSON act by its op's fields. Only the owner draws
// with the pencil: the page never sends a pencil act.
func (act *boardAct) check() (int, string) {
	fields, known := boardOpFields[act.Op]
	if !known || act.FromPage && (boardReplyOps[act.Op] && act.Ref != 0 || act.Op == "pencil") { // the page never answers a message
		return http.StatusBadRequest, "invalid"
	}
	if act.Selection != nil {
		selection, ok := act.Selection.check()
		if !ok {
			return http.StatusBadRequest, "invalid"
		}
		act.Selection = &selection
	}
	return act.checkFields(fields)
}

// checkFields validates an act: its op's fields and only those, each in its
// rule and size, and no line the session would read as C3's own.
func (act boardAct) checkFields(fields boardFields) (int, string) {
	if act.ClientID == "" || len(act.ClientID) > boardClientIDBytes {
		return http.StatusBadRequest, "invalid"
	}
	present := map[string]bool{
		"part": act.Part != "", "text": act.Text != "", "quote": act.Quote != "", "ref": act.Ref != 0,
		"note": act.Note != "", "value": act.Value != "", "emoji": act.Emoji != "", "selection": act.Selection != nil,
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
// the session's messages (whose part a missing part takes; a reply may answer
// any message on the board, and the session's line quotes its start), and the
// rate, which charges a client_id once: its retries and replays are free.
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
			if !ok || answered.Dir != "out" && !boardReplyOps[act.Op] {
				return errBoardRef
			}
			if act.Part == "" {
				act.Part = answered.Part
			}
			if boardReplyOps[act.Op] {
				act.RefText, act.IsRefFromPage = board.replySnippet(answered), answered.FromPage
			}
			if boardReplyOps[act.Op] && answered.Op == "pencil" {
				act.RefSelection = answered.Selection
			}
		}
		if !isNewClientID {
			return nil
		}
		if retryAfter = chargeMinute(&board.acts, b.web.now(), channel.BoardActsPerMinute); retryAfter > 0 {
			return errBoardRate
		}
		return nil
	})
	return act, number, retryAfter, err
}

// replySnippet is the start of message answered as it now reads (its latest
// edit or transcript; a file's name when it has no caption), on one line.
func (board *boardState) replySnippet(answered boardLogEntry) string {
	text := cmp.Or(answered.Text, answered.Name)
	for _, entry := range board.log {
		if entry.ID == answered.ID && entry.Op == "edit" && (entry.Status == "" || entry.Status == c3types.VoiceStatusHeard) {
			text = entry.Text
		}
	}
	snippet := []rune(boardSnippetBrackets.Replace(strings.Join(strings.Fields(text), " ")))
	if len(snippet) > boardReplySnippetRunes {
		return string(snippet[:boardReplySnippetRunes]) + "…"
	}
	return string(snippet)
}

// boardSnippetBrackets keep a quoted message from reading as a C3 header.
var boardSnippetBrackets = strings.NewReplacer("[", "(", "]", ")")

// chargeMinute counts one use at now in uses, which keeps the last minute's,
// unless limit are there already: then it counts nothing and returns the
// seconds until the oldest leaves the minute.
func chargeMinute(uses *[]time.Time, now time.Time, limit int) int {
	recent := (*uses)[:0]
	for _, at := range *uses {
		if now.Sub(at) < time.Minute {
			recent = append(recent, at)
		}
	}
	*uses = recent
	if len(recent) >= limit {
		return int((time.Minute - now.Sub(recent[0]) + time.Second - 1) / time.Second)
	}
	*uses = append(recent, now)
	return 0
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
			if act.FileID != "" {
				kind := "document"
				if strings.HasPrefix(act.MIME, "image/") {
					kind = "photo"
				}
				inbound.Attachments = []c3types.Attachment{{Kind: kind, FileID: act.FileID, MIME: act.MIME, Size: act.Size, Name: act.Name}}
			}
		},
		checkOpen: func() error { return c.board.withOpenBoard(number, func(*boardState) error { return nil }) },
		source:    func(inbound *c3types.Inbound) *intake.Source { return boardSource(inbound, act.FromPage) },
		echo: func(inbound *c3types.Inbound, _ string) {
			entry := boardLogEntry{Dir: "in", ID: inbound.MessageID, Op: act.Op, Part: act.Part, Text: act.logText(), Ref: act.Ref, From: inbound.Sender.Username,
				Segment: act.Segment, HasMore: act.HasMore, Recording: act.Recording, Status: act.Unsent, FromPage: act.FromPage,
				Name: act.Name, MIME: act.MIME, Size: act.Size}
			if err := c.board.withBoard(number, func(board *boardState) error { return c.board.appendLocked(board, entry) }); err != nil {
				c.board.logf("board: log %q action %d: %v", name, inbound.MessageID, err)
			}
		},
	}
}

// boardSource is a board act's intake source, as a Telegram message has one:
// the board number as its chat, the act id as its message and update, the
// viewer as its sender, C3's line as its text, and whether the page sent it.
func boardSource(inbound *c3types.Inbound, isFromPage bool) *intake.Source {
	sender := inbound.Sender.UserID
	source := &intake.Source{Channel: inbound.Channel, ChatID: inbound.ChatID, MessageID: inbound.MessageID, UpdateID: inbound.MessageID,
		SenderID: &sender, Text: inbound.Text, FromPage: isFromPage, Attachments: make([]intake.SourceAttachment, 0, len(inbound.Attachments))}
	for _, attachment := range inbound.Attachments {
		source.Attachments = append(source.Attachments, intake.SourceAttachment{Kind: attachment.Kind, FileID: attachment.FileID,
			Size: attachment.Size, MIME: attachment.MIME, Name: attachment.Name})
	}
	return source
}

// boardPageActNote follows C3's header on an act the page's script sent.
const boardPageActNote = "(sent by the page, not typed by the owner)"

// boardLine is what the session reads for one act: C3's header, then the
// viewer's checked fields. A reply to a Magic Pencil selection carries that
// selection on a line of its own after the act's own words (a voice note's
// transcript follows it).
func boardLine(name string, act boardAct, messageID int64) string {
	line := actLine(name, act, messageID)
	if len(act.RefSelection) == 0 {
		return line
	}
	return line + "\n" + pencilLine(act.Ref, act.RefSelection)
}

func actLine(name string, act boardAct, messageID int64) string {
	header := "[board " + name
	if act.Part != "" {
		header += " · " + act.Part
	}
	header += fmt.Sprintf(" · msg %d", messageID)
	if boardReplyOps[act.Op] && act.Ref > 0 {
		whose := "msg"
		if act.IsRefFromPage {
			whose = "the page's msg"
		}
		header += fmt.Sprintf(" · reply to %s %d %q", whose, act.Ref, act.RefText)
	}
	header += "]"
	if act.FromPage {
		header += " " + boardPageActNote
	}
	switch act.Op {
	case "voice":
		return header + " " + voiceLine(act)
	case "file": // named first, as a voice note's line is: the stored path has no name
		return strings.TrimSuffix(fmt.Sprintf("%s File %q:\n%s", header, act.Name, act.Text), "\n")
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

// voiceLine names a voice act's place in its recording. The broker adds the
// transcript (or why there is none) on the next line; a part that never
// reached C3 says so instead.
func voiceLine(act boardAct) string {
	line := "Voice note:"
	switch {
	case act.HasMore:
		line = fmt.Sprintf("Voice note (part %d; more follows):", act.Segment)
	case act.Segment > 0:
		line = fmt.Sprintf("Voice note (part %d, the last):", act.Segment)
	}
	switch act.Unsent {
	case voiceUnsentCancelled:
		return line + " the recording was cancelled; this part was not sent."
	case voiceUnsentRefused:
		return line + " C3 couldn't take this part; it was not sent."
	}
	return line
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
	case "react":
		return act.Emoji
	default:
		return act.Text // a file's caption; a voice act has none
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
