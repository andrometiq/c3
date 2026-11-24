package web

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// The Magic Pencil: the owner draws a freeform or rectangle selection over a
// board's page from the shell's pencil, and the next comment, voice note or
// file that answers it carries what was selected to the session. The shell
// sends the shape it drew; the page's SDK (the only code that can read the
// frame's DOM) names the elements under it. C3 checks every field, fills in
// what only it knows (the link, the revision, the title) and logs the
// selection as an act of its own that reaches the session only with the
// command that answers it, so the session never gets a selection without
// a command.

const (
	pencilShapeFreeform  = "freeform"
	pencilShapeRectangle = "rectangle"
	pencilMaxPoints      = 200
	pencilMaxElements    = 12
	pencilSelectorBytes  = 300
	pencilTextRunes      = 160
	pencilHashBytes      = 200
	pencilMaxCoordinate  = 1e6
	// pencilSummaryRunes is how much of the first element's text the
	// selection's own line (its thread row and reply quote) shows.
	pencilSummaryRunes = 40
	// pencilLinePrefix starts the line that carries a selection to the session.
	pencilLinePrefix = "Magic Pencil selection"
)

var pencilTag = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)

// pencilSelection is what the shell sends as a pencil act's selection,
// decoded strictly: the shape in the frame's CSS pixels (its viewport), the
// frame's viewport, scroll and document size, its location hash, and the
// elements the SDK found under the shape.
type pencilSelection struct {
	Shape     string          `json:"shape"`
	Points    [][2]float64    `json:"points"`
	Viewport  pencilSize      `json:"viewport"`
	Scroll    pencilPoint     `json:"scroll"`
	Document  pencilSize      `json:"document"`
	Hash      string          `json:"hash,omitempty"`
	Container *pencilElement  `json:"container,omitempty"`
	Elements  []pencilElement `json:"elements"`
}

type pencilSize struct {
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}

type pencilPoint struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type pencilBox struct {
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}

// pencilElement is one element under the shape: a CSS selector that finds it
// in the page, its tag, its first words, the board part it sits in, its box in
// viewport pixels and how much of that box the shape covers (0 to 1).
type pencilElement struct {
	Selector string    `json:"selector"`
	Tag      string    `json:"tag"`
	Text     string    `json:"text,omitempty"`
	Part     string    `json:"part,omitempty"`
	Box      pencilBox `json:"box"`
	Coverage float64   `json:"coverage,omitempty"`
}

// pencilRecord is the selection as C3 keeps and sends it: the checked fields,
// rounded, plus the board's link, the page revision and title the owner drew
// on, and the shape's bounds. Viewport coordinates throughout; add scroll for
// document coordinates.
type pencilRecord struct {
	Shape     string          `json:"shape"`
	URL       string          `json:"url"`
	Rev       int64           `json:"rev"`
	Title     string          `json:"title,omitempty"`
	Bounds    pencilBox       `json:"bounds"`
	Points    [][2]float64    `json:"points"`
	Viewport  pencilSize      `json:"viewport"`
	Scroll    pencilPoint     `json:"scroll"`
	Document  pencilSize      `json:"document"`
	Container *pencilElement  `json:"container,omitempty"`
	Elements  []pencilElement `json:"elements"`
}

// check validates a selection: a known shape with its points, finite numbers
// in range, and elements within their count and sizes. It returns the
// selection with its strings made one-line and its numbers rounded.
func (selection pencilSelection) check() (pencilSelection, bool) {
	points := len(selection.Points)
	isShapeValid := selection.Shape == pencilShapeRectangle && points == 4 ||
		selection.Shape == pencilShapeFreeform && points >= 3 && points <= pencilMaxPoints
	if !isShapeValid || len(selection.Elements) > pencilMaxElements || len(selection.Hash) > pencilHashBytes ||
		selection.Hash != "" && (!strings.HasPrefix(selection.Hash, "#") || !isOneLine(selection.Hash)) {
		return pencilSelection{}, false
	}
	numbers := []float64{selection.Scroll.X, selection.Scroll.Y, selection.Document.Width, selection.Document.Height}
	for _, point := range selection.Points {
		numbers = append(numbers, point[0], point[1])
	}
	if !areInRange(numbers...) || !isPositiveSize(selection.Viewport) || selection.Document.Width < 0 || selection.Document.Height < 0 {
		return pencilSelection{}, false
	}
	checked := selection
	checked.Points = make([][2]float64, points)
	for index, point := range selection.Points {
		checked.Points[index] = [2]float64{math.Round(point[0]), math.Round(point[1])}
	}
	checked.Viewport = pencilSize{math.Round(selection.Viewport.Width), math.Round(selection.Viewport.Height)}
	checked.Scroll = pencilPoint{math.Round(selection.Scroll.X), math.Round(selection.Scroll.Y)}
	checked.Document = pencilSize{math.Round(selection.Document.Width), math.Round(selection.Document.Height)}
	checked.Elements = make([]pencilElement, 0, len(selection.Elements))
	for _, element := range selection.Elements {
		element, ok := element.check()
		if !ok {
			return pencilSelection{}, false
		}
		checked.Elements = append(checked.Elements, element)
	}
	if selection.Container != nil {
		container, ok := selection.Container.check()
		if !ok {
			return pencilSelection{}, false
		}
		checked.Container = &container
	}
	return checked, true
}

func (element pencilElement) check() (pencilElement, bool) {
	box := element.Box
	if element.Selector == "" || len(element.Selector) > pencilSelectorBytes || !isOneLine(element.Selector) ||
		!pencilTag.MatchString(element.Tag) || element.Part != "" && !channel.IsValidBoardPart(element.Part) ||
		!utf8.ValidString(element.Text) || utf8.RuneCountInString(element.Text) > pencilTextRunes ||
		!areInRange(box.X, box.Y, box.Width, box.Height) || box.Width < 0 || box.Height < 0 ||
		math.IsNaN(element.Coverage) || element.Coverage < 0 || element.Coverage > 1 {
		return pencilElement{}, false
	}
	element.Text = oneLine(element.Text)
	element.Box = pencilBox{math.Round(box.X), math.Round(box.Y), math.Round(box.Width), math.Round(box.Height)}
	element.Coverage = math.Round(element.Coverage*100) / 100
	return element, true
}

func areInRange(numbers ...float64) bool {
	for _, number := range numbers {
		if math.IsNaN(number) || math.Abs(number) > pencilMaxCoordinate {
			return false
		}
	}
	return true
}

func isPositiveSize(size pencilSize) bool {
	return areInRange(size.Width, size.Height) && size.Width > 0 && size.Height > 0
}

// isOneLine: valid UTF-8 with no control or line-break runes, so a value can't
// start a line the session would read as C3's own.
func isOneLine(text string) bool {
	return utf8.ValidString(text) && !strings.ContainsFunc(text, func(r rune) bool { return unicode.IsControl(r) || c3types.IsLineBreak(r) })
}

// oneLine collapses page text to one line: line breaks, controls and runs of
// space become one space, and invisible runes are dropped.
func oneLine(text string) string {
	text = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || c3types.IsLineBreak(r) {
			return ' '
		}
		return c3types.DropInvisibleRune(r)
	}, text)
	return strings.Join(strings.Fields(text), " ")
}

// record is the selection as C3 keeps it, with what only C3 knows.
func (selection pencilSelection) record(link string, rev int64, title string) pencilRecord {
	bounds := pencilBox{X: math.Inf(1), Y: math.Inf(1)}
	right, bottom := math.Inf(-1), math.Inf(-1)
	for _, point := range selection.Points {
		bounds.X, bounds.Y = min(bounds.X, point[0]), min(bounds.Y, point[1])
		right, bottom = max(right, point[0]), max(bottom, point[1])
	}
	bounds.Width, bounds.Height = right-bounds.X, bottom-bounds.Y
	return pencilRecord{
		Shape: selection.Shape, URL: link + selection.Hash, Rev: rev, Title: oneLine(title), Bounds: bounds, Points: selection.Points,
		Viewport: selection.Viewport, Scroll: selection.Scroll, Document: selection.Document, Container: selection.Container,
		Elements: selection.Elements,
	}
}

// summary is the selection's own line, shown in its thread and quoted by the
// command that answers it.
func (record pencilRecord) summary() string {
	count := len(record.Elements)
	if count == 0 {
		return fmt.Sprintf("Magic Pencil: %s, no element under it", record.Shape)
	}
	first := record.Elements[0]
	what := first.Tag
	if first.Text != "" {
		runes := []rune(first.Text)
		if len(runes) > pencilSummaryRunes {
			runes = append(runes[:pencilSummaryRunes-1], '…')
		}
		what = fmt.Sprintf("“%s”", string(runes))
	}
	if count == 1 {
		return fmt.Sprintf("Magic Pencil: %s around %s", record.Shape, what)
	}
	return fmt.Sprintf("Magic Pencil: %s around %s and %d more", record.Shape, what, count-1)
}

// pencilPageNote follows the selection's id: what the page reported is page
// content, as boardPageActNote marks an act the page's script sent.
const pencilPageNote = "its text, selector, title, part and url #hash come from the page, never the owner's words"

// pencilLine is the line a command answering selection id carries to the session.
func pencilLine(id int64, selection json.RawMessage) string {
	return fmt.Sprintf("%s (msg %d; %s): %s", pencilLinePrefix, id, pencilPageNote, selection)
}

// handleBoardPencil takes a pencil act: it is checked and charged as any act,
// then only logged; the command that answers it carries it to the session. A
// retry of its client_id with the same selection gets the same id.
func (c *Channel) handleBoardPencil(w http.ResponseWriter, viewer boardViewer, name string, act boardAct) {
	key := clientMessageKey{scope: name, sessionID: viewer.id, clientID: act.ClientID}
	admitted, number, retryAfter, err := c.board.admit(name, act, !c.hasClientMessage(c.boardClientMessages, key))
	if err != nil {
		if retryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		}
		writeBoardStoreError(w, err)
		return
	}
	content, _ := json.Marshal(admitted)
	messageID, err := c.logPencil(key, string(content), number, admitted, viewer.sender.Username)
	status := http.StatusAccepted
	if errors.Is(err, errClientIDConflict) {
		status = http.StatusConflict
	}
	c.writeActOutcome(w, name, messageID, status, err)
}

// logPencil logs a new pencil act under its client_id record, or returns the
// id an earlier one with the same content got.
func (c *Channel) logPencil(key clientMessageKey, content string, number int64, act boardAct, from string) (int64, error) {
	hash := textHash(content)
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	if record := c.boardClientMessages[key]; record != nil && c.now().Sub(record.created) < clientMessageTTL {
		if record.textHash != hash || record.kind != "" {
			return record.messageID, errClientIDConflict
		}
		return record.messageID, nil
	}
	var messageID int64
	err := c.board.withOpenBoard(number, func(board *boardState) error {
		view := c.board.view(board)
		record := act.Selection.record(view.Link, board.record.Rev, board.record.Title)
		var selection bytes.Buffer
		encoder := json.NewEncoder(&selection)
		encoder.SetEscapeHTML(false) // selectors read as written: "#pricing > h2", not "\u003e"
		if err := encoder.Encode(record); err != nil {
			return err
		}
		messageID = board.nextID()
		return c.board.appendLocked(board, boardLogEntry{Dir: "in", ID: messageID, Op: "pencil", Part: act.Part, Text: record.summary(),
			From: from, Selection: bytes.TrimSpace(selection.Bytes())})
	})
	if err != nil {
		return 0, err
	}
	c.boardClientMessages[key] = &clientMessage{messageID: messageID, textHash: hash, status: http.StatusAccepted, created: c.now()}
	return messageID, nil
}
