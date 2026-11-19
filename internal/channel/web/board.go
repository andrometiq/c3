package web

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// boardChannel is the board channel: every board is its own route (board,
// number, no topic) on the web listener. It shares web's operator, logins and
// front, never web's stream, replay or message ids.
type boardChannel struct {
	web    *Channel
	assets fs.FS // board.html, board.js, board-sdk.js, board-starter.html

	mu         sync.Mutex
	host       channel.Host // nil until Start
	directory  string
	key        []byte
	boards     map[string]*boardState
	lastNumber int64

	heldCountFailure sync.Once // a held-count error is logged once per run
}

// boardState is one board in memory, guarded by boardChannel.mu.
type boardState struct {
	name    string
	record  boardRecord
	log     []boardLogEntry // the newest BoardLogHistoryEntries lines
	lastSeq int64
	lastID  int64 // one message-id counter for both directions
	holder  *channel.RouteHolder
	waiting int
	acts    []time.Time // when each act with a new client_id came, in the last minute
	spoken  []time.Time // when each reply C3 synthesized for this board was asked for, in the last minute
	streams map[*boardStream]struct{}
}

type boardStream struct {
	viewer string
	events chan boardEvent
	done   chan struct{}
	once   sync.Once
}

type boardEvent struct {
	seq  int64 // the log seq; 0 for live-only events
	kind string
	data []byte
}

var (
	errBoardClosed   = errors.New("board: the board is closed")
	errNoBoard       = errors.New("board: no such board")
	errBoardsStopped = errors.New("board: channel not started")
	errNotVoice      = errors.New("board: message is no voice act on this board")
	errVoiceSettled  = errors.New("board: the voice act already has its status")
)

var (
	_ channel.Channel            = (*boardChannel)(nil)
	_ channel.BoardKeeper        = (*boardChannel)(nil)
	_ channel.PresenceNotifier   = (*boardChannel)(nil)
	_ channel.ReadbackSender     = (*boardChannel)(nil)
	_ channel.VoiceStatusSender  = (*boardChannel)(nil)
	_ channel.LocalAudioProvider = (*boardChannel)(nil)
	_ channel.BoardProvider      = (*Channel)(nil)
)

func newBoardChannel(web *Channel) *boardChannel {
	return &boardChannel{web: web, assets: pages, boards: make(map[string]*boardState)}
}

func newBoardState(name string, record boardRecord) *boardState {
	return &boardState{name: name, record: record, streams: make(map[*boardStream]struct{})}
}

// BoardChannel implements channel.BoardProvider.
func (c *Channel) BoardChannel() channel.Channel { return c.board }

func (b *boardChannel) Name() string { return channel.BoardChannelName }

func (b *boardChannel) Capabilities() c3types.Capabilities {
	return c3types.Capabilities{
		// RichText false: the shell and the SDK's threads show text as typed.
		Channel: channel.BoardChannelName, RichText: false, MaxMessageRunes: channel.BoardTextMaxRunes,
		EditMessages: true, Typing: true, Reactions: true, MediaKinds: []c3types.MediaKind{c3types.MediaFile},
		OriginalFile: true, MaxSendBytes: channel.BoardPageMaxBytes,
		Inbound: c3types.InboundCaps{SupportsReplyContext: true},
	}
}

// Start loads the boards from web's state directory, so web must have started.
func (b *boardChannel) Start(_ context.Context, host channel.Host) error {
	if host == nil {
		return errors.New("board: nil host")
	}
	if _, ok := host.(channel.BoardHost); !ok {
		return errors.New("board: host cannot close board routes or count held messages")
	}
	b.web.persistMu.Lock()
	stateDirectory := b.web.stateDirectory
	b.web.persistMu.Unlock()
	if stateDirectory == "" {
		return errors.New("board: the web channel has not loaded its state")
	}
	directory := filepath.Join(stateDirectory, boardsDirectoryName)
	boards, err := loadBoards(directory, host)
	if err != nil {
		return err
	}
	key, err := loadOrCreateBoardKey(filepath.Join(directory, boardKeyName))
	if err != nil {
		return err
	}
	var lastNumber int64
	for _, board := range boards {
		lastNumber = max(lastNumber, board.record.Number)
		if seed, ok := host.(queuedIDHost); ok {
			queued, err := seed.MaxQueuedMessageID(channel.BoardChannelName, board.record.Number, nil)
			if err != nil {
				return fmt.Errorf("board: read %q held queue for the id seed: %w", board.name, err)
			}
			board.lastID = max(board.lastID, queued)
		}
	}
	b.mu.Lock()
	b.host, b.directory, b.key, b.boards, b.lastNumber = host, directory, key, boards, lastNumber
	b.mu.Unlock()
	host.Logf("board: serving %d boards", len(boards))
	return nil
}

func (b *boardChannel) Stop() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.host = nil
	for _, board := range b.boards {
		board.closeStreams(func(*boardStream) bool { return true })
	}
	return nil
}

// withBoard runs fn under the store lock on the started board numbered
// number. fn never calls the host, so it may hold the lock.
func (b *boardChannel) withBoard(number int64, fn func(*boardState) error) error {
	return b.with(func(board *boardState) bool { return board.record.Number == number }, fn)
}

// withOpenBoard is withBoard for the session's sends: a closed board takes none.
func (b *boardChannel) withOpenBoard(number int64, fn func(*boardState) error) error {
	return b.withBoard(number, func(board *boardState) error {
		if board.record.Closed {
			return errBoardClosed
		}
		return fn(board)
	})
}

func (b *boardChannel) withBoardNamed(name string, fn func(*boardState) error) error {
	return b.with(func(board *boardState) bool { return board.name == name }, fn)
}

func (b *boardChannel) with(match func(*boardState) bool, fn func(*boardState) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.host == nil {
		return errBoardsStopped
	}
	for _, board := range b.boards {
		if match(board) {
			return fn(board)
		}
	}
	return errNoBoard
}

// boardHost is the started host's board extension, or nil.
func (b *boardChannel) boardHost() (channel.Host, channel.BoardHost) {
	b.mu.Lock()
	defer b.mu.Unlock()
	extension, _ := b.host.(channel.BoardHost)
	return b.host, extension
}

func (b *boardChannel) logf(format string, args ...any) {
	if host, _ := b.boardHost(); host != nil {
		host.Logf(format, args...)
	}
}

// SendReply takes the session's text, or one HTML file as the board's next
// page. Held and other status texts are shell notices only; a text that answers
// a message (ReplyTo) is always logged, whatever it starts with.
func (b *boardChannel) SendReply(args c3types.ReplyArgs) (int64, error) {
	if args.Channel != "" && args.Channel != channel.BoardChannelName || args.TopicID != nil {
		return 0, errors.New("board: destination is not a board route")
	}
	if args.Poll != nil || len(args.Buttons) != 0 {
		return 0, fmt.Errorf("%w: rich outbound content", errUnsupported)
	}
	if len(args.Media) != 0 {
		return b.sendPage(args)
	}
	if strings.TrimSpace(args.Text) == "" {
		return 0, errors.New("board: empty message")
	}
	if args.ReplyTo == nil && isStatusText(args.Text) {
		err := b.withBoard(args.ChatID, func(board *boardState) error {
			board.broadcast(jsonEvent("notice", map[string]string{"text": args.Text}))
			return nil
		})
		if err == nil {
			go b.refreshWaiting(args.ChatID)
		}
		return 0, err
	}
	var id int64
	err := b.withOpenBoard(args.ChatID, func(board *boardState) error {
		id = board.nextID()
		return b.appendLocked(board, boardLogEntry{Dir: "out", ID: id, Op: "message", Text: args.Text, ReplyTo: replyToID(args.ReplyTo)})
	})
	return id, err
}

func (b *boardChannel) sendPage(args c3types.ReplyArgs) (int64, error) {
	page, err := readBoardPage(args.Media)
	if err != nil {
		return 0, err
	}
	warnings := lintBoardPage(page)
	var entry boardLogEntry
	err = b.withOpenBoard(args.ChatID, func(board *boardState) error {
		record := board.record
		record.Rev++
		record.Title = pageTitle(page, board.name)
		directory := filepath.Join(b.directory, board.name)
		if err := storeBoardRevision(directory, record.Rev, page); err != nil {
			return err
		}
		if err := saveBoardRecord(directory, record); err != nil {
			return err
		}
		dropExpiredBoardRevision(directory, record.Rev)
		board.record = record
		entry = boardLogEntry{Dir: "out", ID: board.nextID(), Op: "page", Text: args.Media[0].Caption, ReplyTo: replyToID(args.ReplyTo), Rev: record.Rev}
		if len(warnings) != 0 {
			board.broadcast(jsonEvent("notice", map[string]string{"text": fmt.Sprintf("⚠️ Page %d: %s.", record.Rev, strings.Join(warnings, "; "))}))
		}
		return b.appendLocked(board, entry)
	})
	if err == nil && len(warnings) != 0 {
		b.logf("board: page %d lint (stored anyway): %v", entry.Rev, warnings)
	}
	return entry.ID, err
}

func (b *boardChannel) SendTyping(chatID int64, threadID *int64) error {
	if threadID != nil {
		return errors.New("board: boards have no topics")
	}
	return b.withOpenBoard(chatID, func(board *boardState) error {
		board.broadcast(board.statusEvent(true))
		return nil
	})
}

// EditMessage edits only the session's own text messages, never a viewer's.
func (b *boardChannel) EditMessage(args c3types.EditArgs) (*c3types.EditResult, error) {
	if args.Channel != "" && args.Channel != channel.BoardChannelName {
		return nil, errors.New("board: destination is not a board route")
	}
	if len(args.Buttons) != 0 {
		return nil, fmt.Errorf("%w: buttons", errUnsupported)
	}
	err := b.withOpenBoard(args.ChatID, func(board *boardState) error {
		original, ok := board.entry(args.MessageID)
		if !ok || original.Dir != "out" || original.Op != "message" {
			return fmt.Errorf("board: message %d is not one of the session's messages on this board", args.MessageID)
		}
		return b.appendLocked(board, boardLogEntry{Dir: "out", ID: args.MessageID, Op: "edit", Part: original.Part, Text: args.Text})
	})
	if err != nil {
		return nil, err
	}
	return &c3types.EditResult{MessageID: args.MessageID}, nil
}

func (b *boardChannel) React(args c3types.ReactArgs) error {
	if args.Channel != "" && args.Channel != channel.BoardChannelName {
		return errors.New("board: destination is not a board route")
	}
	if !isBoardEmoji(args.Emoji) {
		return fmt.Errorf("board: %q is not one emoji", args.Emoji)
	}
	return b.withOpenBoard(args.ChatID, func(board *boardState) error {
		target, ok := board.entry(args.MessageID)
		if !ok {
			return fmt.Errorf("board: no message %d on this board", args.MessageID)
		}
		return b.appendLocked(board, boardLogEntry{Dir: "out", Op: "reaction", Part: target.Part, Text: args.Emoji, Ref: args.MessageID})
	})
}

// SendReadback lands a voice act's transcript as an edit of that act. Only
// the broker calls it, as it alone calls SendVoiceStatus.
func (b *boardChannel) SendReadback(args c3types.ReadbackArgs) (int64, error) {
	if args.TopicID != nil || args.ReplyTo == nil {
		return 0, errors.New("board: a readback names a voice act on a board")
	}
	return *args.ReplyTo, b.setVoiceStatus(args.ChatID, *args.ReplyTo, c3types.VoiceStatusHeard, args.Transcript)
}

// SendVoiceStatus lands C3's notice about a voice act (no speech, failed,
// slow) as an edit of that act. A no-speech or slow edit carries C3's own
// wording, whatever the text.
func (b *boardChannel) SendVoiceStatus(args c3types.VoiceStatusArgs) error {
	if args.TopicID != nil {
		return errors.New("board: boards have no topics")
	}
	text := voiceStatusText(args.Status, args.Text)
	if strings.TrimSpace(text) == "" {
		return fmt.Errorf("board: no voice status %q with text %q", args.Status, args.Text)
	}
	return b.setVoiceStatus(args.ChatID, args.MessageID, args.Status, text)
}

// voiceStatusText is what a status edit keeps: C3's own wording for no speech
// and slow, the broker's notice for a failure, and "" for any other status.
func voiceStatusText(status, notice string) string {
	switch status {
	case c3types.VoiceStatusNoSpeech:
		return c3types.VoiceNoSpeechNotice
	case c3types.VoiceStatusSlow:
		return c3types.VoiceSlowNotice
	case c3types.VoiceStatusFailed:
		return notice
	}
	return ""
}

// setVoiceStatus logs a status edit of voice act id, also on a board made
// read-only since: it finishes the owner's own earlier act. An act takes one
// final status, and a slow one only before it.
func (b *boardChannel) setVoiceStatus(number, id int64, status, text string) error {
	return b.withBoard(number, func(board *boardState) error {
		voice, ok := board.entry(id)
		if !ok || voice.Dir != "in" || voice.Op != "voice" {
			return fmt.Errorf("%w: %d", errNotVoice, id)
		}
		if previous := board.voiceStatus(voice); previous != "" && (previous != c3types.VoiceStatusSlow || status == c3types.VoiceStatusSlow) {
			return fmt.Errorf("%w: %d is %s", errVoiceSettled, id, previous)
		}
		return b.appendLocked(board, boardLogEntry{Dir: "in", ID: id, Op: "edit", Part: voice.Part, Text: text, From: voice.From, Status: status})
	})
}

// voiceStatus is a voice act's latest status: its own for a part that was
// never sent, else its last status edit's, or "".
func (board *boardState) voiceStatus(voice boardLogEntry) string {
	status := voice.Status
	for _, entry := range board.log {
		if entry.ID == voice.ID && entry.Dir == "in" && entry.Op == "edit" && entry.Status != "" {
			status = entry.Status
		}
	}
	return status
}

// LocalAudioPath resolves a voice act's audio, which the web channel stores.
func (b *boardChannel) LocalAudioPath(fileID string) (string, error) {
	return b.web.LocalAudioPath(fileID)
}

func (b *boardChannel) DownloadAttachment(string) (string, error) {
	return "", fmt.Errorf("%w: attachments", errUnsupported)
}

func (b *boardChannel) StopPoll(int64, int64) (*c3types.PollResult, error) {
	return nil, fmt.Errorf("%w: polls", errUnsupported)
}

func (b *boardChannel) CreateTopic(int64, string) (int64, error) {
	return 0, fmt.Errorf("%w: topics", errUnsupported)
}

func (b *boardChannel) ValidateTopic(int64, int64) error {
	return fmt.Errorf("%w: topics", errUnsupported)
}

// SetRouteHolder implements channel.PresenceNotifier; the broker calls it with
// no lock held, so the waiting count is read from the queue right here.
func (b *boardChannel) SetRouteHolder(chatID int64, topicID *int64, holder *channel.RouteHolder) {
	if topicID != nil {
		return
	}
	var current *channel.RouteHolder
	if holder != nil {
		copy := *holder
		current = &copy
	}
	err := b.withBoard(chatID, func(board *boardState) error {
		board.holder = current
		board.broadcast(board.statusEvent(false))
		return nil
	})
	if err == nil {
		b.refreshWaiting(chatID)
	}
}

// refreshWaiting reads the held count without the store lock (a BoardHost
// call) and tells the board's viewers when it changed. Without a count (the
// durable queue is off for this run) the waiting count keeps its last value.
func (b *boardChannel) refreshWaiting(number int64) {
	_, extension := b.boardHost()
	if extension == nil {
		return
	}
	count, err := extension.HeldCount(number)
	if err != nil {
		b.heldCountFailure.Do(func() { b.logf("board: no held counts, waiting counts keep their last value: %v", err) })
		return
	}
	_ = b.withBoard(number, func(board *boardState) error {
		if board.waiting != count {
			board.waiting = count
			board.broadcast(board.statusEvent(false))
		}
		return nil
	})
}

// CreateBoard implements channel.BoardKeeper. A taken name returns the stored
// board unchanged, whoever owns it.
func (b *boardChannel) CreateBoard(name string, owner channel.BoardOwner) (channel.Board, bool, error) {
	if !channel.IsValidBoardName(name) || boardNameIsReserved(name) {
		return channel.Board{}, false, fmt.Errorf("board: %q is not a board name (%s, not %q)", name, channel.BoardNamePattern, "assets")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.host == nil {
		return channel.Board{}, false, errBoardsStopped
	}
	if board := b.boards[name]; board != nil {
		return b.view(board), false, nil
	}
	record := boardRecord{Number: b.lastNumber + 1, Owner: owner, Title: name}
	if err := createBoardDirectory(b.directory, name, record); err != nil {
		return channel.Board{}, false, err
	}
	b.lastNumber = record.Number
	b.boards[name] = newBoardState(name, record)
	return b.view(b.boards[name]), true, nil
}

func (b *boardChannel) BoardByName(name string) (channel.Board, bool) {
	var found channel.Board
	err := b.withBoardNamed(name, func(board *boardState) error { found = b.view(board); return nil })
	return found, err == nil
}

func (b *boardChannel) BoardByNumber(number int64) (channel.Board, bool) {
	var found channel.Board
	err := b.withBoard(number, func(board *boardState) error { found = b.view(board); return nil })
	return found, err == nil
}

func (b *boardChannel) Boards() []channel.Board {
	b.mu.Lock()
	defer b.mu.Unlock()
	boards := make([]channel.Board, 0, len(b.boards))
	for _, board := range b.boards {
		boards = append(boards, b.view(board))
	}
	sort.Slice(boards, func(i, j int) bool { return boards[i].Number < boards[j].Number })
	return boards
}

func (b *boardChannel) StarterHTML() (string, error) {
	starter, err := fs.ReadFile(b.assets, "board-starter.html")
	if err != nil {
		return "", fmt.Errorf("board: starter page unavailable in this build: %w", err)
	}
	return string(starter), nil
}

func (b *boardChannel) view(board *boardState) channel.Board {
	return channel.Board{
		Number: board.record.Number, Name: board.name, Owner: board.record.Owner,
		Link: b.web.boardLinkBase() + "/board/" + board.name, IsClosed: board.record.Closed,
	}
}

// capability is HMAC(key, board, viewer): it lets a frame load only its own
// board's page, for as long as the viewer it names stays valid.
func (b *boardChannel) capability(name, viewer string) string {
	b.mu.Lock()
	key := b.key
	b.mu.Unlock()
	if len(key) == 0 {
		return ""
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(name + "\n" + viewer))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (b *boardChannel) isCapabilityValid(name, capability string, viewers []string) bool {
	for _, viewer := range viewers {
		if want := b.capability(name, viewer); want != "" && hmac.Equal([]byte(want), []byte(capability)) {
			return true
		}
	}
	return false
}

// appendLocked logs one entry and sends it to the board's viewers. b.mu held.
func (b *boardChannel) appendLocked(board *boardState, entry boardLogEntry) error {
	entry.Seq, entry.At = board.lastSeq+1, b.web.now().UTC()
	if entry.From == "" {
		entry.From = "agent"
	}
	if entry.Part == "" && entry.ReplyTo > 0 {
		if answered, ok := board.entry(entry.ReplyTo); ok {
			entry.Part = answered.Part
		}
	}
	if err := appendBoardLog(filepath.Join(b.directory, board.name), entry); err != nil {
		return err
	}
	board.remember(entry)
	board.broadcast(entryEvent(entry))
	return nil
}

func (board *boardState) remember(entry boardLogEntry) {
	board.lastSeq = entry.Seq
	board.lastID = max(board.lastID, entry.ID)
	board.log = append(board.log, entry)
	if extra := len(board.log) - channel.BoardLogHistoryEntries; extra > 0 {
		board.log = board.log[extra:]
	}
}

func (board *boardState) nextID() int64 {
	board.lastID++
	return board.lastID
}

// entry finds the message an id names; edits and reactions carry no new id.
func (board *boardState) entry(id int64) (boardLogEntry, bool) {
	for _, entry := range board.log {
		if id > 0 && entry.ID == id && entry.Op != "edit" {
			return entry, true
		}
	}
	return boardLogEntry{}, false
}

func (board *boardState) statusEvent(isTyping bool) boardEvent {
	var holder any
	if board.holder != nil {
		holder = map[string]string{"cli": board.holder.CLI, "cwd": board.holder.CWD}
	}
	return jsonEvent("status", map[string]any{
		"live": board.holder != nil, "waiting": board.waiting, "closed": board.record.Closed,
		"typing": isTyping, "holder": holder,
	})
}

// entryEvent names a viewer's action "action" and every other entry by op,
// so a voice act's transcript is an "edit" like the session's own edits.
func entryEvent(entry boardLogEntry) boardEvent {
	event := jsonEvent(entry.Op, entry)
	if entry.Dir == "in" && entry.Op != "edit" {
		event.kind = "action"
	}
	event.seq = entry.Seq
	return event
}

func jsonEvent(kind string, payload any) boardEvent {
	data, _ := json.Marshal(payload)
	return boardEvent{kind: kind, data: data}
}

// broadcast never blocks: a stream too slow to take an event is closed and
// catches up by Last-Event-ID. b.mu held.
func (board *boardState) broadcast(event boardEvent) {
	for stream := range board.streams {
		select {
		case stream.events <- event:
		default:
			stream.close()
			delete(board.streams, stream)
		}
	}
}

func (board *boardState) closeStreams(match func(*boardStream) bool) {
	for stream := range board.streams {
		if match(stream) {
			stream.close()
			delete(board.streams, stream)
		}
	}
}

func (stream *boardStream) close() { stream.once.Do(func() { close(stream.done) }) }

// closeViewerStreams ends a signed-out viewer's board streams.
func (b *boardChannel) closeViewerStreams(viewer string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, board := range b.boards {
		board.closeStreams(func(stream *boardStream) bool { return stream.viewer == viewer })
	}
}

func replyToID(replyTo *int64) int64 {
	if replyTo == nil || *replyTo < 0 {
		return 0
	}
	return *replyTo
}
