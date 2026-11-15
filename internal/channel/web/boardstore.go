package web

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

// Board storage lives under <web state dir>/boards: <name>/board.json, the
// kept revisions <name>/pages/<rev>.html, the append-only <name>/log.jsonl,
// and capability.key, the frame capability's HMAC key.
const (
	boardsDirectoryName = "boards"
	boardRecordName     = "board.json"
	boardLogName        = "log.jsonl"
	boardPagesName      = "pages"
	boardKeyName        = "capability.key"
	boardKeyBytes       = 32
)

// boardRecord is board.json. Rev is the current page revision, 0 before the
// first page; it is the one switch that decides which stored page is served.
type boardRecord struct {
	Number int64              `json:"number"`
	Owner  channel.BoardOwner `json:"owner"`
	Title  string             `json:"title"`
	Rev    int64              `json:"rev"`
	Closed bool               `json:"closed"`
}

// boardLogEntry is one log.jsonl line, one /state log item and one replayable
// SSE event. Dir is "in" for a viewer's action and "out" for the session's.
type boardLogEntry struct {
	Seq     int64     `json:"seq"`
	At      time.Time `json:"at"`
	Dir     string    `json:"dir"`
	ID      int64     `json:"id"`
	Op      string    `json:"op"`
	Part    string    `json:"part"`
	Text    string    `json:"text"`
	Ref     int64     `json:"ref"`
	ReplyTo int64     `json:"reply_to"`
	From    string    `json:"from"`
	Rev     int64     `json:"rev,omitempty"`
}

// loadBoards reads every board. A board whose record is unreadable, or two
// boards with one number, fail the load: a board skipped here could have its
// number given to a new board and receive the old one's held messages.
func loadBoards(directory string, logger stateLogger) (map[string]*boardState, error) {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("board: create boards directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return nil, fmt.Errorf("board: protect boards directory: %w", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("board: list boards: %w", err)
	}
	boards := make(map[string]*boardState)
	owners := make(map[int64]string)
	for _, entry := range entries {
		if !entry.IsDir() || !channel.IsValidBoardName(entry.Name()) {
			continue
		}
		board, err := loadBoard(filepath.Join(directory, entry.Name()), entry.Name(), logger)
		if err != nil {
			return nil, err
		}
		if other, taken := owners[board.record.Number]; taken {
			return nil, fmt.Errorf("board: boards %q and %q share number %d", other, board.name, board.record.Number)
		}
		owners[board.record.Number] = board.name
		boards[board.name] = board
	}
	return boards, nil
}

func loadBoard(directory, name string, logger stateLogger) (*boardState, error) {
	data, err := os.ReadFile(filepath.Join(directory, boardRecordName))
	if err != nil {
		return nil, fmt.Errorf("board: read %q: %w", name, err)
	}
	var record boardRecord
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil || record.Number <= 0 || record.Rev < 0 {
		return nil, fmt.Errorf("board: %q has an invalid %s", name, boardRecordName)
	}
	board := newBoardState(name, record)
	if err := board.loadLog(filepath.Join(directory, boardLogName), logger); err != nil {
		return nil, err
	}
	return board, nil
}

// loadLog replays log.jsonl into memory. A torn last line (a crash mid-append)
// is cut off so the next append starts on a line of its own.
func (board *boardState) loadLog(path string, logger stateLogger) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("board: read %q log: %w", board.name, err)
	}
	if torn := len(data) - (bytes.LastIndexByte(data, '\n') + 1); torn > 0 {
		data = data[:len(data)-torn]
		if err := os.Truncate(path, int64(len(data))); err != nil {
			return fmt.Errorf("board: cut torn %q log line: %w", board.name, err)
		}
		logger.Logf("board: %q log ended in a torn line; cut %d bytes", board.name, torn)
	}
	for index, line := range bytes.Split(data, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var entry boardLogEntry
		if err := json.Unmarshal(line, &entry); err != nil || entry.Seq <= board.lastSeq {
			logger.Logf("board: %q skipping corrupt log line %d", board.name, index+1)
			continue
		}
		board.remember(entry)
	}
	return nil
}

// createBoardDirectory builds the board in a hidden staging directory and
// renames it into place, so a crash never leaves a board without its record.
func createBoardDirectory(root, name string, record boardRecord) error {
	staging, err := os.MkdirTemp(root, "."+name+"-")
	if err != nil {
		return fmt.Errorf("board: stage %q: %w", name, err)
	}
	defer os.RemoveAll(staging)
	if err := os.Mkdir(filepath.Join(staging, boardPagesName), 0o700); err != nil {
		return fmt.Errorf("board: stage %q pages: %w", name, err)
	}
	if err := saveBoardRecord(staging, record); err != nil {
		return err
	}
	if err := os.Rename(staging, filepath.Join(root, name)); err != nil {
		return fmt.Errorf("board: create %q: %w", name, err)
	}
	return syncDirectory(root)
}

func saveBoardRecord(directory string, record boardRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if err := writeAtomic(filepath.Join(directory, boardRecordName), append(data, '\n')); err != nil {
		return fmt.Errorf("board: save %s: %w", boardRecordName, err)
	}
	return nil
}

func appendBoardLog(directory string, entry boardLogEntry) error {
	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(directory, boardLogName), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("board: open log: %w", err)
	}
	_, err = file.Write(append(line, '\n'))
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("board: append log: %w", err)
	}
	return nil
}

func boardRevisionPath(directory string, rev int64) string {
	return filepath.Join(directory, boardPagesName, strconv.FormatInt(rev, 10)+".html")
}

func storeBoardRevision(directory string, rev int64, page []byte) error {
	if err := writeAtomic(boardRevisionPath(directory, rev), page); err != nil {
		return fmt.Errorf("board: store revision %d: %w", rev, err)
	}
	return nil
}

// dropExpiredBoardRevision removes the revision that rev pushed out of the
// kept window; call it only once board.json names rev. A failed removal only
// leaves an unserved file behind.
func dropExpiredBoardRevision(directory string, rev int64) {
	if expired := rev - channel.BoardRevisionsKept; expired > 0 {
		_ = os.Remove(boardRevisionPath(directory, expired))
	}
}

// readBoardPage loads the one HTML file an agent sends as a board's page.
func readBoardPage(media []c3types.MediaItem) ([]byte, error) {
	if len(media) != 1 {
		return nil, errors.New("board: send exactly one HTML file per page")
	}
	item := media[0]
	if item.Kind != c3types.MediaFile || item.Path == "" || item.URL != "" {
		return nil, fmt.Errorf("board: a page must be one local HTML file (kind %q, path, no URL)", c3types.MediaFile)
	}
	if _, ok := allowedDocumentExtension(item.Path); !ok {
		return nil, fmt.Errorf("board: %q is not an .html or .htm file", filepath.Base(item.Path))
	}
	// Stat before opening: opening a FIFO would block the send.
	if info, err := os.Stat(item.Path); err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("board: %q is not a readable regular file", item.Path)
	}
	file, err := os.Open(item.Path)
	if err != nil {
		return nil, fmt.Errorf("board: open page: %w", err)
	}
	defer file.Close()
	if info, err := file.Stat(); err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("board: %q changed while opening", item.Path)
	}
	page, err := io.ReadAll(io.LimitReader(file, channel.BoardPageMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("board: read page: %w", err)
	}
	if len(page) > channel.BoardPageMaxBytes {
		return nil, fmt.Errorf("board: page is over the %d-byte (5 MiB) limit", channel.BoardPageMaxBytes)
	}
	return page, nil
}

func loadOrCreateBoardKey(path string) ([]byte, error) {
	key, err := os.ReadFile(path)
	if err == nil {
		if len(key) != boardKeyBytes {
			return nil, fmt.Errorf("board: %s must hold %d bytes", boardKeyName, boardKeyBytes)
		}
		return key, os.Chmod(path, 0o600)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("board: read %s: %w", boardKeyName, err)
	}
	key = make([]byte, boardKeyBytes)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	if err := writeAtomic(path, key); err != nil {
		return nil, fmt.Errorf("board: save %s: %w", boardKeyName, err)
	}
	return key, nil
}

// boardNameIsReserved: "assets" names the shell's asset path, never a board.
func boardNameIsReserved(name string) bool { return "/board/"+name+"/" == channel.BoardAssetPathPrefix }
