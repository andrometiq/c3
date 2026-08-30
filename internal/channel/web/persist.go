package web

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	stateVersion              = 1
	sessionTouchWriteInterval = 5 * time.Minute
	replayCompactLineLimit    = 5 * replayLimit
)

type persistedState struct {
	Version        int                      `json:"version"`
	NextReplyID    int64                    `json:"next_reply_id"`
	NextInboundID  int64                    `json:"next_inbound_id"`
	NextEventID    int64                    `json:"next_event_id"`
	Sessions       []persistedSession       `json:"sessions"`
	ClientMessages []persistedClientMessage `json:"client_messages"`
}

type persistedSession struct {
	Key      string    `json:"key"`
	UserID   int64     `json:"user_id"`
	Username string    `json:"username"`
	Created  time.Time `json:"created"`
	LastSeen time.Time `json:"last_seen"`
}

type persistedClientMessage struct {
	Session    string    `json:"session"`
	ClientID   string    `json:"client_id"`
	MessageID  int64     `json:"message_id"`
	TextSHA256 string    `json:"text_sha256"`
	Outcome    string    `json:"outcome"`
	At         time.Time `json:"at"`
}

type persistedReplayEvent struct {
	Sequence int64         `json:"sequence"`
	Kind     string        `json:"kind"`
	Payload  streamPayload `json:"payload"`
}

type stateLogger interface {
	Logf(format string, args ...any)
}

func stateDir() (string, error) {
	base := os.Getenv("XDG_STATE_HOME")
	if base == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("web: resolve home directory: %w", err)
		}
		base = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(base, "c3", "web"), nil
}

func sessionKey(cookieValue string) string {
	sum := sha256.Sum256([]byte(cookieValue))
	return hex.EncodeToString(sum[:])
}

func textHash(text string) [sha256.Size]byte {
	return sha256.Sum256([]byte(text))
}

func (c *Channel) loadState(logger stateLogger) error {
	directory, err := stateDir()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return fmt.Errorf("web: create state directory: %w", err)
	}
	if err := os.Chmod(directory, 0700); err != nil {
		return fmt.Errorf("web: protect state directory: %w", err)
	}

	c.persistMu.Lock()
	c.stateDirectory = directory
	c.stateReady = false
	c.authMu.Lock()
	c.sessions = make(map[string]*session)
	c.authMu.Unlock()
	c.clientMu.Lock()
	c.clientMessages = make(map[clientMessageKey]*clientMessage)
	c.clientMu.Unlock()
	c.replay = nil
	c.replayLineCount = 0
	c.idMu.Lock()
	c.nextReplyID = 0
	c.nextInboundFloor = 0
	c.nextEventID = 0
	c.idMu.Unlock()
	pruned, err := c.loadSessions(filepath.Join(directory, "sessions.json"), logger)
	if err == nil {
		err = c.loadReplay(filepath.Join(directory, "replay.jsonl"), logger)
	}
	if err == nil {
		c.stateReady = true
	}
	c.persistMu.Unlock()
	if err != nil {
		return err
	}
	if pruned {
		if err := c.saveSessions(); err != nil {
			return fmt.Errorf("web: save pruned state: %w", err)
		}
	}
	return nil
}

func (c *Channel) loadSessions(path string, logger stateLogger) (bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("web: read sessions state: %w", err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		return false, fmt.Errorf("web: protect sessions state: %w", err)
	}

	var stored persistedState
	if err := decodeState(data, &stored); err != nil {
		c.ignoreCorruptFile(path, err, logger)
		return false, nil
	}

	now := c.now()
	sessions := make(map[string]*session, len(stored.Sessions))
	pruned := false
	for _, saved := range stored.Sessions {
		if now.Sub(saved.LastSeen) >= sessionIdleTTL {
			pruned = true
			continue
		}
		sessions[saved.Key] = &session{
			userID: saved.UserID, username: saved.Username,
			created: saved.Created, lastSeen: saved.LastSeen, savedLastSeen: saved.LastSeen,
		}
	}

	clientMessages := make(map[clientMessageKey]*clientMessage, len(stored.ClientMessages))
	inboundFloor := stored.NextInboundID
	for _, saved := range stored.ClientMessages {
		if saved.MessageID > inboundFloor {
			inboundFloor = saved.MessageID
		}
		if now.Sub(saved.At) >= clientMessageTTL {
			pruned = true
			continue
		}
		if sessions[saved.Session] == nil {
			pruned = true
			continue
		}
		hashBytes, decodeErr := hex.DecodeString(saved.TextSHA256)
		if decodeErr != nil {
			c.ignoreCorruptFile(path, fmt.Errorf("decode client-message text_sha256: %w", decodeErr), logger)
			return false, nil
		}
		var hash [sha256.Size]byte
		copy(hash[:], hashBytes)
		status := 0
		if saved.Outcome == "accepted" {
			status = http.StatusAccepted
		} else {
			status = http.StatusForbidden
		}
		key := clientMessageKey{sessionID: saved.Session, clientID: saved.ClientID}
		clientMessages[key] = &clientMessage{
			messageID: saved.MessageID, textHash: hash, status: status, created: saved.At,
		}
	}

	c.authMu.Lock()
	c.sessions = sessions
	c.authMu.Unlock()
	c.clientMu.Lock()
	c.clientMessages = clientMessages
	c.clientMu.Unlock()
	c.idMu.Lock()
	c.nextReplyID = stored.NextReplyID
	c.nextInboundFloor = inboundFloor
	c.nextEventID = stored.NextEventID
	c.idMu.Unlock()
	return pruned, nil
}

func decodeState(data []byte, stored *persistedState) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(stored); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing data")
	}
	if stored.Version != stateVersion || stored.NextReplyID < 0 || stored.NextInboundID < 0 || stored.NextEventID < 0 {
		return errors.New("unsupported or invalid state version")
	}
	seenSessions := make(map[string]bool, len(stored.Sessions))
	for _, saved := range stored.Sessions {
		if !validSHA256(saved.Key) || seenSessions[saved.Key] || saved.UserID <= 0 || saved.Created.IsZero() || saved.LastSeen.IsZero() {
			return errors.New("invalid session record")
		}
		seenSessions[saved.Key] = true
	}
	seenClients := make(map[clientMessageKey]bool, len(stored.ClientMessages))
	for _, saved := range stored.ClientMessages {
		key := clientMessageKey{sessionID: saved.Session, clientID: saved.ClientID}
		if !validSHA256(saved.Session) || saved.ClientID == "" || saved.MessageID <= 0 || !validSHA256(saved.TextSHA256) || saved.At.IsZero() || saved.Outcome != "accepted" && saved.Outcome != "forbidden" || seenClients[key] {
			return errors.New("invalid client-message record")
		}
		seenClients[key] = true
	}
	return nil
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func (c *Channel) loadReplay(path string, logger stateLogger) error {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("web: read replay state: %w", err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		_ = file.Close()
		return fmt.Errorf("web: protect replay state: %w", err)
	}

	var events []streamEvent
	var maxSequence int64
	lineCount := 0
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		lineCount++
		var saved persistedReplayEvent
		if err := json.Unmarshal(scanner.Bytes(), &saved); err != nil || saved.Sequence <= maxSequence || saved.Kind != "message" && saved.Kind != "own" && saved.Kind != "edit" {
			logger.Logf("web: skipping corrupt replay.jsonl line %d", lineCount)
			continue
		}
		maxSequence = saved.Sequence
		events = append(events, streamEvent{sequence: saved.Sequence, kind: saved.Kind, payload: saved.Payload})
		if len(events) > replayLimit {
			events = append([]streamEvent(nil), events[len(events)-replayLimit:]...)
		}
	}
	scanErr := scanner.Err()
	closeErr := file.Close()
	if scanErr != nil {
		c.ignoreCorruptFile(path, scanErr, logger)
		c.replay = nil
		c.replayLineCount = 0
		return nil
	}
	if closeErr != nil {
		return fmt.Errorf("web: close replay state: %w", closeErr)
	}

	c.replay = events
	c.replayLineCount = lineCount
	c.idMu.Lock()
	if maxSequence > c.nextEventID {
		c.nextEventID = maxSequence
	}
	for _, event := range events {
		if (event.kind == "message" || event.kind == "edit") && event.payload.MessageID > c.nextReplyID {
			c.nextReplyID = event.payload.MessageID
		}
		if event.kind == "own" && event.payload.MessageID > c.nextInboundFloor {
			c.nextInboundFloor = event.payload.MessageID
		}
	}
	c.idMu.Unlock()
	if lineCount > replayCompactLineLimit {
		if err := c.rewriteReplay(path, events); err != nil {
			return fmt.Errorf("web: compact replay state: %w", err)
		}
		c.replayLineCount = len(events)
	}
	return nil
}

func (c *Channel) saveSessions() error {
	c.persistMu.Lock()
	defer c.persistMu.Unlock()
	if !c.stateReady {
		return nil
	}

	stored := persistedState{Version: stateVersion}
	c.idMu.Lock()
	stored.NextReplyID = c.nextReplyID
	stored.NextInboundID = c.nextInboundFloor
	stored.NextEventID = c.nextEventID
	c.idMu.Unlock()
	type sessionTouch struct {
		current  *session
		lastSeen time.Time
	}
	touches := make(map[string]sessionTouch, len(c.sessions))
	c.authMu.Lock()
	stored.Sessions = make([]persistedSession, 0, len(c.sessions))
	for key, current := range c.sessions {
		stored.Sessions = append(stored.Sessions, persistedSession{
			Key: key, UserID: current.userID, Username: current.username,
			Created: current.created, LastSeen: current.lastSeen,
		})
		touches[key] = sessionTouch{current: current, lastSeen: current.lastSeen}
	}
	c.authMu.Unlock()
	sort.Slice(stored.Sessions, func(i, j int) bool { return stored.Sessions[i].Key < stored.Sessions[j].Key })

	c.clientMu.Lock()
	stored.ClientMessages = make([]persistedClientMessage, 0, len(c.clientMessages))
	for key, record := range c.clientMessages {
		record.mu.Lock()
		outcome := ""
		if record.status == http.StatusAccepted {
			outcome = "accepted"
		} else if record.status == http.StatusForbidden {
			outcome = "forbidden"
		}
		if outcome != "" {
			stored.ClientMessages = append(stored.ClientMessages, persistedClientMessage{
				Session: key.sessionID, ClientID: key.clientID, MessageID: record.messageID,
				TextSHA256: hex.EncodeToString(record.textHash[:]), Outcome: outcome, At: record.created,
			})
		}
		record.mu.Unlock()
	}
	c.clientMu.Unlock()
	sort.Slice(stored.ClientMessages, func(i, j int) bool {
		if stored.ClientMessages[i].Session == stored.ClientMessages[j].Session {
			return stored.ClientMessages[i].ClientID < stored.ClientMessages[j].ClientID
		}
		return stored.ClientMessages[i].Session < stored.ClientMessages[j].Session
	})

	data, err := json.Marshal(stored)
	if err != nil {
		return fmt.Errorf("web: marshal sessions state: %w", err)
	}
	data = append(data, '\n')
	if err := writeAtomic(filepath.Join(c.stateDirectory, "sessions.json"), data); err != nil {
		return err
	}
	c.authMu.Lock()
	for key, touch := range touches {
		if current := c.sessions[key]; current == touch.current && current.savedLastSeen.Before(touch.lastSeen) {
			current.savedLastSeen = touch.lastSeen
		}
	}
	c.authMu.Unlock()
	return nil
}

func (c *Channel) persistSessions(reason string) {
	if err := c.saveSessions(); err != nil && c.host != nil {
		c.host.Logf("web: persist %s: %v", reason, err)
	}
}

// replayMu must be held so file order, ring order, and sequence order agree.
func (c *Channel) appendReplay(event streamEvent) error {
	c.persistMu.Lock()
	defer c.persistMu.Unlock()
	if !c.stateReady {
		return nil
	}
	saved := persistedReplayEvent{Sequence: event.sequence, Kind: event.kind, Payload: event.payload}
	line, err := json.Marshal(saved)
	if err != nil {
		return fmt.Errorf("web: marshal replay event: %w", err)
	}
	line = append(line, '\n')
	path := filepath.Join(c.stateDirectory, "replay.jsonl")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	created := false
	if errors.Is(err, os.ErrNotExist) {
		file, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0600)
		created = err == nil
		if errors.Is(err, os.ErrExist) {
			file, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
			created = false
		}
	}
	if err != nil {
		return fmt.Errorf("web: open replay state: %w", err)
	}
	if err := file.Chmod(0600); err != nil {
		_ = file.Close()
		return fmt.Errorf("web: protect replay state: %w", err)
	}
	if _, err := file.Write(line); err != nil {
		_ = file.Close()
		return fmt.Errorf("web: append replay state: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("web: sync replay state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("web: close replay state: %w", err)
	}
	if created {
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return fmt.Errorf("web: sync replay state directory: %w", err)
		}
	}
	c.replayLineCount++
	if c.replayLineCount > replayCompactLineLimit {
		if err := c.rewriteReplay(path, c.replay); err != nil {
			return fmt.Errorf("web: compact replay state: %w", err)
		}
		c.replayLineCount = len(c.replay)
	}
	return nil
}

func (c *Channel) rewriteReplay(path string, events []streamEvent) error {
	var data []byte
	for _, event := range events {
		line, err := json.Marshal(persistedReplayEvent{Sequence: event.sequence, Kind: event.kind, Payload: event.payload})
		if err != nil {
			return err
		}
		data = append(data, line...)
		data = append(data, '\n')
	}
	return writeAtomic(path, data)
}

func writeAtomic(path string, data []byte) error {
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+"-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func syncDirectory(directory string) error {
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer directoryFile.Close()
	return directoryFile.Sync()
}

func (c *Channel) ignoreCorruptFile(path string, reason error, logger stateLogger) {
	corruptPath := fmt.Sprintf("%s.corrupt-%d", path, c.now().Unix())
	if err := os.Rename(path, corruptPath); err != nil {
		logger.Logf("web: corrupt %s ignored (%v); rename failed: %v", filepath.Base(path), reason, err)
		return
	}
	if directory, err := os.Open(filepath.Dir(path)); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	logger.Logf("web: corrupt %s renamed to %s: %v", filepath.Base(path), filepath.Base(corruptPath), reason)
}
