package web

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

const (
	voiceBodyLimit        = 12 << 20
	voiceTranscodeTimeout = 90 * time.Second
)

var errVoiceDecode = errors.New("could not decode audio")

func (c *Channel) handleVoiceNote(w http.ResponseWriter, r *http.Request) {
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Now().Add(95 * time.Second))
	_ = controller.SetWriteDeadline(time.Now().Add(95 * time.Second))
	if !c.sameOrigin(r) {
		writeSendError(w, http.StatusForbidden, "not allowed")
		return
	}
	sessionID, current, ok := c.authenticate(r, true)
	if !ok {
		writeSendError(w, http.StatusUnauthorized, "sign in again")
		return
	}
	clientID := strings.TrimSpace(r.Header.Get("X-Client-Id"))
	if clientID == "" {
		writeSendError(w, http.StatusBadRequest, "missing client id")
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(strings.ToLower(mediaType), "audio/") {
		writeSendError(w, http.StatusUnsupportedMediaType, "audio content type required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, voiceBodyLimit)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeSendError(w, http.StatusRequestEntityTooLarge, "voice note too large")
		} else {
			writeSendError(w, http.StatusBadRequest, "invalid voice note")
		}
		return
	}

	messageID, status, err := c.acceptVoiceInbound(r.Context(), sessionID, current, clientID, mediaType, body)
	switch {
	case errors.Is(err, errClientIDConflict):
		writeSendError(w, http.StatusConflict, err.Error())
		return
	case errors.Is(err, errVoiceDecode):
		writeSendError(w, http.StatusUnprocessableEntity, errVoiceDecode.Error())
		return
	case err != nil:
		if c.host != nil {
			c.host.Logf("web: voice-note storage failed: %v", err)
		}
		writeSendError(w, http.StatusInternalServerError, "voice upload unavailable")
		return
	case status == http.StatusForbidden:
		writeSendError(w, status, "not allowed")
		return
	}
	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "2")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, "{\"message_id\":%d}\n", messageID)
}

func (c *Channel) acceptVoiceInbound(ctx context.Context, sessionID string, current session, clientID, mediaType string, body []byte) (int64, int, error) {
	key := clientMessageKey{sessionID: sessionID, clientID: clientID}
	now := c.now()
	hash := sha256.Sum256(body)
	pruned := false
	c.clientMu.Lock()
	for existingKey, existing := range c.clientMessages {
		if now.Sub(existing.created) >= clientMessageTTL {
			delete(c.clientMessages, existingKey)
			pruned = true
		}
	}
	record := c.clientMessages[key]
	c.clientMu.Unlock()
	if record == nil {
		candidate := &clientMessage{
			messageID: c.nextInboundMessageID(), textHash: hash, kind: "voice", created: now,
		}
		c.clientMu.Lock()
		record = c.clientMessages[key]
		if record == nil {
			record = candidate
			c.clientMessages[key] = record
		}
		c.clientMu.Unlock()
	}

	record.mu.Lock()
	if record.kind != "voice" || record.textHash != hash {
		messageID := record.messageID
		record.mu.Unlock()
		if pruned {
			c.persistSessions("stale client-message pruning")
		}
		return messageID, http.StatusConflict, errClientIDConflict
	}
	if record.status != 0 {
		messageID, status := record.messageID, record.status
		record.mu.Unlock()
		if pruned {
			c.persistSessions("stale client-message pruning")
		}
		return messageID, status, nil
	}
	if record.voiceFileID == "" {
		fileID, size, duration, err := c.storeVoice(ctx, mediaType, body)
		if err != nil {
			messageID := record.messageID
			record.mu.Unlock()
			return messageID, 0, err
		}
		record.voiceFileID = fileID
		record.voiceSize = size
		record.voiceDuration = duration
	}
	inbound := &c3types.Inbound{
		Channel: Name, ChatID: c.operatorID, MessageID: record.messageID,
		Sender: currentSender(current), Timestamp: now, ConvKind: c3types.ConvKindDM,
		Kind: c3types.InboundMessage, V: c3types.InboundRecordVersion,
		Attachments: []c3types.Attachment{{
			Kind: "voice", FileID: record.voiceFileID, MIME: "audio/ogg", Size: record.voiceSize,
		}},
	}
	switch c.host.GateInbound(inbound) {
	case channel.GateInboundAllow:
		if !c.host.Emit(inbound) {
			messageID := record.messageID
			record.mu.Unlock()
			if pruned {
				c.persistSessions("stale client-message pruning")
			}
			return messageID, http.StatusServiceUnavailable, nil
		}
		record.status = http.StatusAccepted
		messageID, duration := record.messageID, record.voiceDuration
		record.mu.Unlock()
		c.persistSessions("client-message insert")
		voice := true
		c.publish(streamEvent{kind: "own", payload: streamPayload{
			MessageID: messageID, ClientID: clientID, Timestamp: streamTimestamp(inbound.Timestamp),
			Voice: &voice, DurationSeconds: duration,
		}})
		c.pruneVoiceFiles()
		return messageID, http.StatusAccepted, nil
	default:
		path, _ := c.localAudioPath(record.voiceFileID, false)
		if path != "" {
			_ = os.Remove(path)
		}
		record.voiceFileID = ""
		record.voiceSize = 0
		record.voiceDuration = 0
		record.status = http.StatusForbidden
		messageID := record.messageID
		record.mu.Unlock()
		c.persistSessions("client-message insert")
		return messageID, http.StatusForbidden, nil
	}
}

func currentSender(current session) c3types.Sender {
	return c3types.Sender{UserID: current.userID, Username: current.username}
}

func (c *Channel) storeVoice(parent context.Context, mediaType string, body []byte) (string, int64, float64, error) {
	directory, err := c.ensureVoiceDirectory()
	if err != nil {
		return "", 0, 0, err
	}
	fileID, err := randomHexID()
	if err != nil {
		return "", 0, 0, err
	}
	inputPath := filepath.Join(directory, fileID+voiceExtension(mediaType))
	outputPath := filepath.Join(directory, fileID+".oga")
	file, err := os.OpenFile(inputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", 0, 0, err
	}
	if _, err = file.Write(body); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(inputPath)
		return "", 0, 0, err
	}

	ctx, cancel := context.WithTimeout(parent, voiceTranscodeTimeout)
	defer cancel()
	command := exec.CommandContext(ctx, "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
		"-i", inputPath, "-vn", "-ac", "1", "-ar", "48000", "-c:a", "libopus",
		"-b:a", "32k", "-f", "ogg", outputPath)
	if output, runErr := command.CombinedOutput(); runErr != nil {
		_ = os.Remove(inputPath)
		_ = os.Remove(outputPath)
		if c.host != nil {
			c.host.Logf("web: ffmpeg rejected voice note: %v (%s)", runErr, strings.TrimSpace(string(output)))
		}
		return "", 0, 0, errVoiceDecode
	}
	if err := os.Chmod(outputPath, 0o600); err != nil {
		_ = os.Remove(inputPath)
		_ = os.Remove(outputPath)
		return "", 0, 0, err
	}
	if err := os.Remove(inputPath); err != nil {
		_ = os.Remove(outputPath)
		return "", 0, 0, err
	}
	info, err := os.Stat(outputPath)
	if err != nil {
		_ = os.Remove(outputPath)
		return "", 0, 0, err
	}
	return fileID, info.Size(), probeVoiceDuration(outputPath), nil
}

func voiceExtension(mediaType string) string {
	switch strings.ToLower(mediaType) {
	case "audio/webm":
		return ".webm"
	case "audio/mp4", "audio/aac", "audio/x-m4a":
		return ".mp4"
	case "audio/ogg", "audio/opus":
		return ".ogg"
	case "audio/mpeg", "audio/mp3":
		return ".mpeg"
	case "audio/wav", "audio/wave", "audio/x-wav":
		return ".wav"
	default:
		return ".bin"
	}
}

func probeVoiceDuration(path string) float64 {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	output, err := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-show_entries",
		"format=duration", "-of", "csv=p=0", path).Output()
	if err != nil {
		return 0
	}
	duration, err := strconv.ParseFloat(strings.TrimSpace(string(output)), 64)
	if err != nil || duration < 0 {
		return 0
	}
	return duration
}

func randomHexID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func (c *Channel) voiceDirectoryPath() (string, error) {
	directory := c.stateDirectory
	if directory == "" {
		var err error
		directory, err = stateDir()
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(directory, "voice"), nil
}

func (c *Channel) ensureVoiceDirectory() (string, error) {
	directory, err := c.voiceDirectoryPath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return "", err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("web: voice directory is not a private directory")
	}
	return directory, nil
}

// LocalAudioPath resolves only retained OGG files minted by this channel.
func (c *Channel) LocalAudioPath(fileID string) (string, error) {
	return c.localAudioPath(fileID, true)
}

func (c *Channel) localAudioPath(fileID string, requireFile bool) (string, error) {
	decoded, err := hex.DecodeString(fileID)
	if err != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != fileID {
		return "", errors.New("web: invalid local audio id")
	}
	directory, err := c.voiceDirectoryPath()
	if err != nil {
		return "", err
	}
	path := filepath.Join(directory, fileID+".oga")
	if filepath.Dir(path) != filepath.Clean(directory) {
		return "", errors.New("web: local audio path escaped voice directory")
	}
	if !requireFile {
		return path, nil
	}
	directoryInfo, err := os.Lstat(directory)
	if err != nil || !directoryInfo.IsDir() || directoryInfo.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("web: local audio directory unavailable")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", errors.New("web: local audio unavailable")
	}
	return path, nil
}

func (c *Channel) pruneVoiceFiles() {
	keep := c.voiceRetention
	if keep < 0 {
		return
	}
	directory, err := c.voiceDirectoryPath()
	if err != nil {
		return
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return
	}
	type retainedFile struct {
		path     string
		name     string
		modified time.Time
	}
	files := make([]retainedFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".oga" {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		files = append(files, retainedFile{
			path: filepath.Join(directory, entry.Name()), name: entry.Name(), modified: info.ModTime(),
		})
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].modified.Equal(files[j].modified) {
			return files[i].name > files[j].name
		}
		return files[i].modified.After(files[j].modified)
	})
	if keep >= len(files) {
		return
	}
	for _, file := range files[keep:] {
		if err := os.Remove(file.path); err != nil && c.host != nil {
			c.host.Logf("web: prune retained voice %s: %v", file.name, err)
		}
	}
}

// SendReadback replaces the optimistic own voice row with its transcript.
func (c *Channel) SendReadback(args c3types.ReadbackArgs) (int64, error) {
	if err := c.checkDestination(Name, args.ChatID, args.TopicID); err != nil {
		return 0, err
	}
	if args.ReplyTo == nil || *args.ReplyTo <= 0 {
		return 0, errors.New("web: readback requires a positive source message id")
	}
	voice := true
	c.publish(streamEvent{kind: "edit", payload: streamPayload{
		MessageID: *args.ReplyTo, Text: args.Transcript, Voice: &voice,
	}})
	return *args.ReplyTo, nil
}
