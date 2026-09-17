package main

import (
	"bytes"
	"encoding/json"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Andrometiq/c3/internal/ipc"
)

const (
	permissionPendingTTL      = 30 * time.Minute
	permissionTranscriptPoll  = 500 * time.Millisecond
	maxTranscriptLineBytes    = 16 << 20
	transcriptReadBufferBytes = 64 << 10
	requestIDAlphabet         = "abcdefghijkmnopqrstuvwxyz"
	permissionSettledOldError = "op not implemented yet: permission_settled"
	permissionSettledOldLog   = "broker predates permission_settled — settling disabled until restart"
)

var openPermissionTranscript = os.Open

type permissionSnapshot struct {
	since           time.Time
	transcriptPath  string
	mainOffset      int64
	subagentOffsets map[string]int64
}

type pendingTranscriptPermission struct {
	since           time.Time
	transcriptPath  string
	mainOffset      int64
	subagentOffsets map[string]int64
	settling        bool
	outcome         string
}

type permissionSettlement struct {
	requestID string
	outcome   string
}

type transcriptScanStats struct {
	start     int64
	bytesRead int64
}

func (a *adapter) capturePermissionSnapshot() permissionSnapshot {
	snapshot := permissionSnapshot{since: time.Now(), transcriptPath: a.permissionTranscriptPath()}
	a.observePermissionTranscriptPath(snapshot.transcriptPath)
	if snapshot.transcriptPath == "" {
		return snapshot
	}
	if info, err := os.Stat(snapshot.transcriptPath); err == nil && info.Mode().IsRegular() {
		snapshot.mainOffset = info.Size()
	}
	snapshot.subagentOffsets = snapshotSubagentOffsets(snapshot.transcriptPath)
	return snapshot
}

func (a *adapter) permissionTranscriptPath() string {
	exactFound := false
	if instanceID := instanceIDFromEnv(); instanceID != "" {
		entry, ok := resolveTerminalHandoff(instanceID)
		exactFound = ok
		if ok && entry.TranscriptPath != "" {
			return entry.TranscriptPath
		}
	}
	if entry, ok := a.currentStableIdentity(); ok {
		return entry.TranscriptPath
	}
	// Startup-only fallback until recovery registers the stable identity.
	if !exactFound && a.ownerKeyOK {
		if entry, ok := resolveTerminalHandoff(a.ownerKey); ok {
			return entry.TranscriptPath
		}
	}
	return ""
}

func snapshotSubagentOffsets(transcriptPath string) map[string]int64 {
	offsets := map[string]int64{}
	_ = filepath.WalkDir(subagentTranscriptRoot(transcriptPath), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(entry.Name()) != ".jsonl" {
			return nil
		}
		info, err := entry.Info()
		if err == nil && info.Mode().IsRegular() {
			offsets[path] = info.Size()
		}
		return nil
	})
	return offsets
}

func subagentTranscriptRoot(transcriptPath string) string {
	base := filepath.Base(transcriptPath)
	sessionID := strings.TrimSuffix(base, filepath.Ext(base))
	return filepath.Join(filepath.Dir(transcriptPath), sessionID, "subagents")
}

func (a *adapter) addPendingPermission(requestID string, snapshot permissionSnapshot) {
	if requestID == "" || a.permSettleDisabled.Load() {
		return
	}
	a.observePermissionTranscriptPath(snapshot.transcriptPath)
	a.permMu.Lock()
	a.initPermissionStateLocked()
	if a.permSettleDisabled.Load() {
		a.permMu.Unlock()
		return
	}
	if _, exists := a.permPending[requestID]; exists {
		a.permMu.Unlock()
		return
	}
	if _, exists := a.permWithoutWatcher[requestID]; exists {
		a.permMu.Unlock()
		return
	}
	if snapshot.transcriptPath == "" {
		a.permWithoutWatcher[requestID] = struct{}{}
		a.permMu.Unlock()
		return
	}
	if snapshot.subagentOffsets == nil {
		snapshot.subagentOffsets = map[string]int64{}
	}
	a.permPending[requestID] = &pendingTranscriptPermission{
		since:           snapshot.since,
		transcriptPath:  snapshot.transcriptPath,
		mainOffset:      snapshot.mainOffset,
		subagentOffsets: snapshot.subagentOffsets,
	}
	a.permTranscriptPath = snapshot.transcriptPath
	if !a.permWatcherRunning {
		a.permWatcherRunning = true
		go a.watchPendingPermissions()
	}
	a.permMu.Unlock()
}

func (a *adapter) initPermissionStateLocked() {
	if a.permPending == nil {
		a.permPending = map[string]*pendingTranscriptPermission{}
	}
	if a.permWithoutWatcher == nil {
		a.permWithoutWatcher = map[string]struct{}{}
	}
	if a.permWake == nil {
		a.permWake = make(chan struct{}, 1)
	}
}

// observePermissionTranscriptPath records the first usable transcript path.
// Prompts relayed before that point deliberately remain unwatched: there is no
// safe historical boundary to scan. Make that late handoff visible once, then
// discard the bookkeeping rather than retroactively scanning those prompts.
func (a *adapter) observePermissionTranscriptPath(path string) {
	if path == "" {
		return
	}
	a.permMu.Lock()
	a.initPermissionStateLocked()
	firstKnown := a.permTranscriptPath == ""
	a.permTranscriptPath = path
	withoutWatcher := 0
	if firstKnown {
		withoutWatcher = len(a.permWithoutWatcher)
		a.permWithoutWatcher = map[string]struct{}{}
	}
	a.permMu.Unlock()
	if firstKnown && withoutWatcher > 0 {
		log.Printf("perm: transcript path became available with %d pending permission(s) without a watcher; not scanning retroactively", withoutWatcher)
	}
}

func (a *adapter) removePendingPermission(requestID string) {
	a.permMu.Lock()
	a.initPermissionStateLocked()
	for id := range a.permPending {
		if strings.EqualFold(id, requestID) {
			delete(a.permPending, id)
			break
		}
	}
	for id := range a.permWithoutWatcher {
		if strings.EqualFold(id, requestID) {
			delete(a.permWithoutWatcher, id)
			break
		}
	}
	wake := a.permWake
	a.permMu.Unlock()
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (a *adapter) watchPendingPermissions() {
	for {
		a.pollPendingPermissions(time.Now())
		a.permMu.Lock()
		if len(a.permPending) == 0 {
			a.permWatcherRunning = false
			a.permMu.Unlock()
			return
		}
		wake := a.permWake
		a.permMu.Unlock()

		timer := time.NewTimer(permissionTranscriptPoll)
		if a.runCtx == nil {
			select {
			case <-timer.C:
			case <-wake:
				if !timer.Stop() {
					<-timer.C
				}
			}
			continue
		}
		select {
		case <-timer.C:
		case <-wake:
			if !timer.Stop() {
				<-timer.C
			}
		case <-a.runCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			a.permMu.Lock()
			a.permWatcherRunning = false
			a.permMu.Unlock()
			return
		}
	}
}

func (a *adapter) pollPendingPermissions(now time.Time) {
	a.permMu.Lock()
	a.initPermissionStateLocked()
	var expiredWithWatcher []string
	for id, pending := range a.permPending {
		if now.Sub(pending.since) >= permissionPendingTTL {
			delete(a.permPending, id)
			if pending.transcriptPath != "" {
				expiredWithWatcher = append(expiredWithWatcher, id)
			}
		}
	}
	a.permMu.Unlock()
	sort.Strings(expiredWithWatcher)
	for _, id := range expiredWithWatcher {
		log.Printf("permission %s expired without a transcript settle", id)
	}

	if current := a.permissionTranscriptPath(); current != "" {
		a.observePermissionTranscriptPath(current)
	}
	settlements := a.queuedPermissionSettlements()
	transcriptPath, offset, earliest, ok := a.permissionFileStart("", false)
	if !ok {
		a.sendPermissionSettlements(settlements)
		return
	}

	foundSettlements, _, _ := a.scanPermissionTranscript(transcriptPath, offset, false)
	settlements = append(settlements, foundSettlements...)
	root := subagentTranscriptRoot(transcriptPath)
	var files []string
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(entry.Name()) != ".jsonl" {
			return nil
		}
		info, err := entry.Info()
		if err == nil && info.Mode().IsRegular() && !info.ModTime().Before(earliest) {
			files = append(files, path)
		}
		return nil
	})
	sort.Strings(files)
	for _, path := range files {
		_, fileOffset, _, found := a.permissionFileStart(path, true)
		if !found {
			continue
		}
		foundSettlements, _, _ := a.scanPermissionTranscript(path, fileOffset, true)
		settlements = append(settlements, foundSettlements...)
	}
	a.sendPermissionSettlements(settlements)
}

func (a *adapter) permissionFileStart(path string, isSubagent bool) (string, int64, time.Time, bool) {
	a.permMu.Lock()
	defer a.permMu.Unlock()
	active := a.permTranscriptPath
	var earliest time.Time
	var offset int64
	found := false
	for _, pending := range a.permPending {
		if pending.transcriptPath != active || pending.settling || pending.outcome != "" {
			continue
		}
		candidate := pending.mainOffset
		if isSubagent {
			candidate = pending.subagentOffsets[path]
		}
		if !found || candidate < offset {
			offset = candidate
		}
		if earliest.IsZero() || pending.since.Before(earliest) {
			earliest = pending.since
		}
		found = true
	}
	if path == "" {
		path = active
	}
	return path, offset, earliest, found && path != ""
}

func (a *adapter) scanPermissionTranscript(path string, offset int64, isSubagent bool) ([]permissionSettlement, transcriptScanStats, error) {
	stats := transcriptScanStats{start: offset}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return nil, stats, err
	}
	shrinkLogged := false
	if info.Size() < offset {
		offset = info.Size()
		stats.start = offset
	}
	if a.clampPermissionOffsets(path, isSubagent, info.Size()) {
		log.Printf("perm: transcript shrank path=%q; advancing permission cursor(s) to new size %d", path, info.Size())
		shrinkLogged = true
	}
	file, err := openPermissionTranscript(path)
	if err != nil {
		return nil, stats, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() {
		return nil, stats, err
	}
	if opened.Size() < offset {
		offset = opened.Size()
		stats.start = offset
	}
	if a.clampPermissionOffsets(path, isSubagent, opened.Size()) && !shrinkLogged {
		log.Printf("perm: transcript shrank path=%q; advancing permission cursor(s) to new size %d", path, opened.Size())
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, stats, err
	}

	scanner := oversizedToolIDScanner{}
	lineMatches := map[string]permissionSettlement{}
	allMatches := map[string]permissionSettlement{}
	lineStart := offset
	feed := func(fragment []byte) {
		scanner.Write(fragment, func(hash uint32) {
			mergeSettlements(lineMatches, a.matchPermissionHash(path, isSubagent, lineStart, hash, "unknown"))
		})
	}
	stats, err = scanTranscriptRecords(file, offset, func(start, end int64, line []byte, oversized bool) bool {
		if !oversized {
			content := bytes.TrimSuffix(bytes.TrimSuffix(line, []byte{'\n'}), []byte{'\r'})
			if len(content) > maxTranscriptLineBytes {
				feed(content)
			} else {
				for _, result := range parseTranscriptToolResults(content) {
					mergeSettlements(lineMatches, a.matchPermissionToolUse(path, isSubagent, start, result.toolUseID, result.outcome))
				}
			}
		}
		mergeSettlementMap(allMatches, lineMatches)
		a.advancePermissionOffsets(path, isSubagent, start, end)
		lineStart = end
		scanner = oversizedToolIDScanner{}
		lineMatches = map[string]permissionSettlement{}
		return true
	}, feed)
	return settlementValues(allMatches), stats, err
}

type transcriptToolResult struct {
	toolUseID string
	outcome   string
}

func parseTranscriptToolResults(line []byte) []transcriptToolResult {
	var entry struct {
		Type    string `json:"type"`
		Message struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
	}
	if json.Unmarshal(line, &entry) != nil || entry.Type != "user" {
		return nil
	}
	var content []json.RawMessage
	if json.Unmarshal(entry.Message.Content, &content) != nil {
		return nil
	}
	var results []transcriptToolResult
	for _, raw := range content {
		var item struct {
			Type      string `json:"type"`
			ToolUseID string `json:"tool_use_id"`
			IsError   *bool  `json:"is_error"`
		}
		// Decode each block independently so one malformed or future-shaped
		// element cannot hide valid tool_result blocks beside it.
		if json.Unmarshal(raw, &item) != nil || item.Type != "tool_result" || item.ToolUseID == "" {
			continue
		}
		outcome := "allow"
		if item.IsError != nil && *item.IsError {
			outcome = "unknown"
		}
		results = append(results, transcriptToolResult{toolUseID: item.ToolUseID, outcome: outcome})
	}
	return results
}

func (a *adapter) matchPermissionToolUse(path string, isSubagent bool, lineStart int64, toolUseID, outcome string) []permissionSettlement {
	return a.matchPermissionCandidates(path, isSubagent, lineStart, requestIDCandidates(toolUseID), outcome)
}

func (a *adapter) matchPermissionHash(path string, isSubagent bool, lineStart int64, hash uint32, outcome string) []permissionSettlement {
	return a.matchPermissionCandidates(path, isSubagent, lineStart, requestIDCandidatesFromHash(hash), outcome)
}

func (a *adapter) matchPermissionCandidates(path string, isSubagent bool, lineStart int64, candidates []string, outcome string) []permissionSettlement {
	a.permMu.Lock()
	defer a.permMu.Unlock()
	var matches []permissionSettlement
	for requestID, pending := range a.permPending {
		if pending.transcriptPath != a.permTranscriptPath || pending.settling || pending.outcome != "" {
			continue
		}
		offset := pending.mainOffset
		if isSubagent {
			offset = pending.subagentOffsets[path]
		}
		if offset > lineStart {
			continue
		}
		for _, candidate := range candidates {
			if strings.EqualFold(requestID, candidate) {
				matches = append(matches, permissionSettlement{requestID: requestID, outcome: outcome})
				break
			}
		}
	}
	return matches
}

func (a *adapter) clampPermissionOffsets(path string, isSubagent bool, size int64) bool {
	a.permMu.Lock()
	defer a.permMu.Unlock()
	changed := false
	for _, pending := range a.permPending {
		if pending.transcriptPath != a.permTranscriptPath {
			continue
		}
		if isSubagent {
			if pending.subagentOffsets[path] > size {
				pending.subagentOffsets[path] = size
				changed = true
			}
		} else if pending.mainOffset > size {
			pending.mainOffset = size
			changed = true
		}
	}
	return changed
}

func (a *adapter) advancePermissionOffsets(path string, isSubagent bool, lineStart, lineEnd int64) {
	a.permMu.Lock()
	defer a.permMu.Unlock()
	for _, pending := range a.permPending {
		if pending.transcriptPath != a.permTranscriptPath {
			continue
		}
		if isSubagent {
			if pending.subagentOffsets[path] <= lineStart {
				pending.subagentOffsets[path] = lineEnd
			}
		} else if pending.mainOffset <= lineStart {
			pending.mainOffset = lineEnd
		}
	}
}

func (a *adapter) sendPermissionSettlements(settlements []permissionSettlement) {
	if a.permSettleDisabled.Load() {
		return
	}
	for _, settlement := range settlements {
		if a.permSettleDisabled.Load() {
			return
		}
		if !a.beginPermissionSettle(settlement) {
			continue
		}
		conn := a.currentConn()
		if conn == nil {
			a.finishPermissionSettle(settlement.requestID, false)
			continue
		}
		err := conn.WriteJSON(ipc.PermissionSettledMsg{
			Op: ipc.OpPermissionSettled, RequestID: settlement.requestID, Outcome: settlement.outcome,
		})
		if err != nil {
			log.Printf("perm: broker settle write failed id=%s outcome=%s: %v", settlement.requestID, settlement.outcome, err)
		}
		a.finishPermissionSettle(settlement.requestID, err == nil)
	}
}

func (a *adapter) handleBrokerError(message string) {
	if message != permissionSettledOldError {
		log.Printf("broker error: %s", message)
		return
	}
	if !a.permSettleDisabled.CompareAndSwap(false, true) {
		return
	}
	log.Print(permissionSettledOldLog)
	a.permMu.Lock()
	a.initPermissionStateLocked()
	a.permPending = map[string]*pendingTranscriptPermission{}
	a.permWithoutWatcher = map[string]struct{}{}
	wake := a.permWake
	a.permMu.Unlock()
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (a *adapter) queuedPermissionSettlements() []permissionSettlement {
	a.permMu.Lock()
	defer a.permMu.Unlock()
	var settlements []permissionSettlement
	for requestID, pending := range a.permPending {
		if pending.outcome != "" && !pending.settling {
			settlements = append(settlements, permissionSettlement{requestID: requestID, outcome: pending.outcome})
		}
	}
	return settlements
}

func (a *adapter) beginPermissionSettle(settlement permissionSettlement) bool {
	a.permMu.Lock()
	defer a.permMu.Unlock()
	pending, ok := a.permPending[settlement.requestID]
	if !ok || pending.settling {
		return false
	}
	if pending.outcome == "" {
		pending.outcome = settlement.outcome
	}
	pending.settling = true
	return true
}

func (a *adapter) finishPermissionSettle(requestID string, sent bool) {
	a.permMu.Lock()
	pending, ok := a.permPending[requestID]
	if ok {
		if sent {
			delete(a.permPending, requestID)
		} else {
			pending.settling = false
		}
	}
	wake := a.permWake
	a.permMu.Unlock()
	if sent {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func mergeSettlements(dst map[string]permissionSettlement, values []permissionSettlement) {
	for _, value := range values {
		if _, exists := dst[value.requestID]; !exists {
			dst[value.requestID] = value
		}
	}
}

func mergeSettlementMap(dst, src map[string]permissionSettlement) {
	for id, value := range src {
		if _, exists := dst[id]; !exists {
			dst[id] = value
		}
	}
}

func settlementValues(values map[string]permissionSettlement) []permissionSettlement {
	ids := make([]string, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]permissionSettlement, 0, len(ids))
	for _, id := range ids {
		out = append(out, values[id])
	}
	return out
}

type oversizedToolIDScanner struct {
	matched   int
	capturing bool
	invalid   bool
	hash      uint32
}

var toolUseIDToken = []byte(`"tool_use_id":"`)

func (s *oversizedToolIDScanner) Write(data []byte, found func(uint32)) {
	for _, current := range data {
		if s.capturing {
			switch current {
			case '"':
				if !s.invalid {
					found(s.hash)
				}
				s.capturing = false
				s.invalid = false
				s.matched = 0
			case '\\':
				s.invalid = true
			default:
				if !s.invalid {
					s.hash = fnv1aByte(s.hash, current)
				}
			}
			continue
		}
		if current == toolUseIDToken[s.matched] {
			s.matched++
			if s.matched == len(toolUseIDToken) {
				s.capturing = true
				s.invalid = false
				s.hash = 2166136261
				s.matched = 0
			}
		} else if current == toolUseIDToken[0] {
			s.matched = 1
		} else {
			s.matched = 0
		}
	}
}

func requestIDCandidates(toolUseID string) []string {
	// Claude's host hashes the ASCII toolu_... bytes. Keeping every
	// intermediate as uint32 is Go's equivalent of the host's >>> 0 and avoids
	// a signed remainder indexing the 25-character alphabet negatively.
	hash := uint32(2166136261)
	for _, current := range []byte(toolUseID) {
		hash = fnv1aByte(hash, current)
	}
	return requestIDCandidatesFromHash(hash)
}

func requestIDCandidatesFromHash(hash uint32) []string {
	candidates := make([]string, 0, 11)
	candidates = append(candidates, requestIDFromHash(hash))
	// The host tries D(id), then D(id+":0") through D(id+":9").
	for i := 0; i < 10; i++ {
		candidateHash := fnv1aByte(hash, ':')
		for _, current := range []byte(strconv.Itoa(i)) {
			candidateHash = fnv1aByte(candidateHash, current)
		}
		candidates = append(candidates, requestIDFromHash(candidateHash))
	}
	return candidates
}

func fnv1aByte(hash uint32, current byte) uint32 {
	// FNV-1a is xor-THEN-multiply. Reversing these operations is FNV-1 and
	// silently produces different permission ids.
	return (hash ^ uint32(current)) * 16777619
}

func requestIDFromHash(hash uint32) string {
	var id [5]byte
	for index := range id {
		id[index] = requestIDAlphabet[hash%25]
		hash /= 25
	}
	return string(id[:])
}
