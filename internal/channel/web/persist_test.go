package web

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/c3types"
)

func persistentHost() *fakeHost {
	return &fakeHost{
		webConfig: Config{}, telegram: telegramConfig{MasterUserID: 42, DMChatID: 42},
		registered: true, allowed: true,
	}
}

func startPersistentChannel(t *testing.T, host *fakeHost, now func() time.Time) *Channel {
	t.Helper()
	c := New()
	if now != nil {
		c.now = now
	}
	c.listenFunc = func(_, _ string) (net.Listener, error) { return newFakeListener(), nil }
	if err := c.Start(context.Background(), host); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return c
}

func persistentSend(c *Channel, cookie *http.Cookie, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	r := request(http.MethodPost, "/send", body, cookie, false)
	r.Header.Set("Origin", c.baseURL)
	c.routes().ServeHTTP(recorder, r)
	return recorder
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	first := startPersistentChannel(t, persistentHost(), nil)
	cookieValue, err := first.createSession(loginToken{userID: 42, username: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	cookie := &http.Cookie{Name: sessionCookieName, Value: cookieValue}

	replyIDs := make([]int64, 0, 3)
	for index := 1; index <= 3; index++ {
		id, err := first.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: fmt.Sprintf("reply-%d", index)})
		if err != nil {
			t.Fatal(err)
		}
		replyIDs = append(replyIDs, id)
	}
	first.publish(streamEvent{
		kind: "own",
		payload: streamPayload{
			MessageID: 99, ClientID: "persist-own", Text: "browser question", Timestamp: streamTimestamp(first.now()),
		},
	})
	if first.nextEventID != 4 {
		t.Fatalf("nextEventID=%d, want 4", first.nextEventID)
	}
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	second := startPersistentChannel(t, persistentHost(), nil)
	defer second.Stop()
	if second.nextInboundFloor != 99 {
		t.Fatalf("inbound floor from own replay event=%d, want 99", second.nextInboundFloor)
	}
	if key, current, ok := second.authenticate(request(http.MethodGet, "/", "", cookie, false), false); !ok || key != sessionKey(cookieValue) || current.userID != 42 {
		t.Fatalf("persisted cookie authentication=%q/%+v/%v", key, current, ok)
	}

	recorder, cancel, done := startSSE(second, cookie, "", "")
	waitFor(t, func() bool {
		_, body := recorder.snapshot()
		return strings.Contains(body, "browser question")
	})
	_, body := recorder.snapshot()
	for _, expected := range []string{
		"id: 1\nevent: message", "\"message_id\":1", "id: 2\nevent: message", "\"message_id\":2",
		"id: 3\nevent: message", "\"message_id\":3", "id: 4\nevent: own", "\"message_id\":99",
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("restart replay missing %q: %q", expected, body)
		}
	}
	if strings.Contains(body, "history may be incomplete") {
		t.Fatalf("complete restart replay was flagged incomplete: %q", body)
	}
	cancel()
	<-done

	resumed, resumedCancel, resumedDone := startSSE(second, cookie, "4", "")
	waitFor(t, func() bool { return streamCount(second) == 1 })
	_, resumedBody := resumed.snapshot()
	if !strings.Contains(resumedBody, "event: prefs") || strings.Contains(resumedBody, "event: message") || strings.Contains(resumedBody, "event: own") || strings.Contains(resumedBody, "event: edit") || strings.Contains(resumedBody, "history may be incomplete") {
		t.Fatalf("Last-Event-ID at ring tip replayed data: %q", resumedBody)
	}
	resumedCancel()
	<-resumedDone

	nextReply, err := second.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "after restart"})
	if err != nil {
		t.Fatal(err)
	}
	if nextReply != 4 || second.nextEventID != 5 {
		t.Fatalf("continued reply/event ids=%d/%d, want 4/5 (old replies %v)", nextReply, second.nextEventID, replyIDs)
	}

	directory, err := stateDir()
	if err != nil {
		t.Fatal(err)
	}
	assertMode(t, directory, 0700)
	for _, name := range []string{"sessions.json", "replay.jsonl"} {
		assertMode(t, filepath.Join(directory, name), 0600)
	}
	sessionsData, err := os.ReadFile(filepath.Join(directory, "sessions.json"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sessionsData, []byte(cookieValue)) || !bytes.Contains(sessionsData, []byte(sessionKey(cookieValue))) {
		t.Fatal("sessions state contains the raw cookie or omits its hash")
	}
	replayData, err := os.ReadFile(filepath.Join(directory, "replay.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(replayData, []byte(cookieValue)) {
		t.Fatal("replay state contains the raw cookie")
	}
}

func TestStatusOnlyReplyIDSurvivesRestart(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	first := startPersistentChannel(t, persistentHost(), nil)
	statusID, err := first.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "📨 Held — nothing lost."})
	if err != nil {
		t.Fatal(err)
	}
	if statusID != 1 || len(first.replay) != 0 {
		t.Fatalf("status id/ring=%d/%d, want 1/0", statusID, len(first.replay))
	}
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	second := startPersistentChannel(t, persistentHost(), nil)
	defer second.Stop()
	replyID, err := second.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "after status restart"})
	if err != nil {
		t.Fatal(err)
	}
	if replyID <= statusID {
		t.Fatalf("reply id after status-only restart=%d, want >%d", replyID, statusID)
	}
}

func TestInboundIDFloorSurvivesClientMessageTTL(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	currentTime := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	first := startPersistentChannel(t, persistentHost(), func() time.Time { return currentTime })
	cookieValue, err := first.createSession(loginToken{userID: 42, username: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	cookie := &http.Cookie{Name: sessionCookieName, Value: cookieValue}
	accepted := persistentSend(first, cookie, `{"text":"first","client_id":"first"}`)
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("first send status=%d body=%s", accepted.Code, accepted.Body.String())
	}
	var firstResult struct {
		MessageID int64 `json:"message_id"`
	}
	if err := json.Unmarshal(accepted.Body.Bytes(), &firstResult); err != nil {
		t.Fatal(err)
	}
	currentTime = currentTime.Add(clientMessageTTL)
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	second := startPersistentChannel(t, persistentHost(), func() time.Time { return currentTime })
	defer second.Stop()
	next := persistentSend(second, cookie, `{"text":"second","client_id":"second"}`)
	if next.Code != http.StatusAccepted {
		t.Fatalf("second send status=%d body=%s", next.Code, next.Body.String())
	}
	var nextResult struct {
		MessageID int64 `json:"message_id"`
	}
	if err := json.Unmarshal(next.Body.Bytes(), &nextResult); err != nil {
		t.Fatal(err)
	}
	if nextResult.MessageID <= firstResult.MessageID {
		t.Fatalf("inbound id after TTL restart=%d, want >%d", nextResult.MessageID, firstResult.MessageID)
	}
}

func TestWholeFileCorruptReplayKeepsStoredCounterFloors(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	directory, err := stateDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	stored := persistedState{Version: stateVersion, NextReplyID: 41, NextInboundID: 51, NextEventID: 61, Sessions: []persistedSession{}, ClientMessages: []persistedClientMessage{}}
	data, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "sessions.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "replay.jsonl"), bytes.Repeat([]byte("x"), 2<<20), 0600); err != nil {
		t.Fatal(err)
	}
	host := persistentHost()
	c := startPersistentChannel(t, host, nil)
	defer c.Stop()
	if c.nextReplyID != 41 || c.nextInboundFloor != 51 || c.nextEventID != 61 || len(c.replay) != 0 {
		t.Fatalf("restored floors/replay=%d/%d/%d/%d, want 41/51/61/0", c.nextReplyID, c.nextInboundFloor, c.nextEventID, len(c.replay))
	}
	if got := c.nextInboundMessageID(); got != 52 {
		t.Fatalf("next inbound id=%d, want 52", got)
	}
	replyID, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "after corrupt replay"})
	if err != nil {
		t.Fatal(err)
	}
	if replyID != 42 || c.nextEventID != 62 {
		t.Fatalf("next reply/event ids=%d/%d, want 42/62", replyID, c.nextEventID)
	}
	matches, err := filepath.Glob(filepath.Join(directory, "replay.jsonl.corrupt-*"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("corrupt replay rename matches=%v err=%v", matches, err)
	}
	if !strings.Contains(host.logText(), "corrupt replay.jsonl renamed") {
		t.Fatalf("missing corrupt replay log: %q", host.logText())
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode=%#o, want %#o", filepath.Base(path), got, want)
	}
}

func TestLoadPrunesIdleSessionsAndStaleClientMessages(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	base := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return base }
	first := startPersistentChannel(t, persistentHost(), now)
	activeValue, err := first.createSession(loginToken{userID: 42, username: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	idleValue, err := first.createSession(loginToken{userID: 42, username: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	activeCookie := &http.Cookie{Name: sessionCookieName, Value: activeValue}
	response := persistentSend(first, activeCookie, `{"text":"old request","client_id":"stale-client"}`)
	if response.Code != http.StatusAccepted {
		t.Fatalf("send status=%d body=%s", response.Code, response.Body.String())
	}
	first.authMu.Lock()
	idle := first.sessions[sessionKey(idleValue)]
	idle.lastSeen = base.Add(-sessionIdleTTL)
	idle.savedLastSeen = idle.lastSeen
	first.authMu.Unlock()
	first.clientMu.Lock()
	first.clientMessages[clientMessageKey{sessionID: sessionKey(activeValue), clientID: "stale-client"}].created = base.Add(-clientMessageTTL)
	first.clientMu.Unlock()
	if err := first.saveSessions(); err != nil {
		t.Fatal(err)
	}
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	second := startPersistentChannel(t, persistentHost(), now)
	defer second.Stop()
	if _, _, ok := second.authenticate(request(http.MethodGet, "/", "", activeCookie, false), false); !ok {
		t.Fatal("active session was pruned")
	}
	if _, _, ok := second.authenticate(request(http.MethodGet, "/", "", &http.Cookie{Name: sessionCookieName, Value: idleValue}, false), false); ok {
		t.Fatal("idle session survived load")
	}
	if len(second.clientMessages) != 0 {
		t.Fatalf("stale client messages=%d, want 0", len(second.clientMessages))
	}

	directory, _ := stateDir()
	data, err := os.ReadFile(filepath.Join(directory, "sessions.json"))
	if err != nil {
		t.Fatal(err)
	}
	var stored persistedState
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Sessions) != 1 || len(stored.ClientMessages) != 0 {
		t.Fatalf("pruned state=%+v", stored)
	}
}

func TestSessionLastSeenIsPersistedAtMostEveryFiveMinutes(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	currentTime := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	c := startPersistentChannel(t, persistentHost(), func() time.Time { return currentTime })
	defer c.Stop()
	cookieValue, err := c.createSession(loginToken{userID: 42, username: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	cookie := &http.Cookie{Name: sessionCookieName, Value: cookieValue}
	directory, _ := stateDir()
	path := filepath.Join(directory, "sessions.json")

	readLastSeen := func() time.Time {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var stored persistedState
		if err := json.Unmarshal(data, &stored); err != nil {
			t.Fatal(err)
		}
		if len(stored.Sessions) != 1 {
			t.Fatalf("sessions=%d, want 1", len(stored.Sessions))
		}
		return stored.Sessions[0].LastSeen
	}

	currentTime = currentTime.Add(time.Minute)
	if _, _, ok := c.authenticate(request(http.MethodGet, "/", "", cookie, false), true); !ok {
		t.Fatal("session did not authenticate")
	}
	if got := readLastSeen(); !got.Equal(currentTime.Add(-time.Minute)) {
		t.Fatalf("last_seen persisted too early: %s", got)
	}
	if err := c.saveSessions(); err != nil {
		t.Fatal(err)
	}
	if got := readLastSeen(); !got.Equal(currentTime) {
		t.Fatalf("unrelated state rewrite persisted last_seen=%s, want %s", got, currentTime)
	}

	currentTime = currentTime.Add(4 * time.Minute)
	if _, _, ok := c.authenticate(request(http.MethodGet, "/", "", cookie, false), true); !ok {
		t.Fatal("session did not authenticate")
	}
	if got := readLastSeen(); !got.Equal(currentTime.Add(-4 * time.Minute)) {
		t.Fatalf("last_seen persisted before rewritten touch interval: %s", got)
	}

	currentTime = currentTime.Add(time.Minute)
	if _, _, ok := c.authenticate(request(http.MethodGet, "/", "", cookie, false), true); !ok {
		t.Fatal("session did not authenticate")
	}
	if got := readLastSeen(); !got.Equal(currentTime) {
		t.Fatalf("last_seen=%s, want %s", got, currentTime)
	}
}

func TestCorruptSessionsAreRenamedAndIgnored(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	directory, err := stateDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "sessions.json"), []byte(`{"version":1,"sessions":[`), 0600); err != nil {
		t.Fatal(err)
	}
	host := persistentHost()
	c := startPersistentChannel(t, host, nil)
	defer c.Stop()
	if len(c.sessions) != 0 {
		t.Fatalf("loaded corrupt sessions=%d", len(c.sessions))
	}
	matches, err := filepath.Glob(filepath.Join(directory, "sessions.json.corrupt-*"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("corrupt rename matches=%v err=%v", matches, err)
	}
	if !strings.Contains(host.logText(), "corrupt sessions.json renamed") {
		t.Fatalf("missing corrupt-state log: %q", host.logText())
	}
}

func TestInvalidTextDigestRenamesSessionsAsCorrupt(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	directory, err := stateDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	key := sessionKey("not-a-real-cookie")
	stored := persistedState{
		Version:  stateVersion,
		Sessions: []persistedSession{{Key: key, UserID: 42, Username: "operator", Created: now, LastSeen: now}},
		ClientMessages: []persistedClientMessage{{
			Session: key, ClientID: "bad-hash", MessageID: 1, TextSHA256: strings.Repeat("z", 64), Outcome: "accepted", At: now,
		}},
	}
	data, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "sessions.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	host := persistentHost()
	c := startPersistentChannel(t, host, func() time.Time { return now })
	defer c.Stop()
	matches, err := filepath.Glob(filepath.Join(directory, "sessions.json.corrupt-*"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("invalid-digest corrupt rename matches=%v err=%v", matches, err)
	}
	if len(c.sessions) != 0 || len(c.clientMessages) != 0 {
		t.Fatalf("invalid-digest state loaded sessions/client messages=%d/%d", len(c.sessions), len(c.clientMessages))
	}
}

func TestCorruptReplayLineIsSkipped(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	directory, err := stateDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	first, _ := json.Marshal(persistedReplayEvent{Sequence: 1, Kind: "message", Payload: streamPayload{MessageID: 1, Text: "first"}})
	second, _ := json.Marshal(persistedReplayEvent{Sequence: 2, Kind: "edit", Payload: streamPayload{MessageID: 1, Text: "edited"}})
	data := append(append(append(first, '\n'), []byte("not json\n")...), second...)
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(directory, "replay.jsonl"), data, 0600); err != nil {
		t.Fatal(err)
	}
	host := persistentHost()
	c := startPersistentChannel(t, host, nil)
	defer c.Stop()
	if len(c.replay) != 2 || c.replay[0].sequence != 1 || c.replay[1].sequence != 2 || c.nextEventID != 2 {
		t.Fatalf("replay=%+v next=%d", c.replay, c.nextEventID)
	}
	if count := strings.Count(host.logText(), "skipping corrupt replay.jsonl line"); count != 1 {
		t.Fatalf("corrupt-line logs=%d: %q", count, host.logText())
	}
}

func TestFirstReplayAppendCreatesProtectedFile(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	c := startPersistentChannel(t, persistentHost(), nil)
	defer c.Stop()
	directory, err := stateDir()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "replay.jsonl")
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replay file exists before first event: %v", err)
	}
	if _, err := c.SendReply(c3types.ReplyArgs{Channel: Name, ChatID: 42, Text: "first durable event"}); err != nil {
		t.Fatal(err)
	}
	assertMode(t, path, 0600)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if lines := bytes.Count(data, []byte{'\n'}); lines != 1 {
		t.Fatalf("first replay append lines=%d, want 1", lines)
	}
}

func TestReplayCompactionKeepsLatestEvents(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	directory, err := stateDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "replay.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	encoder := json.NewEncoder(file)
	for sequence := int64(1); sequence <= replayCompactLineLimit+1; sequence++ {
		if err := encoder.Encode(persistedReplayEvent{
			Sequence: sequence, Kind: "message",
			Payload: streamPayload{MessageID: sequence, Text: fmt.Sprintf("event-%d", sequence)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	c := startPersistentChannel(t, persistentHost(), nil)
	defer c.Stop()
	wantFirst := int64(replayCompactLineLimit + 2 - replayLimit)
	if len(c.replay) != replayLimit || c.replay[0].sequence != wantFirst || c.replay[replayLimit-1].sequence != replayCompactLineLimit+1 || c.nextEventID != replayCompactLineLimit+1 || c.nextReplyID != replayCompactLineLimit+1 {
		t.Fatalf("compacted ring len/first/last/event/reply=%d/%d/%d/%d/%d", len(c.replay), c.replay[0].sequence, c.replay[replayLimit-1].sequence, c.nextEventID, c.nextReplyID)
	}

	compacted, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer compacted.Close()
	scanner := bufio.NewScanner(compacted)
	var sequences []int64
	for scanner.Scan() {
		var saved persistedReplayEvent
		if err := json.Unmarshal(scanner.Bytes(), &saved); err != nil {
			t.Fatal(err)
		}
		sequences = append(sequences, saved.Sequence)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(sequences) != replayLimit || sequences[0] != wantFirst || sequences[len(sequences)-1] != replayCompactLineLimit+1 {
		t.Fatalf("compacted file sequences=%v", sequences)
	}
}

func TestClientMessageConflictSurvivesRestart(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	first := startPersistentChannel(t, persistentHost(), nil)
	cookieValue, err := first.createSession(loginToken{userID: 42, username: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	cookie := &http.Cookie{Name: sessionCookieName, Value: cookieValue}
	accepted := persistentSend(first, cookie, `{"text":"original","client_id":"same-client"}`)
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("accepted status=%d body=%s", accepted.Code, accepted.Body.String())
	}
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	second := startPersistentChannel(t, persistentHost(), nil)
	defer second.Stop()
	conflict := persistentSend(second, cookie, `{"text":"changed","client_id":"same-client"}`)
	if conflict.Code != http.StatusConflict || !strings.Contains(conflict.Body.String(), errClientIDConflict.Error()) {
		t.Fatalf("restart conflict status/body=%d/%q", conflict.Code, conflict.Body.String())
	}
}
