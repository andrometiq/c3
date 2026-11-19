package web

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
)

const (
	boardSpeakBodyLimit = 1 << 10
	// Each reply C3 synthesizes may cost the provider; a reply already in the
	// audio cache costs nothing and is not counted.
	boardSpokenPerMinute = 12
)

var errNotAgentMessage = errors.New("board: id names no message of the session on this board")

// handleBoardSpeak is POST /board/<name>/speak {"id": N}: the session's
// message N on this board, read aloud through the web chat's synthesis (its
// queue, audio cache and provider), as {"token"} for GET
// /board/<name>/audio/<token>. Only the shell asks, and it plays the audio:
// the frame has no network.
func (c *Channel) handleBoardSpeak(w http.ResponseWriter, r *http.Request) {
	c.postDeadline(w, r)
	if !c.sameOrigin(r) {
		writeBoardError(w, http.StatusForbidden, "forbidden")
		return
	}
	if _, ok := c.boardRequest(w, r, true); !ok {
		return
	}
	var body struct {
		ID int64 `json:"id"`
	}
	if err := decodeJSONLimit(w, r, &body, boardSpeakBodyLimit); err != nil || body.ID <= 0 {
		writeBoardError(w, http.StatusBadRequest, "invalid")
		return
	}
	number, text, err := c.board.agentMessage(r.PathValue("name"), body.ID)
	switch {
	case errors.Is(err, errNotAgentMessage):
		writeBoardError(w, http.StatusNotFound, "not_found")
		return
	case err != nil:
		writeBoardStoreError(w, err)
		return
	}
	if token, ok := c.audioTokenForMessage(number, body.ID); ok {
		writeBoardSpeech(w, token)
		return
	}
	retryAfter, err := c.board.chargeSpoken(number)
	if err != nil {
		writeBoardStoreError(w, err)
		return
	}
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		writeBoardError(w, http.StatusTooManyRequests, "rate")
		return
	}
	controller := http.NewResponseController(w)
	_ = controller.SetReadDeadline(time.Time{})
	_ = controller.SetWriteDeadline(time.Time{})
	token, err := c.speakNow(r, number, body.ID, text)
	switch {
	case r.Context().Err() != nil: // the shell went away: no one to answer
	case err != nil:
		c.board.logf("board: speak board %d message %d: %v", number, body.ID, err)
		writeBoardError(w, http.StatusServiceUnavailable, "unavailable")
	default:
		writeBoardSpeech(w, token)
	}
}

func writeBoardSpeech(w http.ResponseWriter, token string) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"token": token})
}

// handleBoardAudio serves audio C3 synthesized for this board only.
func (c *Channel) handleBoardAudio(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if _, ok := c.boardRequest(w, r, false); !ok {
		return
	}
	board, ok := c.board.BoardByName(r.PathValue("name"))
	if !ok {
		writeBoardError(w, http.StatusNotFound, "not_found")
		return
	}
	audio, ok := c.audioForToken(board.Number, r.PathValue("token"))
	if !ok {
		writeBoardError(w, http.StatusNotFound, "not_found")
		return
	}
	serveMP3(w, r, audio.created, audio.audio)
}

// agentMessage is the session's message id on board name, as last edited,
// with the board's number. A closed board's messages can still be heard.
func (b *boardChannel) agentMessage(name string, id int64) (int64, string, error) {
	var number int64
	var text string
	err := b.withBoardNamed(name, func(board *boardState) error {
		message, ok := board.entry(id)
		if !ok || message.Dir != "out" || message.Op != "message" {
			return errNotAgentMessage
		}
		number, text = board.record.Number, message.Text
		for _, entry := range board.log {
			if entry.ID == id && entry.Dir == "out" && entry.Op == "edit" {
				text = entry.Text
			}
		}
		return nil
	})
	return number, text, err
}

// chargeSpoken counts one synthesis for the board, or returns the seconds to
// wait when it has had boardSpokenPerMinute this minute.
func (b *boardChannel) chargeSpoken(number int64) (int, error) {
	retryAfter := 0
	err := b.withBoard(number, func(board *boardState) error {
		retryAfter = chargeMinute(&board.spoken, b.web.now(), boardSpokenPerMinute)
		return nil
	})
	return retryAfter, err
}
