package telegram

import (
	"errors"
	"fmt"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/karthikeyan5/c3/internal/c3types"
)

// StopPoll force-closes a bot-sent poll and returns its final aggregate tally.
// This is the DETERMINISTIC read path for poll results: the passive `poll`
// update only arrives on close, so an agent that wants the current standing
// without waiting calls stop_poll. Telegram's stopPoll(chat_id, message_id)
// returns the stopped Poll with final per-option counts.
//
// The returned PollResult is also the value surfaced directly to the agent as
// the stop_poll tool's MCP result (no gate — it is a synchronous tool return,
// not a pushed inbound event). The passive on-close `poll` update for the same
// poll will additionally arrive and be routed via dispatchPollUpdate; that is
// idempotent (the agent sees the close once via the tool return and the close
// event refreshes the same retained tally).
func (c *Channel) StopPoll(chatID, messageID int64) (*c3types.PollResult, error) {
	if c.bot == nil {
		return nil, errors.New("telegram: channel not started")
	}
	if err := c.rate.Wait(c.ctx, chatID); err != nil {
		return nil, fmt.Errorf("telegram: rate-wait: %w", err)
	}
	poll, err := c.bot.StopPoll(chatID, messageID, &gotgbot.StopPollOpts{})
	if err != nil {
		c.recordOutboundErr(err)
		return nil, c.scrubTokenf("telegram: StopPoll: %w", err)
	}
	c.recordOutboundSuccess()
	if poll == nil {
		return nil, errors.New("telegram: StopPoll returned a nil poll")
	}

	tally := pollTallyFromGotgbot(poll)
	// Refresh the retained tally if this is a poll we sent (keeps the sentPolls
	// snapshot current; harmless no-op for a poll absent from the bounded map).
	// UpdateTally mutates under the map lock — the poll-loop goroutine
	// (dispatchPollUpdate) can touch the same *sentPoll concurrently, so a
	// Get→mutate→Put read-modify-write would race.
	c.sentPolls.UpdateTally(poll.Id, tally)

	opts := make([]c3types.PollOptionTally, 0, len(tally.Options))
	for _, o := range tally.Options {
		opts = append(opts, c3types.PollOptionTally{Text: o.Text, VoterCount: o.VoterCount})
	}
	return &c3types.PollResult{
		PollID:      poll.Id,
		Question:    tally.Question,
		TotalVoters: tally.TotalVoters,
		IsClosed:    tally.IsClosed,
		Options:     opts,
	}, nil
}
