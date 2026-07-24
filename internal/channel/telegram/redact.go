package telegram

import (
	"errors"
	"fmt"
	"strings"
)

// The bot token is a full-control credential: anyone holding it can call
// getUpdates and read every message in the operator's groups and DMs, and can
// post, edit, and delete as the bot.
//
// Bot-API URLs embed it in their PATH (`/bot<token>/method`, `/file/bot<token>/…`).
// Go's net/http reports transport failures as *url.Error, whose Error() prints
// the FULL URL — so any `fmt.Errorf("…: %w", err)` on a Bot-API request quietly
// copies the token into the message. Those messages do not stay in the log:
// they are returned as MCP tool results, i.e. straight into the coding agent's
// context, from where they get echoed to the terminal, pasted into a chat reply,
// or attached to a bug report against this PUBLIC repo.
//
// So every error that could have transited a Bot-API URL goes through scrubToken
// before it leaves this package. (v0.1.0 release audit, 2026-07-25.)

// redactToken replaces every occurrence of token in s. An empty token is a
// no-op, so an unconfigured channel cannot accidentally redact everything.
func redactToken(s, token string) string {
	if token == "" {
		return s
	}
	return strings.ReplaceAll(s, token, "<redacted>")
}

// scrubToken returns err with the bot token removed from its message. The
// result deliberately does NOT wrap the original: wrapping would keep the
// unredacted error reachable through errors.Unwrap/As, which is exactly the
// leak we are closing. Sentinel comparisons that matter are preserved by
// callers checking before scrubbing.
func (c *Channel) scrubToken(err error) error {
	if err == nil {
		return nil
	}
	token := c.cfg.BotToken
	if token == "" {
		return err
	}
	msg := err.Error()
	if !strings.Contains(msg, token) {
		return err
	}
	return errors.New(redactToken(msg, token))
}

// scrubTokenf formats an error message and removes the bot token from the
// result. Use for the download path and anywhere else a Bot-API URL can reach
// an error string.
func (c *Channel) scrubTokenf(format string, args ...any) error {
	return c.scrubToken(fmt.Errorf(format, args...))
}
