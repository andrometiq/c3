package telegram

import (
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/PaulSonOfLars/gotgbot/v2"
)

// convKindFromChat maps a Telegram Chat onto c3types.Inbound.ConvKind.
//
// This exists so the BROKER never has to guess. Gate picks which allowlist to
// consult from the conversation kind, and it used to derive that from the sign
// of the chat id — a fact about Telegram's identifier space applied inside
// channel-neutral code. Telegram states the kind outright in Chat.Type, so the
// channel that owns the id space answers the question instead of the broker
// inferring it. See the ConvKind field doc in internal/c3types/types.go.
//
// Chat.Type is one of "private", "group", "supergroup", "channel"
// (https://core.telegram.org/bots/api#chat). Anything unrecognised — including
// the empty string, which is what a hand-built test fixture or a stripped
// payload produces — falls back to the id sign rather than defaulting, because
// silently calling a real DM a group would deny the operator's own messages.
func convKindFromChat(c gotgbot.Chat) string {
	switch c.Type {
	case "private":
		return c3types.ConvKindDM
	case "group", "supergroup", "channel":
		return c3types.ConvKindGroup
	}
	return convKindFromChatID(c.Id)
}

// convKindFromChatID is the fallback for the one inbound path that carries a
// bare chat id and no Chat: a poll result reconstructed from a stored sentPoll.
// Telegram's Bot API defines chat ids as positive for private chats and
// negative for groups, supergroups and channels — authoritative for THIS
// channel's ids, which is precisely why the conversion belongs here and not in
// the broker.
func convKindFromChatID(id int64) string {
	if id > 0 {
		return c3types.ConvKindDM
	}
	return c3types.ConvKindGroup
}
