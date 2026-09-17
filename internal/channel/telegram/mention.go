package telegram

import (
	"strings"
	"unicode"
	"unicode/utf16"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/swarm"
)

func isMuteCommand(text string) bool {
	t := strings.TrimSpace(text)
	if i := strings.IndexAny(t, " \t\n\r"); i >= 0 {
		t = t[:i]
	}
	if i := strings.IndexByte(t, '@'); i >= 0 {
		t = t[:i]
	}
	return strings.EqualFold(t, "/mute")
}

func swarmKey(in *c3types.Inbound) swarm.Key {
	k := swarm.Key{ChatID: in.ChatID}
	if in.TopicID != nil {
		k.TopicID = *in.TopicID
	}
	return k
}

func (c *Channel) addressedToMe(msg *gotgbot.Message, text string) bool {
	me := strings.ToLower(c.myUsername())
	if me == "" {
		return false
	}
	for _, u := range mentionUsernames(msg, text) {
		if strings.EqualFold(u, me) {
			return true
		}
	}
	if msg != nil && msg.ReplyToMessage != nil && msg.ReplyToMessage.From != nil {
		if strings.EqualFold(msg.ReplyToMessage.From.Username, me) {
			return true
		}
	}
	return false
}

func mentionUsernames(msg *gotgbot.Message, text string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(u string) {
		u = strings.TrimPrefix(strings.TrimSpace(u), "@")
		if u == "" {
			return
		}
		k := strings.ToLower(u)
		if seen[k] {
			return
		}
		seen[k] = true
		out = append(out, u)
	}
	if msg != nil {
		collectEntityMentions(msg.Text, msg.Entities, add)
		collectEntityMentions(msg.Caption, msg.CaptionEntities, add)
	}
	for _, u := range atMentionsInText(text) {
		add(u)
	}
	return out
}

func collectEntityMentions(body string, entities []gotgbot.MessageEntity, add func(string)) {
	if body == "" || len(entities) == 0 {
		return
	}
	units := utf16.Encode([]rune(body))
	for _, e := range entities {
		switch e.Type {
		case "text_mention":
			if e.User != nil {
				add(e.User.Username)
			}
		case "mention":
			start, end := int(e.Offset), int(e.Offset+e.Length)
			if start >= 0 && end <= len(units) && start < end {
				add(string(utf16.Decode(units[start:end])))
			}
		}
	}
}

func atMentionsInText(text string) []string {
	var out []string
	for i := 0; i < len(text); i++ {
		if text[i] != '@' {
			continue
		}
		if i > 0 {
			r := rune(text[i-1])
			if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
				continue
			}
		}
		j := i + 1
		for j < len(text) {
			r := rune(text[j])
			if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
				break
			}
			j++
		}
		if j > i+1 {
			out = append(out, text[i+1:j])
		}
	}
	return out
}
