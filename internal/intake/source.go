// Package intake holds private inbound provenance shared by channels and queues.
package intake

// Source is the original provider occurrence, independent of presentation and route.
type Source struct {
	Channel     string             `json:"channel"`
	ChatID      int64              `json:"chat_id,string"`
	TopicID     *int64             `json:"topic_id,string"`
	MessageID   int64              `json:"message_id,string"`
	SenderID    *int64             `json:"sender_id,string"`
	UpdateID    int64              `json:"update_id,string"`
	Text        string             `json:"text"`
	Attachments []SourceAttachment `json:"attachments"`
}

type SourceAttachment struct {
	Kind   string `json:"kind"`
	FileID string `json:"file_id"`
	Size   int64  `json:"size"`
	MIME   string `json:"mime"`
	Name   string `json:"name"`
}

// Clone transfers ownership, including nullable IDs and the attachment slice.
func (s *Source) Clone() *Source {
	if s == nil {
		return nil
	}
	cp := *s
	if s.TopicID != nil {
		id := *s.TopicID
		cp.TopicID = &id
	}
	if s.SenderID != nil {
		id := *s.SenderID
		cp.SenderID = &id
	}
	cp.Attachments = append([]SourceAttachment{}, s.Attachments...)
	return &cp
}

// Optional permits legacy and synthetic callers to omit provider provenance.
func Optional(sources []*Source) *Source {
	if len(sources) > 1 {
		panic("intake: more than one source for an occurrence")
	}
	if len(sources) == 0 {
		return nil
	}
	return sources[0].Clone()
}

func (s *Source) SameOccurrence(other *Source) bool {
	return s != nil && other != nil && s.Channel == other.Channel &&
		s.ChatID == other.ChatID && s.UpdateID == other.UpdateID &&
		((s.TopicID == nil && other.TopicID == nil) ||
			(s.TopicID != nil && other.TopicID != nil && *s.TopicID == *other.TopicID))
}
