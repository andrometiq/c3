package mappings

import "time"

// UpsertTopic inserts a new topic or updates an existing one (matched by
// channel + chat_id + topic_id). Creates the channel entry if missing.
func (mf *MappingsFile) UpsertTopic(channel string, t Topic) {
	if mf.Channels == nil {
		mf.Channels = map[string]ChannelConfig{}
	}
	cc := mf.Channels[channel]
	for i, existing := range cc.Topics {
		if existing.ChatID == t.ChatID && existing.TopicID == t.TopicID {
			cc.Topics[i] = t
			mf.Channels[channel] = cc
			return
		}
	}
	cc.Topics = append(cc.Topics, t)
	mf.Channels[channel] = cc
}

// UpsertMapping inserts a new cwd → mapping or updates an existing one. When
// updating, a zero CreatedAt on the new value is replaced with the existing
// entry's CreatedAt so update-flows can leave that field unset.
func (mf *MappingsFile) UpsertMapping(cwd string, m Mapping) {
	if mf.Mappings == nil {
		mf.Mappings = map[string]Mapping{}
	}
	if existing, ok := mf.Mappings[cwd]; ok {
		if m.CreatedAt.IsZero() {
			m.CreatedAt = existing.CreatedAt
		}
	}
	mf.Mappings[cwd] = m
}

// UpsertSessionAttachment records (or replaces) the last-attached route for a
// (CLI family, stable session id) pair. The new value's Detached defaults false,
// so re-attaching after a detach clears the tombstone. No-op on an incomplete
// key. An explicit namespaced write supersedes an ambiguous legacy entry.
func (mf *MappingsFile) UpsertSessionAttachment(cli, id string, sa SessionAttachment) {
	if cli == "" || id == "" {
		return
	}
	if mf.SessionAttachmentsByCLI == nil {
		mf.SessionAttachmentsByCLI = map[string]map[string]SessionAttachment{}
	}
	if mf.SessionAttachmentsByCLI[cli] == nil {
		mf.SessionAttachmentsByCLI[cli] = map[string]SessionAttachment{}
	}
	mf.SessionAttachmentsByCLI[cli][id] = sa
	delete(mf.SessionAttachments, id)
}

// LookupSessionAttachment returns the namespaced entry for a CLI/session pair.
// It deliberately does not consult the legacy map: only the broker's serialized
// ClaimLegacySessionAttachment path may turn an unqualified record into identity
// evidence.
func (mf *MappingsFile) LookupSessionAttachment(cli, id string) (SessionAttachment, bool) {
	if mf == nil || cli == "" || id == "" {
		return SessionAttachment{}, false
	}
	sa, ok := mf.SessionAttachmentsByCLI[cli][id]
	return sa, ok
}

// ClaimLegacySessionAttachment atomically migrates one unqualified legacy
// record to the requesting CLI family. If any other family already owns the
// same stable id, the legacy record is not evidence for this caller. Callers
// must serialize this mutation.
func (mf *MappingsFile) ClaimLegacySessionAttachment(cli, id string) (SessionAttachment, bool) {
	if mf == nil || cli == "" || id == "" {
		return SessionAttachment{}, false
	}
	if sa, ok := mf.LookupSessionAttachment(cli, id); ok {
		return sa, true
	}
	for family, entries := range mf.SessionAttachmentsByCLI {
		if family == cli {
			continue
		}
		if _, owned := entries[id]; owned {
			return SessionAttachment{}, false
		}
	}
	sa, ok := mf.SessionAttachments[id]
	if !ok {
		return SessionAttachment{}, false
	}
	mf.UpsertSessionAttachment(cli, id, sa)
	return sa, true
}

// TombstoneSessionAttachment marks a namespaced session attachment as
// deliberately detached, so a later resume does NOT auto-recover it.
func (mf *MappingsFile) TombstoneSessionAttachment(cli, id string) {
	if mf == nil || cli == "" || id == "" {
		return
	}
	if entries := mf.SessionAttachmentsByCLI[cli]; entries != nil {
		sa, ok := entries[id]
		if !ok {
			return
		}
		sa.Detached = true
		entries[id] = sa
	}
}

// PruneSessionAttachments deletes entries older than ttl (since LastAttachedAt).
// Returns the count removed. Bounds growth of the store.
func (mf *MappingsFile) PruneSessionAttachments(now time.Time, ttl time.Duration) int {
	if mf == nil {
		return 0
	}
	n := 0
	for id, sa := range mf.SessionAttachments {
		if now.Sub(sa.LastAttachedAt) >= ttl {
			delete(mf.SessionAttachments, id)
			n++
		}
	}
	for cli, entries := range mf.SessionAttachmentsByCLI {
		for id, sa := range entries {
			if now.Sub(sa.LastAttachedAt) >= ttl {
				delete(entries, id)
				n++
			}
		}
		if len(entries) == 0 {
			delete(mf.SessionAttachmentsByCLI, cli)
		}
	}
	return n
}

// AllSessionAttachments returns legacy and namespaced entries for non-identity
// uses such as picker recency. Recovery must use Lookup/Claim instead.
func (mf *MappingsFile) AllSessionAttachments() []SessionAttachment {
	if mf == nil {
		return nil
	}
	out := make([]SessionAttachment, 0, len(mf.SessionAttachments))
	for _, sa := range mf.SessionAttachments {
		out = append(out, sa)
	}
	for _, entries := range mf.SessionAttachmentsByCLI {
		for _, sa := range entries {
			out = append(out, sa)
		}
	}
	return out
}
