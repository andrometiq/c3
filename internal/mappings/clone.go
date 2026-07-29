package mappings

// Clone returns a deep copy of mf. Used by the broker's copy-on-write
// mutation path so concurrent readers always see an immutable snapshot.
// All nested maps and slices are duplicated; nil sub-values stay nil.
//
// Performance note: mappings.json is bounded by user config size
// (channels, groups, topics, cwd-mappings — typically tens to hundreds
// of entries total). Deep-copying on every mutation is cheap relative
// to the work mutations already do (atomic file rewrite, etc.).
func (mf *MappingsFile) Clone() *MappingsFile {
	if mf == nil {
		return nil
	}
	out := &MappingsFile{
		SchemaVersion: mf.SchemaVersion,
		AutoUpdate:    mf.AutoUpdate,
	}
	if mf.AutoAttachOnResume != nil { // deep-copy the pointer per the Clone contract (cf. Invasive below)
		v := *mf.AutoAttachOnResume
		out.AutoAttachOnResume = &v
	}
	if mf.Channels != nil {
		out.Channels = make(map[string]ChannelConfig, len(mf.Channels))
		for k, v := range mf.Channels {
			out.Channels[k] = cloneChannelConfig(v)
		}
	}
	if mf.Codex != nil {
		c := *mf.Codex
		out.Codex = &c
	}
	if mf.Mappings != nil {
		out.Mappings = make(map[string]Mapping, len(mf.Mappings))
		for k, v := range mf.Mappings {
			out.Mappings[k] = v
		}
	}
	if mf.Plugins != nil {
		out.Plugins = make(map[string]map[string]any, len(mf.Plugins))
		for k, v := range mf.Plugins {
			if v == nil {
				out.Plugins[k] = nil
				continue
			}
			inner := make(map[string]any, len(v))
			for k2, v2 := range v {
				inner[k2] = v2
			}
			out.Plugins[k] = inner
		}
	}
	if mf.Allowlist != nil {
		al := Allowlist{}
		if mf.Allowlist.Users != nil {
			al.Users = append([]int64(nil), mf.Allowlist.Users...)
		}
		if mf.Allowlist.Groups != nil {
			al.Groups = append([]int64(nil), mf.Allowlist.Groups...)
		}
		out.Allowlist = &al
	}
	if mf.Notifications != nil {
		nc := NotificationsConfig{}
		if mf.Notifications.Invasive != nil {
			v := *mf.Notifications.Invasive
			nc.Invasive = &v
		}
		out.Notifications = &nc
	}
	if mf.SessionAttachments != nil {
		out.SessionAttachments = make(map[string]SessionAttachment, len(mf.SessionAttachments))
		for k, v := range mf.SessionAttachments {
			out.SessionAttachments[k] = cloneSessionAttachment(v)
		}
	}
	if mf.SessionAttachmentsByCLI != nil {
		out.SessionAttachmentsByCLI = make(map[string]map[string]SessionAttachment, len(mf.SessionAttachmentsByCLI))
		for cli, entries := range mf.SessionAttachmentsByCLI {
			cloned := make(map[string]SessionAttachment, len(entries))
			for id, sa := range entries {
				cloned[id] = cloneSessionAttachment(sa)
			}
			out.SessionAttachmentsByCLI[cli] = cloned
		}
	}
	return out
}

func cloneSessionAttachment(sa SessionAttachment) SessionAttachment {
	if sa.TopicID != nil {
		t := *sa.TopicID
		sa.TopicID = &t
	}
	return sa
}

func cloneChannelConfig(cc ChannelConfig) ChannelConfig {
	out := cc
	if cc.Groups != nil {
		out.Groups = make(map[string]GroupConfig, len(cc.Groups))
		for k, v := range cc.Groups {
			out.Groups[k] = v
		}
	}
	if cc.Topics != nil {
		out.Topics = append([]Topic(nil), cc.Topics...)
	}
	if cc.RichInbound != nil {
		v := *cc.RichInbound
		out.RichInbound = &v
	}
	return out
}
