package mappings

// AllowlistOrEmpty returns the current allowlist, or an empty one if unset.
// Safe to call on a nil MappingsFile (returns an empty allowlist).
//
// Migration shim: brokers built before the allowlist schema landed wrote
// mappings.json without an `allowlist` key. On first read of such a file,
// `mf.Allowlist` is nil; this helper lets callers treat that case
// identically to "allowlist present but empty" without sprinkling nil
// checks across the channel filter, the pairing state machine, etc.
func (mf *MappingsFile) AllowlistOrEmpty() Allowlist {
	if mf == nil || mf.Allowlist == nil {
		return Allowlist{}
	}
	return *mf.Allowlist
}

// IsUserAllowed reports whether userID is in the DM-cleared user set.
func (mf *MappingsFile) IsUserAllowed(userID int64) bool {
	if mf == nil || mf.Allowlist == nil {
		return false
	}
	for _, u := range mf.Allowlist.Users {
		if u == userID {
			return true
		}
	}
	return false
}

// IsGuest reports whether userID is in the guest set. Guests are not operators.
func (mf *MappingsFile) IsGuest(userID int64) bool {
	if mf == nil || mf.Allowlist == nil || userID <= 0 {
		return false
	}
	for _, g := range mf.Allowlist.Guests {
		if g == userID {
			return true
		}
	}
	return false
}

// IsGroupAllowed reports whether chatID is in the group-cleared set.
func (mf *MappingsFile) IsGroupAllowed(chatID int64) bool {
	if mf == nil || mf.Allowlist == nil {
		return false
	}
	for _, g := range mf.Allowlist.Groups {
		if g == chatID {
			return true
		}
	}
	return false
}

// AddAllowedUser appends userID to the allowlist if not already present.
// Idempotent. Allocates mf.Allowlist if nil.
func (mf *MappingsFile) AddAllowedUser(userID int64) {
	if mf.Allowlist == nil {
		mf.Allowlist = &Allowlist{}
	}
	for _, u := range mf.Allowlist.Users {
		if u == userID {
			return
		}
	}
	mf.Allowlist.Users = append(mf.Allowlist.Users, userID)
}

// AddAllowedGroup appends chatID to the allowlist if not already present.
// Idempotent. Allocates mf.Allowlist if nil.
func (mf *MappingsFile) AddAllowedGroup(chatID int64) {
	if mf.Allowlist == nil {
		mf.Allowlist = &Allowlist{}
	}
	for _, g := range mf.Allowlist.Groups {
		if g == chatID {
			return
		}
	}
	mf.Allowlist.Groups = append(mf.Allowlist.Groups, chatID)
}
