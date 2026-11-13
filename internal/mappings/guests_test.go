package mappings

import "testing"

func guestFile(users, guests []int64, dmChat int64) *MappingsFile {
	return &MappingsFile{
		SchemaVersion: 1,
		Channels:      map[string]ChannelConfig{"telegram": {DMChatID: dmChat, PrivateHolder: &PrivateHolder{CLI: "genie"}}},
		Allowlist:     &Allowlist{Users: users, Guests: guests},
	}
}

func TestValidateGuests(t *testing.T) {
	if err := guestFile([]int64{7}, []int64{501}, 7).Validate(); err != nil {
		t.Fatalf("valid guests refused: %v", err)
	}
	for name, mf := range map[string]*MappingsFile{
		"guest is also a user":   guestFile([]int64{7, 501}, []int64{501}, 7),
		"guest is the DM chat":   guestFile([]int64{7}, []int64{501}, 501),
		"guest is not a user id": guestFile([]int64{7}, []int64{-100}, 7),
	} {
		if err := mf.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestIsGuestAndClone(t *testing.T) {
	mf := guestFile([]int64{7}, []int64{501}, 7)
	if !mf.IsGuest(501) || mf.IsGuest(7) || mf.IsGuest(0) || (*MappingsFile)(nil).IsGuest(501) {
		t.Fatal("IsGuest mismatch")
	}
	clone := mf.Clone()
	clone.Allowlist.Guests[0] = 999
	clone.Channels["telegram"].PrivateHolder.CLI = "other"
	if mf.Allowlist.Guests[0] != 501 || mf.Channels["telegram"].PrivateHolder.CLI != "genie" {
		t.Fatal("clone shares guest state")
	}
}
