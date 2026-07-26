package broker

import (
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/mappings"
)

// isPrivateChat decides WHICH allowlist Gate consults — the DM-cleared user set
// or the allowlisted group set. Getting it wrong decides who is let in, so these
// cases are a trust boundary, not a formatting detail.
func TestIsPrivateChat_ChannelStatementBeatsTheIDSign(t *testing.T) {
	cases := []struct {
		name string
		in   *c3types.Inbound
		want bool
		why  string
	}{
		{
			name: "ConvKind dm overrides a negative id",
			in:   &c3types.Inbound{Channel: "telegram", ChatID: -1009123456789, ConvKind: c3types.ConvKindDM},
			want: true,
			why:  "the channel stated the kind; the id sign is a Telegram heuristic and must not override a fact",
		},
		{
			name: "ConvKind group overrides a positive id",
			in:   &c3types.Inbound{Channel: "telegram", ChatID: 42, ConvKind: c3types.ConvKindGroup},
			want: false,
			why:  "same, in the direction that matters: a group must not be gated against the DM-cleared user set",
		},
		{
			name: "telegram, no ConvKind, positive id — legacy DM",
			in:   &c3types.Inbound{Channel: "telegram", ChatID: 42},
			want: true,
			why:  "every record queued before ConvKind existed looks like this; the sign fallback must keep working",
		},
		{
			name: "telegram, no ConvKind, negative id — legacy group",
			in:   &c3types.Inbound{Channel: "telegram", ChatID: -1009123456789},
			want: false,
			why:  "as above, the other way",
		},
		{
			name: "unset channel, no ConvKind, positive id — legacy DM",
			in:   &c3types.Inbound{ChatID: 42},
			want: true,
			why:  "in-package fixtures and any pre-existing record without a Channel must not change behaviour",
		},
		{
			name: "UNKNOWN channel, no ConvKind, positive id — fails CLOSED",
			in:   &c3types.Inbound{Channel: "slack", ChatID: 42},
			want: false,
			why: "this is the whole point of the field. Telegram's positive==private convention describes " +
				"Telegram's id space and nobody else's. An unlabelled inbound from another channel must be " +
				"treated as a group so it needs an explicit group allowlist entry, rather than being matched " +
				"against a DM-cleared user id",
		},
		{
			name: "nil inbound",
			in:   nil,
			want: false,
			why:  "no inbound is not a private chat",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPrivateChat(tc.in); got != tc.want {
				t.Errorf("isPrivateChat() = %v, want %v\nwhy this matters: %s", got, tc.want, tc.why)
			}
		})
	}
}

// The leak this closes, end to end: a message that is really from a GROUP must
// never be cleared by the DM allowlist just because its identifier happens to be
// positive. Before ConvKind the broker had no way to know, because it read the
// id's sign; now the channel says so and Gate believes the channel.
func TestGate_GroupLabelledInboundIsNotClearedByTheDMAllowlist(t *testing.T) {
	b := pairingTestBroker(t)
	b.mutateMappings(func(mf *mappings.MappingsFile) { mf.AddAllowedUser(42) })

	// User 42 is DM-cleared. This message carries 42 as its conversation id, but
	// the channel states it arrived in a GROUP — a shape a non-Telegram channel
	// can produce trivially, since only Telegram signs group ids negative.
	in := &c3types.Inbound{
		Channel:  "telegram",
		ChatID:   42,
		ConvKind: c3types.ConvKindGroup,
		Sender:   c3types.Sender{UserID: 42},
		Text:     "hello",
	}
	if got := b.Gate(in); got == GateAllow {
		t.Fatal("a group-labelled inbound was cleared by the DM user allowlist; the group set is the only " +
			"allowlist that may clear it, and that group was never allowlisted")
	}

	// Control: allowlisting the group itself does clear it, so the deny above is
	// the trust boundary working, not the gate being broken.
	b.mutateMappings(func(mf *mappings.MappingsFile) { mf.AddAllowedGroup(42) })
	if got := b.Gate(in); got != GateAllow {
		t.Errorf("after allowlisting the group, Gate = %v, want GateAllow", got)
	}
}

// The complement: a DM-labelled inbound whose id is negative must still reach
// the DM allowlist. Without this, adopting ConvKind would silently deny real
// operator messages on any channel that does not sign its ids Telegram-style.
func TestGate_DMLabelledInboundReachesTheUserAllowlist(t *testing.T) {
	b := pairingTestBroker(t)
	b.mutateMappings(func(mf *mappings.MappingsFile) { mf.AddAllowedUser(42) })

	in := &c3types.Inbound{
		Channel:  "telegram",
		ChatID:   -1009123456789,
		ConvKind: c3types.ConvKindDM,
		Sender:   c3types.Sender{UserID: 42},
		Text:     "hello",
	}
	if got := b.Gate(in); got != GateAllow {
		t.Errorf("DM-labelled inbound from an allowlisted user: got %v, want GateAllow", got)
	}
}
