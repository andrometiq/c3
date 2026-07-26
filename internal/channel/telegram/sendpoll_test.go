package telegram

import (
	"strings"
	"testing"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
)

// TestSendPoll_HonorsButtons is the M1 regression for the poll send path: the
// gate rides a kept inline keyboard on the FIRST emitted part, which for a
// buttons+poll reply is the poll part. sendPoll previously ignored args.Buttons,
// silently dropping the keyboard. It must now build the markup — proven here
// because an INVALID keyboard (an empty row) returns the build error BEFORE any
// rate-wait or network call. A non-nil bot lets sendPoll pass its channel-started
// guard; the button build runs before c.rate.Wait, so no network is touched.
func TestSendPoll_HonorsButtons(t *testing.T) {
	c := &Channel{bot: &gotgbot.Bot{Token: "test", BotClient: &gotgbot.BaseBotClient{}}}
	args := c3types.ReplyArgs{
		ChatID: -100,
		Poll: &c3types.PollSpec{
			Question: "Lunch?",
			Options:  []string{"Pizza", "Tacos"},
		},
		Buttons: [][]c3types.Button{{}}, // empty row → build error if honored
	}
	_, err := c.sendPoll(args)
	if err == nil || !strings.Contains(err.Error(), "row 1 is empty") {
		t.Fatalf("sendPoll must honor (build) args.Buttons; got %v", err)
	}
}
