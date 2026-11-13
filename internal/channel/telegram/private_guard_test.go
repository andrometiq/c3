package telegram

import (
	"context"
	"errors"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

// guardHost refuses broker-originated sends to one private chat.
type guardHost struct {
	fakeHost
	blocked int64
}

func (h *guardHost) AllowBrokerSend(chatID int64, topicID *int64) bool {
	return chatID != h.blocked || topicID != nil
}

func TestBrokerTextToPrivateChatIsRefused(t *testing.T) {
	c := makeChannel(&guardHost{blocked: 501})

	if _, err := c.SendReply(c3types.ReplyArgs{ChatID: 501, Text: "notice"}); !errors.Is(err, errPrivateChat) {
		t.Fatalf("broker reply err = %v, want errPrivateChat", err)
	}
	if _, err := c.SendReadback(c3types.ReadbackArgs{ChatID: 501, Transcript: "t"}); !errors.Is(err, errPrivateChat) {
		t.Fatalf("readback err = %v", err)
	}
	if _, err := c.SendApprovalCard(context.Background(), c3types.ReplyArgs{ChatID: 501, Text: "card"}); !errors.Is(err, errPrivateChat) {
		t.Fatalf("approval card err = %v", err)
	}
	if _, err := c.SendApprovalDocument(context.Background(), c3types.ReplyArgs{ChatID: 501}, "f", nil); !errors.Is(err, errPrivateChat) {
		t.Fatalf("approval document err = %v", err)
	}

	holder := c3types.ReplyArgs{ChatID: 501, Text: "answer"}
	holder.MarkHolderSend()
	if _, err := c.SendReply(holder); errors.Is(err, errPrivateChat) {
		t.Fatal("the holder's own reply was refused")
	}
	if _, err := c.SendReply(c3types.ReplyArgs{ChatID: 42, Text: "notice"}); errors.Is(err, errPrivateChat) {
		t.Fatal("broker text to another chat was refused")
	}
}
