package telegram

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
)

func TestConvertInbound_TextMessage(t *testing.T) {
	msg := &gotgbot.Message{
		MessageId: 868,
		From:      &gotgbot.User{Id: 42, Username: "alice"},
		Chat:      gotgbot.Chat{Id: -1001234567890},
		Date:      1715151931,
		Text:      "hello",
	}
	in := convertInbound("telegram", msg, "", nil)
	if in == nil {
		t.Fatal("expected Inbound, got nil")
	}
	if in.Text != "hello" {
		t.Errorf("Text=%q", in.Text)
	}
	if in.ChatID != -1001234567890 {
		t.Errorf("ChatID=%d", in.ChatID)
	}
	if in.Sender.UserID != 42 || in.Sender.Username != "alice" {
		t.Errorf("Sender=%+v", in.Sender)
	}
	if in.TopicID != nil {
		t.Errorf("TopicID should be nil for non-topic, got %v", in.TopicID)
	}
}

func TestConvertInbound_TopicMessage(t *testing.T) {
	msg := &gotgbot.Message{
		MessageId:       100,
		MessageThreadId: 281,
		From:            &gotgbot.User{Id: 42},
		Chat:            gotgbot.Chat{Id: -100},
		Text:            "in topic",
	}
	in := convertInbound("telegram", msg, "", nil)
	if in.TopicID == nil || *in.TopicID != 281 {
		t.Errorf("TopicID = %v, want &281", in.TopicID)
	}
}

func TestConvertInbound_VoiceMessage(t *testing.T) {
	msg := &gotgbot.Message{
		MessageId: 1,
		From:      &gotgbot.User{Id: 42},
		Chat:      gotgbot.Chat{Id: -100},
		Voice: &gotgbot.Voice{
			FileId:       "AwACAgUAAyEFAAT...",
			FileUniqueId: "u1",
			Duration:     5,
			MimeType:     "audio/ogg",
			FileSize:     2997348,
		},
	}
	in := convertInbound("telegram", msg, "[Transcribed voice]: ", nil)
	if in == nil {
		t.Fatal("nil")
	}
	if len(in.Attachments) != 1 || in.Attachments[0].Kind != "voice" {
		t.Errorf("Attachments = %+v", in.Attachments)
	}
	if in.Attachments[0].FileID != "AwACAgUAAyEFAAT..." {
		t.Errorf("FileID = %q", in.Attachments[0].FileID)
	}
}

func TestConvertInbound_ReplyContext(t *testing.T) {
	parent := &gotgbot.Message{
		MessageId: 281,
		From:      &gotgbot.User{Id: 999, Username: "examplebot"},
		Chat:      gotgbot.Chat{Id: -100},
		Text:      "previous message",
	}
	msg := &gotgbot.Message{
		MessageId:      900,
		From:           &gotgbot.User{Id: 42, Username: "alice"},
		Chat:           gotgbot.Chat{Id: -100},
		ReplyToMessage: parent,
		Text:           "reply",
	}
	in := convertInbound("telegram", msg, "", nil)
	if in.ReplyTo == nil {
		t.Fatal("ReplyTo nil")
	}
	if in.ReplyTo.MessageID != 281 {
		t.Errorf("ReplyTo.MessageID=%d", in.ReplyTo.MessageID)
	}
	if in.ReplyTo.User.Username != "examplebot" {
		t.Errorf("ReplyTo.User=%+v", in.ReplyTo.User)
	}
	if in.ReplyTo.Text != "previous message" {
		t.Errorf("ReplyTo.Text=%q", in.ReplyTo.Text)
	}
}

func TestConvertInbound_PhotoPicksHighestResolution(t *testing.T) {
	msg := &gotgbot.Message{
		MessageId: 1,
		From:      &gotgbot.User{Id: 42},
		Chat:      gotgbot.Chat{Id: -100},
		Photo: []gotgbot.PhotoSize{
			{FileId: "small", Width: 100, Height: 100},
			{FileId: "big", Width: 1024, Height: 1024},
			{FileId: "med", Width: 500, Height: 500},
		},
		Caption: "look",
	}
	in := convertInbound("telegram", msg, "", nil)
	if len(in.Attachments) != 1 || in.Attachments[0].Kind != "photo" {
		t.Fatalf("Attachments=%+v", in.Attachments)
	}
	if in.Attachments[0].FileID != "big" {
		t.Errorf("expected highest-res FileID=big, got %q", in.Attachments[0].FileID)
	}
	if in.Text != "look" {
		t.Errorf("Caption should populate Text, got %q", in.Text)
	}
}

func TestConvertInbound_ServiceMessageDropped(t *testing.T) {
	msg := &gotgbot.Message{
		MessageId:         5,
		Chat:              gotgbot.Chat{Id: -100},
		ForumTopicCreated: &gotgbot.ForumTopicCreated{Name: "new topic", IconColor: 0},
	}
	if in := convertInbound("telegram", msg, "", nil); in != nil {
		t.Errorf("expected nil for forum_topic_created service message, got %+v", in)
	}
}

func TestConvertInbound_NilMessageReturnsNil(t *testing.T) {
	if in := convertInbound("telegram", nil, "", nil); in != nil {
		t.Errorf("expected nil, got %+v", in)
	}
}

func TestConvertInbound_RichMessage(t *testing.T) {
	msg := &gotgbot.Message{
		MessageId: 5,
		From:      &gotgbot.User{Id: 42, Username: "alice"},
		Chat:      gotgbot.Chat{Id: -100},
		Date:      1715151931,
		// No Text — the body is in rich_message (captured separately by poll.go).
	}
	rich := json.RawMessage(`{"blocks":[{"type":"heading","size":1,"text":"Hi"},{"type":"paragraph","text":"there"}]}`)
	in := convertInbound("telegram", msg, "", rich)
	if in == nil {
		t.Fatal("nil")
	}
	if in.Text != "# Hi\n\nthere" {
		t.Errorf("Text=%q", in.Text)
	}
}

func TestConvertInbound_RichMessageWithMedia(t *testing.T) {
	msg := &gotgbot.Message{MessageId: 6, From: &gotgbot.User{Id: 1}, Chat: gotgbot.Chat{Id: -100}}
	rich := json.RawMessage(`{"blocks":[{"type":"photo","photo":[{"file_id":"pid","file_size":3,"width":9,"height":9}],"caption":{"text":"pic"}}]}`)
	in := convertInbound("telegram", msg, "", rich)
	if in.Text != "[photo: pic]" {
		t.Errorf("Text=%q", in.Text)
	}
	if len(in.Attachments) != 1 || in.Attachments[0].FileID != "pid" {
		t.Fatalf("Attachments=%+v", in.Attachments)
	}
}

func TestConvertInbound_NoRichRawUsesPlainText(t *testing.T) {
	msg := &gotgbot.Message{MessageId: 7, From: &gotgbot.User{Id: 1}, Chat: gotgbot.Chat{Id: -100}, Text: "plain"}
	in := convertInbound("telegram", msg, "", nil) // toggle-off / non-rich path
	if in.Text != "plain" {
		t.Errorf("Text=%q", in.Text)
	}
}

func TestConvertInbound_RichDecodeFailFallsBackToMarker(t *testing.T) {
	msg := &gotgbot.Message{MessageId: 8, From: &gotgbot.User{Id: 1}, Chat: gotgbot.Chat{Id: -100}}
	in := convertInbound("telegram", msg, "", json.RawMessage(`{bad`))
	if in.Text != "[rich message]" {
		t.Errorf("Text=%q", in.Text)
	}
}

func TestConvertInbound_CapturesMediaGroupAndForwardOrigins(t *testing.T) {
	tests := []struct {
		name   string
		origin gotgbot.MessageOrigin
		want   *c3types.ForwardOrigin
	}{
		{
			name: "user full name",
			origin: gotgbot.MessageOriginUser{SenderUser: gotgbot.User{
				FirstName: "Alice", LastName: "Example", Username: "alice",
			}},
			want: &c3types.ForwardOrigin{Kind: "user", Name: "Alice Example"},
		},
		{
			name:   "user username fallback",
			origin: gotgbot.MessageOriginUser{SenderUser: gotgbot.User{Username: "fallback"}},
			want:   &c3types.ForwardOrigin{Kind: "user", Name: "fallback"},
		},
		{
			name:   "hidden user",
			origin: gotgbot.MessageOriginHiddenUser{SenderUserName: "Hidden Sender"},
			want:   &c3types.ForwardOrigin{Kind: "hidden_user", Name: "Hidden Sender"},
		},
		{
			name:   "chat",
			origin: gotgbot.MessageOriginChat{SenderChat: gotgbot.Chat{Title: "Source Group"}},
			want:   &c3types.ForwardOrigin{Kind: "chat", Name: "Source Group"},
		},
		{
			name:   "channel",
			origin: gotgbot.MessageOriginChannel{Chat: gotgbot.Chat{Title: "Source Channel"}},
			want:   &c3types.ForwardOrigin{Kind: "channel", Name: "Source Channel"},
		},
		{name: "absent"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msg := &gotgbot.Message{
				MessageId: 12, From: &gotgbot.User{Id: 42}, Chat: gotgbot.Chat{Id: -100},
				Text: "forwarded", MediaGroupId: "album-7", ForwardOrigin: test.origin,
			}
			in := convertInbound("telegram", msg, "", nil)
			if in.MediaGroupID != "album-7" {
				t.Fatalf("MediaGroupID = %q, want album-7", in.MediaGroupID)
			}
			if !reflect.DeepEqual(in.ForwardOrigin, test.want) {
				t.Fatalf("ForwardOrigin = %+v, want %+v", in.ForwardOrigin, test.want)
			}
		})
	}
}
