package telegram

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/queue"
	"github.com/PaulSonOfLars/gotgbot/v2"
)

func TestDispatchSourcePersistsRawOccurrenceAndEdit(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannel(h)
	msg := textMsg("", 42)
	msg.From = nil
	msg.Caption = "raw caption"
	msg.MessageThreadId = 7
	msg.Photo = []gotgbot.PhotoSize{{FileId: "small", Width: 10, Height: 10, FileSize: 1}, {FileId: "selected", Width: 20, Height: 20, FileSize: 123}}
	c.dispatchMessage(1001, msg, false, nil)
	msg.Caption = "edited caption"
	c.dispatchMessage(1002, msg, true, nil)
	if len(h.sources) != 2 {
		t.Fatalf("captured=%d", len(h.sources))
	}
	dir := t.TempDir()
	s, err := queue.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	rk := queue.RouteKey{Channel: "telegram", ChatID: 42, TopicID: &msg.MessageThreadId}
	for i, source := range h.sources {
		if err := s.Append(rk, h.emitted[i], source); err != nil {
			t.Fatal(err)
		}
	}
	s, err = queue.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	rows, err := s.PeekTracked(rk, -1)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	want := `{"channel":"telegram","chat_id":"42","topic_id":"7","message_id":"100","sender_id":null,"update_id":"1001","text":"raw caption","attachments":[{"kind":"photo","file_id":"selected","size":123,"mime":"","name":""}]}`
	data, err := json.Marshal(rows[0].Source)
	if err != nil || string(data) != want {
		t.Fatalf("source=%s err=%v", data, err)
	}
	if rows[1].Source.UpdateID != 1002 || rows[1].Source.MessageID != rows[0].Source.MessageID || rows[1].Source.Text != "edited caption" || !rows[1].Inbound.Edited {
		t.Fatalf("edit lost its occurrence: %+v", rows[1])
	}
}

func TestDispatchSourceNullTopicEmptyAttachmentsAndRawStickerText(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannel(h)
	msg := textMsg("hello", 42)
	c.dispatchMessage(1, msg, false, nil)
	msg.Text = ""
	msg.Sticker = &gotgbot.Sticker{FileId: "sticker", Emoji: "label"}
	c.dispatchMessage(2, msg, false, nil)
	dir := t.TempDir()
	store, err := queue.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	route := queue.RouteKey{Channel: "telegram", ChatID: 42}
	for i, source := range h.sources {
		if err := store.Append(route, h.emitted[i], source); err != nil {
			t.Fatal(err)
		}
	}
	store, err = queue.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	rows, err := store.PeekTracked(route, -1)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	encoded, err := json.Marshal(rows[0].Source)
	want := `{"channel":"telegram","chat_id":"42","topic_id":null,"message_id":"100","sender_id":"42","update_id":"1","text":"hello","attachments":[]}`
	if err != nil || string(encoded) != want {
		t.Fatalf("source=%s err=%v", encoded, err)
	}
	if rows[1].Source == nil || rows[1].Source.Text != "" || rows[1].Inbound.Text != "label" {
		t.Fatal("source used rendered sticker text")
	}
}

func TestDispatchSourceAdmittedMessagesOnly(t *testing.T) {
	for _, decision := range []channel.GateInboundDecision{channel.GateInboundDrop, channel.GateInboundPairConsumed} {
		h := &fakeHost{decision: decision}
		makeChannel(h).dispatchMessage(1, textMsg("hello", 42), false, nil)
		if len(h.sources) != 0 {
			t.Fatal("gate emitted source")
		}
	}
	h := &fakeHost{decision: channel.GateInboundAllow, cmdHandled: true}
	makeChannel(h).dispatchMessage(1, textMsg("/status", 42), false, nil)
	if len(h.sources) != 0 {
		t.Fatal("command emitted source")
	}
}

func TestDispatchSourceRichAttachmentOrderAndPhantomSuppression(t *testing.T) {
	h := &fakeHost{decision: channel.GateInboundAllow}
	c := makeChannel(h)
	c.editSupp = newEditSuppressor(8192, 48*time.Hour)
	msg := textMsg("", 42)
	raw := json.RawMessage(`{"blocks":[{"type":"paragraph","text":"rendered words"},{"type":"video","video":{"file_id":"video-first","file_size":5,"mime_type":"video/mp4"}},{"type":"photo","photo":[{"file_id":"small","width":1,"height":1},{"file_id":"photo-second","width":10,"height":10,"file_size":99}]}]}`)
	c.dispatchMessage(1001, msg, false, raw)
	c.dispatchMessage(1002, msg, true, raw)
	if len(h.sources) != 1 || h.sources[0].Text != "" || !strings.Contains(h.emitted[0].Text, "rendered words") {
		t.Fatal("rich source used rendered text or phantom edit was admitted")
	}
	dir := t.TempDir()
	store, err := queue.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	route := queue.RouteKey{Channel: "telegram", ChatID: 42}
	if err := store.Append(route, h.emitted[0], h.sources[0]); err != nil {
		t.Fatal(err)
	}
	store, err = queue.NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecoverOnStartup(); err != nil {
		t.Fatal(err)
	}
	rows, err := store.PeekTracked(route, -1)
	if err != nil || len(rows) != 1 || rows[0].Source == nil || rows[0].Source.Text != "" {
		t.Fatalf("rich source was not recovered: %+v err=%v", rows, err)
	}
	atts := rows[0].Source.Attachments
	if len(atts) != 2 || atts[0].FileID != "video-first" || atts[1].FileID != "photo-second" {
		t.Fatalf("received attachment order changed: %+v", atts)
	}
}
