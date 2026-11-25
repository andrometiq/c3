package intake

import (
	"encoding/json"
	"testing"
)

// from_page is on the wire only for a board act the page's script sent, so a
// Telegram source keeps its exact bytes.
func TestSourceNamesFromPageOnlyWhenSet(t *testing.T) {
	sender := int64(8)
	source := Source{Channel: "board", ChatID: 3, MessageID: 5, SenderID: &sender, UpdateID: 5, Text: "line", Attachments: []SourceAttachment{}}
	plain, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"channel":"board","chat_id":"3","topic_id":null,"message_id":"5","sender_id":"8","update_id":"5","text":"line","attachments":[]}`
	if string(plain) != want {
		t.Fatalf("wire=%s want=%s", plain, want)
	}
	source.FromPage = true
	marked, _ := json.Marshal(source)
	if string(marked) != want[:len(want)-1]+`,"from_page":true}` {
		t.Fatalf("page act wire=%s", marked)
	}
	var back Source
	if err := json.Unmarshal(marked, &back); err != nil || !back.FromPage || back.Clone().FromPage != true {
		t.Fatalf("decoded %+v err %v", back, err)
	}
}
