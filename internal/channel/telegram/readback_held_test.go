package telegram

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/PaulSonOfLars/gotgbot/v2"
)

func TestReadbackHeldCountPreservesBandsAndSourceQuote(t *testing.T) {
	transcripts := []string{strings.Repeat("&", 3000)}
	for _, size := range []int{30, 2000, 5000, 12000, 33000} {
		transcript, _ := rbSentenceDoc(size, 40)
		transcripts = append(transcripts, transcript)
	}
	for _, transcript := range transcripts {
		method, original, band := renderReadback(transcript)
		client := &captureBotClient{}
		c := &Channel{bot: &gotgbot.Bot{Token: "test", BotClient: client}, ctx: context.Background(), rate: newRateLimiter(), endpoints: []string{""}}
		source := int64(51)
		if _, err := c.SendReadbackWithHeld(c3types.ReadbackArgs{ChatID: 42, ReplyTo: &source, Transcript: transcript}, 3); err != nil {
			t.Fatal(err)
		}
		if client.method != method {
			t.Fatalf("band %s changed method: %s", band, client.method)
		}
		encoded, err := json.Marshal(client.params["reply_parameters"])
		if err != nil {
			t.Fatal(err)
		}
		var quote struct {
			MessageID int64 `json:"message_id"`
		}
		if err := json.Unmarshal(encoded, &quote); err != nil || quote.MessageID != source {
			t.Fatalf("source quote=%s err=%v", encoded, err)
		}
		field := "text"
		if method == "sendDocument" {
			field = "caption"
			original = readbackCaption(transcript)
		}
		payload, _ := client.params[field].(string)
		if method == "sendRichMessage" {
			payload, _ = client.params["rich_message"].(map[string]any)["html"].(string)
		}
		if !strings.Contains(payload, "📨 3 held in queue.") || !strings.HasPrefix(payload, original) {
			t.Fatalf("band %s lost original rendering or held suffix: %s", band, payload)
		}
		if strings.Contains(payload, "Live route:") {
			t.Fatal(payload)
		}
	}
}

func TestReadbackHeldCaptionBudget(t *testing.T) {
	transcript := strings.Repeat("<&😀", 2000)
	suffix := "\n📨 1000 held in queue."
	caption := readbackCaptionWithSuffix(transcript, suffix)
	if uint16Len(caption) > readbackCaptionMaxU16 || !strings.HasSuffix(caption, suffix) {
		t.Fatalf("caption length=%d suffix=%v", uint16Len(caption), strings.HasSuffix(caption, suffix))
	}
}
