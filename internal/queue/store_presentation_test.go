package queue

import (
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

func TestAppendStripsMergedPresentationFromPersistedCopy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		append func(*Store, RouteKey, *c3types.Inbound) error
	}{
		{
			name: "Append",
			append: func(store *Store, key RouteKey, in *c3types.Inbound) error {
				return store.Append(key, in)
			},
		},
		{
			name: "AppendTracked",
			append: func(store *Store, key RouteKey, in *c3types.Inbound) error {
				_, err := store.AppendTracked(key, in)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newStore(t)
			key := RouteKey{Channel: "telegram", ChatID: -100}
			poisoned := &c3types.Inbound{
				Channel: "telegram", ChatID: -100, MessageID: 12, Text: "first\nsecond",
				Merged: []c3types.MergedSource{
					{MessageID: 11, Text: "first"},
					{MessageID: 12, Text: "second"},
				},
				Attachments: []c3types.Attachment{
					{Kind: "photo", FileID: "P11", SourceMessageID: 11},
					{Kind: "document", FileID: "D12", SourceMessageID: 12},
				},
			}

			if err := tc.append(store, key, poisoned); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			rows, err := store.Peek(key, -1)
			if err != nil {
				t.Fatalf("Peek: %v", err)
			}
			if len(rows) != 1 {
				t.Fatalf("persisted row count = %d, want 1", len(rows))
			}
			if len(rows[0].Merged) != 0 {
				t.Errorf("persisted row leaked Merged: %+v", rows[0].Merged)
			}
			for i, attachment := range rows[0].Attachments {
				if attachment.SourceMessageID != 0 {
					t.Errorf("persisted attachment %d leaked SourceMessageID: %+v", i, attachment)
				}
			}
			if len(poisoned.Merged) != 2 || poisoned.Attachments[0].SourceMessageID != 11 || poisoned.Attachments[1].SourceMessageID != 12 {
				t.Fatalf("append normalization mutated caller presentation: %+v", poisoned)
			}
		})
	}
}
