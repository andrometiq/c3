package queue

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/intake"
)

// recordTooLarge measures the entire private envelope, including JSON escaping.
func recordTooLarge(rec storedInbound) (bool, error) {
	data, err := json.Marshal(rec)
	return len(data) > MaxRecordBytes, err
}

func completeVoiceRecord(rec *storedInbound, fileID, text, oversizeText string, outcome intake.STTOutcome) (intake.STTOutcome, error) {
	source := attachmentSource(rec.Source, &rec.Inbound)
	states := storedAttachmentsState(*rec)
	rec.Text = text
	rec.AttachmentsState = states.WithOutcome(source, fileID, outcome)
	if len(rec.AttachmentsState) == 0 {
		return outcome, nil // preserve metadata-free legacy completion behavior
	}
	large, err := recordTooLarge(*rec)
	if err != nil {
		return intake.STTOutcome{}, err
	}
	if large && outcome.STT == intake.STTDone {
		outcome = intake.STTOutcome{STT: intake.STTFailed, Error: "transcript_too_large"}
		rec.Text = oversizeText
		rec.AttachmentsState = states.WithOutcome(source, fileID, outcome)
		large, err = recordTooLarge(*rec)
	}
	if err != nil {
		return intake.STTOutcome{}, err
	}
	if large {
		return intake.STTOutcome{}, fmt.Errorf("queue: voice record still exceeds %d bytes after terminal fallback", MaxRecordBytes)
	}
	return outcome, nil
}

// SyncRoute re-establishes durability of visible rows after an uncertain write.
// The route owner must retain exclusive access through verification and fsync.
func (s *Store) SyncRoute(rk RouteKey) error {
	f, err := os.OpenFile(s.jsonlPath(rk), os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("queue: open for voice retry fsync: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("queue: voice retry fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := s.syncDir(); err != nil {
		return err
	}
	s.refreshIndex(rk)
	return nil
}

// AppendVoiceOutcome is a revision append with a stable per-target/file identity.
// A full sibling vector is not evidence that a sibling's revision was appended.
func (s *Store) AppendVoiceOutcome(rk RouteKey, originalID, fileID string, original c3types.Inbound, revision *c3types.Inbound, oversizeText string, source *intake.Source, states intake.AttachmentsState, outcome intake.STTOutcome) (id string, final intake.STTOutcome, err error) {
	if err := outcome.Validate(); err != nil {
		return "", final, err
	}
	if revision == nil || fileID == "" {
		return "", final, fmt.Errorf("queue: voice revision requires inbound and file ID")
	}
	identity, err := json.Marshal(struct {
		OriginalID string
		FileID     string
		Source     *intake.Source
		Inbound    c3types.Inbound
	}{originalID, fileID, source, original})
	if err != nil {
		return "", final, err
	}
	id = fmt.Sprintf("voice-%x", sha256.Sum256(identity))
	rows, err := s.PeekTracked(rk, -1)
	if err != nil {
		return "", final, err
	}
	for _, row := range rows {
		if row.RecordID != id || row.Inbound.DrainedFrom != "" {
			continue
		}
		stateSource := attachmentSource(row.Source, &row.Inbound)
		if err := row.AttachmentsState.Validate(stateSource); err != nil {
			return "", final, err
		}
		var terminal bool
		final, terminal = row.AttachmentsState.TerminalOutcome(stateSource, fileID)
		if !terminal {
			return "", final, fmt.Errorf("queue: voice revision identity has no terminal outcome")
		}
		if err := s.SyncRoute(rk); err != nil {
			return "", intake.STTOutcome{}, err
		}
		return id, final, nil
	}
	id, err = s.appendTrackedPrepared(rk, revision, "", nil, source, states, func(rec *storedInbound) error {
		rec.RecordID = id
		var err error
		final, err = completeVoiceRecord(rec, fileID, rec.Text, oversizeText, outcome)
		return err
	})
	if err != nil {
		return "", intake.STTOutcome{}, err
	}
	return id, final, nil
}
