package intake

import "fmt"

type STTState string

const (
	STTPending       STTState = "pending"
	STTDone          STTState = "done"
	STTFailed        STTState = "failed"
	STTNotApplicable STTState = "not_applicable"
)

// STTOutcome carries raw audio results independently of presentation text.
type STTOutcome struct {
	STT        STTState
	Transcript string
	Error      string
}

func (o STTOutcome) Validate() error {
	switch o.STT {
	case STTDone:
		if o.Transcript != "" && o.Error == "" {
			return nil
		}
	case STTFailed:
		if o.Error != "" && o.Transcript == "" {
			return nil
		}
	}
	return fmt.Errorf("intake: invalid terminal STT outcome %q", o.STT)
}

type AttachmentState struct {
	Index      int      `json:"index"`
	STT        STTState `json:"stt"`
	Transcript string   `json:"transcript,omitempty"`
	Error      string   `json:"error,omitempty"`
}

type AttachmentsState []AttachmentState

func (s AttachmentsState) Clone() AttachmentsState {
	if s == nil {
		return nil
	}
	return append(AttachmentsState{}, s...)
}

// NewAttachmentsState never reconstructs a transcript from presentation text.
func NewAttachmentsState(source *Source, pending []string) AttachmentsState {
	if source == nil {
		return nil
	}
	states := make(AttachmentsState, len(source.Attachments))
	for i, att := range source.Attachments {
		state := AttachmentState{Index: i, STT: STTNotApplicable}
		if att.Kind == "voice" {
			state.STT, state.Error = STTFailed, "metadata_unavailable"
			if att.FileID == "" {
				state.Error = "missing_file_id"
			} else {
				for _, id := range pending {
					if id == att.FileID {
						state.STT, state.Error = STTPending, ""
						break
					}
				}
			}
		}
		states[i] = state
	}
	return states
}

func (s AttachmentsState) Validate(source *Source) error {
	if source != nil && len(s) != len(source.Attachments) {
		return fmt.Errorf("intake: attachment state count %d differs from source count %d", len(s), len(source.Attachments))
	}
	for i, state := range s {
		if state.Index != i {
			return fmt.Errorf("intake: attachment state index %d at position %d", state.Index, i)
		}
		if source != nil {
			voice := source.Attachments[i].Kind == "voice"
			if (voice && state.STT == STTNotApplicable) || (!voice && state.STT != STTNotApplicable) {
				return fmt.Errorf("intake: attachment kind and STT state disagree at index %d", i)
			}
		}
		switch state.STT {
		case STTPending, STTNotApplicable:
			if state.Transcript != "" || state.Error != "" {
				return fmt.Errorf("intake: nonterminal attachment state has terminal data at index %d", i)
			}
		default:
			if err := (STTOutcome{STT: state.STT, Transcript: state.Transcript, Error: state.Error}).Validate(); err != nil {
				return err
			}
		}
	}
	return nil
}

// WithOutcome fans a shared file's result out to all matching voice indices.
func (s AttachmentsState) WithOutcome(source *Source, fileID string, outcome STTOutcome) AttachmentsState {
	out := s.Clone()
	if source == nil {
		return out
	}
	for i, att := range source.Attachments {
		if att.Kind == "voice" && att.FileID == fileID {
			out[i] = AttachmentState{Index: i, STT: outcome.STT, Transcript: outcome.Transcript, Error: outcome.Error}
		}
	}
	return out
}

// TerminalOutcome requires every matching voice index to agree on a terminal result.
func (s AttachmentsState) TerminalOutcome(source *Source, fileID string) (STTOutcome, bool) {
	var final STTOutcome
	found := false
	if source == nil || fileID == "" || len(s) != len(source.Attachments) {
		return final, false
	}
	for i, att := range source.Attachments {
		if att.Kind != "voice" || att.FileID != fileID {
			continue
		}
		state := s[i]
		outcome := STTOutcome{STT: state.STT, Transcript: state.Transcript, Error: state.Error}
		if state.Index != i || outcome.Validate() != nil || (found && final != outcome) {
			return STTOutcome{}, false
		}
		final, found = outcome, true
	}
	return final, found
}
