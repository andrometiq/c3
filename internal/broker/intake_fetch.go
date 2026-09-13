package broker

import (
	"encoding/json"
	"fmt"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/intake"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/queue"
)

type intakeFetchMessage struct {
	c3types.Inbound
	Intake *ipc.IntakeMetadata `json:"intake,omitempty"`
}

func intakeMessage(row queue.TrackedInbound) intakeFetchMessage {
	states := row.AttachmentsState.Clone()
	if states == nil {
		states = intake.AttachmentsState{}
	}
	return intakeFetchMessage{Inbound: row.Inbound, Intake: &ipc.IntakeMetadata{
		Source: row.Source.Clone(), AttachmentsState: states,
	}}
}

type fetchQueueResponse struct {
	ipc.FetchQueueResp
	intakeMessages []intakeFetchMessage
}

// Frame sizing and the socket writer use this same projection.
func (r fetchQueueResponse) MarshalJSON() ([]byte, error) {
	if r.intakeMessages == nil {
		return json.Marshal(r.FetchQueueResp)
	}
	if len(r.Messages) != len(r.intakeMessages) {
		return nil, fmt.Errorf("fetch_queue: intake message count differs from inbound count")
	}
	return json.Marshal(struct {
		Op             ipc.Op                   `json:"op"`
		ID             string                   `json:"id"`
		Messages       []intakeFetchMessage     `json:"messages,omitempty"`
		Remaining      int                      `json:"remaining"`
		Err            string                   `json:"err,omitempty"`
		LeaseToken     string                   `json:"lease_token,omitempty"`
		Members        []ipc.FetchReceiptMember `json:"members,omitempty"`
		ReceiptTrailer string                   `json:"receipt_trailer,omitempty"`
	}{r.Op, r.ID, r.intakeMessages, r.Remaining, r.Err, r.LeaseToken, r.Members, r.ReceiptTrailer})
}

func (w *RouteWorker) handleIntakePeek(job *FetchJob) {
	result := FetchResult{}
	defer func() { job.ResultCh <- result }()
	rows, err := w.visibleAttemptRows(-1)
	if err != nil {
		result.Err = err
		return
	}
	result.Remaining = len(rows)
	limit := len(rows)
	if !job.All && job.Limit >= 0 {
		limit = min(limit, job.Limit)
	}
	for _, row := range rows[:limit] {
		message := intakeMessage(row)
		messages := append(result.Messages, message.Inbound)
		metadata := append(result.intakeMessages, message)
		frame := fetchQueueResponse{attemptFetchResponse(job.RespID, "", messages, nil, len(rows)), metadata}
		encoded, err := json.Marshal(frame)
		if err != nil {
			result.Err = err
			return
		}
		if len(encoded)+job.FrameReserve+1 > ipc.MaxFrameSize {
			if len(result.Messages) == 0 && job.FrameReserve == 0 {
				result.Err = fmt.Errorf("fetch_queue: intake head exceeds frame size; row remains queued")
			}
			break
		}
		result.Messages, result.intakeMessages = messages, metadata
		result.Remaining--
	}
}
