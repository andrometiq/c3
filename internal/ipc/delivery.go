package ipc

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/Andrometiq/c3/internal/c3types"
)

// P1: "Malformed or unknown-version offers are ignored (legacy), never partially honoured."
// Raw hello storage keeps an invalid optional offer from invalidating the hello.
type DeliveryEligibility struct {
	Eligible bool   `json:"eligible"`
	Reason   string `json:"reason,omitempty"`
}
type DeliveryLive struct {
	Channel DeliveryEligibility `json:"channel"`
	Inbox   DeliveryEligibility `json:"inbox"`
}
type DeliveryOffer struct {
	Version  int          `json:"version"`
	Live     DeliveryLive `json:"live"`
	Receipts string       `json:"receipts"`
	Fetch    string       `json:"fetch"`
}
type DeliveryAcceptance struct {
	Version int      `json:"version"`
	Modes   []string `json:"modes"`
}
type DeliverMsg struct {
	Op         Op              `json:"op"`
	Token      string          `json:"token"`
	Transport  string          `json:"transport"`
	DeadlineMS int64           `json:"deadline_ms"`
	Inbound    c3types.Inbound `json:"inbound"`
}
type AttemptResultMsg struct {
	Op      Op     `json:"op"`
	Token   string `json:"token"`
	Outcome string `json:"outcome"`
	Reason  string `json:"reason"`
}
type DeliveryReportMsg struct {
	Op   Op           `json:"op"`
	Live DeliveryLive `json:"live"`
}

func ParseDeliveryOffer(raw []byte) *DeliveryOffer {
	var offer DeliveryOffer
	if StrictJSON(raw, &offer) != nil || offer.Version != 1 || offer.Receipts != "transcript" || (offer.Fetch != "receipt" && offer.Fetch != "consume") {
		return nil
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || !ValidDeliveryLive(fields["live"]) {
		return nil
	}
	return &offer
}
func ValidDeliveryLive(raw []byte) bool {
	var live map[string]map[string]json.RawMessage
	if json.Unmarshal(raw, &live) != nil || len(live) != 2 {
		return false
	}
	for _, key := range []string{"channel", "inbox"} {
		value := live[key]["eligible"]
		if string(value) != "true" && string(value) != "false" {
			return false
		}
	}
	var parsed DeliveryLive
	return StrictJSON(raw, &parsed) == nil
}
func StrictJSON(raw []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		if err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (d *DeliveryAcceptance) ChannelOnly() bool {
	return d != nil && d.Version == 1 && len(d.Modes) == 1 && d.Modes[0] == "channel"
}
