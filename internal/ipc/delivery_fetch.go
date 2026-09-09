package ipc

import (
	"fmt"
	"strings"
)

// Members are in the same order as messages, including replacement notices.
// Revisions are opaque SHA-256 digests of the stored tracked row.
type FetchReceiptMember struct {
	RecordID string `json:"record_id"`
	Revision string `json:"revision"`
}

type FetchConfirmReq struct {
	Op         Op     `json:"op"`
	LeaseToken string `json:"lease_token"`
}

const FetchReceiptStart = "[C3_FETCH_RECEIPT_V1]"
const FetchReceiptEnd = "[/C3_FETCH_RECEIPT_V1]"

// FetchReceiptTrailer has no final newline. The adapter appends it verbatim
// after the rendered body; no prose may follow the closing delimiter.
func FetchReceiptTrailer(token string, members []FetchReceiptMember) string {
	var out strings.Builder
	fmt.Fprintf(&out, "%s\ngroup %s\n", FetchReceiptStart, token)
	for _, m := range members {
		fmt.Fprintf(&out, "member %s %s\n", m.RecordID, m.Revision)
	}
	out.WriteString(FetchReceiptEnd)
	return out.String()
}
