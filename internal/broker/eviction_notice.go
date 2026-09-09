package broker

import (
	"fmt"
	"github.com/Andrometiq/c3/internal/queue"
	"strings"
	"time"
)

func evictionNotice(aged, overCount, remaining int, countErr error, retention bool) string {
	var parts []string
	if aged > 0 {
		recovery := fmt.Sprintf("recoverable from trash for %d days", int(queue.TrashTTL/(24*time.Hour)))
		if !retention {
			recovery = "trash retention unavailable"
		}
		parts = append(parts, fmt.Sprintf("%d message(s) expired after %d days (%s)", aged, int(queue.MaxAge/(24*time.Hour)), recovery))
	}
	if overCount > 0 {
		parts = append(parts, fmt.Sprintf("queue full — dropped %d oldest", overCount))
	}
	held := fmt.Sprintf("%d message(s) remain held.", remaining)
	if countErr != nil {
		held = "Remaining held count unavailable; check the broker log."
	}
	return "⚠️ " + strings.Join(parts, "; ") + "; " + held
}
