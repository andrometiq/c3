package mcptools

import (
	"context"
	"errors"
	"time"
)

// RouteResultTimeout bounds additive state-change operations that an older
// protocol-v1 broker may not understand. Those brokers answer unknown ops with
// OpError today, but the timeout also covers older builds that stay silent.
const RouteResultTimeout = 30 * time.Second

// ErrRouteResultTimeout is surfaced verbatim as the MCP tool error when a
// broker never answers set_output_route or targeted release.
var ErrRouteResultTimeout = errors.New("broker did not answer; it may predate set_output_route/release_result — update the broker")

// WaitRouteResult waits for one state-change response, a caller cancellation,
// or the wall-clock compatibility timeout. abandon atomically removes the
// caller's pending entry and reports whether it won the race with dispatch. If
// dispatch already claimed the entry, its buffered response wins even when the
// cancellation/timeout signal became ready at the same instant.
func WaitRouteResult[T any](ctx context.Context, timeout time.Duration, result <-chan T, abandon func() bool) (T, error) {
	if timeout <= 0 {
		timeout = RouteResultTimeout
	}
	select {
	case value := <-result:
		return value, nil
	case <-ctx.Done():
		if !abandon() {
			return <-result, nil
		}
		var zero T
		return zero, ctx.Err()
	case <-time.After(timeout):
		if !abandon() {
			return <-result, nil
		}
		var zero T
		return zero, ErrRouteResultTimeout
	}
}
