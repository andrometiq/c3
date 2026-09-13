package broker

import (
	"context"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/intake"
)

type inboundOccurrence struct {
	*c3types.Inbound
	source *intake.Source
}

func occurrence(in *c3types.Inbound, source *intake.Source) inboundOccurrence {
	if in == nil {
		return inboundOccurrence{}
	}
	cp := cloneVoiceInbound(*in)
	return inboundOccurrence{Inbound: &cp, source: source.Clone()}
}

func unsourcedInbounds(batch []*c3types.Inbound) []inboundOccurrence {
	out := make([]inboundOccurrence, len(batch))
	for i, in := range batch {
		out[i] = inboundOccurrence{Inbound: in}
	}
	return out
}

func (w *RouteWorker) flushInbounds(ctx context.Context, batch []*c3types.Inbound) {
	w.flushOccurrences(ctx, unsourcedInbounds(batch))
}

func (w *RouteWorker) forwardOrFallbackCovering(ctx context.Context, in *c3types.Inbound, sources []*c3types.Inbound, covered int, coveredIDs []string, idsKnown bool) {
	w.forwardOccurrences(ctx, in, unsourcedInbounds(sources), covered, coveredIDs, idsKnown)
}

func (w *RouteWorker) trackPendingAck(sources []*c3types.Inbound, ids ...string) {
	w.trackPendingOccurrences(unsourcedInbounds(sources), ids...)
}
