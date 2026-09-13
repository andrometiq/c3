package broker

import (
	"context"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/intake"
)

type inboundOccurrence struct {
	*c3types.Inbound
	source           *intake.Source
	attachmentsState intake.AttachmentsState
}

func occurrence(in *c3types.Inbound, source *intake.Source, states ...intake.AttachmentsState) inboundOccurrence {
	if in == nil {
		return inboundOccurrence{}
	}
	cp := cloneVoiceInbound(*in)
	var state intake.AttachmentsState
	if len(states) > 1 {
		panic("broker: more than one attachment state for an occurrence")
	}
	if len(states) == 1 {
		state = states[0].Clone()
	}
	return inboundOccurrence{Inbound: &cp, source: source.Clone(), attachmentsState: state}
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

// attachmentStateSource supplies indices for legacy inbounds without provenance.
func attachmentStateSource(source *intake.Source, in c3types.Inbound) *intake.Source {
	if source != nil {
		return source
	}
	source = &intake.Source{}
	for _, att := range in.Attachments {
		source.Attachments = append(source.Attachments, intake.SourceAttachment{Kind: att.Kind, FileID: att.FileID})
	}
	return source
}
