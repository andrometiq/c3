package broker

import (
	"context"
	"errors"
	"testing"

	"github.com/Andrometiq/c3/internal/c3types"
)

func TestPluginHostSynthesizeWithoutRegistration(t *testing.T) {
	host := &PluginHost{}
	_, err := host.Synthesize(context.Background(), c3types.SpeechRequest{Text: "hello"})
	if !errors.Is(err, c3types.ErrNoSynthesizer) {
		t.Fatalf("error = %v, want ErrNoSynthesizer", err)
	}
}

func TestPluginHostSynthesizeUsesRegisteredCallback(t *testing.T) {
	host := &PluginHost{}
	host.RegisterSynthesizer(func(_ context.Context, req c3types.SpeechRequest) (c3types.SpeechResult, error) {
		return c3types.SpeechResult{Audio: []byte(req.Text), MIME: "audio/mpeg", Provider: "fake"}, nil
	})
	result, err := host.Synthesize(context.Background(), c3types.SpeechRequest{Text: "hello"})
	if err != nil || string(result.Audio) != "hello" || result.Provider != "fake" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}
