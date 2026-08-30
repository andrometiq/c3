package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/mappings"
	"github.com/Andrometiq/c3/internal/plugin/builtins/tts"
)

func runTTS(args []string) error {
	return runTTSWithWriter(args, os.Stdout)
}

func runTTSWithWriter(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: c3-broker tts check | c3-broker tts say <text…>")
	}
	if _, err := ensureEmbeddedReleaseTTS(); err != nil {
		return fmt.Errorf("materialize TTS runtime: %w", err)
	}
	path, err := mappings.DefaultPath()
	if err != nil {
		return err
	}
	mapping, err := mappings.Read(path)
	if err != nil {
		return fmt.Errorf("read mappings: %w", err)
	}
	br := broker.New(mapping)
	defer br.Shutdown()

	switch args[0] {
	case "check":
		if len(args) != 1 {
			return fmt.Errorf("usage: c3-broker tts check")
		}
		cfg, err := tts.LoadConfig(br.Plugins)
		if err != nil {
			return err
		}
		output, err := tts.Check(context.Background(), cfg)
		if err != nil {
			return err
		}
		_, err = stdout.Write(output)
		return err
	case "say":
		if len(args) < 2 || strings.TrimSpace(strings.Join(args[1:], " ")) == "" {
			return fmt.Errorf("usage: c3-broker tts say <text…>")
		}
		if err := tts.Register(br.Plugins); err != nil {
			return err
		}
		result, err := br.Plugins.Synthesize(context.Background(), c3types.SpeechRequest{
			Text: strings.Join(args[1:], " "),
		})
		if err != nil {
			return err
		}
		_, err = stdout.Write(result.Audio)
		return err
	default:
		return fmt.Errorf("usage: c3-broker tts check | c3-broker tts say <text…>")
	}
}
