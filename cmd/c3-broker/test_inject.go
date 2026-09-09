package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Andrometiq/c3/internal/broker"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

func runInject(args []string) error {
	f := flag.NewFlagSet("inject", flag.ContinueOnError)
	var req ipc.TestInjectReq
	socket := f.String("socket", "", "required explicit scratch broker socket; never auto-starts a broker")
	f.Int64Var(&req.Topic, "topic", 0, "test topic id")
	f.StringVar(&req.Text, "text", "", "generic synthetic text, or final voice transcript")
	f.Int64Var(&req.From, "from", 1, "synthetic sender id")
	voice := f.Bool("voice", false, "persist a voice placeholder then resolve its transcript")
	photo := f.Bool("photo", false, "include synthetic photo metadata")
	f.IntVar(&req.Count, "count", 1, "1 or 2 messages in one debounce window")
	f.IntVar(&req.VoiceDelayMS, "voice-delay-ms", 0, "delay local transcription, 0..60000")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *socket == "" || f.NArg() != 0 || (*voice && *photo) {
		return errors.New("inject requires --socket and mutually exclusive --voice/--photo")
	}
	req.Op = ipc.OpTestInject
	req.Kind = "text"
	if *voice {
		req.Kind = "voice"
	}
	if *photo {
		req.Kind = "photo"
	}
	nc, err := net.DialTimeout("unix", *socket, time.Second)
	if err != nil {
		return err
	}
	defer nc.Close()
	if err := nc.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	conn := ipc.NewConn(nc)
	if err := conn.WriteJSON(ipc.HelloMsg{Op: ipc.OpHello, CLI: "c3-broker-cli", PID: os.Getpid()}); err != nil {
		return err
	}
	raw, err := conn.ReadFrame()
	if err != nil {
		return err
	}
	if op, err := ipc.PeekOp(raw); err != nil || op != ipc.OpHelloAck {
		return fmt.Errorf("unexpected broker greeting: %s", raw)
	}
	if err := conn.WriteJSON(req); err != nil {
		return err
	}
	raw, err = conn.ReadFrame()
	if err != nil {
		return err
	}
	var response ipc.TestInjectResp
	if err := json.Unmarshal(raw, &response); err != nil {
		return err
	}
	fmt.Println(string(raw))
	if !response.Accepted {
		return fmt.Errorf("injection refused: %s", response.Err)
	}
	return nil
}

// Separate from runDaemon: no production paths, runtime repair, update checker,
// real channel registration, singleton discovery or credential loading.
func runTestServe(args []string) error {
	f := flag.NewFlagSet("test-serve", flag.ContinueOnError)
	allow := f.Bool("allow-test-inject", os.Getenv("C3_ALLOW_TEST_INJECT") == "1", "explicitly enable synthetic local input")
	state := f.String("state", "", "required NEW absolute private scratch directory; socket is <state>/c3.sock")
	if err := f.Parse(args); err != nil {
		return err
	}
	if !*allow {
		return errors.New("test-serve requires --allow-test-inject or C3_ALLOW_TEST_INJECT=1")
	}
	if !filepath.IsAbs(*state) || f.NArg() != 0 {
		return errors.New("test-serve requires a new absolute --state directory")
	}
	// Refuse existing directories, including production state and symlinks.
	if err := os.Mkdir(*state, 0700); err != nil {
		return fmt.Errorf("create NEW scratch state: %w", err)
	}
	for name, value := range map[string]string{
		"XDG_RUNTIME_DIR": *state, "XDG_CONFIG_HOME": filepath.Join(*state, "config"),
		"XDG_STATE_HOME": filepath.Join(*state, "state"), "XDG_CACHE_HOME": filepath.Join(*state, "cache"),
		"C3_QUEUE_DIR": filepath.Join(*state, "queue"),
	} {
		if err := os.Setenv(name, value); err != nil {
			return err
		}
	}
	lf, err := os.OpenFile(filepath.Join(*state, "broker.log"), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer lf.Close()
	log.SetOutput(io.MultiWriter(os.Stderr, lf))
	mf := &mappings.MappingsFile{SchemaVersion: 1, Mappings: map[string]mappings.Mapping{},
		Allowlist: &mappings.Allowlist{Groups: []int64{broker.TestInjectChatID}},
		Channels: map[string]mappings.ChannelConfig{broker.TestInjectChannel: {
			DefaultGroup: "matrix", Groups: map[string]mappings.GroupConfig{"matrix": {ChatID: broker.TestInjectChatID}}, DebounceMS: 250,
		}},
	}
	path, err := mappings.DefaultPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	if err := mappings.Write(path, mf); err != nil {
		return err
	}
	b := broker.New(mf)
	defer b.Shutdown()
	if err := b.EnableTestInjection(); err != nil {
		return err
	}
	srv, err := broker.Listen(filepath.Join(*state, "c3.sock"), b)
	if err != nil {
		return err
	}
	defer srv.Stop()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sig)
	log.Print("TEST INJECTION scratch broker ready")
	<-sig
	return nil
}
