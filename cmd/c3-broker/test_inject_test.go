package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScratchServeRequiresOptInAndNewDirectory(t *testing.T) {
	t.Setenv("C3_ALLOW_TEST_INJECT", "")
	state := filepath.Join(t.TempDir(), "scratch")
	if err := runTestServe([]string{"--state", state}); err == nil {
		t.Fatal("ungated startup")
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatal("refused startup touched state")
	}
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	if err := runTestServe([]string{"--allow-test-inject", "--state", state}); err == nil {
		t.Fatal("reused existing state")
	}
	if err := runTestServe([]string{"--allow-test-inject", "--state", "relative"}); err == nil {
		t.Fatal("accepted relative state")
	}
}

func TestInjectRequiresExplicitSocket(t *testing.T) {
	if err := runInject([]string{"--topic", "1", "--text", "example"}); err == nil {
		t.Fatal("used implicit socket")
	}
	if err := runInject([]string{"--socket", "unused", "--voice", "--photo"}); err == nil {
		t.Fatal("ambiguous media")
	}
}
