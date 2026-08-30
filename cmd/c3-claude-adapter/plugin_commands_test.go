package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOnTheGoCommandFiles(t *testing.T) {
	tests := []struct {
		name string
		want []string
	}{
		{
			name: "on-the-go.md",
			want: []string{"mcp__plugin_c3_c3__attach", "`expr` set to `web`", "reply-tool", "answered at the laptop", "start on-the-go mode", "switch to the web chat", "do not treat the bare phrase"},
		},
		{
			name: "off-the-go.md",
			want: []string{"mcp__plugin_c3_c3__attach", "held immediately before", "If it is unknown, ask", "currently in Telegram mode"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join("..", "..", "plugins", "c3", "commands", test.name)
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			if len(strings.TrimSpace(string(body))) == 0 {
				t.Fatalf("%s is empty", path)
			}
			for _, want := range test.want {
				if !strings.Contains(string(body), want) {
					t.Errorf("%s missing %q", path, want)
				}
			}
		})
	}
}
