// Package buildidentity separates executable identity from release ordering.
package buildidentity

import (
	"bytes"
	"debug/buildinfo"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

// ID is injected by source and release builds, independently of version.Version.
var ID = "dev"

const Symbol = "github.com/Andrometiq/c3/internal/buildidentity.ID"
const marker = "C3_BUILD_ID_V1"

func Current() string {
	// Keep the format marker in the executable, including stripped releases.
	if strings.Contains(ID, marker) || ID == "" {
		return "dev"
	}
	return ID
}

type Installed struct{ Path, Build, Contract string }

// Read inspects Go's embedded build settings without executing the candidate.
// A pre-feature executable has no format marker and is deliberately unreadable.
func Read(path string) (Installed, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Installed{}, err
	}
	if !bytes.Contains(data, []byte(marker)) {
		return Installed{}, fmt.Errorf("missing build identity")
	}
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return Installed{}, err
	}
	result := Installed{Path: path, Build: "dev"}
	for _, setting := range info.Settings {
		if setting.Key == "-ldflags" {
			for _, field := range strings.Fields(setting.Value) {
				if value, ok := strings.CutPrefix(field, Symbol+"="); ok {
					result.Build = value
				}
			}
		}
	}
	contract := regexp.MustCompile(`C3_MCP_CONTRACT_V1:([a-f0-9]{64}):`).FindSubmatch(data)
	if len(contract) == 2 {
		result.Contract = string(contract[1])
	}
	return result, nil
}

// Claude follows plugins/c3/.mcp.json: command lookup through PATH.
func Claude() (Installed, error) {
	path, err := exec.LookPath("c3-claude-adapter")
	if err != nil {
		return Installed{}, err
	}
	return Read(path)
}
