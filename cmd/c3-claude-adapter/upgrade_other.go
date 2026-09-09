//go:build !linux && !darwin

package main

import (
	"fmt"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const upgradeSupported = false

func upgradeStdio(a *adapter) (mcp.Transport, error) { return &mcp.StdioTransport{}, nil }
func upgradeExec(string, []string, []string) error   { return fmt.Errorf("self-exec unsupported") }
