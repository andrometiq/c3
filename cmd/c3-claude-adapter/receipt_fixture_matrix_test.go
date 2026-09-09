package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type receiptExpectation struct {
	Transport string `json:"transport"`
	Token     string `json:"token"`
	Attempt   string `json:"attempt"`
	Accept    *bool  `json:"accept"`
	Reason    string `json:"reason"`
}

// The sidecar describes evidence, never a reconstructed host record. Every JSONL
// file in a version directory must have one explicit expectation per record.
// The live collector writes null only for genuinely unverified envelopes.
func TestVersionedDeliveryReceiptCorpus(t *testing.T) {
	files, err := filepath.Glob("testdata/claude-*/*.jsonl")
	if err != nil || len(files) == 0 {
		t.Fatalf("missing corpus: %v", err)
	}
	for _, path := range files {
		t.Run(strings.TrimPrefix(path, "testdata/"), func(t *testing.T) {
			sidecar, err := os.ReadFile(strings.TrimSuffix(path, ".jsonl") + ".expect.json")
			if err != nil {
				t.Fatal(err)
			}
			var expectations []receiptExpectation
			if err = json.Unmarshal(sidecar, &expectations); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			scanner := bufio.NewScanner(f)
			scanner.Buffer(make([]byte, 4096), 4*1024*1024)
			index := 0
			for scanner.Scan() {
				if index >= len(expectations) {
					t.Fatal("record has no evidence expectation")
				}
				e := expectations[index]
				record := append([]byte(nil), scanner.Bytes()...)
				index++
				t.Run(e.Transport+"/"+e.Attempt+"/"+strings.Split(e.Reason, " ")[0], func(t *testing.T) {
					if !json.Valid(record) {
						t.Fatal("invalid host record")
					}
					if e.Accept == nil {
						if !strings.HasPrefix(e.Reason, "TODO:") {
							t.Fatal("unverified record needs an explicit TODO")
						}
						t.Skip(e.Reason)
					}
					if e.Transport != "channel" && e.Transport != "inbox" {
						t.Fatal("deliveryReceipt accepts only live transports; fetch needs a verified tool-result predicate")
					}
					if e.Token == "" || e.Attempt == "" {
						t.Fatal("fixture expectation has no correlation")
					}
					cross := e.Transport == "inbox"
					if got := deliveryReceipt(record, e.Token, cross, e.Attempt); got != *e.Accept {
						t.Fatalf("receipt=%v want %v: %s", got, *e.Accept, e.Reason)
					}
					if deliveryReceipt(record, "WRONG-TOKEN", cross, e.Attempt) {
						t.Fatal("accepted wrong token")
					}
					if deliveryReceipt(record, e.Token, cross, "wrong:999") {
						t.Fatal("accepted wrong attempt")
					}
					if cross && *e.Accept {
						// Remove independently: provenance and the exact host
						// prefix must each be required, not just the token.
						var obj map[string]any
						if err := json.Unmarshal(record, &obj); err != nil {
							t.Fatal(err)
						}
						deletePeerProvenance(obj)
						missing, _ := json.Marshal(obj)
						if deliveryReceipt(missing, e.Token, true, e.Attempt) {
							t.Fatal("accepted without peer provenance")
						}
						if err := json.Unmarshal(record, &obj); err != nil {
							t.Fatal(err)
						}
						stripPeerPrefix(obj)
						missing, _ = json.Marshal(obj)
						if deliveryReceipt(missing, e.Token, true, e.Attempt) {
							t.Fatal("accepted without host peer prefix")
						}
					}
				})
			}
			if err := scanner.Err(); err != nil {
				t.Fatal(err)
			}
			if index == 0 || index != len(expectations) {
				t.Fatalf("records=%d expectations=%d", index, len(expectations))
			}
		})
	}
}

func deletePeerProvenance(value any) {
	switch v := value.(type) {
	case map[string]any:
		delete(v, "origin")
		delete(v, "isMeta")
		for _, child := range v {
			deletePeerProvenance(child)
		}
	case []any:
		for _, child := range v {
			deletePeerProvenance(child)
		}
	}
}
func stripPeerPrefix(value any) {
	const prefix = "Another Claude session sent a message:\n"
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			if text, ok := child.(string); ok {
				v[key] = strings.ReplaceAll(text, prefix, "")
			} else {
				stripPeerPrefix(child)
			}
		}
	case []any:
		for i, child := range v {
			if text, ok := child.(string); ok {
				v[i] = strings.ReplaceAll(text, prefix, "")
			} else {
				stripPeerPrefix(child)
			}
		}
	}
}
