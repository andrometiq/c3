package main

import (
	"bufio"
	"errors"
	"io"
)

// scanTranscriptRecords visits complete JSONL records with bounded memory.
// An incomplete last line is never visited, so callers retain its starting
// offset for the next poll. Oversized records stream to oversize (when set),
// then visit receives nil; callers must never infer delivery from fragments.
func scanTranscriptRecords(input io.Reader, offset int64, visit func(int64, int64, []byte, bool) bool, oversize func([]byte)) (transcriptScanStats, error) {
	stats := transcriptScanStats{start: offset}
	reader := bufio.NewReaderSize(input, transcriptReadBufferBytes)
	start := offset
	var line []byte
	oversized := false
	for {
		fragment, err := reader.ReadSlice('\n')
		stats.bytesRead += int64(len(fragment))
		if !oversized && len(line)+len(fragment) <= maxTranscriptLineBytes+2 {
			line = append(line, fragment...)
		} else {
			if !oversized {
				oversized = true
				if oversize != nil {
					oversize(line)
				}
				line = nil
			}
			if oversize != nil {
				oversize(fragment)
			}
		}
		if err == nil {
			end := offset + stats.bytesRead
			if !visit(start, end, line, oversized) {
				return stats, nil
			}
			start, line, oversized = end, nil, false
			continue
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			return stats, nil
		}
		return stats, err
	}
}
