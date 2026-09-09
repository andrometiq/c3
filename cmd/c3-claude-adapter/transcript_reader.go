package main

import (
	"bufio"
	"errors"
	"io"
	"os"
)

// scanTranscriptRecords visits complete JSONL records with bounded memory.
// An incomplete last line is never visited, so callers retain its starting
// offset for the next poll. Oversized records stream to oversize (when set),
// then visit receives nil; callers must never infer delivery from fragments.
func scanTranscriptRecords(input io.Reader, offset int64, visit func(int64, int64, []byte, bool) bool, oversize func([]byte), discardState ...*bool) (transcriptScanStats, error) {
	stats := transcriptScanStats{start: offset}
	reader := bufio.NewReaderSize(input, transcriptReadBufferBytes)
	start := offset
	var line []byte
	oversized := false
	limit := maxTranscriptLineBytes + 2
	if len(discardState) > 0 {
		oversized = *discardState[0]
		limit = maxTranscriptLineBytes
		defer func() { *discardState[0] = oversized }()
	}
	for {
		fragment, err := reader.ReadSlice('\n')
		stats.bytesRead += int64(len(fragment))
		if !oversized && len(line)+len(fragment) <= limit {
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

// scanReceiptRecords applies the shared visitor to a bounded file snapshot.
// Incomplete ordinary records retry from their start; oversized records advance
// with a persistent discard bit, so no later fragment can become a receipt.
func scanReceiptRecords(path string, offset int64, discarding *bool, match func([]byte) bool) (int64, bool) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return offset, false
	}
	f, err := os.Open(path)
	if err != nil {
		return offset, false
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return offset, false
	}
	if info.Size() < offset {
		offset = 0
		*discarding = false
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return offset, false
	}
	next, found := offset, false
	stats, _ := scanTranscriptRecords(io.LimitReader(f, min(info.Size()-offset, 32<<20)), offset, func(_, end int64, line []byte, oversized bool) bool {
		next = end
		found = !oversized && match(line)
		return !found
	}, nil, discarding)
	if *discarding {
		next = offset + stats.bytesRead
	}
	return next, found
}
