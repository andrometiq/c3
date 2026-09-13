package queue

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/intake"
)

// AppendTrackedSource carries private provenance alongside the public payload.
func (s *Store) AppendTrackedSource(rk RouteKey, in *c3types.Inbound, source *intake.Source, voicePending ...string) (string, error) {
	return s.appendTracked(rk, in, "", voicePending, source)
}

// firstSource is bounded by live queue files, including rows behind a cursor
// until compaction. Rewrites preserve source; source-bearing appends serialize
// this scan and write. Atomic removal may end a row's lifetime during the scan.
// Drain destinations participate independently of the captured source route.
func (s *Store) firstSource(source *intake.Source) (*intake.Source, error) {
	source = source.Clone()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("queue: source scan: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		first, err := sourceInFile(filepath.Join(s.dir, entry.Name()), source)
		if err != nil {
			return nil, fmt.Errorf("queue: source scan: %w", err)
		}
		if first == nil {
			continue
		}
		if !reflect.DeepEqual(first, source) {
			topic := "null"
			if source.TopicID != nil {
				topic = strconv.FormatInt(*source.TopicID, 10)
			}
			log.Printf("queue: source conflict channel=%s chat_id=%d topic_id=%s update_id=%d message_id=%d first_message_id=%d; keeping first",
				source.Channel, source.ChatID, topic, source.UpdateID, source.MessageID, first.MessageID)
		}
		return first, nil
	}
	return source, nil
}

func sourceInFile(path string, source *intake.Source) (*intake.Source, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for sc.Scan() {
		var row struct {
			Source *intake.Source `json:"_c3_source"`
		}
		if json.Unmarshal(sc.Bytes(), &row) == nil && source.SameOccurrence(row.Source) {
			return row.Source.Clone(), nil
		}
	}
	return nil, sc.Err()
}
