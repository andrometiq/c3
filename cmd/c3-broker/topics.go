package main

import (
	"fmt"
	"sort"
	"strings"
)

// runTopics queries the running broker for the topics registry + claim
// state and prints a human-readable rendering. Identical content to the
// adapter-side `topics` MCP tool, but invokable from any CLI's
// pure-shell command wrapper (2026-05-09: keep CLI-specific
// layers thin; logic lives in the broker / its subcommands).
func runTopics() error {
	msg, err := fetchTopicsList()
	if err != nil {
		return err
	}
	if len(msg.Topics) == 0 {
		fmt.Println("no topics configured")
		return nil
	}
	// Sort: group, then name.
	sort.Slice(msg.Topics, func(i, j int) bool {
		a, b := msg.Topics[i], msg.Topics[j]
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		return a.Name < b.Name
	})
	var b strings.Builder
	fmt.Fprintln(&b, "known topics:")
	for _, tp := range msg.Topics {
		if tp.RouteKind == "dm" {
			row := fmt.Sprintf("  • %s (chat %d, no topic)", tp.Name, tp.ChatID)
			if tp.ClaimedBy != nil {
				row += fmt.Sprintf(" — held by %s pid %d", tp.ClaimedBy.CLI, tp.ClaimedBy.PID)
			} else {
				row += " — free"
			}
			fmt.Fprintln(&b, row)
			continue
		}
		grp := tp.Group
		if grp == "" {
			grp = "(no-group)"
		}
		row := fmt.Sprintf("  • %s/%s (chat %d, thread %d)",
			grp, tp.Name, tp.ChatID, tp.TopicID)
		if tp.ClaimedBy != nil {
			row += fmt.Sprintf(" — held by %s pid %d",
				tp.ClaimedBy.CLI, tp.ClaimedBy.PID)
		} else {
			row += " — free"
		}
		fmt.Fprintln(&b, row)
	}
	fmt.Print(b.String())
	return nil
}
