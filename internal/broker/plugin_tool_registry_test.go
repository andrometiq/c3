package broker

import (
	"testing"

	"github.com/Andrometiq/c3/internal/plugin"
)

// TestToolRegistry_RefusesDuplicateName pins that a second plugin cannot take a
// tool name the first already registered.
//
// The defect it guards: Add plain-assigned into the map, so a second plugin
// claiming an existing name silently REPLACED the first — the incumbent's tool
// vanished from List() with nothing logged, and which plugin won was decided by
// load order. plugin.ToolRegistry's own doc tells authors to prefix tool names
// "to avoid collisions across plugins", which was advice the implementation
// never enforced.
//
// First-writer-wins is the deliberate choice: a refused registration is a log
// line its author reads and fixes, while a silent replacement is a tool that
// stops existing for reasons nobody can see. The registry has no consumers yet —
// which is exactly why fixing it is free now and a breaking change once a third
// party has written against the old behaviour, with the plugin API about to
// freeze for v0.1.0.
func TestToolRegistry_RefusesDuplicateName(t *testing.T) {
	h := &PluginHost{tools: map[string]plugin.Tool{}}
	reg := &toolRegistry{host: h}

	reg.Add(plugin.Tool{Name: "stt_retranscribe", Description: "the incumbent"})
	reg.Add(plugin.Tool{Name: "stt_retranscribe", Description: "the impostor"})

	got := reg.List()
	if len(got) != 1 {
		t.Fatalf("registry holds %d tools after a duplicate Add, want 1: %+v", len(got), got)
	}
	if got[0].Description != "the incumbent" {
		t.Errorf("a second plugin took a tool name the first had registered: List() reports %q. "+
			"The first plugin's tool has silently stopped existing, and which plugin wins is decided "+
			"by load order rather than by anything the author can see", got[0].Description)
	}
}

// A DIFFERENT name is not a collision — the guard must not turn into "one tool".
func TestToolRegistry_DistinctNamesBothRegister(t *testing.T) {
	h := &PluginHost{tools: map[string]plugin.Tool{}}
	reg := &toolRegistry{host: h}

	reg.Add(plugin.Tool{Name: "stt_retranscribe"})
	reg.Add(plugin.Tool{Name: "stt_vocabulary"})

	if got := reg.List(); len(got) != 2 {
		t.Fatalf("two DIFFERENT tool names registered %d tools, want 2: %+v — the duplicate guard is "+
			"matching too broadly and plugins cannot expose more than one tool", len(got), got)
	}
}

// Remove must free the name, or a plugin can never replace its own tool (e.g. on
// reconfigure) and the refusal becomes a permanent lockout.
func TestToolRegistry_RemoveFreesTheName(t *testing.T) {
	h := &PluginHost{tools: map[string]plugin.Tool{}}
	reg := &toolRegistry{host: h}

	reg.Add(plugin.Tool{Name: "stt_retranscribe", Description: "first"})
	reg.Remove("stt_retranscribe")
	reg.Add(plugin.Tool{Name: "stt_retranscribe", Description: "second"})

	got := reg.List()
	if len(got) != 1 || got[0].Description != "second" {
		t.Errorf("a plugin could not re-register a tool it had removed: %+v. The duplicate refusal "+
			"must guard against two plugins colliding, not stop one plugin from replacing its own tool", got)
	}
}
