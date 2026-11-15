package channel

import (
	"strings"
	"testing"
)

func TestBoardNameRule(t *testing.T) {
	for _, name := range []string{"a", "0", "plan", "c3-board", "a-", strings.Repeat("a", 40)} {
		if !IsValidBoardName(name) {
			t.Errorf("IsValidBoardName(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"", "-a", "Plan", "a_b", "a.b", "a/b", "../a", "a b", "a\n", "é", strings.Repeat("a", 41)} {
		if IsValidBoardName(name) {
			t.Errorf("IsValidBoardName(%q) = true, want false", name)
		}
	}
}

func TestBoardPartRule(t *testing.T) {
	for _, part := range []string{"1", "1.5", "Plan:intro", "a_b-c", ".", strings.Repeat("x", 64)} {
		if !IsValidBoardPart(part) {
			t.Errorf("IsValidBoardPart(%q) = false, want true", part)
		}
	}
	for _, part := range []string{"", "a b", "a/b", "a\n", "<b>", "\"", "é", strings.Repeat("x", 65)} {
		if IsValidBoardPart(part) {
			t.Errorf("IsValidBoardPart(%q) = true, want false", part)
		}
	}
}
