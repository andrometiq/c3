package web

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/Andrometiq/c3/internal/channel"
)

func readPage(t *testing.T, name string) string {
	t.Helper()
	contents, err := pages.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(contents)
}

// The shell writes agent strings (title, holder, notices) only as text: the
// only DOM properties it assigns are on this list, its one URL sink is the
// frame's own page URL, and it calls no HTML or code sink.
func TestShellWritesAgentStringsOnlyToAllowedSinks(t *testing.T) {
	shell := readPage(t, "board.js")
	allowed := map[string]bool{"textContent": true, "title": true, "hidden": true, "mode": true, "src": true, "typing": true, "holder": true, "rev": true}
	for _, match := range regexp.MustCompile(`\.([A-Za-z_$][\w$]*)\s*=[^=]`).FindAllStringSubmatch(shell, -1) {
		if !allowed[match[1]] {
			t.Errorf("board.js assigns .%s, which is not an allowed sink", match[1])
		}
	}
	if sources := regexp.MustCompile(`\.src\s*=[^=][^\n]*`).FindAllString(shell, -1); len(sources) != 1 || sources[0] != ".src = pageURL();" {
		t.Errorf("board.js .src assignments = %q, want only the frame's own page URL", sources)
	}
	for _, sink := range []string{"inner" + "HTML", "outer" + "HTML", "insertAdjacent", "document.write", "eval(", "Function(", "srcdoc", ".style", "href", "setAttribute", "location.assign", "location.replace", "location.reload", "location.href", "window.open", "createContextualFragment", "setTimeout(\"", "setTimeout('", "setTimeout(`"} {
		if strings.Contains(shell, sink) {
			t.Errorf("board.js uses %q", sink)
		}
	}
	if regexp.MustCompile(`\blocation\s*=[^=]`).MatchString(shell) {
		t.Error("board.js navigates by assigning location")
	}

	html := readPage(t, "board.html")
	if scripts := regexp.MustCompile(`<script[^>]*>`).FindAllString(html, -1); len(scripts) != 1 || scripts[0] != `<script src="/board/assets/board.js">` || !strings.Contains(html, `<script src="/board/assets/board.js"></script>`) {
		t.Errorf("board.html scripts = %q, want only the shell script, empty", scripts)
	}
	if regexp.MustCompile(`(?i)\son\w+\s*=`).MatchString(html) || strings.Contains(html, "://") || strings.Contains(html, "srcdoc") {
		t.Error("board.html carries an inline handler, an outside URL or srcdoc")
	}
	if frames := regexp.MustCompile(`<iframe[^>]*>`).FindAllString(html, -1); len(frames) != 1 || !strings.Contains(frames[0], `sandbox="allow-scripts"`) || strings.Contains(frames[0], " src=") {
		t.Errorf("board.html frames = %q, want one sandboxed frame whose src only the shell sets", frames)
	}
}

// The limits the shell checks before posting an act, and the quote limit the
// SDK clips a selection to, are the contract's.
func TestShellAndSDKUseTheContractLimits(t *testing.T) {
	if !strings.Contains(readPage(t, "board-sdk.js"), "const QUOTE_RUNES = "+strconv.Itoa(channel.BoardQuoteMaxRunes)+";") {
		t.Errorf("board-sdk.js QUOTE_RUNES differs from the contract's %d", channel.BoardQuoteMaxRunes)
	}
	shell := readPage(t, "board.js")
	want := map[string]int{
		"textRunes": channel.BoardTextMaxRunes, "quoteRunes": channel.BoardQuoteMaxRunes, "valueBytes": channel.BoardTapValueMaxBytes,
		"keepBytes": channel.BoardKeepMaxBytes, "logEntries": channel.BoardLogHistoryEntries,
	}
	for name, limit := range want {
		match := regexp.MustCompile(name + `: (\d+)(?: << (\d+))?`).FindStringSubmatch(shell)
		if match == nil {
			t.Errorf("board.js has no %s limit", name)
			continue
		}
		value, _ := strconv.Atoi(match[1])
		if match[2] != "" {
			shift, _ := strconv.Atoi(match[2])
			value <<= shift
		}
		if value != limit {
			t.Errorf("board.js %s = %d, contract says %d", name, value, limit)
		}
	}
	if !strings.Contains(shell, "const PART = /"+channel.BoardPartPattern+"/;") {
		t.Errorf("board.js part pattern differs from %s", channel.BoardPartPattern)
	}
}

// C3 inlines the SDK into a <script> element, so its source must not end or
// comment out that element.
func TestSDKCanBeInlinedAsTheFirstScript(t *testing.T) {
	sdk := strings.ToLower(readPage(t, "board-sdk.js"))
	for _, breaker := range []string{"</script", "<!--"} {
		if strings.Contains(sdk, breaker) {
			t.Errorf("board-sdk.js contains %q", breaker)
		}
	}
}

// The starter a new board's attach carries is parts plus one c3.threads()
// call, with nothing the frame blocks or the lint warns about.
func TestStarterIsPartsAndOneThreadsCall(t *testing.T) {
	starter := readPage(t, "board-starter.html")
	if scripts := regexp.MustCompile(`(?s)<script[^>]*>(.*?)</script>`).FindAllStringSubmatch(starter, -1); len(scripts) != 1 || strings.TrimSpace(scripts[0][1]) != "c3.threads();" {
		t.Fatalf("starter scripts = %q, want one c3.threads() call", scripts)
	}
	parts := regexp.MustCompile(`<(\w+)[^>]*\sdata-c3-part="([^"]*)"`).FindAllStringSubmatchIndex(starter, -1)
	if len(parts) < 2 {
		t.Fatalf("starter has %d parts, want at least two", len(parts))
	}
	seen := map[string]bool{}
	for _, part := range parts {
		tag, address := starter[part[2]:part[3]], starter[part[4]:part[5]]
		if !channel.IsValidBoardPart(address) || seen[address] {
			t.Errorf("starter part address %q is invalid or reused", address)
		}
		seen[address] = true
		end := strings.Index(starter[part[1]:], "</"+tag+">")
		if end < 0 || strings.Contains(starter[part[1]:part[1]+end], "<script") {
			t.Errorf("starter part %q is unclosed or holds a script", address)
		}
	}
	for _, blocked := range []string{"://", "localStorage", "sessionStorage", "fetch(", "XMLHttpRequest", "<form", "target="} {
		if strings.Contains(starter, blocked) {
			t.Errorf("starter contains %q", blocked)
		}
	}
}
