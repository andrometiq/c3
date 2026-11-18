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
	allowed := map[string]bool{"textContent": true, "title": true, "hidden": true, "mode": true, "src": true, "typing": true, "holder": true, "rev": true,
		"disabled": true, "ariaPressed": true}
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

// The shell takes a message only from its sandboxed frame: that frame's window
// and the opaque origin ("null") of a sandbox without allow-same-origin.
func TestShellTakesMessagesOnlyFromItsSandboxedFrame(t *testing.T) {
	shell := readPage(t, "board.js")
	listeners := regexp.MustCompile(`addEventListener\("message", [^\n]*\n([^\n]*)`).FindAllStringSubmatch(shell, -1)
	if len(listeners) != 1 {
		t.Fatalf("board.js has %d message listeners, want one", len(listeners))
	}
	guard := strings.TrimSpace(listeners[0][1])
	if !strings.HasPrefix(guard, "if (event.source !== frame.contentWindow || event.origin !== \"null\" || ") || !strings.HasSuffix(guard, ") return;") {
		t.Fatalf("board.js message guard = %q, want the frame's window and the opaque origin checked first", guard)
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

// Only the shell opens the microphone, and only from a real tap on its own
// button: the frame gets no microphone, and no frame message reaches the
// recorder (the page can only name the part it is on).
func TestOnlyTheShellsOwnButtonOpensTheMicrophone(t *testing.T) {
	shell := readPage(t, "board.js")
	if strings.Count(shell, "getUserMedia(") != 1 || !strings.Contains(functionBody(t, shell, "async function startRecording("), "getUserMedia(") {
		t.Fatal("board.js opens the microphone outside startRecording")
	}
	starts := regexp.MustCompile(`startRecording\(\)`).FindAllStringIndex(shell, -1)
	click := strings.Index(shell, `strip.mic.addEventListener("click", (event) => {`)
	trusted := strings.Index(shell, "if (!event.isTrusted) return;")
	if len(starts) != 2 || click < 0 || trusted < click || starts[1][0] < trusted || starts[1][0]-click > 300 {
		t.Fatalf("startRecording is called %d times; want once, from the mic's click listener after its isTrusted check", len(starts)-1)
	}
	handler := functionBody(t, shell, "async function onFrameMessage(")
	for _, recorder := range []string{"startRecording", "recording", "getUserMedia", "MediaRecorder", "upload("} {
		if strings.Contains(handler, recorder) {
			t.Errorf("a frame message reaches %q", recorder)
		}
	}
	frame := regexp.MustCompile(`<iframe[^>]*>`).FindString(readPage(t, "board.html"))
	if strings.Contains(frame, "allow=") || strings.Contains(frame, "allow-same-origin") {
		t.Errorf("board.html frame %q delegates a permission or its origin", frame)
	}
}

// The strip's Close is "Make read-only", and its confirm says what it does.
func TestMakeReadOnlySaysItIsForGood(t *testing.T) {
	if !strings.Contains(readPage(t, "board.html"), `<button id="close" type="button" hidden>Make read-only</button>`) {
		t.Error("board.html's close button is not labelled Make read-only")
	}
	if !strings.Contains(readPage(t, "board.js"), `confirm("This makes the board read-only for good.")`) {
		t.Error("board.js does not confirm that read-only is for good")
	}
}

// functionBody is a top-level shell function's source, from its declaration to
// its closing brace at the shell's indent.
func functionBody(t *testing.T, source, declaration string) string {
	t.Helper()
	start := strings.Index(source, declaration)
	if start < 0 {
		t.Fatalf("no %q", declaration)
	}
	end := strings.Index(source[start:], "\n  }\n")
	if end < 0 {
		t.Fatalf("%q has no end", declaration)
	}
	return source[start : start+end]
}
