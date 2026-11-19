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

// The shell writes agent and page strings (title, holder, notices, the chat's
// title, line, quick answers and thread) only as text: the only DOM properties
// it assigns are on this list, its URL sinks are the frame's own page URL and
// its player's board audio, and it calls no HTML or code sink.
func TestShellWritesAgentStringsOnlyToAllowedSinks(t *testing.T) {
	shell := readPage(t, "board.js")
	allowed := map[string]bool{"textContent": true, "title": true, "hidden": true, "mode": true, "src": true, "typing": true, "holder": true, "rev": true,
		"disabled": true, "ariaPressed": true, "checked": true, "value": true, "placeholder": true, "scrollTop": true, "ariaLabel": true,
		"ariaBusy": true, "width": true, "height": true, "fillStyle": true}
	for _, match := range regexp.MustCompile(`\.([A-Za-z_$][\w$]*)\s*=[^=]`).FindAllStringSubmatch(shell, -1) {
		if !allowed[match[1]] {
			t.Errorf("board.js assigns .%s, which is not an allowed sink", match[1])
		}
	}
	if sources := regexp.MustCompile(`\.src\s*=[^=][^\n]*`).FindAllString(shell, -1); strings.Join(sources, " ") != ".src = `${base}/audio/${name}`; .src = pageURL();" {
		t.Errorf("board.js .src assignments = %q, want only the player's board audio and the frame's own page URL", sources)
	}
	for _, sink := range []string{"inner" + "HTML", "outer" + "HTML", "insertAdjacent", "document.write", "eval(", "Function(", "srcdoc", "href", "setAttribute", "location.assign", "location.replace", "location.reload", "location.href", "window.open", "createContextualFragment", "setTimeout(\"", "setTimeout('", "setTimeout(`"} {
		if strings.Contains(shell, sink) {
			t.Errorf("board.js uses %q", sink)
		}
	}
	if regexp.MustCompile(`\blocation\s*=[^=]`).MatchString(shell) {
		t.Error("board.js navigates by assigning location")
	}
	// Its two style sinks take numbers only: place() puts its own mic and recording bar, ringTo() sizes the ring
	// behind its mics from the recording's loudness.
	place := functionBody(t, shell, "function place(")
	if strings.Count(shell, ".style") != 2 || !strings.Contains(place, ".style.setProperty(side, `${Math.round(box[side])}px`)") ||
		!strings.Contains(functionBody(t, shell, "function ringTo("), `mic.style.setProperty("--level", level.toFixed(2));`) {
		t.Error("board.js writes a style outside place() and ringTo(), or one of them writes more than a number")
	}
	if calls := regexp.MustCompile(`\bringTo\([^)]*\)`).FindAllString(shell, -1); strings.Join(calls, " ") != "ringTo(ring) ringTo(0) ringTo(level)" {
		t.Errorf("board.js ringTo() calls = %q, want only the smoothed loudness and 0", calls)
	}
	if calls := regexp.MustCompile(`\bplace\([^)]*\)?\)`).FindAllString(shell, -1); strings.Join(calls, " ") != "place(controls.mic, mic) place(controls.recorder, recorderBox(frameBox, mic)) place(element, box)" ||
		!strings.Contains(functionBody(t, shell, "function drawVoice("), "const mic = micBox(frameBox);") {
		t.Errorf("board.js place() calls = %q, want only the mic at micBox and the bar at recorderBox", calls)
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
		"keepBytes": channel.BoardKeepMaxBytes, "logEntries": channel.BoardLogHistoryEntries, "fileBytes": boardFileLimit,
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

// Only the shell opens the microphone, and only from a real tap on one of its
// own mic buttons: the frame gets no microphone, and no frame message reaches
// the recorder (the page can only name the part it is on).
func TestOnlyTheShellsOwnButtonOpensTheMicrophone(t *testing.T) {
	shell := readPage(t, "board.js")
	if strings.Count(shell, "getUserMedia(") != 1 || !strings.Contains(functionBody(t, shell, "async function startRecording("), "getUserMedia(") {
		t.Fatal("board.js opens the microphone outside startRecording")
	}
	tap := functionBody(t, shell, "function onMicTap(")
	trusted := strings.Index(tap, "if (!event.isTrusted) return;")
	if starts := regexp.MustCompile(`startRecording\(\)`).FindAllStringIndex(shell, -1); len(starts) != 2 || trusted < 0 || strings.Index(tap, "startRecording()") < trusted {
		t.Fatalf("startRecording is called %d times; want once, from onMicTap after its isTrusted check", len(starts)-1)
	}
	if strings.Count(shell, "onMicTap") != 3 || !strings.Contains(shell, `controls.mic.addEventListener("click", onMicTap);`) ||
		!strings.Contains(shell, `controls.chatMic.addEventListener("click", onMicTap);`) {
		t.Error("onMicTap is reached from something other than the shell's two mic buttons' clicks")
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

// Spoken replies play in the shell, never the frame: the page names only a
// reply's id (a positive integer), the player takes only this board's audio
// (a token C3 gave, checked, or the silent unlock clip), and starting the mic
// stops it first.
func TestOnlyTheShellPlaysRepliesAndThePageNamesOnlyAnID(t *testing.T) {
	shell := readPage(t, "board.js")
	if !strings.Contains(functionBody(t, shell, "async function onFrameMessage("), "const problem = speakForPage(value);") ||
		!strings.Contains(functionBody(t, shell, "function speakForPage("), "if (id !== null && !(Number.isSafeInteger(id) && id > 0)) return \"invalid\";") {
		t.Error("a speak message reaches the player without its id checked")
	}
	if calls := regexp.MustCompile(`playAudio\([^)]*\)`).FindAllString(shell, -1); strings.Join(calls, " ") != `playAudio(token) playAudio(name) playAudio("unlock")` ||
		!strings.Contains(functionBody(t, shell, "async function fetchSpeech("), "/^[0-9a-f]{32}$/.test(body.token)") {
		t.Errorf("playAudio calls = %q, want only a checked token from C3 and the unlock clip", calls)
	}
	if !strings.Contains(functionBody(t, shell, "async function startRecording("), "stopSpeech(); // recording and speaking never overlap") ||
		!strings.Contains(functionBody(t, shell, "async function playNext("), "if (speaking || recording || isAudioBlocked") {
		t.Error("the mic and the player can overlap")
	}
	if !strings.Contains(readPage(t, "board.html"), `<audio id="player" preload="auto" hidden></audio>`) {
		t.Error("board.html has no player of the shell's own")
	}
	if sdk := readPage(t, "board-sdk.js"); !strings.Contains(sdk, `const speak = (id) => request("speak", { value: id ?? null });`) || strings.Contains(sdk, "fetch(") {
		t.Error("board-sdk.js does not ask the shell to speak, or reaches for the network itself")
	}
}

// The status panel's Close is "Make read-only", and its confirm says what it does.
func TestMakeReadOnlySaysItIsForGood(t *testing.T) {
	panel := regexp.MustCompile(`<div id="panel" popover>.*?</div>`).FindString(readPage(t, "board.html"))
	if !strings.Contains(panel, `<button id="close" type="button" hidden>Make read-only</button>`) {
		t.Error("board.html's status panel has no close button labelled Make read-only")
	}
	if !strings.Contains(readPage(t, "board.js"), `confirm("This makes the board read-only for good.")`) {
		t.Error("board.js does not confirm that read-only is for good")
	}
}

// The page fills the window: no strip over it and no bar under it. C3's own
// state is a small circle over its top-right corner, whose panel names the
// board and the live session, and a notice is a toast under the circle.
func TestShellIsTheFrameAndAStatusCircle(t *testing.T) {
	html := readPage(t, "board.html")
	body := html[strings.Index(html, "<body>"):]
	if strings.Contains(html, "<header") || strings.Contains(html, "<footer") || !strings.HasPrefix(body, "<body>\n<iframe id=\"page\"") {
		t.Error("board.html draws a bar around the frame, or the frame is not the body's first element")
	}
	if !regexp.MustCompile(`#page \{[^}]*width: 100%; height: 100%;`).MatchString(html) || !strings.Contains(html, "body { height: 100dvh; overflow: hidden; }") {
		t.Error("board.html's frame does not fill the window")
	}
	status := regexp.MustCompile(`<button id="status"[^>]*>`).FindString(html)
	panel := regexp.MustCompile(`<div id="panel" popover>.*?</div>`).FindString(html)
	if !strings.Contains(status, `popovertarget="panel"`) || !strings.Contains(panel, `id="title"`) || !strings.Contains(panel, `id="session"`) || !strings.Contains(panel, `id="notice"`) {
		t.Errorf("board.html status circle %q and panel %q: want the circle to open a panel naming the board, the session and the notice", status, panel)
	}
	if !strings.Contains(html, `<p id="toast" role="status" hidden></p>`) {
		t.Error("board.html has no toast for notices")
	}
	shell := readPage(t, "board.js")
	if !strings.Contains(functionBody(t, shell, "function receive("), "notify(data.text); // the shell only: never the frame") {
		t.Error("board.js does not keep a notice in the shell")
	}
}

// A page names where its Send sits, but can't use that to hide C3's own
// controls: a malformed spot is refused, and the mic is a circle of a fixed
// size range, kept inside the frame's visible box with its row below the
// status circle, so the recording bar beside it can't meet the circle either.
func TestPageCannotPlaceTheMicOverTheShellsOwnControls(t *testing.T) {
	shell := readPage(t, "board.js")
	if !strings.Contains(shell, "const MIC = { minPx: 44, maxPx: 56,") {
		t.Error("board.js mic size range is not 44 to 56 px")
	}
	read := functionBody(t, shell, "function readComposer(")
	if !strings.Contains(read, `every((side) => Number.isFinite(dock[side]))`) || !strings.Contains(read, `typeof hasText !== "boolean"`) {
		t.Error("readComposer takes a spot or text state without checking it")
	}
	box := functionBody(t, shell, "function micBox(")
	for _, clamp := range []string{
		"const size = clamp(Math.min(dock.width, dock.height), MIC.minPx, MIC.maxPx);",
		"const left = clamp(frameBox.left + dock.x + (dock.width - size) / 2, view.left, view.right - size);",
		"let top = clamp(frameBox.top + dock.y + (dock.height - size) / 2, view.top, view.bottom - size);",
		"const circleBottom = controls.status.getBoundingClientRect().bottom + MIC.marginPx;",
		"if (top < circleBottom) top = circleBottom;",
	} {
		if !strings.Contains(box, clamp) {
			t.Errorf("micBox lacks %q", clamp)
		}
	}
	if !strings.Contains(functionBody(t, shell, "async function onFrameMessage("), "const next = readComposer({ value, dock, hasText });") {
		t.Error("a focus message reaches the composer without readComposer")
	}
}

// A page chooses when the chat sheet opens and, for a page that draws its own
// chat, where the docked mic sits. So a press on a mic counts only if it went
// down on that mic at least 500 ms after the mic appeared or last moved: a
// press that began earlier, or elsewhere, never starts a recording.
func TestAMicPressCountsOnlyIfItBeganOnTheSettledMic(t *testing.T) {
	shell := readPage(t, "board.js")
	if !strings.Contains(shell, "const MIC_SETTLE_MS = 500;") || !strings.Contains(functionBody(t, shell, "function settleMic("), "micSettlesAt = performance.now() + MIC_SETTLE_MS;") {
		t.Fatal("board.js has no 500 ms settle time for its mic")
	}
	if !strings.Contains(functionBody(t, shell, "function onMicPress("), "micPress = event.isTrusted ? { mic: event.currentTarget, at: performance.now() } : null;") ||
		!strings.Contains(shell, `for (const mic of [controls.mic, controls.chatMic]) mic.addEventListener("pointerdown", onMicPress);`) {
		t.Error("a mic press isn't timed when it goes down")
	}
	if !strings.Contains(functionBody(t, shell, "function onMicTap("), "if (press ? press.mic !== event.currentTarget || press.at < micSettlesAt : event.detail !== 0) return;") {
		t.Error("a tap on the mic is judged by something other than where and when its press began")
	}
	if !strings.Contains(functionBody(t, shell, "function drawVoice("), "if (spot !== placedMic) settleMic(); // it appeared or the page moved it") {
		t.Error("the docked mic takes a tap right after it appears or moves")
	}
	if !strings.Contains(functionBody(t, shell, "function openChat("), "settleMic();") ||
		!strings.Contains(functionBody(t, shell, "function drawComposer("), "if (wasMicHidden && !controls.chatMic.hidden) settleMic();") {
		t.Error("the chat sheet's mic takes a tap right after the sheet opens or the mic comes back")
	}
}

// The page names the chat's part and may give a title, one line and quick
// answers, nothing more: each string is checked and clipped, each quick answer
// is a tap C3 would take, and they reach the sheet only as text.
func TestThePageGivesTheChatOnlyCheckedText(t *testing.T) {
	shell := readPage(t, "board.js")
	read := functionBody(t, shell, "function readChat(")
	for _, rule := range []string{
		`if (!(value === null || typeof value === "string" && PART.test(value) && value !== GENERAL) || ![title, ask, quote].every(isText)) return null;`,
		"new TextEncoder().encode(choice).length <= LIMITS.valueBytes",
		"choices.length > CHAT.choices",
		"title: clip(title ?? \"\", CHAT.titleRunes), ask: clip(ask ?? \"\", CHAT.askRunes)",
	} {
		if !strings.Contains(read, rule) {
			t.Errorf("readChat lacks %q", rule)
		}
	}
	if !strings.Contains(functionBody(t, shell, "async function onFrameMessage("), "const request = readChat(message);") {
		t.Error("a chat message reaches the sheet without readChat")
	}
	draw := functionBody(t, shell, "function drawChat(")
	if !strings.Contains(draw, "controls.chatAsk.textContent = chat.ask;") || !strings.Contains(draw, "element(\"button\", \"\", choice)") {
		t.Error("drawChat shows the page's line or quick answers other than as text")
	}
	if !strings.Contains(functionBody(t, shell, "function element("), "node.append(...children.filter(Boolean));") {
		t.Error("the chat's rows are not built from text nodes")
	}
}

// Only the agent's replies the owner hasn't seen badge the circle: a read mark
// per part, kept in this browser, and nothing else (waiting actions, notices)
// shows a dot. The panel lists the unread parts and shuts on a tap in the page.
func TestOnlyUnreadRepliesBadgeTheCircle(t *testing.T) {
	shell := readPage(t, "board.js")
	if strings.Count(shell, "controls.badge.hidden") != 1 || !strings.Contains(functionBody(t, shell, "function drawBadge("), "controls.badge.hidden = count === 0;") ||
		!strings.Contains(functionBody(t, shell, "function drawBadge("), "const items = unreadParts();") {
		t.Error("something other than the unread replies sets the circle's badge")
	}
	if !strings.Contains(functionBody(t, shell, "function unreadParts("), "entry.id <= Math.max(marks.from, marks.parts.get(entry.part) ?? 0)") ||
		!strings.Contains(shell, "const READ_KEY = `c3.board.read.${name}`;") {
		t.Error("unread is not counted from the read marks this browser keeps")
	}
	if !strings.Contains(functionBody(t, shell, "function markRead("), "if (document.hidden || !state || part !== readingPart) return;") ||
		!strings.Contains(shell, "const READ_AFTER_MS = 1000;") {
		t.Error("a reply counts as read before its chat has shown for a second in a visible tab")
	}
	html := readPage(t, "board.html")
	if !strings.Contains(html, `<ul id="unread"`) || !strings.Contains(html, `<button id="talk" type="button">`) {
		t.Error("the panel has no unread list or no Talk to the agent")
	}
}

// The panel shuts on a tap anywhere else through a clear shell layer over the
// frame, so the tap never reaches the page and the page can't shut the panel.
func TestThePanelShutsThroughTheShellsOwnLayer(t *testing.T) {
	shell, html := readPage(t, "board.js"), readPage(t, "board.html")
	if !strings.Contains(html, `<div id="shield" hidden></div>`) || !regexp.MustCompile(`#shield \{ position: fixed; z-index: 3; inset: 0; \}`).MatchString(html) ||
		!strings.Contains(shell, "controls.shield.hidden = !isPanelOpen;") {
		t.Error("no shell layer covers the frame while the panel is open")
	}
	if strings.Contains(shell, `addEventListener("blur"`) {
		t.Error("the panel still shuts when the shell loses focus, which the page can cause")
	}
}

// The page can shut or switch any chat (the owner's next words then go to the
// part on screen), but never opens the board-level chat, and its acts are
// marked.
func TestThePageSwitchesChatsButNeverOpensTheBoardLevelOne(t *testing.T) {
	shell := readPage(t, "board.js")
	if !strings.Contains(functionBody(t, shell, "async function onFrameMessage("), `: request.part === null ? closeChat() : openChat(request.part, request);`) ||
		strings.Contains(shell, `"owner"`) {
		t.Error("the page can't switch a chat the owner opened from the panel, so the sheet keeps the old thread")
	}
	if !strings.Contains(functionBody(t, shell, "function readChat("), "PART.test(value) && value !== GENERAL") {
		t.Error("the page can open the board-level chat")
	}
	if !strings.Contains(functionBody(t, shell, "async function onFrameMessage("), "await postAct({ ...body, from_page: true })") {
		t.Error("the page's acts reach C3 unmarked")
	}
}

// A chat that opens on unread replies marks where they begin and opens there:
// the mark is taken once, when the chat opens, and drawn above that reply.
func TestAChatOpensAtItsFirstUnreadReply(t *testing.T) {
	shell := readPage(t, "board.js")
	if !strings.Contains(functionBody(t, shell, "function openChat("), "unreadFrom: isNew ? firstUnread(part) : chat.unreadFrom") ||
		!strings.Contains(functionBody(t, shell, "function threadRows("), `entry.id === chat.unreadFrom ? [element("li", "unread-line", "Unread")`) ||
		!strings.Contains(functionBody(t, shell, "function drawChat("), "if (unreadLine) controls.chatScroll.scrollTop = unreadLine.offsetTop - 8;") {
		t.Error("a chat with unread replies doesn't open at the first of them")
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
