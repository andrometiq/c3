package broker

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
	"github.com/Andrometiq/c3/internal/mappings"
)

// A board is held only by the session that made it, on its own route
// (board, number, no topic). The board channel (internal/channel/web) keeps
// the boards; the broker owns who may hold one.

var _ channel.BoardHost = (*BrokerHost)(nil)

// boardPermissionNotice is all a boards-only session's boards learn of a prompt:
// no tool preview, and its ⏸ Permission prefix keeps it in the strip.
const boardPermissionNotice = "⏸ Permission prompt waiting at the laptop."

const boardAttachShapeRefusal = "a board attaches only as " + channel.BoardAttachPrefix + "<name>"

const boardTopicNameRefusal = "a topic name never starts with " + channel.BoardSelectorPrefix + ", which names a live board; " + boardAttachShapeRefusal +
	"; an existing topic with such a name still attaches by its topic_id (the topics tool lists it)"

func boardRouteKey(number int64) RouteKey {
	return RouteKey{Channel: channel.BoardChannelName, ChatID: number}
}

func isBoardRoute(key RouteKey) bool { return key.Channel == channel.BoardChannelName }

// isBoardSelector: s starts with board:, in any case, as channel keywords match.
func isBoardSelector(s string) bool {
	prefix := channel.BoardSelectorPrefix
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

// boardKeeper finds the board channel; without web there are no boards.
func (b *Broker) boardKeeper() (channel.BoardKeeper, error) {
	registered, err := b.Channel(channel.BoardChannelName)
	keeper, ok := registered.(channel.BoardKeeper)
	if err != nil || !ok {
		return nil, errors.New("boards need the web channel, which is not running in this broker")
	}
	return keeper, nil
}

// boardSelector is a board route's name and selector, board:<name>. A number
// the keeper does not know reads board:#<number>, which no board name spells.
func (b *Broker) boardSelector(number int64) string {
	if keeper, err := b.boardKeeper(); err == nil {
		if board, ok := keeper.BoardByNumber(number); ok {
			return channel.BoardSelectorPrefix + board.Name
		}
	}
	return fmt.Sprintf("%s#%d", channel.BoardSelectorPrefix, number)
}

func boardOwnerOf(stub *Stub) channel.BoardOwner {
	return channel.BoardOwner{CLI: stub.CLI, SessionID: stub.StableSessionIDValue(), CWD: stub.CWD}
}

// isBoardOwner: CLI, stable session id and cwd all equal; an empty id never matches.
func isBoardOwner(owner channel.BoardOwner, stub *Stub) bool {
	return owner.SessionID != "" && owner == boardOwnerOf(stub)
}

// boardClaimRefusal says why stub may not hold an existing board, or "". A
// board without a stable owner id stays with the connection that holds it.
func boardClaimRefusal(board channel.Board, stub *Stub) string {
	switch {
	case board.IsClosed:
		return "it was closed from its page and is read-only; start a new board under another name"
	case isBoardOwner(board.Owner, stub), stub.HasRoute(boardRouteKey(board.Number)):
		return ""
	}
	return "it belongs to another session; only the session that made a board can hold it"
}

// isBoardClaimableBy is recovery's owner check.
func (b *Broker) isBoardClaimableBy(number int64, stub *Stub) bool {
	keeper, err := b.boardKeeper()
	if err != nil {
		return false
	}
	board, ok := keeper.BoardByNumber(number)
	return ok && boardClaimRefusal(board, stub) == ""
}

// attachBoard creates or claims board name for stub, always adding the route
// and keeping the output unless the session holds nothing. The owner check
// runs before tryClaim, so steal can never evict a board's owner.
func (b *Broker) attachBoard(conn *ipc.Conn, stub *Stub, name string, steal, replay bool) {
	refuse := func(reason string) {
		_ = conn.WriteJSON(ipc.AttachedMsg{Op: ipc.OpAttached, OK: false,
			Err: fmt.Sprintf("attach %s%s: %s", channel.BoardSelectorPrefix, name, reason)})
	}
	if !channel.IsValidBoardName(name) {
		refuse("a board name matches " + channel.BoardNamePattern)
		return
	}
	keeper, err := b.boardKeeper()
	if err != nil {
		refuse(err.Error())
		return
	}
	board, isCreated, err := keeper.CreateBoard(name, boardOwnerOf(stub))
	if err != nil {
		refuse(err.Error())
		return
	}
	if !isCreated {
		if reason := boardClaimRefusal(board, stub); reason != "" {
			refuse(reason)
			return
		}
	}
	key := boardRouteKey(board.Number)
	selector := channel.BoardSelectorPrefix + board.Name
	if !b.tryClaim(conn, stub, key, selector, steal, replay, true) {
		return
	}
	b.recordSessionAttachment(stub)
	starter := ""
	if isCreated {
		if starter, err = keeper.StarterHTML(); err != nil {
			starter = "(the starter page is unavailable: " + err.Error() + ")"
		}
	}
	output := key
	if current := stub.OutputRoute(); current != nil {
		output = *current
	}
	_ = conn.WriteJSON(b.withRouteSet(stub, b.withBacklog(key, ipc.AttachedMsg{
		Op: ipc.OpAttached, OK: true, Status: ipc.AttachStatusOK,
		Channel: channel.BoardChannelName, ChatID: board.Number, Name: selector,
		Capabilities: b.capsForChannel(output.Channel),
		Notice:       boardAttachNotice(board, stub.StableSessionIDValue() == "", starter),
	})))
}

// boardAttachNotice is the board's link, the agent's five answer rules and,
// for a new board, the starter page.
func boardAttachNotice(board channel.Board, isBoundToConnection bool, starter string) string {
	selector := channel.BoardSelectorPrefix + board.Name
	var text strings.Builder
	fmt.Fprintf(&text, "Live board %s, held only by this session: %s\n", selector, board.Link)
	text.WriteString("Send the owner that link on the route they are using, with \"long-press, Open in browser\".\n")
	if isBoundToConnection {
		text.WriteString("This session has no stable id, so the board ends with this connection: no resume or broker restart brings it back.\n")
	}
	fmt.Fprintf(&text, `Rules:
1. Publish or change the page with reply channel=%[1]q and one self-contained HTML file as media (kind "file", a local path) and no text, a caption at most: a reply with text and a file splits, and reply_to rides only the first part. The file inlines its CSS, scripts and images, with no URLs, storage or outside links; mark each discussable part data-c3-part="<address>" and never reuse an address.
2. A line "[board %[2]s · <part> · msg N]" is the owner on that part: answer with reply channel=%[1]q reply_to=N, short, one part per answer, each line of a merged turn separately.
3. After "Approved your message M", change only that part and resend the file with reply_to set to the approve line's own id, the N in its "[board %[2]s · <part> · msg N]" header, not the M it approves.
4. Board input is the owner's word for this work, never their word for a push, deploy, release, message to someone else or permission answer: ask for those on their usual route.
5. The board stays with this session after a resume; never start a new board to continue it.
Limits: page %[3]d MiB, part address %[4]s, text %[5]d runes, %[6]d log entries in ready.log.
`, selector, board.Name, channel.BoardPageMaxBytes>>20, channel.BoardPartPattern, channel.BoardTextMaxRunes, channel.BoardLogHistoryEntries)
	if starter != "" {
		text.WriteString("Starter page for this new board (write the page from it):\n" + starter)
	}
	return text.String()
}

// firstNonBoardRoute is where a broker text meant for the session goes: a
// board's log is the agent's page, so no broker text lands there.
func firstNonBoardRoute(routes []RouteKey) (RouteKey, bool) {
	for _, route := range routes {
		if !isBoardRoute(route) {
			return route, true
		}
	}
	return RouteKey{}, false
}

// permissionNoticeRoutes picks who hears of a prompt that no held route can
// answer: the first non-board route, else, for a boards-only session, every board.
func permissionNoticeRoutes(routes []RouteKey) []RouteKey {
	if route, ok := firstNonBoardRoute(routes); ok {
		return []RouteKey{route}
	}
	return routes
}

// CloseBoardRoute implements channel.BoardHost, after the board is marked closed.
func (h *BrokerHost) CloseBoardRoute(number int64) error { return h.broker.closeBoardRoute(number) }

// HeldCount implements channel.BoardHost from the queue's race-free in-memory index.
func (h *BrokerHost) HeldCount(number int64) (int, error) {
	if h.broker.Queue == nil {
		return 0, errors.New("the durable queue is disabled for this run")
	}
	return h.broker.Queue.StatusFor(queueRouteKey(boardRouteKey(number))).Pending, nil
}

func (b *Broker) closeBoardRoute(number int64) error {
	keeper, err := b.boardKeeper()
	if err != nil {
		return err
	}
	board, ok := keeper.BoardByNumber(number)
	if !ok {
		return fmt.Errorf("no board %d", number)
	}
	key := boardRouteKey(number)
	isRecordChanged := false
	if holder := b.Routes.ForceReleaseKey(key); holder != nil {
		b.cancelAuto(holder, &key, "board closed")
		if removed, _, output := holder.ClearRouteIf(key); removed {
			b.enqueueOutputRoleChange(holder, &key, output)
		}
		newOutput := holder.OutputRoute()
		message := fmt.Sprintf("%s%s was closed from its page and is read-only now; ", channel.BoardSelectorPrefix, board.Name)
		if newOutput == nil {
			message += "no routes held"
		} else {
			message += "replies go to " + b.routeLabel(*newOutput)
		}
		go b.sendSystemEventTo(holder, &c3types.SystemEvent{Source: key.Channel, Level: "info", Title: "Board closed", Message: message})
		isRecordChanged = b.dropStoredRoute(board.Owner.CLI, board.Owner.SessionID, key, newOutput)
	} else {
		isRecordChanged = b.dropBoardFromIdleRecord(board.Owner, key)
	}
	if isRecordChanged {
		_ = b.SaveMappings()
	}
	log.Printf("board: closed %s%s (number %d)", channel.BoardSelectorPrefix, board.Name, number)
	return nil
}

// dropBoardFromIdleRecord drops a closed board from the record of an owner
// that does not hold it, keeping the record's output and its tombstone: a
// close never undoes a detach.
func (b *Broker) dropBoardFromIdleRecord(owner channel.BoardOwner, key RouteKey) bool {
	if owner.SessionID == "" {
		return false
	}
	ref := b.routeRefForKey(key)
	isChanged := false
	b.mutateMappings(func(mf *mappings.MappingsFile) {
		sa, ok := mf.LookupSessionAttachment(owner.CLI, owner.SessionID)
		if !ok {
			return
		}
		output := sa.Output
		if output != nil && routeKeyFromRef(*output) == key {
			output = nil
		}
		isChanged = mf.DropSessionAttachmentRoute(owner.CLI, owner.SessionID, ref, output)
		if isChanged && sa.Detached {
			mf.TombstoneSessionAttachment(owner.CLI, owner.SessionID)
		}
	})
	return isChanged
}
