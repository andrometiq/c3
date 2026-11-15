package channel

import "regexp"

// Board names and limits, shared by the broker and the web package.
const (
	BoardChannelName     = "board"
	BoardSelectorPrefix  = BoardChannelName + ":"    // board:<name> on reply, edit_message, react, detach and route refs
	BoardAttachPrefix    = "+" + BoardSelectorPrefix // attach expr +board:<name>
	BoardAssetPathPrefix = "/board/assets/"
	BoardNamePattern     = `^[a-z0-9][a-z0-9-]{0,39}$`
	BoardPartPattern     = `^[A-Za-z0-9._:-]{1,64}$`

	BoardPageMaxBytes      = 5 << 20
	BoardTextMaxRunes      = 16000
	BoardQuoteMaxRunes     = 2000
	BoardTapValueMaxBytes  = 64
	BoardKeepMaxBytes      = 64 << 10
	BoardActsPerMinute     = 30 // per board
	BoardRevisionsKept     = 5
	BoardLogHistoryEntries = 500
)

var (
	boardNameRule = regexp.MustCompile(BoardNamePattern)
	boardPartRule = regexp.MustCompile(BoardPartPattern)
)

// IsValidBoardName reports whether name may name a board; it is also the
// board's storage directory name.
func IsValidBoardName(name string) bool { return boardNameRule.MatchString(name) }

// IsValidBoardPart reports whether part may address a data-c3-part element.
func IsValidBoardPart(part string) bool { return boardPartRule.MatchString(part) }
