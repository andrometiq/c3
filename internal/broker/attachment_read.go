package broker

import (
	"github.com/Andrometiq/c3/internal/channel"
	"github.com/Andrometiq/c3/internal/ipc"
)

// maxStubAttachments bounds the attachments remembered per connection; the
// oldest is dropped first.
const maxStubAttachments = 512

// stubAttachment is one recorded download: the conversation it was returned
// to (the stub's stable session id then) and the resolved path.
type stubAttachment struct {
	sessionID string
	path      string
}

// rememberAttachment records the file a successful download_attachment on
// route returned to stub, for the conversation stub is on now, when it is a
// regular file in that channel's attachment cache (a voice inbox path is not).
// A download before the conversation is known records nothing.
func rememberAttachment(stub *Stub, route RouteKey, result map[string]any) {
	dir, err := channel.AttachmentCacheDir(route.Channel)
	if err != nil {
		return
	}
	path, ok := channel.ResolveAttachment(dir, mcpTextOf(result))
	sessionID := stub.StableSessionIDValue()
	if !ok || sessionID == "" {
		return
	}
	key := stubAttachment{sessionID: sessionID, path: path}
	stub.stubMu.Lock()
	defer stub.stubMu.Unlock()
	if _, isKnown := stub.attachments[key]; isKnown {
		return
	}
	if stub.attachments == nil {
		stub.attachments = map[stubAttachment]string{}
	}
	stub.attachments[key] = route.Channel
	stub.attachmentOrder = append(stub.attachmentOrder, key)
	if len(stub.attachmentOrder) > maxStubAttachments {
		delete(stub.attachments, stub.attachmentOrder[0])
		stub.attachmentOrder = stub.attachmentOrder[1:]
	}
}

// isSessionAttachment reports whether req is a Read of a file
// download_attachment returned to the same conversation on its session's one
// live adapter, so a /clear or a /resume of another conversation on that
// connection does not inherit it, and switching back restores it. The hook's
// path counts only when that record holds it and it still resolves, to itself,
// to a regular file in the recorded channel's attachment cache.
func (b *Broker) isSessionAttachment(req ipc.GrantCheckReq) bool {
	if req.AttachmentPath == "" || req.ToolName != "Read" || req.SessionID == "" {
		return false
	}
	stub := b.sessionAdapter(req.CLI, req.SessionID)
	if stub == nil {
		return false
	}
	stub.stubMu.Lock()
	channelName, isKnown := stub.attachments[stubAttachment{sessionID: req.SessionID, path: req.AttachmentPath}]
	stub.stubMu.Unlock()
	if !isKnown {
		return false
	}
	dir, err := channel.AttachmentCacheDir(channelName)
	if err != nil {
		return false
	}
	path, ok := channel.ResolveAttachment(dir, req.AttachmentPath)
	return ok && path == req.AttachmentPath
}
