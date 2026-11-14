package telegram

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode"

	"github.com/Andrometiq/c3/internal/c3types"
)

const (
	attachmentNameLimit   = 150  // bytes in a sanitised attachment name
	attachmentRecordLimit = 4096 // remembered attachments
)

// extensionByMIME is fixed so names are the same on every host;
// mime.ExtensionsByType reads the host's own tables.
var extensionByMIME = map[string]string{
	"application/json": ".json",
	"application/pdf":  ".pdf",
	"application/zip":  ".zip",
	"application/vnd.openxmlformats-officedocument.wordprocessingml.document":   ".docx",
	"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":         ".xlsx",
	"application/vnd.openxmlformats-officedocument.presentationml.presentation": ".pptx",
	"audio/mpeg":    ".mp3",
	"audio/ogg":     ".ogg",
	"image/gif":     ".gif",
	"image/jpeg":    ".jpg",
	"image/png":     ".png",
	"image/webp":    ".webp",
	"text/csv":      ".csv",
	"text/html":     ".html",
	"text/markdown": ".md",
	"text/plain":    ".txt",
	"video/mp4":     ".mp4",
}

// attachmentRecords remembers emitted attachments by file_id so a download can
// keep the original name, dropping the oldest past the limit. The zero value
// is ready to use.
type attachmentRecords struct {
	mu     sync.Mutex
	byID   map[string]c3types.Attachment
	oldest []string // ring of file ids; next is the slot to reuse
	next   int
}

func (r *attachmentRecords) record(attachments []c3types.Attachment) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.byID == nil {
		r.byID, r.oldest = map[string]c3types.Attachment{}, make([]string, attachmentRecordLimit)
	}
	for _, attachment := range attachments {
		if _, ok := r.byID[attachment.FileID]; !ok {
			delete(r.byID, r.oldest[r.next])
			r.oldest[r.next] = attachment.FileID
			r.next = (r.next + 1) % attachmentRecordLimit
		}
		r.byID[attachment.FileID] = attachment
	}
}

func (r *attachmentRecords) lookup(fileID string) c3types.Attachment {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byID[fileID]
}

// attachmentFileName is a download's name before any sniffing: the recorded
// original name, else the base of Telegram's file_path, given an extension
// from the recorded MIME type, or .jpg for a photo, when it has none.
func attachmentFileName(recorded c3types.Attachment, filePath string) string {
	name := sanitiseAttachmentName(recorded.Name)
	if name == "" {
		name = sanitiseAttachmentName(filePath)
	}
	mimeType := recorded.MIME
	if mimeType == "" && recorded.Kind == "photo" {
		mimeType = "image/jpeg" // Telegram serves every photo as JPEG
	}
	if attachmentExtension(name) != "" {
		return name
	}
	return sanitiseAttachmentName(name + extensionForMIME(mimeType))
}

func extensionForMIME(mimeType string) string {
	mediaType, _, _ := strings.Cut(mimeType, ";")
	return extensionByMIME[strings.ToLower(strings.TrimSpace(mediaType))]
}

// sniffExtension types the file at path from its first 512 bytes.
func sniffExtension(path string) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()
	head := make([]byte, 512)
	if n, _ := io.ReadFull(file, head); n > 0 {
		return extensionForMIME(http.DetectContentType(head[:n]))
	}
	return ""
}

// attachmentExtension is name's extension when it reads as one: a dot, then
// at most 15 ASCII letters or digits, at least one a letter. "Invoice no. 12"
// and "v1.2" have none.
func attachmentExtension(name string) string {
	extension := filepath.Ext(name)
	isNotAlphanumeric := func(r rune) bool { return r > unicode.MaxASCII || !unicode.IsLetter(r) && !unicode.IsDigit(r) }
	if len(extension) > 16 || strings.IndexFunc(extension, unicode.IsLetter) < 0 ||
		strings.IndexFunc(extension[1:], isNotAlphanumeric) >= 0 {
		return ""
	}
	return extension
}

// attachmentNameASCII is the ASCII punctuation an attachment name keeps.
// Shells give meaning only to ASCII characters, so with these, letters and
// digits a pasted path is one inert shell word, quoted or not: no expansion,
// globbing, zsh glob qualifier, word split or second command.
const attachmentNameASCII = "._-+,@=%"

// sanitiseAttachmentName keeps name's last path element ('/' and '\' both
// separate) as one shell-safe word: whitespace runs become '_', ASCII other
// than letters, digits and attachmentNameASCII is dropped, as is non-ASCII
// that is not printable (control, bidi overrides, zero-width). Leading dots,
// underscores and dashes go, and the result is capped at attachmentNameLimit
// bytes keeping the extension and valid UTF-8. "" when nothing is left. The
// sender chooses the name and the agent pastes the path into shell commands.
func sanitiseAttachmentName(name string) string {
	name = name[strings.LastIndexAny(name, `/\`)+1:]
	name = strings.Join(strings.Fields(name), "_")
	name = strings.Map(func(r rune) rune {
		isKept := r > unicode.MaxASCII && unicode.IsGraphic(r) ||
			r <= unicode.MaxASCII && (unicode.IsLetter(r) || unicode.IsDigit(r) || strings.ContainsRune(attachmentNameASCII, r))
		if !isKept {
			return -1
		}
		return r
	}, name)
	for strings.Contains(name, "__") {
		name = strings.ReplaceAll(name, "__", "_")
	}
	name = strings.TrimLeft(name, "._-")
	if len(name) <= attachmentNameLimit {
		return name
	}
	extension := attachmentExtension(name)
	stem := strings.ToValidUTF8(name[:attachmentNameLimit-len(extension)], "")
	return strings.TrimRight(stem, "_") + extension
}

// cachedAttachment is a completed download of uniqueID: path itself, else, when
// Telegram reported the size, any regular file named "<uniqueID>_*" of that
// size (an earlier release's naming, or a sniffed extension). The size guards
// the scan: a file_unique_id can hold '_' and so begin another file's name.
func cachedAttachment(dir, uniqueID, path string, size int64) string {
	isComplete := func(info os.FileInfo) bool { // never a symlink: Lstat and DirEntry.Info describe the link itself
		return info.Mode().IsRegular() && (size == 0 || info.Size() == size)
	}
	if info, err := os.Lstat(path); err == nil && isComplete(info) {
		return path
	}
	entries, err := os.ReadDir(dir)
	if err != nil || uniqueID == "" || size == 0 {
		return ""
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), uniqueID+"_") {
			continue
		}
		if info, err := entry.Info(); err == nil && isComplete(info) {
			return filepath.Join(dir, entry.Name())
		}
	}
	return ""
}

// placeDownload moves the finished temporary to "<uniqueID>_<name>" in dir,
// or, while that name holds another file (a stale one, or another id's, since
// a file_unique_id can hold '_'), to the first free "<uniqueID>_<n>_<name>",
// which the size-guarded scan finds next time. It never replaces a file: one
// of the downloaded size is this same attachment (a concurrent or repeated
// download) and is kept. It returns where the download is.
func placeDownload(temp, dir, uniqueID, name string, size int64) (string, error) {
	for n := 0; ; n++ {
		path := filepath.Join(dir, uniqueID+"_"+name)
		if n > 0 {
			path = filepath.Join(dir, fmt.Sprintf("%s_%d_%s", uniqueID, n, name))
		}
		info, err := os.Lstat(path)
		switch {
		case errors.Is(err, os.ErrNotExist):
			return path, os.Rename(temp, path)
		case err != nil:
			return "", err
		case info.Mode().IsRegular() && info.Size() == size:
			return path, os.Remove(temp)
		}
	}
}
