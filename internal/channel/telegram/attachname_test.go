package telegram

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"github.com/PaulSonOfLars/gotgbot/v2"

	"github.com/Andrometiq/c3/internal/c3types"
	"github.com/Andrometiq/c3/internal/channel"
)

func TestAttachmentFileName_KeepsTheOriginalNameAndATypedExtension(t *testing.T) {
	cases := []struct {
		name     string
		recorded c3types.Attachment
		filePath string
		want     string
	}{
		{name: "document with a name", filePath: "documents/file_4977",
			recorded: c3types.Attachment{Kind: "document", Name: "Cash Crunch Report.markdown", MIME: "text/markdown"},
			want:     "Cash Crunch Report.markdown"},
		{name: "document without an extension takes it from the MIME type", filePath: "documents/file_5",
			recorded: c3types.Attachment{Kind: "document", Name: "Quarterly report", MIME: "application/pdf"},
			want:     "Quarterly report.pdf"},
		{name: "a dot inside the name is not an extension", filePath: "documents/file_6",
			recorded: c3types.Attachment{Kind: "document", Name: "Invoice no. 12", MIME: "application/pdf"},
			want:     "Invoice no. 12.pdf"},
		{name: "MIME parameters and case are ignored", filePath: "documents/file_7",
			recorded: c3types.Attachment{Kind: "document", Name: "notes", MIME: "Text/Plain; charset=utf-8"},
			want:     "notes.txt"},
		{name: "photo", filePath: "photos/file_4976",
			recorded: c3types.Attachment{Kind: "photo"}, want: "file_4976.jpg"},
		{name: "photo whose Telegram name has an extension", filePath: "photos/file_8.png",
			recorded: c3types.Attachment{Kind: "photo"}, want: "file_8.png"},
		{name: "nothing recorded keeps the Telegram name", filePath: "voice/file_1.oga", want: "file_1.oga"},
		{name: "unknown type is left for sniffing", filePath: "documents/file_9", want: "file_9"},
		{name: "MIME type not in the table", filePath: "documents/file_10",
			recorded: c3types.Attachment{Kind: "document", Name: "blob", MIME: "application/x-unknown"}, want: "blob"},
		{name: "a name that sanitises to nothing falls back to Telegram's", filePath: "documents/file_11",
			recorded: c3types.Attachment{Kind: "document", Name: "...", MIME: "application/pdf"}, want: "file_11.pdf"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := attachmentFileName(c.recorded, c.filePath); got != c.want {
				t.Fatalf("attachmentFileName = %q, want %q", got, c.want)
			}
		})
	}
}

func TestSanitiseAttachmentName(t *testing.T) {
	cases := []struct{ in, want string }{
		{in: "../../etc/passwd", want: "passwd"},
		{in: `C:\Users\someone\report.pdf`, want: "report.pdf"},
		{in: `a<b>c:d"e|f?g*h.txt`, want: "abcdefgh.txt"},
		{in: "Q3 report $(touch PWNED).pdf", want: "Q3 report (touch PWNED).pdf"},
		{in: "Q3 report `touch PWNED`.pdf", want: "Q3 report touch PWNED.pdf"},
		{in: "a;touch PWNED;.zip", want: "atouch PWNED.zip"},
		{in: "x'$(touch PWNED)'.xlsx", want: "x(touch PWNED).xlsx"},
		{in: "John's CV & cover!.pdf", want: "Johns CV  cover.pdf"},
		{in: "Budget $500.xlsx", want: "Budget 500.xlsx"},
		{in: "line\nbreak\x00\x7f\u0085.txt", want: "linebreak.txt"},
		{in: "invoice\u202Efdp.exe", want: "invoicefdp.exe"},
		{in: "zero\u200Bwidth\uFEFF.txt", want: "zerowidth.txt"},
		{in: "line\u2028sep\u2029para.txt", want: "lineseppara.txt"},
		{in: "..hidden.txt", want: "hidden.txt"},
		{in: "  . two  words .txt  ", want: "two  words .txt"},
		{in: strings.Repeat("a", 200), want: strings.Repeat("a", attachmentNameLimit)},
		{in: strings.Repeat("€", 70) + ".pdf", want: strings.Repeat("€", 48) + ".pdf"},
		{in: "", want: ""},
		{in: "<>|", want: ""},
		{in: "dir/", want: ""},
	}
	for _, c := range cases {
		got := sanitiseAttachmentName(c.in)
		if got != c.want {
			t.Errorf("sanitiseAttachmentName(%q) = %q, want %q", c.in, got, c.want)
		}
		if len(got) > attachmentNameLimit || !utf8.ValidString(got) {
			t.Errorf("sanitiseAttachmentName(%q) = %q: %d bytes, valid UTF-8 %v", c.in, got, len(got), utf8.ValidString(got))
		}
	}
}

// attachmentServer is a channel whose fake Bot API answers getFile with
// getFile and serves body for every download, counting the downloads.
func attachmentServer(t *testing.T, getFile, body string) (*Channel, *atomic.Int32) {
	t.Helper()
	var downloads atomic.Int32
	c := getFileChannelHandler(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/file/") {
			downloads.Add(1)
			_, _ = w.Write([]byte(body))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(getFile))
	})
	return c, &downloads
}

func getFileBody(uniqueID, filePath string, size int) string {
	return fmt.Sprintf(`{"ok":true,"result":{"file_id":"F","file_unique_id":%q,"file_size":%d,"file_path":%q}}`,
		uniqueID, size, filePath)
}

// An attachment the channel emitted downloads under its original name: a
// document keeps its file name, a photo gets .jpg.
func TestDownloadAttachment_KeepsTheNameOfAnEmittedAttachment(t *testing.T) {
	cases := []struct {
		name     string
		message  gotgbot.Message
		filePath string
		want     string
	}{
		{name: "document", filePath: "documents/file_4977", want: "AgADdoc_Cash Crunch Report.markdown",
			message: gotgbot.Message{Document: &gotgbot.Document{FileId: "F", FileUniqueId: "AgADdoc",
				FileName: "Cash Crunch Report.markdown", MimeType: "text/markdown"}}},
		{name: "photo", filePath: "photos/file_4976", want: "AgADdoc_file_4976.jpg",
			message: gotgbot.Message{Photo: []gotgbot.PhotoSize{{FileId: "F", FileUniqueId: "AgADdoc", Width: 800, Height: 600}}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fetchVoiceEnv(t)
			ch, _ := attachmentServer(t, getFileBody("AgADdoc", c.filePath, 5), "bytes")
			message := c.message
			message.MessageId, message.Date = 5, 1715151931
			message.From, message.Chat = &gotgbot.User{Id: 42}, gotgbot.Chat{Id: 42}
			ch.dispatchMessage(1, &message, false, nil)

			path, err := ch.DownloadAttachment("F")
			if err != nil {
				t.Fatalf("download: %v", err)
			}
			if got, _ := os.ReadFile(path); filepath.Base(path) != c.want || string(got) != "bytes" {
				t.Fatalf("downloaded %s = %q, want %s", path, got, c.want)
			}
		})
	}
}

// With no name, MIME type or kind to go on, the downloaded bytes type the file,
// and the next download is served from the cache under that name.
func TestDownloadAttachment_SniffsTheExtensionWhenNothingNamesIt(t *testing.T) {
	fetchVoiceEnv(t)
	png := "\x89PNG\r\n\x1a\n" + strings.Repeat("\x00", 24)
	c, downloads := attachmentServer(t, getFileBody("U", "documents/file_9", len(png)), png)

	for range 2 {
		path, err := c.DownloadAttachment("F")
		if err != nil {
			t.Fatalf("download: %v", err)
		}
		if filepath.Base(path) != "U_file_9.png" {
			t.Fatalf("downloaded %s, want U_file_9.png", path)
		}
	}
	if downloads.Load() != 1 {
		t.Fatalf("%d downloads, want 1 (the second is a cache hit)", downloads.Load())
	}
}

// A file cached under the old naming (<unique id>_<Telegram name>) is still
// served once a record would name it differently.
func TestDownloadAttachment_ServesAFileCachedUnderTheOldName(t *testing.T) {
	attachments, _ := fetchVoiceEnv(t)
	legacy := filepath.Join(attachments, "U_file_1.oga")
	if err := os.MkdirAll(attachments, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(legacy, []byte("OGGDATA"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, downloads := attachmentServer(t, okGetFileBody, "network")
	c.attachmentRecords.record([]c3types.Attachment{{Kind: "document", FileID: "F", Name: "Report.pdf"}})

	path, err := c.DownloadAttachment("F")
	if err != nil || path != legacy || downloads.Load() != 0 {
		t.Fatalf("path %s, err %v, %d downloads; want the legacy file and no download", path, err, downloads.Load())
	}
}

// Neither an in-flight temporary, a same-prefix file of another size, nor a
// symlink is served as the cached attachment.
func TestDownloadAttachment_NeverServesATemporaryWrongSizedFileOrSymlink(t *testing.T) {
	attachments, _ := fetchVoiceEnv(t)
	if err := os.MkdirAll(attachments, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		channel.TempPrefix + "U_file_1.oga.part": "OGGDATA",   // fresh, so the sweep keeps it
		"U_other_file.oga":                       "different", // a longer unique id, e.g. "U_other"
	} {
		if err := os.WriteFile(filepath.Join(attachments, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if runtime.GOOS != "windows" {
		elsewhere := filepath.Join(t.TempDir(), "elsewhere.oga")
		if err := os.WriteFile(elsewhere, []byte("OGGDATA"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(elsewhere, filepath.Join(attachments, "U_link.oga")); err != nil {
			t.Fatal(err)
		}
	}
	c, downloads := attachmentServer(t, okGetFileBody, "NETWORK")

	path, err := c.DownloadAttachment("F")
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	if got, _ := os.ReadFile(path); filepath.Base(path) != "U_file_1.oga" || string(got) != "NETWORK" || downloads.Load() != 1 {
		t.Fatalf("served %s = %q after %d downloads; want a fresh U_file_1.oga", path, got, downloads.Load())
	}
}

func writeCached(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// The exact computed name wins over any other "<unique id>_*" file.
func TestDownloadAttachment_PrefersTheExactPath(t *testing.T) {
	attachments, _ := fetchVoiceEnv(t)
	writeCached(t, attachments, map[string]string{"U_aaa.oga": "OTHERDA", "U_file_1.oga": "EXACTOK"})
	c, downloads := attachmentServer(t, okGetFileBody, "NETWORK")

	path, err := c.DownloadAttachment("F")
	if err != nil || filepath.Base(path) != "U_file_1.oga" || downloads.Load() != 0 {
		t.Fatalf("path %s, err %v, %d downloads; want the exact cached file", path, err, downloads.Load())
	}
}

// Without a size from Telegram the "<unique id>_*" scan is skipped, so another
// id's file is never served; the repeated download keeps the file it wrote.
func TestDownloadAttachment_ScansOnlyWithAReportedSize(t *testing.T) {
	attachments, _ := fetchVoiceEnv(t)
	writeCached(t, attachments, map[string]string{"U_x_other.txt": "plain text"})
	c, downloads := attachmentServer(t, getFileBody("U", "documents/file_9", 0), "plain text")

	for want := range []int32{1, 2} {
		path, err := c.DownloadAttachment("F")
		if err != nil || filepath.Base(path) != "U_file_9.txt" || downloads.Load() != int32(want+1) {
			t.Fatalf("path %s, err %v, %d downloads; want a network U_file_9.txt", path, err, downloads.Load())
		}
	}
}

// A file already at the computed name that is not this download (a stale or
// edited copy, or another id's file) is never replaced, and does not block the
// download: it lands under the first free "<id>_<n>_<name>", which the next
// download serves from the cache.
func TestDownloadAttachment_PlacesTheDownloadBesideAnotherFileAtTheSameName(t *testing.T) {
	attachments, _ := fetchVoiceEnv(t)
	writeCached(t, attachments, map[string]string{"U_file_1.oga": "different"})
	c, downloads := attachmentServer(t, okGetFileBody, "NETWORK")

	for range 2 {
		path, err := c.DownloadAttachment("F")
		if got, _ := os.ReadFile(path); err != nil || filepath.Base(path) != "U_1_file_1.oga" || string(got) != "NETWORK" {
			t.Fatalf("download %s = %q, err %v; want U_1_file_1.oga from the network", path, got, err)
		}
	}
	entries, _ := os.ReadDir(attachments)
	if got, _ := os.ReadFile(filepath.Join(attachments, "U_file_1.oga")); string(got) != "different" || len(entries) != 2 || downloads.Load() != 1 {
		t.Fatalf("existing file now %q, %d entries, %d downloads; want it untouched, no temporary, one download", got, len(entries), downloads.Load())
	}
}

func TestAttachmentRecords_DropTheOldestBeyondTheLimit(t *testing.T) {
	var records attachmentRecords
	for i := range attachmentRecordLimit + 1 {
		records.record([]c3types.Attachment{{FileID: fmt.Sprint(i), Name: fmt.Sprint("name-", i)}})
	}
	records.record([]c3types.Attachment{{FileID: "1", Name: "renamed"}}) // a re-record evicts nothing
	if got := records.lookup("0"); got.Name != "" {
		t.Fatalf("the oldest record survived: %+v", got)
	}
	if records.lookup("1").Name != "renamed" || records.lookup("2").Name != "name-2" ||
		records.lookup(fmt.Sprint(attachmentRecordLimit)).Name != fmt.Sprint("name-", attachmentRecordLimit) {
		t.Fatal("a record within the limit was lost")
	}
}
