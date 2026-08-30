package web

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var documentExtensions = []string{".html", ".htm"}

func (c *Channel) filesDirectoryPath() (string, error) {
	directory := c.stateDirectory
	if directory == "" {
		var err error
		directory, err = stateDir()
		if err != nil {
			return "", err
		}
	}
	return filepath.Join(directory, filesDirectoryName), nil
}

func (c *Channel) ensureFilesDirectory() (string, error) {
	directory, err := c.filesDirectoryPath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("web: create files directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return "", fmt.Errorf("web: protect files directory: %w", err)
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("web: files directory is not a private directory")
	}
	return directory, nil
}

func allowedDocumentExtension(name string) (string, bool) {
	extension := strings.ToLower(filepath.Ext(name))
	for _, allowed := range documentExtensions {
		if extension == allowed {
			return extension, true
		}
	}
	return "", false
}

func (c *Channel) storeDocument(path, name string) (string, int, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", 0, fmt.Errorf("web: document file not found: %s", path)
		}
		return "", 0, fmt.Errorf("web: stat document file %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return "", 0, fmt.Errorf("web: document path is not a regular file: %s", path)
	}
	if info.Size() > maxDocumentBytes {
		return "", 0, fmt.Errorf("web: document file too large: %s is %d bytes, over the %d-byte (5 MiB) send limit", path, info.Size(), maxDocumentBytes)
	}
	extension, ok := allowedDocumentExtension(name)
	if !ok {
		return "", 0, fmt.Errorf("web: document extension %q is not supported; send an .html or .htm file", filepath.Ext(name))
	}

	source, err := os.Open(path)
	if err != nil {
		return "", 0, fmt.Errorf("web: open document file %s: %w", path, err)
	}
	defer source.Close()
	openedInfo, err := source.Stat()
	if err != nil {
		return "", 0, fmt.Errorf("web: inspect opened document file %s: %w", path, err)
	}
	if !openedInfo.Mode().IsRegular() || openedInfo.Size() > maxDocumentBytes {
		return "", 0, fmt.Errorf("web: document file changed while opening: %s", path)
	}
	return c.retainDocument(source, extension)
}

func (c *Channel) retainDocument(source io.Reader, extension string) (string, int, error) {
	directory, err := c.ensureFilesDirectory()
	if err != nil {
		return "", 0, err
	}
	token, err := randomHexID()
	if err != nil {
		return "", 0, fmt.Errorf("web: create document token: %w", err)
	}
	destinationPath := filepath.Join(directory, token+extension)
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY|openNoFollow, 0o600)
	if err != nil {
		return "", 0, fmt.Errorf("web: create retained document: %w", err)
	}
	removeDestination := true
	defer func() {
		if removeDestination {
			_ = os.Remove(destinationPath)
		}
	}()

	written, copyErr := io.Copy(destination, io.LimitReader(source, maxDocumentBytes+1))
	if copyErr == nil && written > maxDocumentBytes {
		copyErr = fmt.Errorf("document grew beyond the %d-byte (5 MiB) send limit while being copied", maxDocumentBytes)
	}
	if copyErr == nil {
		copyErr = destination.Sync()
	}
	closeErr := destination.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return "", 0, fmt.Errorf("web: retain document: %w", copyErr)
	}
	if err := os.Chmod(destinationPath, 0o600); err != nil {
		return "", 0, fmt.Errorf("web: protect retained document: %w", err)
	}
	if err := syncDirectory(directory); err != nil {
		return "", 0, fmt.Errorf("web: sync files directory: %w", err)
	}
	removeDestination = false
	c.pruneFiles()
	return token, int(written), nil
}

func (c *Channel) localFilePath(token string) (string, error) {
	decoded, err := hex.DecodeString(token)
	if err != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != token {
		return "", errors.New("web: invalid local file token")
	}
	directory, err := c.filesDirectoryPath()
	if err != nil {
		return "", err
	}
	directoryInfo, err := os.Lstat(directory)
	if err != nil || !directoryInfo.IsDir() || directoryInfo.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("web: local files directory unavailable")
	}

	var found string
	for _, extension := range documentExtensions {
		path := filepath.Join(directory, token+extension)
		if filepath.Dir(path) != filepath.Clean(directory) {
			return "", errors.New("web: local file path escaped files directory")
		}
		info, statErr := os.Lstat(path)
		if os.IsNotExist(statErr) {
			continue
		}
		if statErr != nil || !info.Mode().IsRegular() || found != "" {
			return "", errors.New("web: local file unavailable")
		}
		found = path
	}
	if found == "" {
		return "", errors.New("web: local file unavailable")
	}
	return found, nil
}

func (c *Channel) pruneFiles() {
	directory, err := c.filesDirectoryPath()
	if err != nil {
		return
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return
	}
	type retainedFile struct {
		path     string
		name     string
		modified time.Time
	}
	files := make([]retainedFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if _, ok := allowedDocumentExtension(entry.Name()); !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		files = append(files, retainedFile{
			path: filepath.Join(directory, entry.Name()), name: entry.Name(), modified: info.ModTime(),
		})
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].modified.Equal(files[j].modified) {
			return files[i].name > files[j].name
		}
		return files[i].modified.After(files[j].modified)
	})
	if filesRetention >= len(files) {
		return
	}
	for _, file := range files[filesRetention:] {
		if err := os.Remove(file.path); err != nil && c.host != nil {
			c.host.Logf("web: prune retained document %s: %v", file.name, err)
		}
	}
}
