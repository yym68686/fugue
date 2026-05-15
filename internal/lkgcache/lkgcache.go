package lkgcache

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
)

type Candidate struct {
	Path string
	Data []byte
}

func PreviousPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	return path + ".previous"
}

func ArchiveDir(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	return path + ".versions"
}

func ArchivePath(path, generation string) string {
	dir := ArchiveDir(path)
	name := sanitizeName(generation)
	if dir == "" || name == "" {
		return ""
	}
	return filepath.Join(dir, name+".json")
}

func WriteCurrent(path, generation string, data []byte, limit int) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if generationPath := ArchivePath(path, generation); generationPath != "" {
		if err := os.MkdirAll(filepath.Dir(generationPath), 0o755); err != nil {
			return err
		}
		if err := AtomicWriteFile(generationPath, data, 0o600); err != nil {
			return fmt.Errorf("write cache archive: %w", err)
		}
	}
	if err := AtomicWriteFile(path, data, 0o600); err != nil {
		return err
	}
	PruneArchives(path, limit)
	return nil
}

func PreservePrevious(path string, validCurrent func([]byte) bool) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil || len(strings.TrimSpace(string(data))) == 0 {
		return err
	}
	if validCurrent != nil && !validCurrent(data) {
		return nil
	}
	return AtomicWriteFile(PreviousPath(path), data, 0o600)
}

func FallbackCandidates(path string) []Candidate {
	out := make([]Candidate, 0)
	if previous := PreviousPath(path); previous != "" {
		if data, err := os.ReadFile(previous); err == nil && len(strings.TrimSpace(string(data))) > 0 {
			out = append(out, Candidate{Path: previous, Data: data})
		}
	}
	entries := archiveEntries(path)
	for _, entry := range entries {
		data, err := os.ReadFile(entry.path)
		if err != nil || len(strings.TrimSpace(string(data))) == 0 {
			continue
		}
		out = append(out, Candidate{Path: entry.path, Data: data})
	}
	return out
}

func AtomicWriteFile(path string, data []byte, perm os.FileMode) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	syncDir(dir)
	return nil
}

func PruneArchives(path string, limit int) {
	if limit <= 0 {
		return
	}
	entries := archiveEntries(path)
	for index, entry := range entries {
		if index < limit {
			continue
		}
		_ = os.Remove(entry.path)
	}
}

type archiveEntry struct {
	path    string
	modTime int64
}

func archiveEntries(path string) []archiveEntry {
	dir := ArchiveDir(path)
	if dir == "" {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	out := make([]archiveEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		out = append(out, archiveEntry{path: filepath.Join(dir, entry.Name()), modTime: info.ModTime().UnixNano()})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].modTime != out[j].modTime {
			return out[i].modTime > out[j].modTime
		}
		return out[i].path > out[j].path
	})
	return out
}

func syncDir(dir string) {
	handle, err := os.Open(dir)
	if err != nil {
		return
	}
	defer handle.Close()
	_ = handle.Sync()
}

func sanitizeName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	var builder strings.Builder
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '_' || r == '-' {
			builder.WriteRune(r)
			continue
		}
		builder.WriteByte('_')
	}
	return strings.Trim(builder.String(), "._-")
}
