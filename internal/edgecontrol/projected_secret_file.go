package edgecontrol

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// readPrivateProjectedFile accepts Kubernetes' atomic Secret projection
// symlinks, but only when the resolved regular file stays below the exact
// mount directory and is private to the container identity. The path is
// resolved and reopened for every read so a ..data rotation takes effect
// without retaining a stale credential.
func readPrivateProjectedFile(path string, maxBytes int64) ([]byte, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || maxBytes <= 0 {
		return nil, errors.New("private projected file path or size is invalid")
	}
	root, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return nil, errors.New("private projected file directory is unavailable")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, errors.New("private projected file is unavailable")
	}
	relative, err := filepath.Rel(root, resolved)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, errors.New("private projected file escaped its mount directory")
	}
	file, err := os.Open(resolved)
	if err != nil {
		return nil, errors.New("private projected file could not be opened")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !privateProjectedFileMode(info) || info.Size() <= 0 || info.Size() > maxBytes {
		return nil, errors.New("private projected file mode or size is invalid")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || len(data) == 0 || int64(len(data)) > maxBytes || int64(len(data)) != info.Size() {
		return nil, errors.New("private projected file read is incomplete")
	}
	return data, nil
}

func privateProjectedFileMode(info os.FileInfo) bool {
	if info == nil {
		return false
	}
	switch info.Mode().Perm() {
	case 0o600:
		return true
	case 0o640:
		// Kubelet applies the Pod fsGroup to read-only Secret volumes and
		// ORs the 0440 read mask into the declared 0600 mode. Accept only
		// the exact effective process group; never accept group write/exec
		// or any world permission.
		stat, ok := info.Sys().(*syscall.Stat_t)
		return ok && int(stat.Gid) == os.Getegid()
	default:
		return false
	}
}
