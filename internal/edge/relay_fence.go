package edge

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"fugue/internal/edge/relayroute"
	"fugue/internal/lkgcache"
)

type relayPolicyFenceRecord struct {
	Edge       string `json:"edge"`
	Slot       string `json:"slot"`
	Verifier   string `json:"verifier"`
	Generation uint64 `json:"generation"`
	Hash       string `json:"payload_sha256"`
}

// One retained, private state file belongs to one Edge/slot and has one
// writer. The publisher must never restore it from an older snapshot.
type relayPolicyFence struct {
	path    string
	last    relayPolicyFenceRecord
	durable bool
}

func openRelayPolicyFence(path, edge, slot string, verifier ed25519.PublicKey) (*relayPolicyFence, error) {
	bad := errors.New("relay fence unavailable or invalid")
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == filepath.Dir(path) ||
		edge == "" || slot == "" || len(verifier) != ed25519.PublicKeySize {
		return nil, bad
	}
	dir, err := os.Stat(filepath.Dir(path))
	if err != nil || !dir.IsDir() || dir.Mode().Perm()&0077 != 0 {
		return nil, bad
	}
	f := &relayPolicyFence{path: path, last: relayPolicyFenceRecord{Edge: edge, Slot: slot, Verifier: hex.EncodeToString(verifier)}}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return f, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, bad
	}
	raw, err := relayReadFile(path, 4096)
	var saved relayPolicyFenceRecord
	if err != nil || relayroute.Decode(bytes.NewReader(raw), &saved) != nil || saved.Edge != edge ||
		saved.Slot != slot || saved.Verifier != f.last.Verifier || saved.Generation == 0 {
		return nil, bad
	}
	hash, err := hex.DecodeString(saved.Hash)
	if err != nil || len(hash) != 32 {
		return nil, bad
	}
	f.last = saved
	f.durable = true
	return f, nil
}

func (f *relayPolicyFence) accept(p *relayroute.Policy) error {
	if f == nil || (f.last.Generation == 0 && p.Mode != "off") {
		return errors.New("relay fence requires a signed off policy to bootstrap")
	}
	hash := hex.EncodeToString(p.Hash[:])
	if p.Generation < f.last.Generation || (p.Generation == f.last.Generation && hash != f.last.Hash) {
		return errors.New("relay policy generation regressed or conflicts")
	}
	if p.Generation == f.last.Generation && f.durable {
		return nil
	}
	// Never resume an older lease in this process after a verified newer lease
	// arrived, even when the retained directory is temporarily unwritable.
	f.last.Generation, f.last.Hash, f.durable = p.Generation, hash, false
	raw, err := json.Marshal(f.last)
	if err != nil {
		return errors.New("relay fence could not be encoded")
	}
	dir, err := os.Open(filepath.Dir(f.path))
	if err != nil {
		return errors.New("relay fence directory could not be opened")
	}
	defer dir.Close()
	info, err := dir.Stat()
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("relay fence directory is not private")
	}
	if err := lkgcache.AtomicWriteFile(f.path, raw, 0600); err != nil {
		return errors.New("relay fence could not be persisted")
	}
	// Treat failure to sync the directory as a failed publication, even if the
	// renamed file is already visible on the current filesystem.
	if err := dir.Sync(); err != nil {
		return errors.New("relay fence directory could not be synced")
	}
	f.durable = true
	return nil
}
