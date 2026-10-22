package store

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"time"
)

// Separate file keeps the original policy readable by the reader-only rollback
// version. Missing/invalid activation disables new v2 writes; readers stay on.
type compilerInputCodecPolicy struct {
	Version int  `json:"version"`
	Enabled bool `json:"enabled"`
}

func (s *Store) compilerInputCodecEnabled() bool {
	s.storagePolicy.mu.RLock()
	defer s.storagePolicy.mu.RUnlock()
	return s.storagePolicy.codec.Version == 1 && s.storagePolicy.codec.Enabled
}
func (s *Store) loadCompilerInputCodecPolicy(path string) {
	var policy compilerInputCodecPolicy
	f, err := os.Open(path)
	missing := os.IsNotExist(err)
	if err == nil {
		raw, readErr := io.ReadAll(io.LimitReader(f, 4097))
		f.Close()
		err = readErr
		if err == nil {
			d := json.NewDecoder(bytes.NewReader(raw))
			d.DisallowUnknownFields()
			if len(raw) > 4096 || d.Decode(&policy) != nil || d.Decode(new(any)) != io.EOF || policy.Version != 1 {
				err = ErrInvalidInput
			}
		}
	}
	if err != nil {
		policy = compilerInputCodecPolicy{}
	}
	s.storagePolicy.mu.Lock()
	defer s.storagePolicy.mu.Unlock()
	s.storagePolicy.codec = policy
	s.storagePolicy.codecReadFailed = err != nil && !missing
	s.storagePolicy.codecObservedAt = time.Now().UTC()
}
