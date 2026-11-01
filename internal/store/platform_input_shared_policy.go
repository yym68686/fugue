package store

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"time"
)

func (s *Store) compilerInputSharedEnabled() bool {
	s.storagePolicy.mu.RLock()
	defer s.storagePolicy.mu.RUnlock()
	return s.storagePolicy.shared.Version == 1 && s.storagePolicy.shared.Enabled
}

// A separate declaration is activated only after every reader has rolled.
// Missing or invalid declarations disable new writes, never existing reads.
func (s *Store) loadCompilerInputSharedPolicy(path string) {
	var p compilerInputCodecPolicy
	f, err := os.Open(path)
	missing := os.IsNotExist(err)
	if err == nil {
		raw, readErr := io.ReadAll(io.LimitReader(f, 4097))
		f.Close()
		err = readErr
		if err == nil {
			d := json.NewDecoder(bytes.NewReader(raw))
			d.DisallowUnknownFields()
			if len(raw) > 4096 || d.Decode(&p) != nil || d.Decode(new(any)) != io.EOF || p.Version != 1 {
				err = ErrInvalidInput
			}
		}
	}
	if err != nil {
		p = compilerInputCodecPolicy{}
	}
	s.storagePolicy.mu.Lock()
	defer s.storagePolicy.mu.Unlock()
	s.storagePolicy.shared = p
	s.storagePolicy.sharedReadFailed = err != nil && !missing
	s.storagePolicy.sharedObservedAt = time.Now().UTC()
}
