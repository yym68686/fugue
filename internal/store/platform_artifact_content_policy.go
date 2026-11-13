package store

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
)

func (s *Store) artifactContentReferencesEnabled() bool {
	s.storagePolicy.mu.RLock()
	defer s.storagePolicy.mu.RUnlock()
	return s.storagePolicy.artifactReferences.Version == 1 && s.storagePolicy.artifactReferences.Enabled
}

// Activation is separate from the original policy so reader-only rollback
// versions can parse their original policy. Missing/invalid files stop new
// reference writes; existing references remain readable unconditionally.
func (s *Store) loadArtifactContentPolicy(path string) {
	var p compilerInputCodecPolicy
	f, err := os.Open(path)
	missing := os.IsNotExist(err)
	if err == nil {
		raw, e := io.ReadAll(io.LimitReader(f, 4097))
		f.Close()
		err = e
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
	s.storagePolicy.artifactReferences = p
	s.storagePolicy.artifactReferencesReadFailed = err != nil && !missing
}
