package store

import (
	"context"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

type platformSessionContent struct {
	content     map[string]any
	fingerprint [32]byte
	bytes       int
}
type platformContentSession struct {
	sync.Mutex
	values map[[32]byte]platformSessionContent
	bytes  int
}

// NewPlatformArtifactReadSession shares caller-owned content across fresh reads
// within ONE read-only request. It never caches an artifact envelope, authority,
// signature verdict, release or expected membership. Every invocation queries
// current database facts and the digest of the actual current body bytes.
// Sessions must not outlive a request or be used for mutation paths.
func (s *Store) NewPlatformArtifactReadSession() func(string) (model.PlatformArtifact, error) {
	bodies := &platformContentSession{values: make(map[[32]byte]platformSessionContent)}
	return func(id string) (model.PlatformArtifact, error) {
		id = strings.TrimSpace(id)
		if id == "" {
			return model.PlatformArtifact{}, ErrInvalidInput
		}
		if !s.usingDatabase() {
			return s.GetPlatformArtifact(id)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		artifact, err := s.pgReadPlatformArtifactSession(ctx, id, bodies)
		if err != nil {
			return model.PlatformArtifact{}, mapDBErr(err)
		}
		return artifact, nil
	}
}

func (s *platformContentSession) owned(key [32]byte, source map[string]any) map[string]any {
	return s.ownedEntry(newPlatformContentEntry(key, source))
}

func (s *platformContentSession) ownedEntry(source platformContentEntry) map[string]any {
	key := source.key
	s.Lock()
	defer s.Unlock()
	if prior, ok := s.values[key]; ok {
		// A caller-mutated map can never impersonate the newly read database
		// bytes, even when its declared ContentHash was left unchanged.
		if fingerprint, supported := platformsafety.FingerprintArtifactContent(prior.content); supported && fingerprint == prior.fingerprint {
			return prior.content
		}
		delete(s.values, key)
		s.bytes -= prior.bytes
	}
	owned := clonePlatformContent(source.content)
	size := source.bytes
	if source.fingerprintOK && len(s.values) < 32 && s.bytes+size <= 32<<20 {
		if s.values == nil {
			s.values = make(map[[32]byte]platformSessionContent)
		}
		s.values[key] = platformSessionContent{owned, source.fingerprint, size}
		s.bytes += size
	}
	return owned
}
