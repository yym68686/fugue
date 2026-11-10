package store

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
)

type platformSessionContent struct {
	content map[string]any
	bytes   int
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
		if equalPlatformJSON(prior.content, source.content) {
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
		s.values[key] = platformSessionContent{owned, size}
		s.bytes += size
	}
	return owned
}

// Compare the caller-owned graph with the private immutable decode of the
// actual current database bytes. Unlike re-fingerprinting, this needs neither
// key sorting nor hashing/copying strings on every request-local reuse. Mutable
// maps/slices are still visited, so a caller mutation cannot impersonate fresh
// content. Only native encoding/json values reach this ownership boundary.
func equalPlatformJSON(a, b any) bool {
	switch x := a.(type) {
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) || (x == nil) != (y == nil) {
			return false
		}
		for key, value := range x {
			other, exists := y[key]
			if !exists || !equalPlatformJSON(value, other) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) || (x == nil) != (y == nil) {
			return false
		}
		for i, value := range x {
			if !equalPlatformJSON(value, y[i]) {
				return false
			}
		}
		return true
	case string:
		y, ok := b.(string)
		return ok && x == y
	case float64:
		y, ok := b.(float64)
		return ok && math.Float64bits(x) == math.Float64bits(y)
	case bool:
		y, ok := b.(bool)
		return ok && x == y
	case nil:
		return b == nil
	default:
		return false
	}
}
