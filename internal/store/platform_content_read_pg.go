package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	"fugue/internal/model"
)

// Every call reads current authority metadata. Only the immutable content body
// may be omitted, and only after PostgreSQL hashes the actual current bytes.
// The declared content_hash is never a cache validator.
const platformArtifactConditionalReadSQL = `WITH artifact AS MATERIALIZED (
 SELECT *, sha256(convert_to(content_json::text, 'UTF8')) AS wire_content_digest
 FROM fugue_platform_artifacts
 WHERE id = $1 OR generation = $1
 ORDER BY updated_at DESC, id ASC LIMIT 1
)
SELECT id, artifact_kind, scope_key, scope_json, schema_version, generation,
 generation_sequence, status, content_hash,
 CASE WHEN wire_content_digest = $2::bytea THEN NULL ELSE content_json END,
 validation_results_json, compatibility_floor, metadata_json,
 created_by_type, created_by_id, provenance_json, created_at, updated_at,
 wire_content_digest
FROM artifact`

type platformContentDigestScanner struct {
	sqlScanner
	digest []byte
}

func (s *platformContentDigestScanner) Scan(dest ...any) error {
	return s.sqlScanner.Scan(append(dest, &s.digest)...)
}

func (s *Store) pgReadPlatformArtifact(ctx context.Context, idOrGeneration string) (artifact model.PlatformArtifact, err error) {
	started := time.Now()
	defer func() {
		rows := 0
		if err == nil {
			rows = 1
		}
		s.recordRead(artifactReadMetric, started, rows, err)
	}()
	s.platformContentHintMu.Lock()
	key, hinted := s.platformContentHints[idOrGeneration]
	s.platformContentHintMu.Unlock()
	var validator []byte
	var pinned map[string]any
	if hinted {
		if content, ok := platformContentDecodings.get(key); ok {
			// Retain the immutable map until the query completes, even if the
			// bounded LRU evicts its entry while this request is in flight.
			pinned = content
			validator = key[:]
		}
	}
	scanner := &platformContentDigestScanner{sqlScanner: s.db.QueryRowContext(ctx, platformArtifactConditionalReadSQL, idOrGeneration, validator)}
	artifact, err = scanPlatformArtifactWithDecoder(scanner, func(raw []byte) (map[string]any, error) {
		if raw == nil {
			if pinned == nil || !bytes.Equal(scanner.digest, validator) {
				return nil, fmt.Errorf("platform artifact conditional read omitted uncached content")
			}
			s.readMetrics[artifactReadMetric].bodyElisions.Add(1)
			return clonePlatformContent(pinned), nil
		}
		s.readMetrics[artifactReadMetric].bodyBytes.Add(uint64(len(raw)))
		actual := sha256.Sum256(raw)
		if !bytes.Equal(actual[:], scanner.digest) {
			return nil, fmt.Errorf("platform artifact content transport digest mismatch")
		}
		content, err := platformContentDecodings.decode(raw)
		if err == nil {
			if _, cached := platformContentDecodings.get(actual); cached {
				s.platformContentHintMu.Lock()
				if s.platformContentHints == nil {
					s.platformContentHints = make(map[string][32]byte)
				}
				if _, exists := s.platformContentHints[idOrGeneration]; !exists && len(s.platformContentHints) >= 128 {
					for old := range s.platformContentHints {
						delete(s.platformContentHints, old)
						break
					}
				}
				s.platformContentHints[idOrGeneration] = actual
				s.platformContentHintMu.Unlock()
			}
		}
		return content, err
	})
	return artifact, err
}
