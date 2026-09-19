package store

import (
	"context"
	"encoding/json"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// The signed artifact's lineage authenticates this digest-addressed input.
// It has no independent release lane or serving authority.
func (s *Store) EnsurePlatformCompilerInput(snapshot platformconfig.RuntimeSnapshot, digest string) error {
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	var content map[string]any
	if err = json.Unmarshal(raw, &content); err != nil {
		return err
	}
	hash, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		return err
	}
	if hash != digest {
		return ErrConflict
	}
	if roundtrip, err := platformconfig.RuntimeSnapshotContentDigest(content); err != nil || roundtrip != hash {
		return ErrConflict
	}
	now := time.Now().UTC()
	value := buildPlatformArtifactContent(model.PlatformArtifact{Content: content, ContentHash: hash, CreatedAt: now, UpdatedAt: now})
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		encoded, err := marshalJSON(content)
		if err != nil {
			return err
		}
		if _, err = s.db.ExecContext(ctx, `INSERT INTO fugue_platform_artifact_contents (content_hash, content_json, size_bytes, created_at, updated_at) VALUES ($1,$2::jsonb,$3,$4,$4) ON CONFLICT (content_hash) DO NOTHING`, hash, encoded, value.SizeBytes, now); err != nil {
			return mapDBErr(err)
		}
		stored, err := s.pgGetPlatformArtifactContent(hash)
		if err != nil {
			return err
		}
		actual, err := platformconfig.RuntimeSnapshotContentDigest(stored.Content)
		if err != nil {
			return err
		}
		if actual != hash {
			return ErrConflict
		}
		return nil
	}
	return s.withLockedState(true, func(st *model.State) error {
		for _, prior := range st.PlatformArtifactContents {
			if prior.ContentHash == hash {
				actual, err := platformconfig.RuntimeSnapshotContentDigest(prior.Content)
				if err != nil {
					return err
				}
				if actual != hash {
					return ErrConflict
				}
				return nil
			}
		}
		st.PlatformArtifactContents = append(st.PlatformArtifactContents, value)
		return nil
	})
}
