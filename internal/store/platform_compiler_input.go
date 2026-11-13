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
	return s.EnsurePlatformCompilerInputContext(context.Background(), snapshot, digest)
}

func (s *Store) EnsurePlatformCompilerInputContext(ctx context.Context, snapshot platformconfig.RuntimeSnapshot, digest string) error {
	if s.usingDatabase() {
		// PostgreSQL writes always use the current local store. Encoding policy
		// may change, but it cannot authorize legacy JSONB or old-chunk writes.
		return s.pgEnsureChunkedInput(ctx, snapshot, digest)
	}
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
