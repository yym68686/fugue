package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/runtimeobservation"
)

// The signed artifact's lineage authenticates this digest-addressed input.
// It has no independent release lane or serving authority.
func (s *Store) EnsurePlatformCompilerInput(snapshot platformconfig.RuntimeSnapshot, digest string) error {
	return s.EnsurePlatformCompilerInputContext(context.Background(), snapshot, digest)
}

func (s *Store) EnsurePlatformCompilerInputContext(ctx context.Context, snapshot platformconfig.RuntimeSnapshot, digest string) error {
	if s.usingDatabase() {
		if s.StoragePolicy().CompilerInputChunks {
			return s.pgEnsureChunkedInput(ctx, snapshot, digest)
		}
		return s.pgEnsurePlatformCompilerInput(ctx, snapshot, digest)
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

func (s *Store) pgEnsurePlatformCompilerInput(parent context.Context, snapshot platformconfig.RuntimeSnapshot, digest string) error {
	raw, err := platformconfig.EncodeRuntimeSnapshot(snapshot)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	if "sha256:"+hex.EncodeToString(sum[:]) != digest {
		return ErrConflict
	}
	ctx, cancel := context.WithTimeout(runtimeobservation.CopyDatabaseOperation(context.Background(), parent), 10*time.Second)
	defer cancel()
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `INSERT INTO fugue_platform_artifact_contents (content_hash, content_json, size_bytes, created_at, updated_at) VALUES ($1,$2::jsonb,$3,$4,$4) ON CONFLICT (content_hash) DO NOTHING`, digest, raw, int64(len(raw)), now); err != nil {
		return mapDBErr(err)
	}
	var stored []byte
	readCtx := runtimeobservation.WithDatabaseOperation(ctx, "compiler-input-read")
	if err := s.db.QueryRowContext(readCtx, `SELECT content_json FROM fugue_platform_artifact_contents WHERE content_hash = $1`, digest).Scan(&stored); err != nil {
		return mapDBErr(err)
	}
	runtimeobservation.DatabaseObservations.RecordOutput(readCtx, int64(len(stored)))
	decoded, err := platformconfig.DecodeRuntimeSnapshotJSON(stored)
	if err != nil {
		return err
	}
	actual, err := platformconfig.RuntimeSnapshotDigest(decoded)
	if err != nil {
		return err
	}
	if actual != digest {
		return ErrConflict
	}
	return nil
}
