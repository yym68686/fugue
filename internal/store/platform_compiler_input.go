package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

func (s *Store) pgEnsurePlatformCompilerInput(parent context.Context, snapshot platformconfig.RuntimeSnapshot, digest string) (resultErr error) {
	observation := compilerInputWriterStats.beginObserved(parent)
	defer func() {
		if recovered := recover(); recovered != nil {
			observation.finish(errors.New("compiler-input writer panic"))
			panic(recovered)
		}
		observation.finish(resultErr)
	}()
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	defer observation.stopWork()
	observation.setWriterContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	observation.advance(compilerInputWriterEncode)
	raw, err := platformconfig.EncodeRuntimeSnapshot(snapshot)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	if "sha256:"+hex.EncodeToString(sum[:]) != digest {
		return ErrConflict
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	observation.advance(compilerInputWriterBeginTx)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer observation.rollback(tx)
	observation.afterBegin()
	observation.advance(compilerInputWriterBarrier)
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		return err
	}
	observation.afterBarrier()
	observation.advance(compilerInputWriterPersist)
	fenced, err := compilerInputWritesFenced(ctx, tx)
	if err != nil {
		return err
	}
	var manifestTable, hasManifest bool
	if err = tx.QueryRowContext(ctx, `SELECT to_regclass('fugue_platform_input_manifests') IS NOT NULL`).Scan(&manifestTable); err != nil {
		return err
	}
	if manifestTable {
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_platform_input_manifests WHERE content_hash=$1)`, digest).Scan(&hasManifest); err != nil {
			return err
		}
	}
	if fenced || hasManifest {
		// Policy controls the preferred encoding, not permission to bypass the
		// retirement fence or replace an existing canonical representation.
		// Keep its original caller budget and transaction.
		return s.pgEnsureChunkedInputTx(ctx, tx, snapshot, digest, observation)
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_platform_artifact_contents (content_hash, content_json, size_bytes, created_at, updated_at, content_kind) VALUES ($1,$2::jsonb,$3,$4,$4,'compiler-input-v1') ON CONFLICT (content_hash) DO NOTHING`, digest, raw, int64(len(raw)), now); err != nil {
		return mapDBErr(err)
	}
	var stored []byte
	readCtx := runtimeobservation.WithDatabaseOperation(ctx, "compiler-input-read")
	if err := tx.QueryRowContext(readCtx, `SELECT content_json FROM fugue_platform_artifact_contents WHERE content_hash = $1`, digest).Scan(&stored); err != nil {
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
	return observation.commit(tx)
}
