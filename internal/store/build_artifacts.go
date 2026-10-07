package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fugue/internal/model"
	"time"
)

func BuildArtifactID(operation, job, ref string) string {
	sum := sha256.Sum256([]byte(operation + "\x00" + job + "\x00" + ref))
	return "artifact_" + hex.EncodeToString(sum[:])
}
func (s *Store) SaveBuildArtifact(a model.BuildArtifact) (model.BuildArtifact, error) {
	if a.OperationID == "" || a.TenantID == "" || a.AppID == "" || a.ImageRef == "" || a.JobName == "" {
		return a, ErrInvalidInput
	}
	a.ID = BuildArtifactID(a.OperationID, a.JobName, a.ImageRef)
	if a.RegisteredAt.IsZero() {
		a.RegisteredAt = time.Now().UTC()
	}
	if a.VerifiedAt != nil && (CanonicalImageDigest(a.Digest) == "" || a.CacheEndpoint == "" || a.ClusterNodeName == "") {
		return a, ErrInvalidInput
	}
	merge := func(old model.BuildArtifact) error {
		if old.ID != "" {
			a.RegisteredAt = old.RegisteredAt
			if old.Digest != "" {
				if a.Digest != "" && a.Digest != old.Digest {
					return ErrConflict
				}
				a = old
			}
		}
		return nil
	}
	if !s.usingDatabase() {
		err := s.withLockedState(true, func(st *model.State) error {
			for i, old := range st.BuildArtifacts {
				if old.ID == a.ID {
					if err := merge(old); err != nil {
						return err
					}
					st.BuildArtifacts[i] = a
					return nil
				}
			}
			st.BuildArtifacts = append(st.BuildArtifacts, a)
			return nil
		})
		return a, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raw, err := json.Marshal(a)
	if err != nil {
		return a, err
	}
	// A digest receipt is immutable. Re-registration cannot erase it, and another
	// digest for a reused build identity is rejected rather than silently rebound.
	err = s.db.QueryRowContext(ctx, `INSERT INTO fugue_build_artifacts(id,body) VALUES($1,$2) ON CONFLICT(id) DO UPDATE SET body=CASE WHEN COALESCE(fugue_build_artifacts.body->>'digest','')<>'' THEN fugue_build_artifacts.body ELSE EXCLUDED.body || jsonb_build_object('registered_at',fugue_build_artifacts.body->'registered_at') END RETURNING body`, a.ID, raw).Scan(&raw)
	if err != nil {
		return a, err
	}
	var saved model.BuildArtifact
	if err = json.Unmarshal(raw, &saved); err != nil {
		return a, err
	}
	if a.Digest != "" && saved.Digest != a.Digest {
		return saved, ErrConflict
	}
	return saved, nil
}
func (s *Store) ListBuildArtifacts() ([]model.BuildArtifact, error) {
	out := []model.BuildArtifact{}
	if !s.usingDatabase() {
		err := s.withLockedState(false, func(st *model.State) error { out = append(out, st.BuildArtifacts...); return nil })
		return out, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT body FROM fugue_build_artifacts ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		var a model.BuildArtifact
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
