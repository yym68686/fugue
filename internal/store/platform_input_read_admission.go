package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
)

// PlatformInputReadSelection is server-derived selection, never an HTTP proof.
// Only publications actually being returned belong here. In particular, a
// serving-only read must not include an unrelated shadow/candidate publication.
// LKGKind/Scope bind the current positive LKG independently of active lanes;
// nil LKG with those fields denotes a genuinely initial absent selector.
type PlatformInputReadSelection struct {
	ArtifactID, ContentHash          string
	ParentID, ParentHash             string
	ReleaseID, ExpectedConsumerSetID string
	LKGKind, LKGScope                string
	LKG                              *model.PlatformLKGSnapshot
	// OriginalInput is used only by explicit full-history reads. Ordinary
	// runtime reads must not require the audit body of an opted-in artifact.
	OriginalInput bool `json:",omitempty"`
}

func compilerInputReadRetainsOriginal(reads []PlatformInputReadSelection) bool {
	for _, read := range reads {
		if read.OriginalInput {
			return true
		}
	}
	return false
}

// CheckPlatformInputRead verifies a COMPLETE dependency closure and consumes
// current verified input pins without R2, restoration, root installation or
// mutation. Callers retain their original identity/selection/ownership checks
// and recheck those after this check before disclosing data or credentials.
func (s *Store) CheckPlatformInputRead(ctx context.Context, selections []PlatformInputReadSelection) error {
	if s == nil || len(selections) == 0 || len(selections) > compilerInputHotRootLimit {
		return ErrInvalidInput
	}
	// Bound and own all caller maps before entering the coherent snapshot.
	raw, err := json.Marshal(selections)
	if err != nil || len(raw) > 32<<20 {
		return ErrInvalidInput
	}
	var reads []PlatformInputReadSelection
	if err := json.Unmarshal(raw, &reads); err != nil {
		return ErrInvalidInput
	}
	for _, read := range reads {
		if err := validateCompilerInputReadSelection(read); err != nil {
			return err
		}
	}
	if len(compilerInputReadRoots(reads)) > compilerInputHotRootLimit {
		return ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	if !s.usingDatabase() {
		return s.checkFilePlatformInputRead(ctx, reads)
	}
	proof, err := s.prepareOrdinaryCompilerInputProof(ctx, nil, nil, nil, nil, reads)
	if err != nil {
		return errors.Join(err, ErrConflict)
	}
	proof, pins, err := s.prepareCompilerInputActivationPins(ctx, proof)
	if err != nil {
		return errors.Join(err, ErrConflict)
	}
	return s.withPreparedCompilerInputActivation(ctx, proof, pins, func(context.Context, *sql.Tx) error { return nil })
}

func validateCompilerInputReadSelection(r PlatformInputReadSelection) error {
	if r.OriginalInput && r.ArtifactID == "" {
		return ErrInvalidInput
	}
	for _, id := range []string{r.ArtifactID, r.ParentID, r.ReleaseID, r.ExpectedConsumerSetID} {
		if len(id) > 512 {
			return ErrInvalidInput
		}
	}
	if (r.ArtifactID == "") != (r.ContentHash == "") || r.ContentHash != "" && !inputDigestPattern.MatchString(r.ContentHash) ||
		(r.ParentID == "") != (r.ParentHash == "") || r.ParentHash != "" && !inputDigestPattern.MatchString(r.ParentHash) {
		return ErrInvalidInput
	}
	if r.LKGKind == "" && r.LKGScope == "" {
		if r.LKG != nil || r.ArtifactID == "" {
			return ErrInvalidInput
		}
	} else {
		if _, _, err := compilerInputLKGSelector(compilerInputLKGSelectorKey(r.LKGKind, r.LKGScope)); err != nil {
			return err
		}
		if r.LKG == nil {
			if r.ArtifactID != "" || r.ParentID != "" || r.ReleaseID != "" || r.ExpectedConsumerSetID != "" {
				return ErrInvalidInput
			}
		} else if r.LKG.ArtifactKind != r.LKGKind || r.LKG.ScopeKey != r.LKGScope || r.LKG.ArtifactID == "" ||
			!inputDigestPattern.MatchString(r.LKG.ContentHash) {
			return ErrInvalidInput
		}
	}
	return nil
}

func compilerInputReadRoots(reads []PlatformInputReadSelection) []inputarchive.HotRoot {
	roots := []inputarchive.HotRoot{}
	seen := map[inputarchive.HotReference]bool{}
	add := func(source string, ref inputarchive.HotReference) {
		// Multiple lanes may select the same immutable publication. Collapse
		// only identical references, including the expected hash: conflicting
		// digest constraints must both reach the graph verifier.
		if ref.ID == "" || seen[ref] {
			return
		}
		seen[ref] = true
		roots = append(roots, inputarchive.HotRoot{Source: source, ID: strconv.Itoa(len(roots)), Reference: ref})
	}
	for _, r := range reads {
		for _, ref := range []inputarchive.HotReference{
			{Kind: "artifact", ID: r.ArtifactID, ExpectedHash: r.ContentHash},
			{Kind: "artifact", ID: r.ParentID, ExpectedHash: r.ParentHash},
			{Kind: "release", ID: r.ReleaseID}, {Kind: "expectation", ID: r.ExpectedConsumerSetID},
		} {
			add("selected-reader", ref)
		}
		if r.LKG != nil {
			add("selected-reader-lkg", inputarchive.HotReference{Kind: "artifact", ID: r.LKG.ArtifactID, ExpectedHash: r.LKG.ContentHash})
		}
	}
	return roots
}

func sameCompilerInputReadLKG(a, b *model.PlatformLKGSnapshot) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	x, y := *a, *b
	// JSON owns the request maps and removes monotonic/location annotations;
	// normalize DB times as well, without weakening any signed or expiry field.
	x.ExpiresAt, x.CreatedAt, x.UpdatedAt = x.ExpiresAt.UTC(), x.CreatedAt.UTC(), x.UpdatedAt.UTC()
	y.ExpiresAt, y.CreatedAt, y.UpdatedAt = y.ExpiresAt.UTC(), y.CreatedAt.UTC(), y.UpdatedAt.UTC()
	return reflect.DeepEqual(x, y)
}

func prepareCompilerInputReadRoots(ctx context.Context, tx *sql.Tx, proof *compilerInputActivationProof, reads []PlatformInputReadSelection) ([]inputarchive.HotRoot, []model.PlatformLKGSnapshot, int, error) {
	roots := compilerInputReadRoots(reads)
	raw, err := json.Marshal(reads)
	if err != nil {
		return nil, nil, 0, err
	}
	bytes := len(raw)
	snapshots := []model.PlatformLKGSnapshot{}
	for _, read := range reads {
		if read.LKGKind == "" {
			continue
		}
		snapshot, err := scanPlatformLKGSnapshot(tx.QueryRowContext(ctx, `SELECT id,artifact_id,artifact_kind,scope_key,
 left(scope_json::text,16385),schema_version,generation,generation_sequence,content_hash,left(artifact_provenance_json::text,16385),
 verified_by_release_id,verification_evidence_hash,left(snapshot_provenance_json::text,16385),expires_at,created_at,updated_at
 FROM fugue_platform_lkg_snapshots WHERE artifact_kind=$1 AND scope_key=$2`, read.LKGKind, read.LKGScope))
		var current *model.PlatformLKGSnapshot
		if err == nil {
			current = &snapshot
		} else if !errors.Is(err, ErrNotFound) {
			return nil, nil, 0, err
		}
		if !sameCompilerInputReadLKG(read.LKG, current) {
			return nil, nil, 0, ErrConflict
		}
		witness := compilerInputActivationTuple{table: "fugue_platform_lkg_snapshots", key: compilerInputLKGSelectorKey(read.LKGKind, read.LKGScope)}
		if err := readCompilerInputActivationTuple(ctx, tx, &witness, false); err != nil {
			return nil, nil, 0, err
		}
		proof.tuples = append(proof.tuples, witness)
		bytes += len(witness.key) + 1024
		if current != nil {
			if !time.Now().Before(current.ExpiresAt) {
				return nil, nil, 0, ErrConflict
			}
			if current.ExpiresAt.Before(proof.expires) {
				proof.expires = current.ExpiresAt
			}
			snapshots = append(snapshots, *current)
		}
	}
	return roots, snapshots, bytes, nil
}
