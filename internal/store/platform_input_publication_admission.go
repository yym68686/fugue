package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

// Server-owned selection, never a client-supplied proof. The canonical current
// artifact, exact rollback generation and LKG selector are read in the SAME RR
// snapshot as their complete signed closures. Publication consumes that proof
// after its ordinary authority/scope locks and installs the real retention root.
type compilerInputPublicationIntent struct {
	id, artifactID, rollbackGeneration string
	lkgReleaseID                       string
	requestBindings                    string
	hasGuard                           bool
	guard                              platformProducerReleaseGuard
}

type compilerInputPublicationPreconditions struct {
	DNS             *model.PlatformDNSPublicationPrecondition `json:"dns,omitempty"`
	Reconfiguration *model.PlatformProducerReconfiguration    `json:"reconfiguration,omitempty"`
}

func compilerInputPublicationRequestBindings(req model.PlatformArtifactReleaseRequest) (string, error) {
	if req.DNSPublicationPrecondition == nil && req.ProducerReconfiguration == nil {
		return "", nil
	}
	raw, err := json.Marshal(compilerInputPublicationPreconditions{req.DNSPublicationPrecondition, req.ProducerReconfiguration})
	if err != nil || len(raw) > 65536 {
		return "", ErrInvalidInput
	}
	return string(raw), nil
}

func compilerInputLKGSelectorKey(kind, scope string) string {
	raw, _ := json.Marshal([]string{kind, scope})
	return string(raw)
}

func compilerInputLKGSelector(key string) (string, string, error) {
	var parts []string
	if len(key) > 4096 || json.Unmarshal([]byte(key), &parts) != nil || len(parts) != 2 ||
		parts[0] == "" || len(parts[0]) > 128 || parts[1] == "" || len(parts[1]) > 512 ||
		compilerInputLKGSelectorKey(parts[0], parts[1]) != key {
		return "", "", ErrInvalidInput
	}
	return parts[0], parts[1], nil
}

func (s *Store) prepareCompilerInputPublication(ctx context.Context, id, rollback string, guard *platformProducerReleaseGuard, requests ...model.PlatformArtifactReleaseRequest) (*compilerInputActivationProof, []compilerInputPinBinding, error) {
	intent := compilerInputPublicationIntent{id: id, rollbackGeneration: rollback}
	if len(requests) > 1 {
		return nil, nil, ErrInvalidInput
	}
	if len(requests) == 1 {
		var err error
		intent.requestBindings, err = compilerInputPublicationRequestBindings(requests[0])
		if err != nil {
			return nil, nil, err
		}
	}
	if guard != nil {
		intent.hasGuard, intent.guard = true, *guard
	}
	proof, err := s.prepareOrdinaryCompilerInputProof(ctx, nil, nil, &intent, nil, nil)
	if err != nil {
		return nil, nil, errors.Join(mapDBErr(err), ErrConflict)
	}
	return s.prepareCompilerInputActivationPins(ctx, proof)
}

func prepareCompilerInputPublicationRoots(ctx context.Context, tx *sql.Tx, proof *compilerInputActivationProof, intent compilerInputPublicationIntent) ([]inputarchive.HotRoot, []model.PlatformLKGSnapshot, error) {
	var kind, scope string
	if intent.lkgReleaseID != "" {
		if intent.id != "" || intent.rollbackGeneration != "" || !intent.hasGuard || len(intent.lkgReleaseID) > 512 {
			return nil, nil, ErrInvalidInput
		}
		if err := tx.QueryRowContext(ctx, `SELECT artifact_id FROM fugue_platform_artifact_releases WHERE id=$1`, intent.lkgReleaseID).Scan(&intent.id); err != nil {
			return nil, nil, err
		}
	}
	if intent.id == "" || len(intent.id) > 512 || len(intent.rollbackGeneration) > 512 {
		return nil, nil, ErrInvalidInput
	}
	if err := tx.QueryRowContext(ctx, `SELECT id,artifact_kind,scope_key FROM fugue_platform_artifacts
 WHERE id=$1 OR generation=$1 ORDER BY updated_at DESC,id ASC LIMIT 1`, intent.id).Scan(&intent.artifactID, &kind, &scope); err != nil {
		return nil, nil, err
	}
	root := func(source string, ref inputarchive.HotReference) inputarchive.HotRoot {
		return inputarchive.HotRoot{Source: source, ID: intent.artifactID, Reference: ref}
	}
	selectors := []string{}
	seenSelectors := map[string]bool{}
	addSelector := func(kind, scope string) error {
		key := compilerInputLKGSelectorKey(kind, scope)
		if _, _, err := compilerInputLKGSelector(key); err != nil {
			return err
		}
		if !seenSelectors[key] {
			seenSelectors[key] = true
			selectors = append(selectors, key)
		}
		return nil
	}
	if err := addSelector(kind, scope); err != nil {
		return nil, nil, err
	}
	roots := []inputarchive.HotRoot{}
	if intent.requestBindings != "" {
		var preconditions compilerInputPublicationPreconditions
		if json.Unmarshal([]byte(intent.requestBindings), &preconditions) != nil {
			return nil, nil, ErrInvalidInput
		}
		retainPublication := func(source string, ref model.PlatformPublicationPrecondition) error {
			if len(ref.ArtifactID) == 0 || len(ref.ArtifactID) > 512 || len(ref.ReleaseID) == 0 || len(ref.ReleaseID) > 512 {
				return ErrInvalidInput
			}
			var refKind, refScope string
			if err := tx.QueryRowContext(ctx, `SELECT artifact_kind,scope_key FROM fugue_platform_artifacts WHERE id=$1`, ref.ArtifactID).Scan(&refKind, &refScope); err != nil {
				return err
			}
			if err := addSelector(refKind, refScope); err != nil {
				return err
			}
			roots = append(roots, root(source, inputarchive.HotReference{Kind: "release", ID: ref.ReleaseID}),
				root(source, inputarchive.HotReference{Kind: "artifact", ID: ref.ArtifactID, ExpectedHash: ref.ContentHash}))
			return nil
		}
		witnessPublication := func(ref model.PlatformPublicationPrecondition) error {
			for _, item := range []compilerInputActivationTuple{
				{table: "fugue_platform_artifacts", key: ref.ArtifactID},
				{table: "fugue_platform_artifact_releases", key: ref.ReleaseID},
			} {
				if len(item.key) == 0 || len(item.key) > 512 {
					return ErrInvalidInput
				}
				if err := readCompilerInputActivationTuple(ctx, tx, &item, false); err != nil {
					return err
				}
				proof.tuples = append(proof.tuples, item)
			}
			return nil
		}
		if r := preconditions.Reconfiguration; r != nil {
			// Reconfiguration keeps this exact serving publication live even
			// when the new policy has a different scope and input closure.
			if err := retainPublication("publication-reconfiguration-serving", r.ServingFull); err != nil {
				return nil, nil, err
			}
			if err := witnessPublication(r.PreviousPolicy); err != nil {
				return nil, nil, err
			}
		}
		if d := preconditions.DNS; d != nil {
			if err := retainPublication("publication-dns-lkg", d.LKGPublication); err != nil {
				return nil, nil, err
			}
			for _, channel := range []string{"shadow", "gray", "full"} {
				if ref := d.Lanes[channel]; ref != nil {
					if err := witnessPublication(*ref); err != nil {
						return nil, nil, err
					}
				}
			}
		}
	}
	if intent.lkgReleaseID != "" {
		roots = append(roots, inputarchive.HotRoot{Source: "lkg-promotion", ID: intent.lkgReleaseID, Reference: inputarchive.HotReference{Kind: "release", ID: intent.lkgReleaseID}})
	} else if intent.rollbackGeneration != "" {
		roots = append(roots, root("publication-rollback", inputarchive.HotReference{Kind: "generation", ID: intent.rollbackGeneration, ArtifactKind: kind, ScopeKey: scope}))
		// Recovery does not reactivate the failed candidate's bytes. Bind its
		// identity/metadata so scope and producer authority cannot change, but
		// do not make a valid target depend on the damaged candidate's input.
		current := compilerInputActivationTuple{table: "fugue_platform_artifacts", key: intent.artifactID}
		if err := readCompilerInputActivationTuple(ctx, tx, &current, false); err != nil {
			return nil, nil, err
		}
		proof.tuples = append(proof.tuples, current)
	} else {
		roots = append(roots, root("publication-current", inputarchive.HotReference{Kind: "artifact", ID: intent.artifactID}))
	}
	if intent.hasGuard {
		// Bind the current policy fence and target lanes in the SAME snapshot.
		// Missing lanes are physical absence witnesses, not implicit permission
		// to ignore a later publication/ABA. Business authority remains checked
		// by the original guarded transaction after restoration.
		policyScope, err := platformproducer.PolicyScopeForTarget(scope)
		if err != nil {
			return nil, nil, ErrConflict
		}
		for _, key := range []string{
			platformsafety.ReleaseLaneKey(model.PlatformArtifactKindPolicySnapshot, policyScope, "shadow"),
			platformsafety.ReleaseLaneKey(kind, scope, "shadow"),
			platformsafety.ReleaseLaneKey(kind, scope, "gray"),
			platformsafety.ReleaseLaneKey(kind, scope, "full"),
		} {
			witness := compilerInputActivationTuple{table: "fugue_platform_release_lanes", key: key}
			if err := readCompilerInputActivationTuple(ctx, tx, &witness, false); err != nil {
				return nil, nil, err
			}
			proof.tuples = append(proof.tuples, witness)
		}
		for _, release := range []struct{ source, id string }{
			{"publication-policy", intent.guard.PolicyReleaseID}, {"publication-previous", intent.guard.PreviousReleaseID},
			{"publication-previous-full", intent.guard.PreviousFullReleaseID}, {"publication-failed", intent.guard.FailedReleaseID},
		} {
			if release.id != "" {
				if release.source != "publication-policy" {
					// Previous publication IDs are authority/CAS witnesses, not
					// a request to reactivate every historical input they mention.
					witness := compilerInputActivationTuple{table: "fugue_platform_artifact_releases", key: release.id}
					if err := readCompilerInputActivationTuple(ctx, tx, &witness, false); err != nil {
						return nil, nil, err
					}
					proof.tuples = append(proof.tuples, witness)
					continue
				}
				roots = append(roots, root(release.source, inputarchive.HotReference{Kind: "release", ID: release.id}))
			}
		}
		if intent.guard.BaselineArtifactID != "" {
			roots = append(roots, root("publication-producer-baseline", inputarchive.HotReference{Kind: "artifact", ID: intent.guard.BaselineArtifactID}))
		}
	}
	proof.publication = &intent
	snapshots := []model.PlatformLKGSnapshot{}
	for _, key := range selectors {
		refKind, refScope, err := compilerInputLKGSelector(key)
		if err != nil {
			return nil, nil, err
		}
		selector := compilerInputActivationTuple{table: "fugue_platform_lkg_snapshots", key: key}
		if err := readCompilerInputActivationTuple(ctx, tx, &selector, false); err != nil {
			return nil, nil, err
		}
		proof.tuples = append(proof.tuples, selector)
		snapshot, err := scanPlatformLKGSnapshot(tx.QueryRowContext(ctx, `SELECT id,artifact_id,artifact_kind,scope_key,
 left(scope_json::text,16385),schema_version,generation,generation_sequence,content_hash,left(artifact_provenance_json::text,16385),
 verified_by_release_id,verification_evidence_hash,left(snapshot_provenance_json::text,16385),expires_at,created_at,updated_at
 FROM fugue_platform_lkg_snapshots WHERE artifact_kind=$1 AND scope_key=$2`, refKind, refScope))
		if errors.Is(err, ErrNotFound) || err == nil && !time.Now().Before(snapshot.ExpiresAt) {
			// Expiration does not erase a retention root. It only means the
			// publication policy cannot select this verified baseline.
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		if snapshot.ExpiresAt.Before(proof.expires) {
			proof.expires = snapshot.ExpiresAt
		}
		roots = append(roots, root("publication-current-lkg", inputarchive.HotReference{Kind: "artifact", ID: snapshot.ArtifactID, ExpectedHash: snapshot.ContentHash}))
		snapshots = append(snapshots, snapshot)
	}
	return roots, snapshots, nil
}

func compilerInputProofHasPublication(proof *compilerInputActivationProof, id, rollback string, guard *platformProducerReleaseGuard) bool {
	if proof == nil || proof.publication == nil || proof.publication.artifactID == "" || proof.publication.lkgReleaseID != "" {
		return false
	}
	intent := *proof.publication
	if intent.id != id || intent.rollbackGeneration != rollback || intent.hasGuard != (guard != nil) {
		return false
	}
	return guard == nil || intent.guard == *guard
}
