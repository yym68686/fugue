package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

// Only the server constructs this intent. Preparation neither registers a
// consumer nor restores an input. Ordinary identity, cursor, lane and audit
// checks still run atomically after the complete proof and pins are consumed.
type compilerInputConsumerIntent struct {
	prospective  *model.PlatformExpectedConsumerSet
	expectedID   string
	heartbeat    *platformcontrol.PlatformConsumerHeartbeatEnvelope
	bytes        int
	artifactKind string
}

func (s *Store) prepareCompilerInputConsumer(ctx context.Context, intent compilerInputConsumerIntent) (*compilerInputActivationProof, []compilerInputPinBinding, error) {
	proof, err := s.prepareOrdinaryCompilerInputProof(ctx, nil, nil, nil, &intent, nil)
	if err != nil {
		return nil, nil, errors.Join(err, ErrConflict)
	}
	return s.prepareCompilerInputActivationPins(ctx, proof)
}

// Resolve the generation namespace inside the SAME RR snapshot as the full
// closure. Optional heartbeat kind/scope fields are filled by the existing
// binding contract later; they cannot select a different input namespace here.
func prepareCompilerInputConsumerRoots(ctx context.Context, tx *sql.Tx, intent *compilerInputConsumerIntent) ([]inputarchive.HotRoot, error) {
	if intent == nil || (intent.prospective != nil) == (intent.heartbeat != nil) || intent.bytes < 0 || intent.bytes > compilerInputHotArtifactBytes || len(intent.expectedID) > 512 || strings.TrimSpace(intent.expectedID) != intent.expectedID {
		return nil, ErrInvalidInput
	}
	roots := []inputarchive.HotRoot{}
	seen := map[inputarchive.HotReference]bool{}
	add := func(kind, id, artifactKind, scope string) error {
		if id == "" {
			return nil
		}
		if len(id) > 512 || len(artifactKind) > 128 || len(scope) > 512 || strings.TrimSpace(id) != id {
			return ErrInvalidInput
		}
		ref := inputarchive.HotReference{Kind: kind, ID: id, ArtifactKind: artifactKind, ScopeKey: scope}
		if !seen[ref] {
			seen[ref] = true
			roots = append(roots, inputarchive.HotRoot{Source: "consumer-admission", ID: intent.expectedID, Reference: ref})
		}
		return nil
	}
	if set := intent.prospective; set != nil {
		intent.artifactKind = set.ArtifactKind
		for _, ref := range []inputarchive.HotReference{
			{Kind: "artifact", ID: set.ReleaseSetID},
			{Kind: "release", ID: set.ArtifactReleaseID},
			{Kind: "generation", ID: set.ExpectedGeneration, ArtifactKind: set.ArtifactKind, ScopeKey: set.ScopeKey},
		} {
			if err := add(ref.Kind, ref.ID, ref.ArtifactKind, ref.ScopeKey); err != nil {
				return nil, err
			}
		}
	} else {
		if intent.expectedID == "" {
			return nil, ErrInvalidInput
		}
		if err := add("expectation", intent.expectedID, "", ""); err != nil {
			return nil, err
		}
		var kind, scope string
		if err := tx.QueryRowContext(ctx, `SELECT left(artifact_kind,129),left(scope_key,513) FROM fugue_platform_expected_consumer_sets WHERE id=$1`, intent.expectedID).Scan(&kind, &scope); err != nil {
			return nil, err
		}
		if len(kind) > 128 || len(scope) > 512 || kind == "" || scope == "" {
			return nil, ErrConflict
		}
		intent.artifactKind = kind
		h := intent.heartbeat
		for _, generation := range []string{h.DesiredGeneration, h.ActualGeneration, h.CandidateGeneration, h.LKGGeneration} {
			if err := add("generation", generation, kind, scope); err != nil {
				return nil, err
			}
		}
	}
	if len(roots) == 0 {
		return nil, ErrInvalidInput
	}
	return roots, nil
}

func cloneCompilerInputConsumerHeartbeat(h platformcontrol.PlatformConsumerHeartbeatEnvelope) (platformcontrol.PlatformConsumerHeartbeatEnvelope, int, error) {
	raw, err := json.Marshal(h)
	if err != nil || len(raw) > 65536 {
		return h, 0, ErrInvalidInput
	}
	var copy platformcontrol.PlatformConsumerHeartbeatEnvelope
	if err := json.Unmarshal(raw, &copy); err != nil {
		return h, 0, err
	}
	return copy, len(raw), nil
}
