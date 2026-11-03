package store

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

// Runs after a real signed DNS shadow/gray/full/LKG lifecycle, in both stores.
// Revoking its external source must block new publication, but cannot trap a
// timed-out producer by making restoration of its exact positive LKG impossible.
func testDNSPositiveRecoveryAfterSourceRevocation(t *testing.T, s *Store, parent model.PlatformArtifact) {
	t.Helper()
	now := time.Now().UTC()
	lkg, err := s.GetPlatformLKG(parent.ArtifactKind, parent.ScopeKey)
	if err != nil || lkg == nil || lkg.ArtifactID != parent.ID {
		t.Fatal("positive recovery baseline missing", err)
	}
	guard := &platformProducerReleaseGuard{Phase: "rollback", BaselineArtifactID: parent.ID}
	var state *model.State
	if s.db != nil {
		if _, err := s.db.Exec(`UPDATE fugue_platform_release_lanes SET frozen=true WHERE scope_key=$1`, platformconfig.GlobalScopeKey); err != nil {
			t.Fatal(err)
		}
		tx, err := s.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := s.pgFullReleaseSetSnapshot(context.Background(), tx, parent); !errors.Is(err, ErrConflict) {
			t.Fatal("ordinary DNS snapshot accepted revoked source", err)
		}
		state, err = s.pgRollbackReleaseSetSnapshot(context.Background(), tx, parent, "full", now, lkg, guard)
		if err != nil {
			t.Fatal("exact positive recovery snapshot required current source", err)
		}
		if _, err := s.pgRollbackReleaseSetSnapshot(context.Background(), tx, parent, "full", now, lkg, nil); !errors.Is(err, ErrConflict) {
			t.Fatal("unguarded rollback bypassed current source", err)
		}
	} else {
		if err := s.withLockedState(false, func(current *model.State) error {
			raw, err := json.Marshal(current)
			if err != nil {
				return err
			}
			return json.Unmarshal(raw, &state)
		}); err != nil {
			t.Fatal(err)
		}
		for i := range state.PlatformReleaseLanes {
			if state.PlatformReleaseLanes[i].ScopeKey == platformconfig.GlobalScopeKey {
				state.PlatformReleaseLanes[i].Frozen = true
			}
		}
	}
	check := func(checkpoint *model.PlatformLKGSnapshot, authority *platformProducerReleaseGuard) error {
		return validateVerifiedTrafficRecoveryAdmission(state, parent, "full", "", celldns.Keys(), now, checkpoint, authority)
	}
	if err := check(lkg, guard); err != nil {
		t.Fatal("positive recovery was blocked by revoked source", err)
	}
	for _, scenario := range []string{"no authority", "wrong baseline", "expired", "tampered signature", "wrong phase"} {
		bad, authority := *lkg, *guard
		var candidate = &authority
		switch scenario {
		case "no authority":
			candidate = nil
		case "wrong baseline":
			authority.BaselineArtifactID = "foreign-baseline"
		case "expired":
			bad.ExpiresAt = now.Add(-time.Second)
		case "tampered signature":
			bad.ContentHash = "sha256:tampered"
		case "wrong phase":
			authority.Phase = "verify"
		}
		if err := check(&bad, candidate); !errors.Is(err, ErrConflict) {
			t.Fatal("unverified recovery bypassed source admission", scenario, err)
		}
	}
	// Recovery still authenticates immutable members and signed membership.
	ids := parent.Content["artifact_ids"].([]any)
	index := platformArtifactIndex(state.PlatformArtifacts, ids[0].(string))
	state.PlatformArtifacts[index].ContentHash = "sha256:tampered"
	if err := check(lkg, guard); !errors.Is(err, ErrConflict) {
		t.Fatal("recovery accepted corrupt member", err)
	}
	current, err := s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, parent.ScopeKey)
	if err != nil || !reflect.DeepEqual(current, lkg) {
		t.Fatal("recovery admission altered positive LKG", err)
	}
	if parent.Content["publication_role"] != platformconfig.PublicationRoleCellDNS {
		t.Fatal("fixture did not exercise DNS-only recovery")
	}
}
