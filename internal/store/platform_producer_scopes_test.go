package store

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestProducerScopeDiscoveryIsBoundedAndIndependent(t *testing.T) {
	s := New(t.TempDir() + "/state.json")
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		for _, scope := range []string{platformproducer.Scope, platformproducer.Scope + ":cell-a", "unrelated"} {
			state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: scope, ReleaseChannel: "shadow", ActiveReleaseID: scope})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListPlatformProducerScopes()
	if err != nil || !reflect.DeepEqual(got, []string{platformproducer.Scope, platformproducer.Scope + ":cell-a"}) {
		t.Fatal(got, err)
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		for i := 0; i < 33; i++ {
			scope := fmt.Sprintf("%s:cell-extra-%d", platformproducer.Scope, i)
			state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: scope, ReleaseChannel: "shadow", ActiveReleaseID: scope})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := s.ListPlatformProducerScopes(); got != nil || !errors.Is(err, ErrConflict) {
		t.Fatal("discovery silently truncated authorities", got, err)
	}
}

func TestPostgresProducerLocksExactImmutablePolicyScope(t *testing.T) {
	for _, scope := range []string{platformproducer.Scope, platformproducer.Scope + ":cell-a", "global"} {
		t.Run(scope, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectBegin()
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			columns := strings.Fields("id artifact_id artifact_kind scope_key scope_json generation release_channel status lane_key fencing_token version idempotency_key candidate_generation serving_unverified_generation verified_lkg_generation pinned_rollback_generation verification_state verification_evidence_json verified_at rollback_target_generation canary_rule_ref override_mode override_expires_at bypassed_invariants_json reason released_by_type released_by_id released_at created_at updated_at")
			now := time.Now().UTC()
			rows := sqlmock.NewRows(columns).AddRow("policy-release", "policy", model.PlatformArtifactKindPolicySnapshot, scope, `{}`, "generation", "shadow", "active", "lane", 1, 1, "", "", "", "", "", "", `{}`, nil, "", "", "", nil, `[]`, "", "", "", now, now, now)
			mock.ExpectQuery(`(?s)FROM fugue_platform_artifact_releases.*WHERE id = \$1`).WithArgs("policy-release").WillReturnRows(rows)
			if scope != "global" {
				sum := sha256.Sum256([]byte("fugue.platform.release-set-promotion/v1\x00" + scope))
				mock.ExpectExec(`SELECT pg_advisory_xact_lock\(\$1\)`).WithArgs(int64(binary.BigEndian.Uint64(sum[:8]))).WillReturnResult(sqlmock.NewResult(0, 1))
			}
			err = pgLockProducerPolicy(context.Background(), tx, &platformProducerReleaseGuard{PolicyReleaseID: "policy-release"})
			if (err == nil) != (scope != "global") {
				t.Fatal(scope, err)
			}
			mock.ExpectRollback()
			tx.Rollback()
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
