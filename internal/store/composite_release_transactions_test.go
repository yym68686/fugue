package store

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"fugue/internal/compositecoordinator"
	"fugue/internal/model"
	"fugue/internal/releasecontract"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestCompositeReleaseTransactionPersistsAndRejectsStaleCAS(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	record, err := s.CreateCompositeReleaseTransaction(storeCompositePlan(t))
	if err != nil {
		t.Fatal(err)
	}
	advanced, err := s.AdvanceCompositeReleaseTransaction(record.ID, record.Revision, record.Plan.Digest, record.Plan.FencingEpoch,
		compositecoordinator.Transition{Kind: compositecoordinator.TransitionBeginApply})
	if err != nil {
		t.Fatal(err)
	}
	if advanced.State != compositecoordinator.StateApplying || advanced.Revision != record.Revision+1 {
		t.Fatalf("advanced record = %#v", advanced)
	}
	if _, err := s.AdvanceCompositeReleaseTransaction(record.ID, record.Revision, record.Plan.Digest, record.Plan.FencingEpoch,
		compositecoordinator.Transition{Kind: compositecoordinator.TransitionBeginApply}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale revision did not fail CAS: %v", err)
	}
	if _, err := s.AdvanceCompositeReleaseTransaction(record.ID, advanced.Revision, record.Plan.Digest, "12",
		compositecoordinator.Transition{Kind: compositecoordinator.TransitionBeginObservation, EvidenceDigest: storeCompositeDigest("1")}); !errors.Is(err, ErrConflict) {
		t.Fatalf("wrong fence did not fail closed: %v", err)
	}

	reopened := New(path)
	if err := reopened.Init(); err != nil {
		t.Fatal(err)
	}
	persisted, err := reopened.GetCompositeReleaseTransaction(record.ID)
	if err != nil || persisted.Digest != advanced.Digest || persisted.Revision != advanced.Revision {
		t.Fatalf("durable record = %#v err=%v", persisted, err)
	}
}

func TestCompositeReleaseTransactionCreateFailsClosedOnCorruptDurableRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateCompositeReleaseTransaction(storeCompositePlan(t)); err != nil {
		t.Fatal(err)
	}
	if err := s.withLockedState(true, func(state *model.State) error {
		state.CompositeTransactions[0].Digest = storeCompositeDigest("9")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateCompositeReleaseTransaction(storeCompositePlan(t)); err == nil || errors.Is(err, ErrConflict) {
		t.Fatalf("corrupt durable record did not fail closed: %v", err)
	}
}

func TestCompositeReleaseTransactionPostgresUsesAtomicPlanFenceRevisionCAS(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	record, err := compositecoordinator.NewRecord("composite-release-pg", storeCompositePlan(t), time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT record_json
FROM fugue_composite_release_transactions
WHERE id = $1`)).WithArgs(record.ID).
		WillReturnRows(sqlmock.NewRows([]string{"record_json"}).AddRow(encoded))
	mock.ExpectExec(`(?s)UPDATE fugue_composite_release_transactions.*WHERE id = \$1 AND plan_digest = \$8 AND fencing_epoch = \$9 AND record_revision = \$10`).
		WithArgs(record.ID, compositecoordinator.StateApplying, 0, -1, int64(2), sqlmock.AnyArg(), sqlmock.AnyArg(), record.Plan.Digest, record.Plan.FencingEpoch, record.Revision).
		WillReturnResult(sqlmock.NewResult(0, 1))
	advanced, err := s.pgAdvanceCompositeReleaseTransaction(record.ID, record.Revision, record.Plan.Digest, record.Plan.FencingEpoch,
		compositecoordinator.Transition{Kind: compositecoordinator.TransitionBeginApply})
	if err != nil || advanced.Revision != 2 || advanced.State != compositecoordinator.StateApplying {
		t.Fatalf("Postgres CAS advanced=%#v err=%v", advanced, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCompositeReleaseTransactionPostgresSchemaIsAdditiveAndFenced(t *testing.T) {
	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS fugue_composite_release_transactions",
		"plan_digest TEXT NOT NULL UNIQUE",
		"fencing_epoch TEXT NOT NULL",
		"record_revision BIGINT NOT NULL CHECK (record_revision > 0)",
		"record_json JSONB NOT NULL",
		"idx_fugue_composite_release_transactions_state_updated",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("composite release schema missing %q", required)
		}
	}
}

func storeCompositePlan(t *testing.T) releasecontract.CompositeReleasePlan {
	t.Helper()
	plan, err := releasecontract.NewCompositeReleasePlan(releasecontract.CompositeReleasePlan{
		BaseCommit: "1111111111111111111111111111111111111111", TargetCommit: "2222222222222222222222222222222222222222",
		ImageActivationPlanDigest: storeCompositeDigest("a"), Generation: "7", FencingEpoch: "11",
		BaseVersions:   []releasecontract.DomainVersion{{Domain: releasecontract.DomainAuthoritativeDNS, Version: storeCompositeDigest("b")}, {Domain: releasecontract.DomainControlPlane, Version: storeCompositeDigest("c")}},
		TargetVersions: []releasecontract.DomainVersion{{Domain: releasecontract.DomainAuthoritativeDNS, Version: storeCompositeDigest("d")}, {Domain: releasecontract.DomainControlPlane, Version: storeCompositeDigest("e")}},
		Steps: []releasecontract.CompositeReleaseStep{
			storeCompositeStep("authoritative-dns", releasecontract.DomainAuthoritativeDNS, "control_plane_release_adapter_authoritative_dns", "b", "d", nil),
			storeCompositeStep("control-plane", releasecontract.DomainControlPlane, "control_plane_release_adapter_control_plane", "c", "e", []string{"authoritative-dns"}),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func storeCompositeStep(id string, domain releasecontract.Domain, adapter, base, target string, depends []string) releasecontract.CompositeReleaseStep {
	return releasecontract.CompositeReleaseStep{
		ID: id, Domain: domain, Adapter: adapter, DependsOn: depends, ActivationIDs: []string{"activate-" + id},
		BaseVersion: storeCompositeDigest(base), TargetVersion: storeCompositeDigest(target),
		ForwardRenderedDigest: storeCompositeDigest("f"), ReverseRenderedDigest: storeCompositeDigest("0"),
		Observation:           releasecontract.CompositeObservationPolicy{HealthEvidenceDigest: storeCompositeDigest("9"), MinimumSamples: "5", WindowSeconds: "120"},
		RollbackBudgetSeconds: "300",
	}
}

func storeCompositeDigest(digit string) string { return "sha256:" + strings.Repeat(digit, 64) }
