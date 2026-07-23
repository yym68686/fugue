package store

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"fugue/internal/compositecoordinator"
	"fugue/internal/releasecontract"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestCompositeRuntimeLaneFileReservationIsAtomicAndDurable(t *testing.T) {
	path := t.TempDir() + "/store.json"
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	lane, err := s.GetCompositeRuntimeLane()
	if err != nil {
		t.Fatal(err)
	}
	if lane.Generation != "0" || lane.FencingEpoch != "0" || lane.Version != 0 || lane.ActiveRecordID != "" {
		t.Fatalf("unexpected empty genesis lane: %#v", lane)
	}
	plan := storeCompositePlanForLane(t, "1", "1", "1")
	record, reserved, err := s.ReserveCompositeReleaseTransaction(plan, lane.Version)
	if err != nil {
		t.Fatal(err)
	}
	if reserved.ActiveRecordID != record.ID || reserved.Generation != "1" || reserved.FencingEpoch != "1" || reserved.Version != 1 {
		t.Fatalf("unexpected reservation: record=%#v lane=%#v", record, reserved)
	}
	reopened := New(path)
	if err := reopened.Init(); err != nil {
		t.Fatal(err)
	}
	durable, err := reopened.GetCompositeRuntimeLane()
	if err != nil || durable.Digest != reserved.Digest || durable.ActiveRecordID != record.ID {
		t.Fatalf("durable lane=%#v err=%v", durable, err)
	}
	second := storeCompositePlanForLane(t, "2", "2", "2")
	if _, _, err := reopened.ReserveCompositeReleaseTransaction(second, reserved.Version); !errors.Is(err, ErrConflict) {
		t.Fatalf("second active record did not fail closed: %v", err)
	}
	if _, _, err := reopened.ReserveCompositeReleaseTransaction(second, 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale lane version did not fail closed: %v", err)
	}
	if _, err := reopened.CreateCompositeReleaseTransaction(second); !errors.Is(err, ErrConflict) {
		t.Fatalf("legacy unreserved create remained open after lane materialization: %v", err)
	}
}

func TestCompositeRuntimeLaneGenesisUsesSettledHistoryAndRejectsPreparedHistory(t *testing.T) {
	t.Run("settled history", func(t *testing.T) {
		s := New(t.TempDir() + "/store.json")
		if err := s.Init(); err != nil {
			t.Fatal(err)
		}
		record, err := s.CreateCompositeReleaseTransaction(storeCompositePlan(t))
		if err != nil {
			t.Fatal(err)
		}
		transitions := []compositecoordinator.Transition{
			{Kind: compositecoordinator.TransitionBeginApply},
			{Kind: compositecoordinator.TransitionBeginObservation, EvidenceDigest: storeCompositeDigest("1")},
			{Kind: compositecoordinator.TransitionCompleteObservation, EvidenceDigest: storeCompositeDigest("2")},
			{Kind: compositecoordinator.TransitionBeginObservation, EvidenceDigest: storeCompositeDigest("3")},
			{Kind: compositecoordinator.TransitionCompleteObservation, EvidenceDigest: storeCompositeDigest("4")},
		}
		for _, transition := range transitions {
			record, err = s.AdvanceCompositeReleaseTransaction(record.ID, record.Revision, record.Plan.Digest, record.Plan.FencingEpoch, transition)
			if err != nil {
				t.Fatal(err)
			}
		}
		lane, err := s.GetCompositeRuntimeLane()
		if err != nil {
			t.Fatal(err)
		}
		if lane.Generation != "7" || lane.FencingEpoch != "11" || lane.LastSettledRecordID != record.ID || lane.ActiveRecordID != "" {
			t.Fatalf("history genesis=%#v", lane)
		}
		next := storeCompositePlanForLane(t, "8", "12", "8")
		_, reserved, err := s.ReserveCompositeReleaseTransaction(next, 0)
		if err != nil || reserved.Generation != "8" || reserved.FencingEpoch != "12" {
			t.Fatalf("next reservation=%#v err=%v", reserved, err)
		}
	})

	t.Run("prepared history", func(t *testing.T) {
		s := New(t.TempDir() + "/store.json")
		if err := s.Init(); err != nil {
			t.Fatal(err)
		}
		if _, err := s.CreateCompositeReleaseTransaction(storeCompositePlan(t)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetCompositeRuntimeLane(); !errors.Is(err, ErrConflict) {
			t.Fatalf("ambiguous prepared history error=%v", err)
		}
	})
}

func TestCompositeRuntimeLanePostgresReservationUsesOneTransactionAndVersionCAS(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	createdAt := time.Date(2026, 7, 23, 22, 0, 0, 0, time.UTC)
	lane, err := compositecoordinator.NewRuntimeLaneFromHistory(nil, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(lane)
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	mock.ExpectExec(`SELECT pg_advisory_xact_lock`).WithArgs(compositeRuntimeLaneAdvisoryLock).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)SELECT generation, fencing_epoch, lane_version,.*active_record_id, active_initial_record_digest, active_plan_digest,.*FOR UPDATE`).
		WithArgs(compositecoordinator.RuntimeLaneKey).
		WillReturnRows(sqlmock.NewRows([]string{
			"generation", "fencing_epoch", "lane_version",
			"active_record_id", "active_initial_record_digest", "active_plan_digest",
			"last_settled_record_id", "last_settled_record_digest", "last_settled_plan_digest",
			"frozen", "freeze_reason", "lane_json", "created_at", "updated_at",
		}).AddRow("0", "0", int64(0), "", "", "", "", "", "", false, "", encoded, createdAt, createdAt))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT record_json FROM fugue_composite_release_transactions ORDER BY updated_at ASC, id ASC`)).
		WillReturnRows(sqlmock.NewRows([]string{"record_json"}))
	mock.ExpectExec(`(?s)INSERT INTO fugue_composite_release_transactions`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`(?s)UPDATE fugue_composite_release_lanes.*WHERE lane_key = \$1 AND lane_version = \$15 AND active_record_id = '' AND frozen = FALSE`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	record, reserved, err := s.pgReserveCompositeReleaseTransaction(storeCompositePlanForLane(t, "1", "1", "9"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if reserved.ActiveRecordID != record.ID || reserved.Version != 1 || reserved.Generation != "1" || reserved.FencingEpoch != "1" {
		t.Fatalf("record=%#v lane=%#v", record, reserved)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCompositeRuntimeLanePostgresBlocksLegacyCreateAfterMaterialization(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	createdAt := time.Date(2026, 7, 23, 22, 0, 0, 0, time.UTC)
	lane, err := compositecoordinator.NewRuntimeLaneFromHistory(nil, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(lane)
	if err != nil {
		t.Fatal(err)
	}
	record, err := compositecoordinator.NewRecord("legacy-composite-record", storeCompositePlan(t), createdAt)
	if err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	mock.ExpectExec(`SELECT pg_advisory_xact_lock`).WithArgs(compositeRuntimeLaneAdvisoryLock).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)SELECT generation, fencing_epoch, lane_version,.*active_record_id, active_initial_record_digest, active_plan_digest,.*FOR UPDATE`).
		WithArgs(compositecoordinator.RuntimeLaneKey).
		WillReturnRows(sqlmock.NewRows([]string{
			"generation", "fencing_epoch", "lane_version",
			"active_record_id", "active_initial_record_digest", "active_plan_digest",
			"last_settled_record_id", "last_settled_record_digest", "last_settled_plan_digest",
			"frozen", "freeze_reason", "lane_json", "created_at", "updated_at",
		}).AddRow("0", "0", int64(0), "", "", "", "", "", "", false, "", encoded, createdAt, createdAt))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT record_json FROM fugue_composite_release_transactions ORDER BY updated_at ASC, id ASC`)).
		WillReturnRows(sqlmock.NewRows([]string{"record_json"}))
	mock.ExpectRollback()
	if _, err := s.pgCreateCompositeReleaseTransaction(record); !errors.Is(err, ErrConflict) {
		t.Fatalf("legacy create error=%v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCompositeRuntimeLanePostgresSchemaIsAdditiveAndFenced(t *testing.T) {
	schema := strings.Join(postgresSchemaStatements, "\n")
	for _, required := range []string{
		"CREATE TABLE IF NOT EXISTS fugue_composite_release_lanes",
		"lane_key TEXT PRIMARY KEY CHECK (lane_key = 'composite-runtime')",
		"lane_version BIGINT NOT NULL CHECK (lane_version >= 0)",
		"active_record_id TEXT NOT NULL DEFAULT ''",
		"active_initial_record_digest TEXT NOT NULL DEFAULT ''",
		"active_plan_digest TEXT NOT NULL DEFAULT ''",
		"frozen BOOLEAN NOT NULL DEFAULT FALSE",
		"lane_json JSONB NOT NULL",
	} {
		if !strings.Contains(schema, required) {
			t.Fatalf("composite runtime lane schema missing %q", required)
		}
	}
}

func storeCompositePlanForLane(t *testing.T, generation, fencing, salt string) releasecontract.CompositeReleasePlan {
	t.Helper()
	plan := storeCompositePlan(t)
	plan.Generation = generation
	plan.FencingEpoch = fencing
	plan.ImageActivationPlanDigest = storeCompositeDigest(salt)
	plan.Digest = ""
	rebuilt, err := releasecontract.NewCompositeReleasePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	return rebuilt
}
