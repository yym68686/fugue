package store

import (
	"encoding/json"
	"testing"

	"fugue/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestPostgresPlatformExpectedConsumerSetCreateGetAndList(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	set, err := normalizePlatformExpectedConsumerSetForStore(expectedConsumerSetFixture(t, 1))
	if err != nil {
		t.Fatalf("normalize fixture: %v", err)
	}

	mock.ExpectQuery(`(?s)INSERT INTO fugue_platform_expected_consumer_sets .*RETURNING`).
		WillReturnRows(expectedConsumerSetRows(t, set))
	created, err := s.CreatePlatformExpectedConsumerSet(set)
	if err != nil {
		t.Fatalf("create postgres expected consumer set: %v", err)
	}
	if created.ID != set.ID || len(created.Consumers) != 1 {
		t.Fatalf("unexpected created expected consumer set: %+v", created)
	}

	mock.ExpectQuery(`(?s)FROM fugue_platform_expected_consumer_sets\s+WHERE id = \$1`).
		WithArgs(set.ID).
		WillReturnRows(expectedConsumerSetRows(t, set))
	got, err := s.GetPlatformExpectedConsumerSet(set.ID)
	if err != nil {
		t.Fatalf("get postgres expected consumer set: %v", err)
	}
	if got.TopologyRevision != set.TopologyRevision || got.Consumers[0].ConsumerID != set.Consumers[0].ConsumerID {
		t.Fatalf("unexpected postgres expected consumer set: %+v", got)
	}

	mock.ExpectQuery(`(?s)FROM fugue_platform_expected_consumer_sets\s+WHERE TRUE.*release_set_id = \$1.*artifact_kind = \$2.*scope_key = \$3.*LIMIT \$4`).
		WithArgs(set.ReleaseSetID, set.ArtifactKind, set.ScopeKey, 10).
		WillReturnRows(expectedConsumerSetRows(t, set))
	listed, err := s.ListPlatformExpectedConsumerSets(model.PlatformExpectedConsumerSetFilter{
		ReleaseSetID: set.ReleaseSetID,
		ArtifactKind: set.ArtifactKind,
		ScopeKey:     set.ScopeKey,
		Limit:        10,
	})
	if err != nil {
		t.Fatalf("list postgres expected consumer sets: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != set.ID {
		t.Fatalf("unexpected postgres expected consumer sets: %+v", listed)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func expectedConsumerSetRows(t *testing.T, set model.PlatformExpectedConsumerSet) *sqlmock.Rows {
	t.Helper()
	scopeJSON, err := json.Marshal(set.Scope)
	if err != nil {
		t.Fatalf("marshal expected consumer scope: %v", err)
	}
	consumersJSON, err := json.Marshal(set.Consumers)
	if err != nil {
		t.Fatalf("marshal expected consumers: %v", err)
	}
	return sqlmock.NewRows([]string{
		"id", "release_set_id", "artifact_release_id", "artifact_kind", "scope_key", "scope_json",
		"expected_generation", "topology_revision", "revision", "requires_consumers",
		"required_cardinality", "optional_cardinality", "heartbeat_deadline",
		"convergence_deadline", "consumers_json", "created_at", "updated_at",
	}).AddRow(
		set.ID, set.ReleaseSetID, set.ArtifactReleaseID, set.ArtifactKind, set.ScopeKey, scopeJSON,
		set.ExpectedGeneration, set.TopologyRevision, set.Revision, set.RequiresConsumers,
		set.RequiredCardinality, set.OptionalCardinality, set.HeartbeatDeadline,
		set.ConvergenceDeadline, consumersJSON, set.CreatedAt, set.UpdatedAt,
	)
}
