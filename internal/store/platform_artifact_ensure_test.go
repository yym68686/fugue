package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformsafety"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestEnsurePlatformArtifactJSONIsIdempotentAcrossLifecycle(t *testing.T) {
	t.Parallel()

	s := newPlatformArtifactEnsureTestStore(t)
	input := platformArtifactEnsureFixture("routegen_one", 100)
	first, created, err := s.EnsurePlatformArtifact(input)
	if err != nil {
		t.Fatalf("ensure first artifact: %v", err)
	}
	if !created || first.GenerationSequence != 1 || first.ID == "" {
		t.Fatalf("expected first ensure to create sequence 1: created=%t artifact=%+v", created, first)
	}
	validated, err := s.ValidatePlatformArtifact(first.ID, []model.PlatformArtifactValidationResult{{
		Name:     "schema",
		Pass:     true,
		Severity: model.RobustnessSeverityInfo,
	}})
	if err != nil {
		t.Fatalf("validate first artifact: %v", err)
	}
	if validated.Status != model.PlatformArtifactStatusValidated {
		t.Fatalf("expected validated artifact, got %+v", validated)
	}

	repeatedInput := platformArtifactEnsureFixture("routegen_one", 100)
	repeatedInput.ID = "caller-generated-different-id"
	repeated, created, err := s.EnsurePlatformArtifact(repeatedInput)
	if err != nil {
		t.Fatalf("repeat equivalent ensure: %v", err)
	}
	if created || repeated.ID != first.ID || repeated.GenerationSequence != first.GenerationSequence || repeated.Status != model.PlatformArtifactStatusValidated {
		t.Fatalf("equivalent ensure did not return existing lifecycle state: created=%t first=%+v repeated=%+v", created, first, repeated)
	}

	second, created, err := s.EnsurePlatformArtifact(platformArtifactEnsureFixture("routegen_two", 80))
	if err != nil {
		t.Fatalf("ensure second generation: %v", err)
	}
	if !created || second.GenerationSequence != 2 || second.ID == first.ID {
		t.Fatalf("expected second generation sequence 2: created=%t artifact=%+v", created, second)
	}

	artifacts, err := s.ListPlatformArtifacts(model.PlatformArtifactFilter{
		ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
		ScopeKey:     first.ScopeKey,
	})
	if err != nil {
		t.Fatalf("list ensured artifacts: %v", err)
	}
	if len(artifacts) != 2 {
		t.Fatalf("expected exactly two generations, got %+v", artifacts)
	}
}

func TestEnsurePlatformArtifactJSONRejectsImmutableConflicts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*model.PlatformArtifact)
	}{
		{
			name: "content",
			mutate: func(artifact *model.PlatformArtifact) {
				artifact.Content = map[string]any{"routes": []any{map[string]any{"weight": 99}}}
			},
		},
		{
			name: "metadata",
			mutate: func(artifact *model.PlatformArtifact) {
				artifact.Metadata["source"] = "different-controller"
			},
		},
		{
			name: "creator",
			mutate: func(artifact *model.PlatformArtifact) {
				artifact.CreatedByID = "another-controller"
			},
		},
		{
			name: "compatibility floor",
			mutate: func(artifact *model.PlatformArtifact) {
				artifact.CompatibilityFloor = "2.0"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := newPlatformArtifactEnsureTestStore(t)
			input := platformArtifactEnsureFixture("routegen_conflict", 100)
			if _, created, err := s.EnsurePlatformArtifact(input); err != nil || !created {
				t.Fatalf("seed artifact: created=%t err=%v", created, err)
			}
			test.mutate(&input)
			if _, created, err := s.EnsurePlatformArtifact(input); !errors.Is(err, ErrConflict) || created {
				t.Fatalf("conflicting ensure = created %t err %v, want ErrConflict", created, err)
			}
		})
	}
}

func TestEnsurePlatformArtifactJSONRejectsCorruptOrDuplicateExistingIdentity(t *testing.T) {
	t.Parallel()

	t.Run("corrupt signature", func(t *testing.T) {
		s := newPlatformArtifactEnsureTestStore(t)
		input := platformArtifactEnsureFixture("routegen_corrupt", 100)
		created, _, err := s.EnsurePlatformArtifact(input)
		if err != nil {
			t.Fatalf("seed artifact: %v", err)
		}
		if err := s.withLockedState(true, func(state *model.State) error {
			for index := range state.PlatformArtifacts {
				if state.PlatformArtifacts[index].ID == created.ID {
					state.PlatformArtifacts[index].Provenance.Signature = "tampered"
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("corrupt stored artifact fixture: %v", err)
		}
		if _, wasCreated, err := s.EnsurePlatformArtifact(input); !errors.Is(err, ErrConflict) || wasCreated {
			t.Fatalf("corrupt existing ensure = created %t err %v, want ErrConflict", wasCreated, err)
		}
	})

	t.Run("duplicate identity", func(t *testing.T) {
		s := newPlatformArtifactEnsureTestStore(t)
		input := platformArtifactEnsureFixture("routegen_duplicate", 100)
		created, _, err := s.EnsurePlatformArtifact(input)
		if err != nil {
			t.Fatalf("seed artifact: %v", err)
		}
		if err := s.withLockedState(true, func(state *model.State) error {
			duplicate := created
			duplicate.ID = "artifact-duplicate"
			state.PlatformArtifacts = append(state.PlatformArtifacts, duplicate)
			return nil
		}); err != nil {
			t.Fatalf("duplicate stored artifact fixture: %v", err)
		}
		if _, wasCreated, err := s.EnsurePlatformArtifact(input); !errors.Is(err, ErrConflict) || wasCreated {
			t.Fatalf("duplicate existing ensure = created %t err %v, want ErrConflict", wasCreated, err)
		}
	})
}

func TestEnsurePlatformArtifactJSONSerializesConcurrentWriters(t *testing.T) {
	t.Parallel()

	s := newPlatformArtifactEnsureTestStore(t)
	const writers = 32
	start := make(chan struct{})
	type result struct {
		artifact model.PlatformArtifact
		created  bool
		err      error
	}
	results := make(chan result, writers)
	var wait sync.WaitGroup
	for index := 0; index < writers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			artifact, created, err := s.EnsurePlatformArtifact(platformArtifactEnsureFixture("routegen_concurrent", 100))
			results <- result{artifact: artifact, created: created, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	createdCount := 0
	artifactID := ""
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent ensure failed: %v", result.err)
		}
		if result.created {
			createdCount++
		}
		if artifactID == "" {
			artifactID = result.artifact.ID
		}
		if result.artifact.ID != artifactID || result.artifact.GenerationSequence != 1 {
			t.Fatalf("concurrent ensure returned divergent artifact: first_id=%s result=%+v", artifactID, result.artifact)
		}
	}
	if createdCount != 1 {
		t.Fatalf("concurrent ensure created %d artifacts, want 1", createdCount)
	}
	artifacts, err := s.ListPlatformArtifacts(model.PlatformArtifactFilter{
		ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
		ScopeKey:     "edge:edge_group=edge-group-a,edge=edge-a",
	})
	if err != nil {
		t.Fatalf("list concurrent artifacts: %v", err)
	}
	if len(artifacts) != 1 || artifacts[0].ID != artifactID {
		t.Fatalf("concurrent ensure persisted unexpected artifacts: %+v", artifacts)
	}
}

func TestEnsurePlatformArtifactPostgresUsesExactIdentity(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	configureTestPlatformArtifactSigning(s)
	input := platformArtifactEnsureFixture("routegen_pg_existing", 100)
	normalized, err := normalizePlatformArtifactForStore(input)
	if err != nil {
		t.Fatalf("normalize existing fixture: %v", err)
	}
	normalized.GenerationSequence = 7
	normalized.Status = model.PlatformArtifactStatusValidated
	normalized, err = platformsafety.SignPlatformArtifact(normalized, s.platformArtifactSigningKeyring())
	if err != nil {
		t.Fatalf("sign existing fixture: %v", err)
	}

	mock.ExpectQuery(`(?s)FROM fugue_platform_artifacts\s+WHERE artifact_kind = \$1 AND scope_key = \$2 AND generation = \$3`).
		WithArgs(normalized.ArtifactKind, normalized.ScopeKey, normalized.Generation).
		WillReturnRows(platformArtifactEnsureRows(t, normalized))
	ensured, created, err := s.EnsurePlatformArtifact(input)
	if err != nil {
		t.Fatalf("ensure existing Postgres artifact: %v", err)
	}
	if created || ensured.ID != normalized.ID || ensured.GenerationSequence != 7 || ensured.Status != model.PlatformArtifactStatusValidated {
		t.Fatalf("unexpected Postgres ensure result: created=%t artifact=%+v", created, ensured)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func TestEnsurePlatformArtifactPostgresRecoversEquivalentConcurrentInsert(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	s := &Store{databaseURL: "postgres://example", db: db, dbReady: true}
	configureTestPlatformArtifactSigning(s)
	input := platformArtifactEnsureFixture("routegen_pg_race", 100)
	existing, err := normalizePlatformArtifactForStore(input)
	if err != nil {
		t.Fatalf("normalize concurrent fixture: %v", err)
	}
	existing.GenerationSequence = 1
	existing, err = platformsafety.SignPlatformArtifact(existing, s.platformArtifactSigningKeyring())
	if err != nil {
		t.Fatalf("sign concurrent fixture: %v", err)
	}

	exactIdentityQuery := `(?s)FROM fugue_platform_artifacts\s+WHERE artifact_kind = \$1 AND scope_key = \$2 AND generation = \$3`
	mock.ExpectQuery(exactIdentityQuery).
		WithArgs(existing.ArtifactKind, existing.ScopeKey, existing.Generation).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectBegin()
	mock.ExpectExec(`(?s)INSERT INTO fugue_platform_artifact_generation_sequences`).
		WithArgs(existing.ArtifactKind, existing.ScopeKey, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)UPDATE fugue_platform_artifact_generation_sequences.*RETURNING last_sequence`).
		WithArgs(existing.ArtifactKind, existing.ScopeKey, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"last_sequence"}).AddRow(int64(1)))
	mock.ExpectQuery(`(?s)INSERT INTO fugue_platform_artifact_contents.*RETURNING content_hash`).
		WithArgs(existing.ContentHash, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(platformArtifactEnsureContentRows(t, existing))
	mock.ExpectQuery(`(?s)INSERT INTO fugue_platform_artifacts.*RETURNING id`).
		WillReturnError(&pgconn.PgError{Code: "23505", Message: "concurrent generation"})
	mock.ExpectRollback()
	mock.ExpectQuery(exactIdentityQuery).
		WithArgs(existing.ArtifactKind, existing.ScopeKey, existing.Generation).
		WillReturnRows(platformArtifactEnsureRows(t, existing))

	ensured, created, err := s.EnsurePlatformArtifact(input)
	if err != nil {
		t.Fatalf("recover concurrent Postgres ensure: %v", err)
	}
	if created || ensured.ID != existing.ID || ensured.GenerationSequence != existing.GenerationSequence {
		t.Fatalf("concurrent Postgres ensure did not return winner: created=%t artifact=%+v", created, ensured)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sqlmock expectations: %v", err)
	}
}

func newPlatformArtifactEnsureTestStore(t *testing.T) *Store {
	t.Helper()
	s := New(filepath.Join(t.TempDir(), "store.json"))
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatalf("init artifact ensure store: %v", err)
	}
	return s
}

func platformArtifactEnsureFixture(generation string, weight int) model.PlatformArtifact {
	return model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
		Scope: model.PlatformArtifactScope{
			ScopeType:   "edge",
			EdgeGroupID: "edge-group-a",
			EdgeID:      "edge-a",
		},
		SchemaVersion:      model.PlatformArtifactSchemaVersionV1,
		Generation:         generation,
		Content:            map[string]any{"routes": []any{map[string]any{"weight": weight}}},
		CompatibilityFloor: model.BundleSchemaVersionV1,
		Metadata:           map[string]string{"source": "rollback-artifact-controller"},
		CreatedByType:      "controller",
		CreatedByID:        "rollback-artifact-controller",
	}
}

func platformArtifactEnsureRows(t *testing.T, artifact model.PlatformArtifact) *sqlmock.Rows {
	t.Helper()
	marshal := func(value any) []byte {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal platform artifact row: %v", err)
		}
		return raw
	}
	return sqlmock.NewRows([]string{
		"id", "artifact_kind", "scope_key", "scope_json", "schema_version", "generation", "generation_sequence", "status", "content_hash",
		"content_json", "validation_results_json", "compatibility_floor", "metadata_json",
		"created_by_type", "created_by_id", "provenance_json", "created_at", "updated_at",
	}).AddRow(
		artifact.ID, artifact.ArtifactKind, artifact.ScopeKey, marshal(artifact.Scope), artifact.SchemaVersion, artifact.Generation, artifact.GenerationSequence, artifact.Status, artifact.ContentHash,
		marshal(artifact.Content), marshal(artifact.ValidationResults), artifact.CompatibilityFloor, marshal(artifact.Metadata),
		artifact.CreatedByType, artifact.CreatedByID, marshal(artifact.Provenance), artifact.CreatedAt, artifact.UpdatedAt,
	)
}

func platformArtifactEnsureContentRows(t *testing.T, artifact model.PlatformArtifact) *sqlmock.Rows {
	t.Helper()
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		t.Fatalf("marshal platform artifact content row: %v", err)
	}
	return sqlmock.NewRows([]string{"content_hash", "content_json", "size_bytes", "created_at", "updated_at"}).
		AddRow(artifact.ContentHash, raw, int64(len(raw)), artifact.CreatedAt, artifact.UpdatedAt)
}
