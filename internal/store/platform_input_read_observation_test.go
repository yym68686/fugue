package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestPlatformInputReadObservationPreservesValidationError(t *testing.T) {
	var s *Store
	detail, err := s.CheckPlatformInputReadObserved(t.Context(), nil)
	if err != ErrInvalidInput || detail.Phase != "selection_validation" || detail.ErrorClass != "invalid_input" {
		t.Fatal("observation changed validation error", detail, err)
	}
}

func TestPlatformInputReadObservationPostgresOriginalWitnessFailure(t *testing.T) {
	for _, mode := range []string{"success", "changed", "locked"} {
		t.Run(mode, func(t *testing.T) {
			s, roots, _ := inputActivationFixture(t, 0)
			detail := PlatformInputReadFailure{}
			ctx := context.WithValue(t.Context(), platformInputReadObservationKey{}, &detail)
			proof, err := s.prepareCompilerInputActivation(ctx, roots)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "changed" {
				if _, err := s.db.ExecContext(ctx, `UPDATE fugue_platform_artifacts SET updated_at=updated_at WHERE id='activation-root'`); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "locked" {
				tx, err := s.db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err := tx.ExecContext(ctx, `SELECT id FROM fugue_platform_artifacts WHERE id='activation-root' FOR UPDATE`); err != nil {
					t.Fatal(err)
				}
			}
			applied := false
			err = s.withPreparedCompilerInputActivation(ctx, proof, nil, func(context.Context, *sql.Tx) error { applied = true; return nil })
			if mode == "success" {
				if err != nil || !applied || detail.Phase != "" {
					t.Fatal("observation changed successful admission", err, detail)
				}
				return
			}
			if !errors.Is(err, ErrConflict) || applied || detail.Phase != "admission" || detail.Relation != "fugue_platform_artifacts" {
				t.Fatal("original rejection changed or was not observed", err, detail)
			}
			sum := sha256.Sum256([]byte("activation-root"))
			if detail.KeyHash != "sha256:"+hex.EncodeToString(sum[:]) {
				t.Fatal("witness identity not independently correlatable", detail)
			}
			if mode == "changed" && (detail.Check != "dependency_changed" || !reflect.DeepEqual(detail.ChangedFields, []string{"xmin", "ctid"})) {
				t.Fatal("physical witness change not retained", detail)
			}
			if mode == "locked" && (detail.Check != "dependency_read" || detail.ErrorClass != "database" || detail.SQLState != "55P03") {
				t.Fatal("original typed SQL cause lost behind existing conflict response", detail)
			}
			original := detail
			observePlatformInputReadFailure(ctx, "prepare", "outer", nil, nil, ErrConflict)
			if !reflect.DeepEqual(original, detail) {
				t.Fatal("outer error replaced original failure")
			}
			raw, _ := json.Marshal(detail)
			if strings.Contains(string(raw), "activation-root") || strings.Contains(string(raw), "SELECT") {
				t.Fatal("raw identity or SQL retained")
			}
		})
	}
}
