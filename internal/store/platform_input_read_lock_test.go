package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPlatformInputReadPostgresConcurrentLKGSelectors(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "present"}[present], func(t *testing.T) {
			s, _, _ := inputActivationFixture(t, 1)
			if _, err := s.ValidatePlatformArtifact("activation-root", []model.PlatformArtifactValidationResult{{Name: "schema", Pass: true}}); err != nil {
				t.Fatal(err)
			}
			reads := []PlatformInputReadSelection{{LKGKind: model.PlatformArtifactKindReleaseSet, LKGScope: "global"}}
			if present {
				insertTaskConfigurationLKG(t, s, "activation-root")
				var err error
				reads[0].LKG, err = s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
				if err != nil || reads[0].LKG == nil {
					t.Fatal(reads, err)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			prepare := func() (*compilerInputActivationProof, []compilerInputPinBinding) {
				t.Helper()
				proof, err := s.prepareOrdinaryCompilerInputProof(ctx, nil, nil, nil, nil, reads)
				if err != nil {
					t.Fatal(err)
				}
				proof, pins, err := s.prepareCompilerInputActivationPins(ctx, proof)
				if err != nil {
					t.Fatal(err)
				}
				return proof, pins
			}
			first, pins := prepare()
			second, otherPins := prepare()
			entered, release := make(chan struct{}), make(chan struct{})
			finished := make(chan error, 1)
			go func() {
				finished <- s.withPreparedCompilerInputActivation(ctx, first, pins, func(context.Context, *sql.Tx) error {
					close(entered)
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
			}()
			defer func() {
				close(release)
				if err := <-finished; err != nil {
					t.Error("first reader", err)
				}
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("first reader did not enter apply", ctx.Err())
			}
			if err := s.withPreparedCompilerInputActivation(ctx, second, otherPins, func(context.Context, *sql.Tx) error { return nil }); err != nil {
				t.Fatal("independent read serialized behind another read", err)
			}
			// Both positive selection and absence must still fence writers,
			// including direct SQL which does not use Fugue advisory locks.
			for _, query := range []string{
				`LOCK TABLE fugue_platform_lkg_snapshots IN SHARE ROW EXCLUSIVE MODE`,
				`INSERT INTO fugue_platform_lkg_snapshots(id,artifact_kind,scope_key) VALUES('neutral-phantom','release_set','neutral-scope')`,
				`UPDATE fugue_platform_lkg_snapshots SET updated_at=updated_at`,
				`DELETE FROM fugue_platform_lkg_snapshots`,
				`TRUNCATE fugue_platform_lkg_snapshots`,
			} {
				tx, err := s.db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout='50ms'`); err != nil {
					_ = tx.Rollback()
					t.Fatal(err)
				}
				_, err = tx.ExecContext(ctx, query)
				_ = tx.Rollback()
				if stringSQLState(err) != "55P03" {
					t.Fatalf("reader did not fence LKG write %q: %v", query, err)
				}
			}
		})
	}
}
