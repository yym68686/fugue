package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPlatformInputReadPostgresConcurrentLKGSelectors(t *testing.T) {
	for _, mode := range []string{"absent", "present", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			s, _, _ := inputActivationFixture(t, 1)
			if _, err := s.ValidatePlatformArtifact("activation-root", []model.PlatformArtifactValidationResult{{Name: "schema", Pass: true}}); err != nil {
				t.Fatal(err)
			}
			reads := []PlatformInputReadSelection{{LKGKind: model.PlatformArtifactKindReleaseSet, LKGScope: "global"}}
			if mode != "absent" {
				insertTaskConfigurationLKG(t, s, "activation-root")
				var err error
				reads[0].LKG, err = s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
				if err != nil || reads[0].LKG == nil {
					t.Fatal(reads, err)
				}
			}
			if mode == "mixed" {
				reads = append(reads, PlatformInputReadSelection{LKGKind: model.PlatformArtifactKindReleaseSet, LKGScope: "neutral-scope"})
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
			// Both positive selection and absence still fence changes to this
			// witness, including SQL outside Fugue's advisory-lock protocol.
			blocked := []string{
				`UPDATE fugue_platform_lkg_snapshots SET updated_at=updated_at`,
				`DELETE FROM fugue_platform_lkg_snapshots`,
				`TRUNCATE fugue_platform_lkg_snapshots`,
				`DROP INDEX idx_fugue_platform_lkg_kind_scope`,
			}
			unrelated := []string{
				`LOCK TABLE fugue_platform_lkg_snapshots IN SHARE ROW EXCLUSIVE MODE`,
				`INSERT INTO fugue_platform_lkg_snapshots(id,artifact_id,artifact_kind,scope_key,generation,content_hash,expires_at,created_at,updated_at)
 VALUES('neutral-phantom','activation-root','release_set','neutral-scope','neutral','neutral',NOW()+INTERVAL '1 hour',NOW(),NOW())`,
			}
			if mode != "present" {
				blocked = append(blocked, unrelated...)
				unrelated = nil
			}
			for _, query := range append(blocked, unrelated...) {
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
				mustBlock := false
				for _, q := range blocked {
					mustBlock = mustBlock || q == query
				}
				if mustBlock && stringSQLState(err) != "55P03" {
					t.Fatalf("reader did not fence LKG write %q: %v", query, err)
				}
				if !mustBlock && err != nil {
					t.Fatalf("present selector blocked an unrelated scope %q: %v", query, err)
				}
			}
		})
	}
}
