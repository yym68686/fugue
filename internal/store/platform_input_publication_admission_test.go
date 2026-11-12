package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
)

func TestPublicationInputSelectorCanonicalBounds(t *testing.T) {
	kind, scope := strings.Repeat("k", 128), strings.Repeat("s", 512)
	for _, scope := range []string{scope, strings.Repeat("<", 512)} {
		if gotKind, gotScope, err := compilerInputLKGSelector(compilerInputLKGSelectorKey(kind, scope)); err != nil || gotKind != kind || gotScope != scope {
			t.Fatal("valid selector boundary rejected", err)
		}
	}
	for _, key := range []string{`["route", "global"]`, `["route"]`, `["route",""]`, compilerInputLKGSelectorKey(kind+"k", scope), compilerInputLKGSelectorKey(kind, scope+"s")} {
		if _, _, err := compilerInputLKGSelector(key); err == nil {
			t.Fatal("malformed or oversized selector accepted")
		}
	}
}

func publicationInputCounts(t *testing.T, s *Store) [3]int64 {
	t.Helper()
	var counts [3]int64
	if err := s.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_artifact_releases),
 (SELECT count(*) FROM fugue_platform_release_messages),COALESCE((SELECT sum(fencing_token) FROM fugue_platform_release_lanes),0)`).Scan(&counts[0], &counts[1], &counts[2]); err != nil {
		t.Fatal(err)
	}
	return counts
}

func publicationInputValidate(t *testing.T, s *Store, id string) {
	t.Helper()
	if _, err := s.ValidatePlatformArtifact(id, []model.PlatformArtifactValidationResult{{Name: "schema", Pass: true}}); err != nil {
		t.Fatal(err)
	}
}

func TestPublicationInputPostgresCommitBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("exercises publication beyond the ordinary ten-second admission budget")
	}
	s, roots, _ := inputActivationFixture(t, 0)
	publicationInputValidate(t, s, "activation-root")
	ordinary, err := s.prepareCompilerInputActivation(t.Context(), roots)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.withPreparedCompilerInputActivation(t.Context(), ordinary, nil, func(ctx context.Context, _ *sql.Tx) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 10*time.Second {
			t.Fatal("ordinary admission lost its short deadline")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, cancelEarly := range []bool{false, true} {
		proof, pins, err := s.prepareCompilerInputPublication(t.Context(), "activation-root", "", nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
		if cancelEarly {
			cancel()
		}
		before := publicationInputCounts(t, s)
		ctx, trace := startPlatformMutationTrace(ctx, "publication-budget-test")
		err = s.consumePreparedCompilerInput(ctx, proof, pins, false, nil, func(ctx context.Context, tx *sql.Tx) error {
			// Represent CPU authority validation, not a SQL statement or lock
			// wait: their five-second/250ms limits must remain unchanged.
			timer := time.NewTimer(11 * time.Second)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
			}
			_, _, _, _, err := s.pgReleasePlatformArtifactInTransaction(ctx, tx, "activation-root", model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal(), nil, trace)
			return err
		})
		cancel()
		if cancelEarly {
			if err == nil || publicationInputCounts(t, s) != before {
				t.Fatal("caller cancellation changed publication", err)
			}
		} else if err != nil || publicationInputCounts(t, s)[0] != before[0]+1 {
			t.Fatal("publication did not commit within its existing budget", err)
		}
	}
}

func TestPublicationInputPostgresReleaseAndRollbackWithoutOldChunks(t *testing.T) {
	s, _, pins := inputActivationFixture(t, 2)
	first := lkgInputRelease(t, s, "activation-artifact-000")
	if _, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(first.ID, completePlatformVerificationRequest(first.FencingToken, true), testPlatformPrincipal()); err != nil {
		t.Fatal(err)
	}
	publicationInputValidate(t, s, "activation-artifact-001")
	if _, err := s.db.Exec(`DROP TABLE fugue_platform_input_chunks`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE pin_key=$1`, pins[0].Key); err != nil {
		t.Fatal(err)
	}
	before := publicationInputCounts(t, s)
	req := model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}
	if _, _, _, _, err := s.ReleasePlatformArtifact("activation-artifact-001", req, testPlatformPrincipal()); !errors.Is(err, ErrConflict) || publicationInputCounts(t, s) != before {
		t.Fatal("cold rollback baseline permitted publication or changed lane", err)
	}
	for _, index := range []int{0, 1} {
		if _, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{Key: pins[index].Key, Digest: pins[index].Digest, MaxRawBytes: 1024, Apply: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE pin_key=$1`, pins[1].Key); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := s.ReleasePlatformArtifact("activation-artifact-001", req, testPlatformPrincipal()); !errors.Is(err, ErrConflict) || publicationInputCounts(t, s) != before {
		t.Fatal("cold candidate permitted publication or changed lane", err)
	}
	if _, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{Key: pins[1].Key, Digest: pins[1].Digest, MaxRawBytes: 1024, Apply: true}); err != nil {
		t.Fatal(err)
	}
	db, err := openPostgresDatabase(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fresh := &Store{db: db, databaseURL: s.databaseURL}
	fresh.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
	_, second, _, baseline, err := fresh.ReleasePlatformArtifact("activation-artifact-001", req, testPlatformPrincipal())
	if err != nil || baseline == nil || second.PinnedRollbackGeneration != "activation-artifact-000" {
		t.Fatal("prepared release unavailable from fresh pool", second, baseline, err)
	}
	rollback := model.PlatformArtifactRollbackRequest{ReleaseChannel: "shadow", ToGeneration: "activation-artifact-000", Reason: "neutral recovery"}
	if _, err := s.db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE pin_key=$1`, pins[0].Key); err != nil {
		t.Fatal(err)
	}
	before = publicationInputCounts(t, s)
	if _, _, _, _, err := fresh.RollbackPlatformArtifact("activation-artifact-001", rollback, testPlatformPrincipal()); !errors.Is(err, ErrConflict) || publicationInputCounts(t, s) != before {
		t.Fatal("cold exact rollback target changed lane", err)
	}
	if _, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{Key: pins[0].Key, Digest: pins[0].Digest, MaxRawBytes: 1024, Apply: true}); err != nil {
		t.Fatal(err)
	}
	// The failed candidate is not being reactivated. Its corrupt signature
	// and unavailable input must not prevent recovery to the independent LKG.
	if _, err := s.db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE pin_key=$1`, pins[1].Key); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_platform_artifacts SET content_json='{"damaged":true}' WHERE id='activation-artifact-001'`); err != nil {
		t.Fatal(err)
	}
	target, recovered, _, _, err := fresh.RollbackPlatformArtifact("activation-artifact-001", rollback, testPlatformPrincipal())
	if err != nil || target.ID != "activation-artifact-000" || recovered.RollbackTargetGeneration != target.Generation {
		t.Fatal("prepared rollback unavailable without old table", target, recovered, err)
	}
}

func TestPublicationInputPostgresRequestBaselineIsIndependentlyPrepared(t *testing.T) {
	for _, kind := range []string{"dns", "reconfiguration"} {
		t.Run(kind, func(t *testing.T) {
			s, _, pins := inputActivationFixture(t, 2)
			first := lkgInputRelease(t, s, "activation-artifact-000")
			publicationInputValidate(t, s, "activation-artifact-001")
			baseline, err := s.GetPlatformArtifact(first.ArtifactID)
			if err != nil {
				t.Fatal(err)
			}
			ref := model.PlatformPublicationPrecondition{ArtifactID: baseline.ID, ContentHash: baseline.ContentHash, ReleaseID: first.ID, FencingToken: first.FencingToken}
			req := model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}
			if kind == "dns" {
				req.DNSPublicationPrecondition = &model.PlatformDNSPublicationPrecondition{LKGPublication: ref}
			} else {
				req.ProducerReconfiguration = &model.PlatformProducerReconfiguration{ServingFull: ref, PreviousPolicy: ref}
			}
			// No LKG selector exists. The candidate alone only needs its own
			// input; the request introduces a distinct serving/rollback root.
			plain, _, err := s.prepareCompilerInputPublication(t.Context(), "activation-artifact-001", "", nil)
			if err != nil || len(plain.inputs) != 1 {
				t.Fatal("fixture candidate closure", err)
			}
			proof, prepared, err := s.prepareCompilerInputPublication(t.Context(), "activation-artifact-001", "", nil, req)
			if err != nil || len(proof.inputs) != 2 || len(prepared) != 2 {
				t.Fatal("request baseline omitted from full preparation", err)
			}
			before := publicationInputCounts(t, s)
			ctx, trace := startPlatformMutationTrace(t.Context(), "publication-request-binding-test")
			if _, _, _, _, err := s.pgReleasePreparedPlatformArtifact(ctx, "activation-artifact-001", req, testPlatformPrincipal(), nil, plain, prepared, trace); !errors.Is(err, ErrInvalidInput) || publicationInputCounts(t, s) != before {
				t.Fatal("request consumed an unrelated candidate-only proof", err)
			}
			if _, err := s.db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE pin_key=$1`, pins[0].Key); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.prepareCompilerInputPublication(t.Context(), "activation-artifact-001", "", nil, req); !errors.Is(err, ErrConflict) || publicationInputCounts(t, s) != before {
				t.Fatal("cold serving baseline was hidden by candidate pins", err)
			}
		})
	}
}

func TestPublicationInputPostgresRequestBindsOtherScopeLKG(t *testing.T) {
	for _, change := range []string{"update", "delete", "signature"} {
		t.Run(change, func(t *testing.T) {
			s, _, _ := inputActivationFixture(t, 2)
			first := lkgInputRelease(t, s, "activation-artifact-000")
			if _, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(first.ID, completePlatformVerificationRequest(first.FencingToken, true), testPlatformPrincipal()); err != nil {
				t.Fatal(err)
			}
			baseline, err := s.GetPlatformArtifact(first.ArtifactID)
			if err != nil {
				t.Fatal(err)
			}
			// The release-set candidate and serving edge-route publication
			// have different scoped selectors, with only the latter present.
			publicationInputValidate(t, s, "activation-root")
			ref := model.PlatformPublicationPrecondition{ArtifactID: baseline.ID, ContentHash: baseline.ContentHash, ReleaseID: first.ID, FencingToken: first.FencingToken}
			req := model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow", ProducerReconfiguration: &model.PlatformProducerReconfiguration{ServingFull: ref, PreviousPolicy: ref}}
			proof, pins, err := s.prepareCompilerInputPublication(t.Context(), "activation-root", "", nil, req)
			if err != nil {
				t.Fatal(err)
			}
			selectors, present := 0, 0
			for _, tuple := range proof.tuples {
				if tuple.table == "fugue_platform_lkg_snapshots" {
					selectors++
					if tuple.xmin != "0" {
						present++
					}
				}
			}
			if selectors != 2 || present != 1 {
				t.Fatal("serving scope selector omitted", selectors, present)
			}
			query := map[string]string{
				"update":    `UPDATE fugue_platform_lkg_snapshots SET updated_at=updated_at`,
				"delete":    `DELETE FROM fugue_platform_lkg_snapshots`,
				"signature": `UPDATE fugue_platform_lkg_snapshots SET snapshot_provenance_json='{}'::jsonb`,
			}[change]
			if _, err := s.db.Exec(query); err != nil {
				t.Fatal(err)
			}
			before := publicationInputCounts(t, s)
			called := false
			err = s.withPreparedCompilerInputActivation(t.Context(), proof, pins, func(context.Context, *sql.Tx) error { called = true; return nil })
			if err == nil || called || publicationInputCounts(t, s) != before {
				t.Fatal("changed serving scope consumed preparation", err, called)
			}
			if change == "signature" {
				if _, _, err := s.prepareCompilerInputPublication(t.Context(), "activation-root", "", nil, req); !errors.Is(err, ErrConflict) {
					t.Fatal("serving scope LKG signature was not verified", err)
				}
			}
		})
	}
}

func TestPublicationInputPostgresRechecksSelectionAndDependencies(t *testing.T) {
	for _, action := range []string{"release", "rollback"} {
		for _, change := range []string{"candidate", "baseline", "pin", "epoch", "keyring", "selector-update", "selector-delete", "selector-aba", "wrong-proof", "wrong-guard", "wrong-target"} {
			t.Run(action+"/"+change, func(t *testing.T) {
				s, _, _ := inputActivationFixture(t, 2)
				first := lkgInputRelease(t, s, "activation-artifact-000")
				if _, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(first.ID, completePlatformVerificationRequest(first.FencingToken, true), testPlatformPrincipal()); err != nil {
					t.Fatal(err)
				}
				publicationInputValidate(t, s, "activation-artifact-001")
				rollback := ""
				if action == "rollback" {
					rollback = "activation-artifact-000"
				}
				proof, pins, err := s.prepareCompilerInputPublication(t.Context(), "activation-artifact-001", rollback, nil)
				if err != nil {
					t.Fatal(err)
				}
				before := publicationInputCounts(t, s)
				var guard *platformProducerReleaseGuard
				switch change {
				case "candidate", "baseline":
					id := "activation-artifact-001"
					if change == "baseline" {
						id = "activation-artifact-000"
					}
					_, err = s.db.Exec(`UPDATE fugue_platform_artifacts SET updated_at=updated_at WHERE id=$1`, id)
				case "pin":
					_, err = s.db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL`)
				case "epoch":
					_, err = s.db.Exec(`UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`)
				case "keyring":
					s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("different", "different", "", "", nil))
				case "selector-update":
					_, err = s.db.Exec(`UPDATE fugue_platform_lkg_snapshots SET updated_at=updated_at`)
				case "selector-delete":
					_, err = s.db.Exec(`DELETE FROM fugue_platform_lkg_snapshots`)
				case "selector-aba":
					_, err = s.db.Exec(`CREATE TEMP TABLE selector_copy AS TABLE fugue_platform_lkg_snapshots; DELETE FROM fugue_platform_lkg_snapshots; INSERT INTO fugue_platform_lkg_snapshots SELECT * FROM selector_copy`)
				case "wrong-proof":
					proof, err = s.prepareCompilerInputActivation(t.Context(), compilerInputLKGPromotionRoots(first.ID))
				case "wrong-guard":
					guard = &platformProducerReleaseGuard{PolicyReleaseID: first.ID}
				case "wrong-target":
					if action == "rollback" {
						rollback = "activation-artifact-001"
					} else {
						proof.publication.id = "different-intent"
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				ctx, trace := startPlatformMutationTrace(context.Background(), "publication-input-test")
				if action == "release" {
					_, _, _, _, err = s.pgReleasePreparedPlatformArtifact(ctx, "activation-artifact-001", model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal(), guard, proof, pins, trace)
				} else {
					_, _, _, _, err = s.pgRollbackPreparedPlatformArtifact(ctx, "activation-artifact-001", model.PlatformArtifactRollbackRequest{ReleaseChannel: "shadow", ToGeneration: rollback, Reason: "neutral recovery"}, testPlatformPrincipal(), guard, proof, pins, trace)
				}
				if err == nil || publicationInputCounts(t, s) != before {
					t.Fatal("changed or unrelated preparation advanced publication", err)
				}
			})
		}
	}
}

func TestPublicationInputPostgresBindsAbsentSelector(t *testing.T) {
	s, _, _ := inputActivationFixture(t, 1)
	publicationInputValidate(t, s, "activation-artifact-000")
	proof, pins, err := s.prepareCompilerInputPublication(t.Context(), "activation-artifact-000", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := s.GetPlatformArtifact("activation-artifact-000")
	if err != nil {
		t.Fatal(err)
	}
	seedVerifiedPlatformLKG(t, s, artifact)
	before := publicationInputCounts(t, s)
	ctx, trace := startPlatformMutationTrace(context.Background(), "publication-input-test")
	if _, _, _, _, err := s.pgReleasePreparedPlatformArtifact(ctx, artifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal(), nil, proof, pins, trace); err == nil || publicationInputCounts(t, s) != before {
		t.Fatal("new selector was not rechecked", err)
	}
}

func TestPublicationInputPostgresScopeLocksPrecedeDependencyRows(t *testing.T) {
	for _, action := range []string{"release", "rollback"} {
		t.Run(action, func(t *testing.T) {
			s, _, _ := inputActivationFixture(t, 2)
			publicationInputValidate(t, s, "activation-artifact-000")
			publicationInputValidate(t, s, "activation-artifact-001")
			rollback := ""
			if action == "rollback" {
				rollback = "activation-artifact-000"
			}
			proof, pins, err := s.prepareCompilerInputPublication(t.Context(), "activation-artifact-001", rollback, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			publisher, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer publisher.Rollback()
			var pid int
			if err := publisher.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
				t.Fatal(err)
			}
			if err := pgLockPlatformReleaseMutation(ctx, publisher, "activation-artifact-001", true); err != nil {
				t.Fatal(err)
			}
			// A standalone artifact uses a shared scope, so take the normal
			// verifier's exclusive scope to observe its precise queued waiter.
			if err := pgLockPromotionScope(ctx, publisher, "global", true); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				workerCtx, trace := startPlatformMutationTrace(ctx, "publication-input-test")
				var err error
				if action == "release" {
					_, _, _, _, err = s.pgReleasePreparedPlatformArtifact(workerCtx, "activation-artifact-001", model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal(), nil, proof, pins, trace)
				} else {
					_, _, _, _, err = s.pgRollbackPreparedPlatformArtifact(workerCtx, "activation-artifact-001", model.PlatformArtifactRollbackRequest{ReleaseChannel: "shadow", ToGeneration: rollback, Reason: "neutral recovery"}, testPlatformPrincipal(), nil, proof, pins, trace)
				}
				done <- err
			}()
			for {
				var waiting bool
				if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND wait_event='advisory')`, pid).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case err := <-done:
					t.Fatal("publication ended before exact scope wait", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			var id string
			if err := publisher.QueryRowContext(ctx, `SELECT id FROM fugue_platform_artifacts WHERE id='activation-artifact-001' FOR UPDATE NOWAIT`).Scan(&id); err != nil {
				t.Fatal("input row locked ahead of scope", err)
			}
			if _, err := publisher.ExecContext(ctx, `UPDATE fugue_platform_artifacts SET updated_at=updated_at WHERE id='activation-artifact-001'`); err != nil {
				t.Fatal(err)
			}
			if err := publisher.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := <-done; !errors.Is(err, ErrConflict) {
				t.Fatal("queued mutation did not invalidate preparation", err)
			}
			if got := publicationInputCounts(t, s); got != [3]int64{} {
				t.Fatal("refused queued publication advanced lane", got)
			}
		})
	}
}
