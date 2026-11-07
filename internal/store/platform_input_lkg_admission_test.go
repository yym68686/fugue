package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/inputarchive"
	"fugue/internal/model"
)

func lkgInputRelease(t *testing.T, s *Store, id string) model.PlatformArtifactRelease {
	t.Helper()
	if _, err := s.ValidatePlatformArtifact(id, []model.PlatformArtifactValidationResult{{Name: "schema", Pass: true}}); err != nil {
		t.Fatal(err)
	}
	_, release, _, _, err := s.ReleasePlatformArtifact(id, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, testPlatformPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	return release
}

func assertLKGInputPromotionUnchanged(t *testing.T, s *Store, releaseID string, beforeHistory, beforeMessages int) {
	t.Helper()
	var state string
	var history, messages int
	if err := s.db.QueryRow(`SELECT verification_state FROM fugue_platform_artifact_releases WHERE id=$1`, releaseID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_lkg_snapshot_history`).Scan(&history); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_release_messages`).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if state == model.PlatformArtifactVerificationStateVerified || history != beforeHistory || messages != beforeMessages {
		t.Fatal("refused admission modified release/history/messages", state, history, messages)
	}
}

func TestLKGInputPostgresPromotionRequiresFullReadbackAndWorksWithoutOldChunks(t *testing.T) {
	s, _, pins := inputActivationFixture(t, 1)
	release := lkgInputRelease(t, s, "activation-artifact-000")
	if _, err := s.db.Exec(`DROP TABLE fugue_platform_input_chunks; UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL`); err != nil {
		t.Fatal(err)
	}
	request := completePlatformVerificationRequest(release.FencingToken, true)
	if _, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(release.ID, request, testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
		t.Fatal("pending readback pin permitted promotion", err)
	}
	assertLKGInputPromotionUnchanged(t, s, release.ID, 0, 1)
	if _, err := RunCompilerInputLocalPin(t.Context(), s.databaseURL, CompilerInputLocalPinOptions{Key: pins[0].Key, Digest: pins[0].Digest, MaxRawBytes: 1024, Apply: true}); err != nil {
		t.Fatal(err)
	}
	// A fresh pool has no decoder/cache state and cannot read the old table.
	db, err := openPostgresDatabase(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fresh := &Store{db: db, databaseURL: s.databaseURL}
	fresh.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
	_, verified, _, lkg, err := fresh.VerifyPlatformArtifactReleaseLKG(release.ID, request, testPlatformPrincipal())
	if err != nil || verified.VerificationState != "verified" || lkg == nil || lkg.VerifiedByReleaseID != release.ID {
		t.Fatal("prepared independent input could not promote", verified, lkg, err)
	}
	if _, _, _, again, err := fresh.VerifyPlatformArtifactReleaseLKG(release.ID, request, testPlatformPrincipal()); err != nil || again == nil || again.ID != lkg.ID {
		t.Fatal("idempotent promotion changed", again, err)
	}
}

func TestLKGInputPostgresRechecksExactReleaseAndAllPreparedDependencies(t *testing.T) {
	for _, change := range []string{"release", "artifact", "pin", "epoch", "keyring", "wrong-root", "wrong-release", "wrong-fence"} {
		t.Run(change, func(t *testing.T) {
			s, roots, _ := inputActivationFixture(t, 1)
			release := lkgInputRelease(t, s, "activation-artifact-000")
			originalReleaseID := release.ID
			proof, err := s.prepareCompilerInputActivation(t.Context(), compilerInputLKGPromotionRoots(release.ID))
			if err != nil {
				t.Fatal(err)
			}
			if change == "wrong-root" {
				proof, err = s.prepareCompilerInputActivation(t.Context(), roots)
				if err != nil {
					t.Fatal(err)
				}
			}
			proof, pins, err := s.prepareCompilerInputActivationPins(t.Context(), proof)
			if err != nil {
				t.Fatal(err)
			}
			request := completePlatformVerificationRequest(release.FencingToken, true)
			switch change {
			case "keyring":
				s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("changed", "changed", "", "", nil))
			case "wrong-release":
				release.ID = "unrelated-release"
			case "wrong-fence":
				request.FencingToken++
			default:
				q := map[string]string{
					"release":  `UPDATE fugue_platform_artifact_releases SET updated_at=updated_at`,
					"artifact": `UPDATE fugue_platform_artifacts SET content_json=content_json WHERE id='activation-artifact-000'`,
					"pin":      `UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL`,
					"epoch":    `UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`,
				}[change]
				if q != "" {
					if _, err := s.db.Exec(q); err != nil {
						t.Fatal(err)
					}
				}
			}
			ctx, trace := startPlatformMutationTrace(context.Background(), "platform-lkg-verify")
			if a, r, m, lkg, err := s.pgVerifyPreparedPlatformArtifactReleaseLKG(ctx, release.ID, request, testPlatformPrincipal(), nil, proof, pins, trace); err == nil || a.ID != "" || r.ID != "" || m.ID != "" || lkg != nil {
				t.Fatal("stale or unrelated proof promoted", a, r, m, lkg, err)
			}
			assertLKGInputPromotionUnchanged(t, s, originalReleaseID, 0, 1)
		})
	}
}

func TestLKGInputPostgresRollbackGenerationNeedsItsOwnPin(t *testing.T) {
	s, _, pins := inputActivationFixture(t, 2)
	first := lkgInputRelease(t, s, "activation-artifact-000")
	if _, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(first.ID, completePlatformVerificationRequest(first.FencingToken, true), testPlatformPrincipal()); err != nil {
		t.Fatal(err)
	}
	second := lkgInputRelease(t, s, "activation-artifact-001")
	if second.PinnedRollbackGeneration != "activation-artifact-000" {
		t.Fatal("fixture did not pin rollback generation", second)
	}
	if _, err := s.db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE pin_key=$1`, pins[0].Key); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(second.ID, completePlatformVerificationRequest(second.FencingToken, false), testPlatformPrincipal()); !errors.Is(err, ErrConflict) {
		t.Fatal("candidate pin hid unavailable rollback input", err)
	}
	assertLKGInputPromotionUnchanged(t, s, second.ID, 1, 3)
}

func TestLKGInputPostgresPublicationLocksPrecedeDependencyRows(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "changed-while-queued"}[changed], func(t *testing.T) {
			s, _, _ := inputActivationFixture(t, 1)
			release := lkgInputRelease(t, s, "activation-artifact-000")
			proof, err := s.prepareCompilerInputActivation(t.Context(), compilerInputLKGPromotionRoots(release.ID))
			if err != nil {
				t.Fatal(err)
			}
			proof, pins, err := s.prepareCompilerInputActivationPins(t.Context(), proof)
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
			var publisherPID int
			if err := publisher.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&publisherPID); err != nil {
				t.Fatal(err)
			}
			if err := pgLockPromotionScope(ctx, publisher, release.ScopeKey, false); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				workerCtx, trace := startPlatformMutationTrace(ctx, "platform-lkg-verify")
				_, _, _, _, err := s.pgVerifyPreparedPlatformArtifactReleaseLKG(workerCtx, release.ID,
					completePlatformVerificationRequest(release.FencingToken, true), testPlatformPrincipal(), nil, proof, pins, trace)
				done <- err
			}()
			// Observe the exact blocking backend, not an unrelated test's lock.
			for {
				var waiting bool
				if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity
 WHERE $1=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock' AND wait_event='advisory')`, publisherPID).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				select {
				case err := <-done:
					t.Fatal("verification ended before publication lock was observed", err)
				case <-ctx.Done():
					t.Fatal("verification did not wait for exact publisher", ctx.Err())
				case <-time.After(time.Millisecond):
				}
			}
			// A publication already holding the scope must still be able to
			// lock dependency rows. Otherwise verifier row->scope and publisher
			// scope->row form a cycle (or a spurious 250ms timeout).
			var id string
			if err := publisher.QueryRowContext(ctx, `SELECT id FROM fugue_platform_artifacts WHERE id=$1 FOR UPDATE NOWAIT`, release.ArtifactID).Scan(&id); err != nil {
				t.Fatal("queued input admission held artifact ahead of publication scope", err)
			}
			if err := publisher.QueryRowContext(ctx, `SELECT id FROM fugue_platform_artifact_releases WHERE id=$1 FOR UPDATE NOWAIT`, release.ID).Scan(&id); err != nil {
				t.Fatal("queued input admission held release ahead of publication scope", err)
			}
			if changed {
				if _, err := publisher.ExecContext(ctx, `UPDATE fugue_platform_artifacts SET updated_at=updated_at WHERE id=$1`, release.ArtifactID); err != nil {
					t.Fatal(err)
				}
			}
			if err := publisher.Commit(); err != nil {
				t.Fatal(err)
			}
			err = <-done
			if changed {
				if !errors.Is(err, ErrConflict) {
					t.Fatal("queued artifact change escaped full tuple recheck", err)
				}
				assertLKGInputPromotionUnchanged(t, s, release.ID, 0, 1)
			} else if err != nil {
				t.Fatal("unchanged publication failed after safe lock ordering", err)
			}
		})
	}
}

func TestLKGInputReleaseSetIncludesScopedPromotionPolicy(t *testing.T) {
	a := model.PlatformArtifact{ID: "neutral-parent", ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: "neutral-scope",
		ContentHash: "sha256:" + strings.Repeat("a", 64), Content: map[string]any{"artifact_ids": []string{"neutral-child"}}, Metadata: map[string]string{"policy_generation": "neutral-policy"}}
	node, err := compilerInputArtifactHotNode(a)
	if err != nil {
		t.Fatal(err)
	}
	want := inputarchive.HotReference{Kind: "generation", ID: "neutral-policy", ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: "neutral-scope"}
	for _, ref := range node.References {
		if ref == want {
			return
		}
	}
	t.Fatal("promotion policy omitted from complete closure", node)
}
