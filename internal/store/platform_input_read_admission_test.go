package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
)

func TestPlatformInputReadFileSelectsCurrentLKGWithoutDependingOnHistory(t *testing.T) {
	s, _ := fileTaskInputFixture(t, true)
	lkg, err := s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
	if err != nil || lkg == nil {
		t.Fatal(lkg, err)
	}
	read := PlatformInputReadSelection{LKGKind: lkg.ArtifactKind, LKGScope: lkg.ScopeKey, LKG: lkg}
	if err := s.withLockedState(true, func(st *model.State) error {
		old := *lkg
		old.ID, old.ArtifactID = "old-unselected-lkg", "missing-historical-artifact"
		old.CreatedAt, old.UpdatedAt, old.ExpiresAt = lkg.CreatedAt.Add(-time.Hour), lkg.UpdatedAt.Add(-time.Hour), time.Now().Add(-time.Minute)
		st.PlatformLKGSnapshots = append(st.PlatformLKGSnapshots, old)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckPlatformInputRead(t.Context(), []PlatformInputReadSelection{read}); err != nil {
		t.Fatal("unselected expired/corrupt historical LKG blocked current input", err)
	}
	if _, err := s.CreateNodeUpdateTask(taskCreateAdmin(), "file-task-updater", "", "", model.NodeUpdateTaskTypeRefreshDesiredState, map[string]string{}); err != nil {
		t.Fatal("file task treated unselected LKG history as a current dependency", err)
	}
	if err := s.withLockedState(true, func(st *model.State) error {
		newer := *lkg
		newer.ID, newer.ArtifactID = "new-current-cold-lkg", "missing-new-current"
		newer.UpdatedAt = lkg.UpdatedAt.Add(time.Minute)
		st.PlatformLKGSnapshots = append(st.PlatformLKGSnapshots, newer)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckPlatformInputRead(t.Context(), []PlatformInputReadSelection{read}); !errors.Is(err, ErrConflict) {
		t.Fatal("old valid LKG substituted for newly selected unavailable authority", err)
	}
	current, err := s.GetPlatformLKG(lkg.ArtifactKind, lkg.ScopeKey)
	if err != nil || current == nil {
		t.Fatal(current, err)
	}
	read.LKG = current
	if err := s.CheckPlatformInputRead(t.Context(), []PlatformInputReadSelection{read}); !errors.Is(err, ErrConflict) {
		t.Fatal("invalid latest LKG fell back to older historical authority", err)
	}
}

func TestPlatformInputReadPostgresColdRestoreAndFreshPool(t *testing.T) {
	s, objects, plan, _ := consumerPreparationFixture(t, "trusted")
	set, err := s.GetPlatformExpectedConsumerSet(plan.Ticket.Contract.ExpectedSetID)
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.GetPlatformArtifact(set.ReleaseSetID)
	if err != nil {
		t.Fatal(err)
	}
	reads := []PlatformInputReadSelection{{ArtifactID: a.ID, ContentHash: a.ContentHash,
		ReleaseID: set.ArtifactReleaseID, ExpectedConsumerSetID: set.ID}}
	before := consumerInputCounts(t, s)
	if err := s.CheckPlatformInputRead(t.Context(), reads); !errors.Is(err, ErrConflict) || before != consumerInputCounts(t, s) {
		t.Fatal("cold read bypassed verified pins or changed state", err)
	}
	r, err := restoreConsumerInputs(t.Context(), s.db, plan.Ticket, plan.VerificationKey, objects)
	if err != nil || r.Usage.GetRequests != 1 || r.Usage.PutRequests != 0 {
		t.Fatal(r, err)
	}
	objects.failGet = true
	db, err := openPostgresDatabase(s.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fresh := &Store{db: db, databaseURL: s.databaseURL}
	fresh.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
	if err := fresh.CheckPlatformInputRead(t.Context(), reads); err != nil || before != consumerInputCounts(t, fresh) {
		t.Fatal("fresh uncached reader needed old chunks/GET or changed business state", err)
	}
	if _, err := db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL WHERE state='verified'`); err != nil {
		t.Fatal(err)
	}
	if err := fresh.CheckPlatformInputRead(t.Context(), reads); !errors.Is(err, ErrConflict) {
		t.Fatal("reader reused stale verified evidence", err)
	}
}

func TestPlatformInputReadPostgresExactLKGSelectorIncludingAbsence(t *testing.T) {
	for _, change := range []string{"initial-empty", "insert", "update", "delete", "aba", "keyring", "pin", "epoch", "expired"} {
		t.Run(change, func(t *testing.T) {
			s, _, _ := inputActivationFixture(t, 1)
			if _, err := s.ValidatePlatformArtifact("activation-root", []model.PlatformArtifactValidationResult{{Name: "schema", Pass: true}}); err != nil {
				t.Fatal(err)
			}
			reads := []PlatformInputReadSelection{{LKGKind: model.PlatformArtifactKindReleaseSet, LKGScope: "global"}}
			if change != "initial-empty" && change != "insert" {
				insertTaskConfigurationLKG(t, s, "activation-root")
				lkg, err := s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
				if err != nil || lkg == nil {
					t.Fatal(lkg, err)
				}
				reads[0].LKG = lkg
			}
			if err := s.CheckPlatformInputRead(t.Context(), reads); err != nil {
				t.Fatal("initial read", err)
			}
			proof, err := s.prepareCompilerInputDependencyProof(t.Context(), nil, nil, nil, nil, reads)
			if err != nil {
				t.Fatal(err)
			}
			proof, pins, err := s.prepareCompilerInputActivationPins(t.Context(), proof)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "insert":
				insertTaskConfigurationLKG(t, s, "activation-root")
			case "update":
				_, err = s.db.Exec(`UPDATE fugue_platform_lkg_snapshots SET updated_at=updated_at`)
			case "delete", "aba":
				_, err = s.db.Exec(`DELETE FROM fugue_platform_lkg_snapshots`)
				if change == "aba" && err == nil {
					insertTaskConfigurationLKG(t, s, "activation-root")
				}
			case "keyring":
				s.ConfigurePlatformArtifactSigning(s.platformArtifactSigningKeyring())
			case "pin":
				_, err = s.db.Exec(`UPDATE fugue_platform_input_local_pins SET state='pending',verified_at=NULL`)
			case "epoch":
				_, err = s.db.Exec(`UPDATE fugue_platform_input_archive_control SET epoch=epoch+1`)
			case "expired":
				_, err = s.db.Exec(`UPDATE fugue_platform_lkg_snapshots SET expires_at=now()-interval '1 second'`)
			}
			if err != nil {
				t.Fatal(err)
			}
			err = s.withPreparedCompilerInputActivation(t.Context(), proof, pins, func(context.Context, *sql.Tx) error { return nil })
			if (err == nil) != (change == "initial-empty") {
				t.Fatal("reader consumed a changed selector/epoch/pin/keyring", change, err)
			}
			if change == "insert" || change == "delete" || change == "expired" {
				if err := s.CheckPlatformInputRead(t.Context(), reads); !errors.Is(err, ErrConflict) {
					t.Fatal("old observed selection accepted a changed current LKG", err)
				}
			}
		})
	}
}

func TestPlatformInputReadFileVerifiesOriginalContentAndIgnoresUnselectedHistory(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("neutral-file-key", "neutral", "", "", nil))
	snapshot, digest := compilerDNSFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	a, err := normalizePlatformArtifactForStore(model.PlatformArtifact{ID: "neutral-file-reader", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
		Generation: "neutral-file-generation", Status: model.PlatformArtifactStatusValidated, Content: map[string]any{"lineage": map[string]any{"input_snapshot_digest": digest}}})
	if err != nil {
		t.Fatal(err)
	}
	a.GenerationSequence = 1
	a, err = platformsafety.SignPlatformArtifact(a, s.platformArtifactSigningKeyring())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.withLockedState(true, func(st *model.State) error { st.PlatformArtifacts = append(st.PlatformArtifacts, a); return nil }); err != nil {
		t.Fatal(err)
	}
	reads := []PlatformInputReadSelection{{ArtifactID: a.ID, ContentHash: a.ContentHash}}
	if err := s.CheckPlatformInputRead(t.Context(), reads); err != nil {
		t.Fatal("valid file original unavailable", err)
	}
	// DNS source observations can legitimately select more than 64 bindings
	// (17 scopes with gray/full/LKG publications). Share the original graph
	// and transport bounds instead of imposing a smaller unrelated read limit.
	many := make([]PlatformInputReadSelection, 205)
	for i := range many {
		many[i] = reads[0]
	}
	if err := s.CheckPlatformInputRead(t.Context(), many); err != nil {
		t.Fatal("bounded multi-scope source selection rejected", err)
	}
	many[len(many)-1].ContentHash = "sha256:" + strings.Repeat("a", 64)
	if err := s.CheckPlatformInputRead(t.Context(), many); !errors.Is(err, ErrConflict) {
		t.Fatal("reference dedup discarded a conflicting selected digest", err)
	}
	// State's claimed content hash is deliberately unchanged. Checking only
	// the manifest/digest label would miss this persisted byte corruption.
	if err := s.withLockedState(true, func(st *model.State) error {
		st.PlatformArtifactContents[0].Content = map[string]any{"unrelated": "corrupt"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckPlatformInputRead(t.Context(), reads); !errors.Is(err, ErrConflict) {
		t.Fatal("file read trusted claimed digest without original content", err)
	}
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		// Existing corrupt immutable content cannot be silently overwritten.
		if err := s.withLockedState(true, func(st *model.State) error { st.PlatformArtifactContents = nil; return nil }); err != nil {
			t.Fatal(err)
		}
		if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.withLockedState(true, func(st *model.State) error {
		st.PlatformArtifacts = append(st.PlatformArtifacts, model.PlatformArtifact{ID: "unselected-history", ContentHash: "invalid"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckPlatformInputRead(t.Context(), reads); err != nil {
		t.Fatal("unselected corrupt history became a reader dependency", err)
	}
}
