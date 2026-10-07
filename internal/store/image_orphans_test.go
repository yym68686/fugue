package store

import (
	"context"
	"database/sql"
	"errors"
	"fugue/internal/imagecachepolicy"
	"fugue/internal/model"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOrphanPolicyAndArtifactDurability(t *testing.T) {
	for _, backend := range []string{"json", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			s := New(filepath.Join(t.TempDir(), "state.json"))
			if backend == "postgres" {
				dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
				if dsn == "" {
					t.Skip("isolated PostgreSQL required")
				}
				if !strings.Contains(dsn, "fugue_test") {
					t.Fatal("test database required")
				}
				db, err := sql.Open("pgx", dsn)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				s = &Store{db: db, databaseURL: dsn}
				tx, err := db.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = s.applyPostgresSchemaTx(context.Background(), tx); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
				if _, err = db.Exec(`TRUNCATE fugue_image_orphan_policies,fugue_image_orphan_decisions,fugue_image_orphan_events,fugue_build_artifacts`); err != nil {
					t.Fatal(err)
				}
			} else if err := s.Init(); err != nil {
				t.Fatal(err)
			}
			p := imagecachepolicy.DefaultOrphanPolicy()
			p.Mode = "retire"
			p.RepositoryPrefixes = []string{"apps"}
			p.QuarantineSeconds = 600
			saved, err := s.UpdateImageOrphanPolicy(p, 0, "admin")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.UpdateImageOrphanPolicy(p, 0, "other"); !errors.Is(err, ErrConflict) {
				t.Fatal("policy compare-and-swap missing", err)
			}
			loaded, err := s.GetImageOrphanPolicy()
			if err != nil || loaded.Generation != 1 || loaded.UpdatedBy != "admin" {
				t.Fatal(loaded, err)
			}
			now := time.Now().UTC()
			a := model.BuildArtifact{AppID: "sample", TenantID: "tenant", OperationID: "op", JobName: "job", ImageRef: "registry.example/apps/sample:unique"}
			a, err = s.SaveBuildArtifact(a)
			if err != nil {
				t.Fatal(err)
			}
			a.Digest = "sha256:" + strings.Repeat("a", 64)
			a.CacheEndpoint = "http://worker:5000"
			a.ClusterNodeName = "worker"
			a.VerifiedAt = &now
			a, err = s.SaveBuildArtifact(a)
			if err != nil {
				t.Fatal(err)
			}
			reg := a
			reg.Digest = ""
			reg.VerifiedAt = nil
			reg, err = s.SaveBuildArtifact(reg)
			if err != nil || reg.Digest != a.Digest {
				t.Fatal("registration erased receipt", reg, err)
			}
			a.Digest = "sha256:" + strings.Repeat("b", 64)
			if _, err = s.SaveBuildArtifact(a); !errors.Is(err, ErrConflict) {
				t.Fatal("receipt rebound", err)
			}
			node := model.ImageCacheNodeInventory{NodeID: model.NewID("node"), ClusterNodeName: model.NewID("worker"), ObservedAt: now, SnapshotComplete: true, ManifestCount: 2}
			m := model.ImageCacheManifest{Repo: "apps/sample", Target: "one", Digest: "sha256:" + strings.Repeat("c", 64), Present: true, LastSeenAt: now}
			partial, err := s.UpsertImageCacheInventory(node, []model.ImageCacheManifest{m})
			if err != nil || partial.SnapshotComplete {
				t.Fatal("missing chunk accepted", partial, err)
			}
			m.Target = "two"
			full, err := s.UpsertImageCacheInventory(node, []model.ImageCacheManifest{m})
			if err != nil || !full.SnapshotComplete {
				t.Fatal("complete chunk set rejected", full, err)
			}
			inventories, err := s.ListImageCacheNodeInventories(model.ImageCacheNodeInventoryFilter{NodeID: node.NodeID})
			if err != nil || len(inventories) != 1 || !inventories[0].SnapshotComplete {
				t.Fatal("completeness not persisted", inventories, err)
			}
			d := model.ImageOrphanDecision{ID: "decision", Ownership: "unknown", State: "retirement_authorized", PolicyGeneration: saved.Generation}
			if err = s.SaveImageOrphanDecisions([]model.ImageOrphanDecision{d}); err != nil {
				t.Fatal(err)
			}
			d.State = "protected"
			if err = s.SaveImageOrphanDecisions([]model.ImageOrphanDecision{d}); err != nil {
				t.Fatal(err)
			}
			decisions, err := s.ListImageOrphanDecisions()
			if err != nil || len(decisions) != 1 || decisions[0].State != "protected" {
				t.Fatal(decisions, err)
			}
			if backend == "postgres" {
				var count int
				if err = s.db.QueryRow(`SELECT count(*) FROM fugue_image_orphan_events WHERE decision_id='decision'`).Scan(&count); err != nil || count != 2 {
					t.Fatal("audit transitions missing", count, err)
				}
			}
		})
	}
}
