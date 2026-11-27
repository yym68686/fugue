package store

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestCurrentFullRollbackMessage(t *testing.T) {
	for _, backend := range []string{"postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := t.Context()
			old := time.Now().UTC().Truncate(time.Microsecond).Add(-48 * time.Hour)
			a := model.PlatformArtifact{ID: "original-artifact", ArtifactKind: model.PlatformArtifactKindReleaseSet, Scope: model.PlatformArtifactScope{ScopeType: "global"}, ScopeKey: "global", Generation: "original-generation", Content: map[string]any{}, CreatedAt: old, UpdatedAt: old}
			r := model.PlatformArtifactRelease{ID: "original-release", ArtifactID: a.ID, ArtifactKind: a.ArtifactKind, ScopeKey: a.ScopeKey, Scope: a.Scope, Generation: a.Generation, ReleaseChannel: "full", Status: "active", Version: 1, FencingToken: 3, ReleasedAt: old, CreatedAt: old, UpdatedAt: old}
			var s *Store
			var put func(model.PlatformReleaseMessage) error
			var replace func() error
			{
				dsn := disposablePhysicalDNSDatabase(t)
				db, err := openPostgresDatabase(dsn)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { db.Close() })
				s = &Store{databaseURL: dsn, db: db, dbReady: true}
				if _, err := pgInsertPlatformArtifact(ctx, db, a); err != nil {
					t.Fatal(err)
				}
				if _, err := pgInsertPlatformArtifactRelease(ctx, db, r); err != nil {
					t.Fatal(err)
				}
				put = func(m model.PlatformReleaseMessage) error {
					_, e := pgInsertPlatformReleaseMessage(ctx, db, m)
					return e
				}
				replace = func() error {
					_, e := db.ExecContext(ctx, "UPDATE fugue_platform_artifact_releases SET status='superseded' WHERE id=$1", r.ID)
					return e
				}
			}

			write := func(m model.PlatformReleaseMessage) {
				t.Helper()
				if err := put(m); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 23; i++ {
				write(buildPlatformReleaseMessage(a, r, model.PlatformReleaseMessageTypeVerifiedLKG, time.Now().UTC()))
			}
			original := buildPlatformReleaseMessage(a, r, model.PlatformReleaseMessageTypeRollback, old)
			wrongTime := original
			wrongTime.ID = "wrong-time"
			wrongTime.CreatedAt = old.Add(time.Microsecond)
			write(wrongTime)
			wrongGeneration := original
			wrongGeneration.ID = "wrong-generation"
			wrongGeneration.Generation = "other"
			write(wrongGeneration)
			if m, err := s.CurrentFullRollbackMessage(ctx, r); err != nil || m != nil {
				t.Fatalf("missing: %v %v", m, err)
			}
			write(original)
			if m, err := s.CurrentFullRollbackMessage(ctx, r); err != nil || m == nil || m.ID != original.ID {
				t.Fatalf("exact history: %v %v", m, err)
			}
			feed, err := s.ListPlatformReleaseMessages(r.ArtifactKind, r.ScopeKey, time.Now().Add(-24*time.Hour), 20)
			if err != nil || len(feed) != 20 {
				t.Fatalf("feed: %d %v", len(feed), err)
			}
			for _, m := range feed {
				if m.ID == original.ID {
					t.Fatal("historical record leaked into recent feed")
				}
			}
			for _, change := range []func(*model.PlatformArtifactRelease){func(r *model.PlatformArtifactRelease) { r.ID = "other" }, func(r *model.PlatformArtifactRelease) { r.Version++ }, func(r *model.PlatformArtifactRelease) { r.FencingToken++ }} {
				stale := r
				change(&stale)
				if m, err := s.CurrentFullRollbackMessage(ctx, stale); m != nil || !errors.Is(err, ErrConflict) {
					t.Fatalf("stale selection: %v %v", m, err)
				}
			}
			duplicate := original
			if _, err := s.db.ExecContext(ctx, `UPDATE fugue_platform_release_messages SET scope_json=scope_json || '{"future-scope":"other"}'::jsonb WHERE id=$1`, original.ID); err != nil {
				t.Fatal(err)
			}
			if m, err := s.CurrentFullRollbackMessage(ctx, r); m != nil || !errors.Is(err, ErrConflict) {
				t.Fatalf("unknown scope field lost: %v %v", m, err)
			}
			if _, err := s.db.ExecContext(ctx, `UPDATE fugue_platform_release_messages SET scope_json=scope_json - 'future-scope' WHERE id=$1`, original.ID); err != nil {
				t.Fatal(err)
			}

			duplicate.ID = "duplicate"
			write(duplicate)
			if m, err := s.CurrentFullRollbackMessage(ctx, r); m != nil || !errors.Is(err, ErrConflict) {
				t.Fatalf("duplicate: %v %v", m, err)
			}
			if err := replace(); err != nil {
				t.Fatal(err)
			}
			if m, err := s.CurrentFullRollbackMessage(ctx, r); m != nil || !errors.Is(err, ErrConflict) {
				t.Fatalf("replaced: %v %v", m, err)
			}
		})
	}
}
