package store

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"fugue/internal/model"
)

func mailTestStore(t *testing.T, postgres bool) *Store {
	t.Helper()
	if !postgres {
		s := New(filepath.Join(t.TempDir(), "state.json"))
		if err := s.Init(); err != nil {
			t.Fatal(err)
		}
		return s
	}
	address := os.Getenv("FUGUE_MAIL_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("set FUGUE_MAIL_TEST_DATABASE_URL to a disposable loopback PostgreSQL")
	}
	u, err := url.Parse(address)
	if err != nil || u.Hostname() != "127.0.0.1" {
		t.Fatal("disposable loopback PostgreSQL required")
	}
	control, err := sql.Open("pgx", address)
	if err != nil {
		t.Fatal(err)
	}
	schema := model.NewID("mail_test")
	if _, err = control.Exec(`CREATE SCHEMA "` + schema + `"`); err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(); control.Exec(`DROP SCHEMA "` + schema + `" CASCADE`); control.Close() })
	if _, err = db.Exec(`CREATE TABLE fugue_mail_platform_config(id TEXT PRIMARY KEY,body JSONB NOT NULL); CREATE TABLE fugue_app_mail_bindings(app_id TEXT PRIMARY KEY,generation BIGINT NOT NULL,body JSONB NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return &Store{db: db, databaseURL: u.String()}
}

func TestAppMailStoreCASOwnershipAndPlatformInterlock(t *testing.T) {
	for _, pg := range []bool{false, true} {
		name := "file"
		if pg {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			s := mailTestStore(t, pg)
			cfg := model.MailPlatformConfig{MailflareAppID: "provider", Enabled: true}
			if err := s.SaveMailPlatformConfig(cfg); err != nil {
				t.Fatal(err)
			}
			b := model.AppMailBinding{AppID: "app", TenantID: "tenant", ProjectID: "project", Hostname: "app.apps.example.test", LocalPart: "support", State: "active"}
			b, err := s.SaveAppMailBinding(b, 0)
			if err != nil || b.Generation != 1 || b.CreatedAt.IsZero() {
				t.Fatal("initial binding", b, err)
			}
			changed := b
			changed.TenantID = "other"
			if _, err = s.SaveAppMailBinding(changed, 1); !errors.Is(err, ErrConflict) {
				t.Fatal("ownership changed", err)
			}
			var winners atomic.Int32
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					v := b
					v.State = "suspended"
					_, e := s.SaveAppMailBinding(v, 1)
					if e == nil {
						winners.Add(1)
					} else if !errors.Is(e, ErrConflict) {
						t.Error(e)
					}
				}()
			}
			wg.Wait()
			if winners.Load() != 1 {
				t.Fatal("CAS winners", winners.Load())
			}
			if err = s.SaveMailPlatformConfig(model.MailPlatformConfig{MailflareAppID: "replacement", Enabled: true}); !errors.Is(err, ErrConflict) {
				t.Fatal("provider replaced with live binding", err)
			}
			if err = s.SaveMailPlatformConfig(model.MailPlatformConfig{MailflareAppID: "provider", Enabled: false}); !errors.Is(err, ErrConflict) {
				t.Fatal("platform disabled with live binding", err)
			}
			b.State = "revoked"
			b, err = s.SaveAppMailBinding(b, 2)
			if err != nil {
				t.Fatal(err)
			}
			b.State = "active"
			if _, err = s.SaveAppMailBinding(b, 3); !errors.Is(err, ErrConflict) {
				t.Fatal("tombstone resurrected", err)
			}
			if err = s.SaveMailPlatformConfig(model.MailPlatformConfig{MailflareAppID: "provider", Enabled: false}); err != nil {
				t.Fatal(err)
			}
			b.AppID = "new-app"
			if _, err = s.SaveAppMailBinding(b, 0); !errors.Is(err, ErrConflict) {
				t.Fatal("disabled platform accepted binding", err)
			}
			if pg {
				return
			}
			snapshot, err := s.CaptureRouteBusinessSnapshot(context.Background())
			if err != nil || snapshot.MailPlatformConfig == nil || len(snapshot.AppMailBindings) != 1 {
				t.Fatal("mail omitted from frozen snapshot", err)
			}
		})
	}
}

func TestAppMailPlatformInterlockInspectsBindingsBeyondFirstPage(t *testing.T) {
	s := mailTestStore(t, false)
	if err := s.SaveMailPlatformConfig(model.MailPlatformConfig{MailflareAppID: "provider", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.withLockedState(true, func(st *model.State) error {
		for i := 0; i < 101; i++ {
			st.AppMailBindings = append(st.AppMailBindings, model.AppMailBinding{AppID: model.NewID("app"), State: "revoked"})
		}
		st.AppMailBindings = append(st.AppMailBindings, model.AppMailBinding{AppID: "zzz-live", State: "active"})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveMailPlatformConfig(model.MailPlatformConfig{MailflareAppID: "provider", Enabled: false}); !errors.Is(err, ErrConflict) {
		t.Fatal("missed binding beyond page", err)
	}
}
