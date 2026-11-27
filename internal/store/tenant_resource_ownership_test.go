package store

import (
	"context"
	"errors"
	"fugue/internal/schemamigrate"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestTenantResourceOwnership(t *testing.T) {
	t.Run("file", func(t *testing.T) { exerciseTenantResourceOwnership(t, newHostedDNSTestStore(t)) })
	t.Run("postgres", func(t *testing.T) {
		url := os.Getenv("FUGUE_TEST_DATABASE_URL")
		if url == "" {
			t.Skip("requires a disposable PostgreSQL test database")
		}
		if !localOwnershipTestDB(url) {
			t.Fatal("requires a test database")
		}
		s := New(url)
		if err := s.Init(); err != nil {
			if err := schemamigrate.MigrateEdgeInstanceFencing(context.Background(), url); err != nil {
				t.Fatal(err)
			}
			if err := s.Init(); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(func() { s.db.Close() })
		exerciseTenantResourceOwnership(t, s)
	})
}

func localOwnershipTestDB(address string) bool {
	u, err := url.Parse(address)
	return err == nil && u.Hostname() == "127.0.0.1" && strings.Contains(u.Path, "fugue_test")
}

func exerciseTenantResourceOwnership(t *testing.T, s *Store) {
	t.Helper()
	a, err := s.CreateTenant(model.NewID("owner"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.CreateTenant(model.NewID("other"))
	if err != nil {
		t.Fatal(err)
	}
	actor := model.Principal{TenantID: a.ID, ActorID: "certificate", ActorType: model.ActorTypeAPIKey}
	name := strings.ReplaceAll(model.NewID("ownership"), "_", "-") + ".example.test"
	zone, err := s.PutHostedZone(model.HostedZone{TenantID: a.ID, ZoneName: name, Status: model.HostedZoneStatusPendingOwnership}, actor)
	if err != nil {
		t.Fatal(err)
	}
	request := model.HostedZone{TenantID: b.ID, ZoneName: name, Status: model.HostedZoneStatusActive}
	if _, err = s.PutHostedZone(request); !errors.Is(err, ErrConflict) {
		t.Fatalf("other owner: %v", err)
	}
	request.TenantID = a.ID
	retry, err := s.PutHostedZone(request)
	if err != nil || retry.ID != zone.ID || retry.Status != zone.Status || !retry.UpdatedAt.Equal(zone.UpdatedAt) {
		t.Fatalf("registration retry: %+v %v", retry, err)
	}
	if _, err = s.DeleteHostedZone(zone, actor); err != nil {
		t.Fatal(err)
	}
	fresh, err := s.PutHostedZone(request)
	if err != nil || fresh.ID == zone.ID {
		t.Fatalf("new instance: %v", err)
	}
	if _, err = s.UpdateHostedZoneStatus(zone); !errors.Is(err, ErrConflict) {
		t.Fatalf("old status update: %v", err)
	}
	if _, err = s.DeleteHostedZone(zone, actor); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old deletion: %v", err)
	}
	creds := model.DataBackendCredentials{AccessKeyID: "fixture-access", SecretAccessKey: "fixture-secret"}
	shared, err := s.CreateDataBackend(model.DataBackend{Name: model.NewID("shared"), Provider: model.DataBackendProviderFugueManaged})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.RotateDataBackendCredentials(shared.ID, a.ID, false, creds, actor); !errors.Is(err, ErrNotFound) {
		t.Fatalf("shared rotation: %v", err)
	}
	if _, err = s.DeleteDataBackend(shared.ID, a.ID, false, actor); !errors.Is(err, ErrNotFound) {
		t.Fatalf("shared deletion: %v", err)
	}
	ws, err := s.CreateDataWorkspace(model.DataWorkspace{Name: "storage", TenantID: a.ID, StorageBackendID: shared.ID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DeleteDataBackend(shared.ID, "", true); !errors.Is(err, ErrConflict) {
		t.Fatalf("referenced backend: %v", err)
	}
	owned, err := s.CreateDataBackend(model.DataBackend{Name: model.NewID("owned"), TenantID: a.ID, Provider: model.DataBackendProviderFugueManaged})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.RotateDataBackendCredentials(owned.ID, a.ID, false, creds, actor); err != nil {
		t.Fatal(err)
	}
	if _, err = s.UpdateDataWorkspace(ws.ID, a.ID, false, DataWorkspaceUpdate{StorageBackendID: &owned.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DeleteDataBackend(shared.ID, "", true); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	c, err := s.UpsertDNSACMEChallenge(model.DNSACMEChallenge{Name: "_acme-challenge." + name, Zone: name, Value: "sample", CreatedBy: actor.ActorType + ":" + actor.ActorID, ExpiresAt: now.Add(time.Hour)}, actor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DeleteAuthorizedDNSACMEChallenge(c.ID, []string{"other.example.test"}, actor); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other certificate name: %v", err)
	}
	if _, err = s.DeleteAuthorizedDNSACMEChallenge(c.ID, []string{name}, actor); err != nil {
		t.Fatal(err)
	}
	// The delete and attach paths must serialize on the same backend row.
	racing, err := s.CreateDataBackend(model.DataBackend{Name: model.NewID("racing"), TenantID: a.ID, Provider: model.DataBackendProviderFugueManaged})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	attached, deleted := make(chan error, 1), make(chan error, 1)
	go func() {
		<-start
		_, e := s.UpdateDataWorkspace(ws.ID, a.ID, false, DataWorkspaceUpdate{StorageBackendID: &racing.ID})
		attached <- e
	}()
	go func() { <-start; _, e := s.DeleteDataBackend(racing.ID, a.ID, false, actor); deleted <- e }()
	close(start)
	aerr, derr := <-attached, <-deleted
	if !(aerr == nil && errors.Is(derr, ErrConflict) || errors.Is(aerr, ErrNotFound) && derr == nil) {
		t.Fatalf("attach: %v, delete: %v", aerr, derr)
	}
	current, err := s.GetDataWorkspace(ws.ID, a.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetDataBackend(current.StorageBackendID, a.ID, false); err != nil {
		t.Fatalf("dangling workspace backend: %v", err)
	}
}
