package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestRouteBusinessSnapshotFileIsDetachedAndConsistent(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	err := s.withLockedState(true, func(state *model.State) error {
		state.Apps = []model.App{{ID: "app", TenantID: "tenant", Spec: model.AppSpec{Env: map[string]string{"CONFIG": "old"}}}, {ID: "deleted", Status: model.AppStatus{Phase: "deleted"}}}
		state.AppDomains = []model.AppDomain{{Hostname: "verified.example", Status: model.AppDomainStatusVerified}, {Hostname: "pending.example", Status: model.AppDomainStatusPending}}
		state.AppReleases = []model.AppRelease{{ID: "release", AppID: "app", UpstreamURL: "http://old"}}
		state.AppTrafficPolicies = []model.AppTrafficPolicy{{ID: "traffic", AppID: "app", StableReleaseID: "release", StableWeight: 100}}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.CaptureRouteBusinessSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Apps) != 1 || len(first.Domains) != 1 || len(first.Releases) != 1 || len(first.TrafficPolicies) != 1 || first.CapturedAt.IsZero() || !strings.HasPrefix(first.Revision, "json:") {
		t.Fatalf("incomplete snapshot: %+v", first)
	}
	first.Apps[0].Spec.Env["CONFIG"] = "mutated"
	second, err := s.CaptureRouteBusinessSnapshot(context.Background())
	if err != nil || second.Apps[0].Spec.Env["CONFIG"] != "old" || second.Revision != first.Revision {
		t.Fatal("snapshot aliases stored configuration", err)
	}
	err = s.withLockedState(true, func(state *model.State) error {
		state.Apps[0].Spec.Env["CONFIG"] = "new"
		state.AppReleases[0].UpstreamURL = "http://new"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	third, err := s.CaptureRouteBusinessSnapshot(context.Background())
	if err != nil || third.Apps[0].Spec.Env["CONFIG"] != "new" || third.Releases[0].UpstreamURL != "http://new" || third.Revision == second.Revision || second.Apps[0].Spec.Env["CONFIG"] != "old" {
		t.Fatal("snapshot mixed business revisions", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.CaptureRouteBusinessSnapshot(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel ignored", err)
	}
}

func TestRouteBusinessSnapshotDatabaseErrorRollsBackWithoutResult(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT transaction_timestamp").WillReturnError(errors.New("snapshot unavailable"))
	mock.ExpectRollback()
	s := &Store{db: db, databaseURL: "postgres://test"}
	snapshot, err := s.CaptureRouteBusinessSnapshot(context.Background())
	if err == nil || snapshot.Revision != "" || len(snapshot.Apps) != 0 {
		t.Fatal("partial snapshot returned", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
