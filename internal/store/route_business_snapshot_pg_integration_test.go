package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestRouteBusinessSnapshotPostgresIsRepeatableAcrossBusinessWrites(t *testing.T) {
	s := billingBatchPGStore(t)
	tenant := billingBatchPGTenant(t, s)
	project, err := s.CreateProject(tenant.ID, "snapshot", "")
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "snapshot-old", "", model.AppSpec{Image: "registry.example/image:v1", Ports: []int{8080}, Replicas: 1})
	if err != nil {
		t.Fatal(err)
	}
	release, err := s.CreateAppRelease(model.AppRelease{TenantID: tenant.ID, AppID: app.ID, Role: model.AppReleaseRoleStable, Status: model.AppReleaseStatusServing, UpstreamURL: "http://old"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var revision string
	if err := tx.QueryRowContext(ctx, `SELECT pg_current_snapshot()::text`).Scan(&revision); err != nil {
		t.Fatal(err)
	}
	writer, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	if _, err := writer.ExecContext(ctx, `UPDATE fugue_apps SET name='snapshot-new' WHERE id=$1`, app.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ExecContext(ctx, `UPDATE fugue_app_releases SET upstream_url='http://new' WHERE id=$1`, release.ID); err != nil {
		t.Fatal(err)
	}
	if err := writer.Commit(); err != nil {
		t.Fatal(err)
	}
	old, err := readRouteBusinessSnapshotTx(ctx, tx)
	if err != nil {
		t.Fatal(err)
	}
	if old.Revision != "postgres:"+revision {
		t.Fatal("snapshot revision changed")
	}
	find := func(snapshot RouteBusinessSnapshot) (string, string) {
		var name, url string
		for _, a := range snapshot.Apps {
			if a.ID == app.ID {
				name = a.Name
			}
		}
		for _, r := range snapshot.Releases {
			if r.ID == release.ID {
				url = r.UpstreamURL
			}
		}
		return name, url
	}
	if name, url := find(old); name != "snapshot-old" || url != "http://old" {
		t.Fatalf("snapshot mixed commits: %s %s", name, url)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	current, err := s.CaptureRouteBusinessSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if name, url := find(current); name != "snapshot-new" || url != "http://new" || !strings.HasPrefix(current.Revision, "postgres:") {
		t.Fatalf("new snapshot missed commit: %s %s", name, url)
	}
}
