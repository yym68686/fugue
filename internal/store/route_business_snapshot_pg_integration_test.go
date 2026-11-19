package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

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
	if _, err := s.UpsertAppTrafficPolicy(model.AppTrafficPolicy{
		TenantID: tenant.ID, AppID: app.ID, Mode: model.AppTrafficModeSingle, StableReleaseID: release.ID, StableWeight: 100,
	}); err != nil {
		t.Fatal(err)
	}
	opID := model.NewID("snapshot_deploy")
	if _, err := s.db.Exec(`INSERT INTO fugue_operations (id,tenant_id,app_id,type,status,execution_mode,requested_by_type,requested_by_id,created_at,updated_at)
VALUES ($1,$2,$3,'deploy','running','managed','system','snapshot-test',now(),now())`, opID, tenant.ID, app.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE fugue_apps SET status_json=status_json || jsonb_build_object('phase','deployed','last_operation_id',$2::text) WHERE id=$1`, app.ID, opID); err != nil {
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
	completed := time.Now().UTC()
	if _, err := writer.ExecContext(ctx, `UPDATE fugue_operations SET status='completed', completed_at=$2 WHERE id=$1`, opID, completed); err != nil {
		t.Fatal(err)
	}
	app.Spec.Image = "registry.example/image:v2"
	if err := s.pgSyncStableReleaseForCompletedDeployTx(ctx, writer, app, model.Operation{
		Type: model.OperationTypeDeploy, DesiredSpec: &app.Spec,
	}, completed); err != nil {
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
	if len(old.Operations) != 1 || old.Operations[0].ID != opID || !old.Operations[0].ActiveDeploy() {
		t.Fatalf("snapshot mixed operation completion with old serving inputs: %+v", old.Operations)
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
	if len(current.Operations) != 1 || current.Operations[0].ID != opID || current.Operations[0].ActiveDeploy() ||
		len(current.TrafficPolicies) != 1 || current.TrafficPolicies[0].StableReleaseID == release.ID || !current.Operations[0].SyncedStableRelease(current.TrafficPolicies[0]) {
		t.Fatalf("missing completed deploy and its automatic policy sync: ops=%+v policies=%+v", current.Operations, current.TrafficPolicies)
	}
	if _, err := s.db.Exec(`UPDATE fugue_app_traffic_policies SET updated_at=updated_at+interval '1 second' WHERE id=$1`, current.TrafficPolicies[0].ID); err != nil {
		t.Fatal(err)
	}
	edited, err := s.CaptureRouteBusinessSnapshot(ctx)
	if err != nil || len(edited.Operations) != 0 {
		t.Fatalf("stale completion remained eligible after unrelated policy write: %+v, %v", edited.Operations, err)
	}
}
