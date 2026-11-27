package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
)

func mutatePostgresResizeService(t *testing.T, f managedPostgresResizeFixture, change func(*model.BackingService)) {
	t.Helper()
	service, err := f.store.GetBackingService(f.service.ID)
	if err != nil {
		t.Fatal(err)
	}
	change(&service)
	raw, err := json.Marshal(service.Spec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.db.ExecContext(t.Context(), `UPDATE fugue_backing_services SET owner_app_id=NULLIF($2,''), spec_json=$3 WHERE id=$1`, service.ID, service.OwnerAppID, raw); err != nil {
		t.Fatal(err)
	}
}

func createPostgresResizePeer(t *testing.T, f managedPostgresResizeFixture) model.App {
	t.Helper()
	app, err := f.store.CreateApp(f.tenant.ID, f.project.ID, "second-client", "", model.AppSpec{Image: "registry.invalid/second:v1", RuntimeID: f.app.Spec.RuntimeID, Replicas: 1})
	if err != nil {
		t.Fatal(err)
	}
	return app
}

func addPostgresResizeBinding(t *testing.T, f managedPostgresResizeFixture) {
	t.Helper()
	app := createPostgresResizePeer(t, f)
	// Current writes reject this topology. Seed historical data to verify
	// that resize independently refuses to act on it.
	if _, err := f.store.db.ExecContext(t.Context(), `INSERT INTO fugue_service_bindings
		(id,tenant_id,app_id,service_id,alias,created_at,updated_at) VALUES ($1,$2,$3,$4,'postgres',now(),now())`,
		model.NewID("binding"), f.tenant.ID, app.ID, f.service.ID); err != nil {
		t.Fatal(err)
	}
}

func seedStoreTestIdentity(t *testing.T, s *Store, tenant, project, app string) {
	t.Helper()
	if _, err := s.db.ExecContext(t.Context(), `INSERT INTO fugue_tenants
		(id,name,slug,status,created_at,updated_at) VALUES ($1,$1,$1,'active',now(),now()) ON CONFLICT (id) DO NOTHING`, tenant); err != nil {
		t.Fatal(err)
	}
	if project == "" {
		return
	}
	if _, err := s.db.ExecContext(t.Context(), `INSERT INTO fugue_projects
		(id,tenant_id,name,slug,created_at,updated_at) VALUES ($1,$2,$1,$1,now(),now()) ON CONFLICT (id) DO NOTHING`, project, tenant); err != nil {
		t.Fatal(err)
	}
	if app == "" {
		return
	}
	if _, err := s.db.ExecContext(t.Context(), `INSERT INTO fugue_apps
		(id,tenant_id,project_id,name,spec_json,status_json,created_at,updated_at)
		VALUES ($1,$2,$3,$1,'{}','{}',now(),now()) ON CONFLICT (id) DO NOTHING`, app, tenant, project); err != nil {
		t.Fatal(err)
	}
}

func putEdgeTestEpoch(t *testing.T, s *Store, group, slot, epoch string, sequence uint64, minHealthy int) {
	t.Helper()
	// These tests exercise heartbeat selection after cutover. Full activation
	// and its signed receipts are covered by edge_activation_test.go.
	if _, err := s.db.ExecContext(t.Context(), `UPDATE fugue_edge_activation SET phase=$1,
state_json=state_json || jsonb_build_object('phase',$1::text,'route_authority',$2::text)
WHERE singleton=true`, model.EdgeActivationPhaseActive, model.EdgeRouteAuthorityActiveEpoch); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(t.Context(), `INSERT INTO fugue_edge_active_epochs
 (edge_group_id,slot,release_epoch,fence_sequence,min_healthy_instances,activated_at,created_at,updated_at)
 VALUES ($1,$2,$3,$4,$5,now(),now(),now()) ON CONFLICT (edge_group_id) DO UPDATE
 SET slot=EXCLUDED.slot, release_epoch=EXCLUDED.release_epoch, fence_sequence=EXCLUDED.fence_sequence,
 min_healthy_instances=EXCLUDED.min_healthy_instances, updated_at=now()`, group, slot, epoch, sequence, minHealthy); err != nil {
		t.Fatal(err)
	}
}

func seedResizeRestorePlan(t *testing.T, f managedPostgresResizeFixture) model.BackupRestorePlan {
	t.Helper()
	target := model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: f.tenant.ID, ProjectID: f.project.ID, AppID: f.app.ID, ServiceName: f.service.Name}
	a, err := f.store.CreateBackupArtifact(model.BackupArtifact{Target: target, Kind: model.BackupArtifactKindAppPGDump, Status: model.BackupArtifactStatusActive})
	if err != nil {
		t.Fatal(err)
	}
	p, err := f.store.CreateBackupRestorePlan(model.BackupRestorePlan{ArtifactID: a.ID, TenantID: f.tenant.ID, ProjectID: f.project.ID, AppID: f.app.ID, Target: target, Mode: model.BackupRestoreModeReplace, Status: model.BackupRestoreStatusPlanned})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func legacyEdgeInstanceUID(edgeID string) string {
	digest := sha256.Sum256([]byte(normalizeEdgeID(edgeID)))
	return "legacy-" + hex.EncodeToString(digest[:12])
}

func defaultEdgeActivationState(now time.Time) model.EdgeActivationState {
	return model.EdgeActivationState{
		Schema: model.EdgeActivationSchemaV1, Phase: model.EdgeActivationPhaseLegacyAuthoritative,
		RouteAuthority: model.EdgeRouteAuthorityLegacy, Generation: 1, CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
	}
}

func clonePlatformExpectedConsumerSet(in model.PlatformExpectedConsumerSet) model.PlatformExpectedConsumerSet {
	out := in
	out.Consumers = slices.Clone(in.Consumers)
	for i := range out.Consumers {
		out.Consumers[i].AcceptedProtocolVersions = slices.Clone(out.Consumers[i].AcceptedProtocolVersions)
		out.Consumers[i].AcceptedSchemaVersions = slices.Clone(out.Consumers[i].AcceptedSchemaVersions)
		out.Consumers[i].CompatibilityCapabilities = slices.Clone(out.Consumers[i].CompatibilityCapabilities)
	}
	return out
}
