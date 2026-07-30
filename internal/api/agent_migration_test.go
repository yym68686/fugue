package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func migrationAgentFixture(t *testing.T) (*store.Store, *Server, model.App, model.Operation, string, string) {
	t.Helper()
	s := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("migration-agent")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "project", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "app", "", model.AppSpec{
		Image: "registry.example/app:v1", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	token, secret, err := s.CreateEnrollmentToken(tenant.ID, "target", time.Hour)
	if err != nil {
		t.Fatalf("create enrollment token: %v", err)
	}
	target, runtimeKey, err := s.ConsumeEnrollmentToken(secret, "target", "https://target.example", nil, "", "")
	if err != nil {
		t.Fatalf("consume enrollment token: %v", err)
	}
	if token.ID == "" || runtimeKey == "" {
		t.Fatal("expected enrollment token and runtime key")
	}
	desired := app.Spec
	desired.RuntimeID = target.ID
	op, err := s.CreateOperation(model.Operation{
		TenantID: tenant.ID, Type: model.OperationTypeMigrate, AppID: app.ID,
		TargetRuntimeID: target.ID, DesiredSpec: &desired,
		RequestedByType: model.ActorTypeAPIKey, RequestedByID: "operator-test",
	})
	if err != nil {
		t.Fatalf("create migration operation: %v", err)
	}
	claimed, found, err := s.ClaimNextPendingOperation()
	if err != nil || !found || claimed.ID != op.ID {
		t.Fatalf("claim migration operation: found=%v claimed=%+v err=%v", found, claimed, err)
	}
	server := NewServer(s, auth.New(s, ""), nil, ServerConfig{})
	return s, server, app, claimed, target.ID, runtimeKey
}

func TestAgentMigrationCompletionRequiresEvidenceAndRecordsFailure(t *testing.T) {
	t.Parallel()
	s, server, app, op, targetID, runtimeKey := migrationAgentFixture(t)
	req := httptest.NewRequest(http.MethodPost, "/v1/agent/operations/"+op.ID+"/complete", strings.NewReader(`{"message":"done"}`))
	req.Header.Set("Authorization", "Bearer "+runtimeKey)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("missing migration evidence status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	failed, err := s.GetOperation(op.ID)
	if err != nil {
		t.Fatalf("get failed migration: %v", err)
	}
	if failed.Status != model.OperationStatusFailed || failed.AssignedRuntimeID != targetID {
		t.Fatalf("invalid completion must fail assigned operation: %+v", failed)
	}
	ledger, found, err := s.LatestAppMigrationLedger(op.ID)
	if err != nil || !found || ledger.CutoverStatus != model.AppMigrationCutoverFailed || !ledger.OldArtifactsProtected || ledger.AppID != app.ID {
		t.Fatalf("expected durable failed migration ledger: found=%v ledger=%+v err=%v", found, ledger, err)
	}
}

func TestAgentMigrationCompletionAcceptsVerifiedEvidence(t *testing.T) {
	t.Parallel()
	s, server, app, op, targetID, runtimeKey := migrationAgentFixture(t)
	if _, err := s.UpdateRuntimeHeartbeatWithLabels(op.SourceRuntimeID, "", map[string]string{runtime.CellRuntimeLabelClusterID: "cluster-old"}); err != nil {
		t.Fatalf("record source cluster identity: %v", err)
	}
	if _, err := s.UpdateRuntimeHeartbeatWithLabels(targetID, "", map[string]string{runtime.CellRuntimeLabelClusterID: "cluster-new"}); err != nil {
		t.Fatalf("record target cluster identity: %v", err)
	}
	ready := true
	physical := 1
	ledger := model.AppMigrationLedger{
		OldRuntimeID: op.SourceRuntimeID, NewRuntimeID: targetID,
		OldClusterID: "cluster-old", NewClusterID: "cluster-new",
		ImageRef: app.Spec.Image, ImageReplicationStatus: model.AppMigrationEvidenceVerified,
		RuntimeObjectStatus: model.AppMigrationEvidenceVerified,
		EndpointRequired:    false, EndpointStatus: model.AppMigrationEvidenceNotApplicable,
		EndpointReady: &ready, PhysicalReplicas: &physical, DesiredReplicas: app.Spec.Replicas,
		Generation: 4, ObservedGeneration: 4,
		CutoverStatus: model.AppMigrationCutoverVerified, OldArtifactsProtected: true,
		ObservedAt: time.Now().UTC(),
	}
	body, err := json.Marshal(map[string]any{"message": "migrated", "migration_ledger": ledger})
	if err != nil {
		t.Fatalf("marshal migration completion: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/agent/operations/"+op.ID+"/complete", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+runtimeKey)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("verified completion status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	completed, err := s.GetOperation(op.ID)
	if err != nil {
		t.Fatalf("get completed migration: %v", err)
	}
	if completed.Status != model.OperationStatusCompleted {
		t.Fatalf("verified migration did not complete: %+v", completed)
	}
	latest, found, err := s.LatestAppMigrationLedger(op.ID)
	if err != nil || !found || latest.CutoverStatus != model.AppMigrationCutoverVerified || latest.NewClusterID != "cluster-new" {
		t.Fatalf("expected verified migration ledger: found=%v ledger=%+v err=%v", found, latest, err)
	}
}

func TestAgentMigrationCompletionRejectsMismatchedSourceCluster(t *testing.T) {
	t.Parallel()
	s, server, app, op, targetID, runtimeKey := migrationAgentFixture(t)
	if _, err := s.UpdateRuntimeHeartbeatWithLabels(op.SourceRuntimeID, "", map[string]string{runtime.CellRuntimeLabelClusterID: "cluster-old-authoritative"}); err != nil {
		t.Fatalf("record source cluster identity: %v", err)
	}
	if _, err := s.UpdateRuntimeHeartbeatWithLabels(targetID, "", map[string]string{runtime.CellRuntimeLabelClusterID: "cluster-new"}); err != nil {
		t.Fatalf("record target cluster identity: %v", err)
	}
	physical := 1
	ledger := model.AppMigrationLedger{
		OldRuntimeID: op.SourceRuntimeID, NewRuntimeID: targetID,
		OldClusterID: "cluster-old-forged", NewClusterID: "cluster-new",
		ImageRef: app.Spec.Image, ImageReplicationStatus: model.AppMigrationEvidenceVerified,
		RuntimeObjectStatus: model.AppMigrationEvidenceVerified,
		EndpointRequired:    false, EndpointStatus: model.AppMigrationEvidenceNotApplicable,
		PhysicalReplicas: &physical, DesiredReplicas: app.Spec.Replicas,
		Generation: 4, ObservedGeneration: 4,
		CutoverStatus: model.AppMigrationCutoverVerified, OldArtifactsProtected: true,
		ObservedAt: time.Now().UTC(),
	}
	body, err := json.Marshal(map[string]any{"message": "migrated", "migration_ledger": ledger})
	if err != nil {
		t.Fatalf("marshal migration completion: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/agent/operations/"+op.ID+"/complete", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+runtimeKey)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), "source cluster identity") {
		t.Fatalf("forged source cluster status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	failed, err := s.GetOperation(op.ID)
	if err != nil || failed.Status != model.OperationStatusFailed {
		t.Fatalf("forged source cluster must fail the migration: op=%+v err=%v", failed, err)
	}
}

func TestAgentMigrationTaskDoesNotUseTargetObservationAsSourceCluster(t *testing.T) {
	t.Parallel()
	_, server, app, op, _, _ := migrationAgentFixture(t)
	now := time.Now().UTC()
	server.managedAppStatusCache.setApp(managedAppStatusCacheKey(app), managedAppStatusCacheEntry{
		ok: true, found: true, clusterID: "cluster-target", refreshedAt: now, expiresAt: now.Add(time.Minute),
	})
	if clusterID := server.sourceClusterIDForAgentTask(context.Background(), op); clusterID != "" {
		t.Fatalf("target-side observation was reused as source cluster identity: %q", clusterID)
	}
}
