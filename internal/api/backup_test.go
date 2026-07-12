package api

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestAppBackupStatusReportsDisabledByDefault(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Backup Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtime, _, err := stateStore.CreateRuntime(tenant.ID, "tenant-owned", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "postgres-app", "", model.AppSpec{
		Image:     "ghcr.io/example/app:latest",
		RuntimeID: runtime.ID,
		Replicas:  1,
		Postgres: &model.AppPostgresSpec{
			Database: "appdb",
		},
		Workspace: &model.AppWorkspaceSpec{MountPath: "/workspace"},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.read", "backup.read"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/backups/status", apiKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Policies []model.BackupPolicy  `json:"policies"`
		Posture  []model.BackupPosture `json:"posture"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.Policies) != 0 {
		t.Fatalf("expected no app backup policies by default, got %+v", response.Policies)
	}
	if len(response.Posture) != 2 {
		t.Fatalf("expected database and storage posture, got %+v", response.Posture)
	}
	for _, posture := range response.Posture {
		if posture.Status != "disabled" {
			t.Fatalf("expected app backup target %s to be disabled by default, got %+v", posture.Target.Type, posture)
		}
		if posture.PolicyID != "" {
			t.Fatalf("expected no policy id for disabled default target, got %+v", posture)
		}
		if posture.Message != "backup is disabled by default" {
			t.Fatalf("expected disabled-by-default message, got %+v", posture)
		}
	}
}

func TestBackupPolicyFromRequestValidatesSchedule(t *testing.T) {
	t.Parallel()

	server := &Server{}
	principal := model.Principal{TenantID: "tenant_a"}
	for _, schedule := range []string{"not cron", "@daily", "CRON_TZ=UTC 0 * * * *"} {
		recorder := httptest.NewRecorder()
		_, ok := server.backupPolicyFromRequest(recorder, principal, backupPolicyRequest{
			Target:   model.BackupTarget{Type: model.BackupTargetAppDatabase},
			Schedule: schedule,
		}, nil)
		if ok || recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "invalid backup schedule") {
			t.Fatalf("expected schedule %q to return a descriptive 400, code=%d body=%s", schedule, recorder.Code, recorder.Body.String())
		}
	}

	recorder := httptest.NewRecorder()
	policy, ok := server.backupPolicyFromRequest(recorder, principal, backupPolicyRequest{
		Target:   model.BackupTarget{Type: model.BackupTargetAppDatabase},
		Schedule: "0 */6 * * *",
	}, nil)
	if !ok || policy.Schedule != "0 */6 * * *" {
		t.Fatalf("expected documented six-hour schedule to be accepted, policy=%+v body=%s", policy, recorder.Body.String())
	}
}

func TestTenantCannotCreateOrRunPlatformBackupTargets(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Backup Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "backup-writer", []string{"backup.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	for _, request := range []struct {
		path string
		body map[string]any
	}{
		{path: "/v1/backups/policies", body: map[string]any{"target": map[string]any{"type": model.BackupTargetControlPlaneDatabase}}},
		{path: "/v1/backups/runs", body: map[string]any{"target": map[string]any{"type": model.BackupTargetControlPlaneDatabase}}},
	} {
		recorder := performJSONRequest(t, server, http.MethodPost, request.path, apiKey, request.body)
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("expected tenant request %s to be forbidden, code=%d body=%s", request.path, recorder.Code, recorder.Body.String())
		}
	}

	defaultPolicy := stateStore.DefaultControlPlaneBackupPolicyID()
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/backups/runs", apiKey, map[string]any{"policy_id": defaultPolicy})
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected platform policy to be hidden from tenant, code=%d body=%s", recorder.Code, recorder.Body.String())
	}

	before, err := stateStore.GetBackupPolicy(defaultPolicy, "", true)
	if err != nil {
		t.Fatalf("get default control-plane policy: %v", err)
	}
	recorder = performJSONRequest(t, server, http.MethodPatch, "/v1/backups/policies/"+defaultPolicy, apiKey, map[string]any{"enabled": !before.Enabled})
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected platform policy patch to be hidden from tenant, code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	after, err := stateStore.GetBackupPolicy(defaultPolicy, "", true)
	if err != nil {
		t.Fatalf("get default control-plane policy after forbidden patch: %v", err)
	}
	if after.Enabled != before.Enabled {
		t.Fatalf("expected forbidden patch to leave platform policy enabled=%t, got %t", before.Enabled, after.Enabled)
	}
}

func TestTenantCannotTargetAnotherTenantsAppOrWorkspaceForBackup(t *testing.T) {
	t.Parallel()

	fixture := newBackupAuthorizationFixture(t)
	requests := []struct {
		name string
		path string
		body map[string]any
	}{
		{
			name: "app policy",
			path: "/v1/backups/policies",
			body: map[string]any{"app_id": fixture.victimApp.ID, "target": map[string]any{"type": model.BackupTargetAppDatabase, "app_id": fixture.victimApp.ID}},
		},
		{
			name: "app run",
			path: "/v1/backups/runs",
			body: map[string]any{"target": map[string]any{"type": model.BackupTargetAppDatabase, "app_id": fixture.victimApp.ID}},
		},
		{
			name: "workspace policy",
			path: "/v1/backups/policies",
			body: map[string]any{"target": map[string]any{"type": model.BackupTargetDataWorkspace, "workspace_id": fixture.victimWorkspace.ID}},
		},
		{
			name: "workspace run",
			path: "/v1/backups/runs",
			body: map[string]any{"target": map[string]any{"type": model.BackupTargetDataWorkspace, "workspace_id": fixture.victimWorkspace.ID}},
		},
	}
	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			recorder := performJSONRequest(t, fixture.server, http.MethodPost, request.path, fixture.attackerKey, request.body)
			if recorder.Code != http.StatusForbidden {
				t.Fatalf("expected cross-tenant backup target to be forbidden, code=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestTenantCannotRunLegacyPolicyPointingAtAnotherTenant(t *testing.T) {
	t.Parallel()

	fixture := newBackupAuthorizationFixture(t)
	policies := []model.BackupPolicy{
		{
			TenantID: fixture.attacker.ID,
			AppID:    fixture.victimApp.ID,
			Name:     "legacy-cross-tenant-app",
			Target: model.BackupTarget{
				Type:     model.BackupTargetAppDatabase,
				TenantID: fixture.attacker.ID,
				AppID:    fixture.victimApp.ID,
			},
			Enabled:  true,
			Status:   model.BackupPolicyStatusActive,
			Schedule: model.BackupDefaultSchedule,
		},
		{
			TenantID: fixture.attacker.ID,
			Name:     "legacy-cross-tenant-workspace",
			Target: model.BackupTarget{
				Type:        model.BackupTargetDataWorkspace,
				TenantID:    fixture.attacker.ID,
				WorkspaceID: fixture.victimWorkspace.ID,
			},
			Enabled:  true,
			Status:   model.BackupPolicyStatusActive,
			Schedule: model.BackupDefaultSchedule,
		},
	}
	for i := range policies {
		policy, err := fixture.stateStore.UpsertBackupPolicy(policies[i])
		if err != nil {
			t.Fatalf("seed legacy mismatch policy: %v", err)
		}
		recorder := performJSONRequest(t, fixture.server, http.MethodPost, "/v1/backups/runs", fixture.attackerKey, map[string]any{"policy_id": policy.ID})
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("expected legacy mismatch policy %s to be forbidden, code=%d body=%s", policy.Name, recorder.Code, recorder.Body.String())
		}

		_, err = fixture.server.runBackup(context.Background(), model.BackupRun{
			TenantID:  policy.TenantID,
			ProjectID: policy.ProjectID,
			AppID:     policy.AppID,
			Target:    policy.Target,
		})
		if !errors.Is(err, errBackupTargetNotAuthorized) {
			t.Fatalf("expected execution boundary to reject legacy mismatch policy %s, got %v", policy.Name, err)
		}
	}

	runs, err := fixture.stateStore.ListBackupRuns(store.BackupRunFilter{PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list backup runs: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("expected rejected legacy policies to create no runs, got %+v", runs)
	}
}

func TestBackupPolicyIDRequiresAuthorizationAndCannotOverwriteExistingPolicy(t *testing.T) {
	t.Parallel()

	fixture := newBackupAuthorizationFixture(t)
	victimPolicy := createBackupAuthorizationPolicy(t, fixture.stateStore, fixture.victimApp, fixture.victimBackend, "victim-policy")
	defaultPolicyID := fixture.stateStore.DefaultControlPlaneBackupPolicyID()
	defaultBefore, err := fixture.stateStore.GetBackupPolicy(defaultPolicyID, "", true)
	if err != nil {
		t.Fatalf("get default policy before overwrite attempts: %v", err)
	}

	requests := []struct {
		name string
		path string
		id   string
	}{
		{name: "generic victim policy", path: "/v1/backups/policies", id: victimPolicy.ID},
		{name: "app victim policy", path: "/v1/apps/" + fixture.attackerApp.ID + "/backups/policies", id: victimPolicy.ID},
		{name: "generic platform policy", path: "/v1/backups/policies", id: defaultPolicyID},
	}
	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			recorder := performJSONRequest(t, fixture.server, http.MethodPost, request.path, fixture.attackerKey, map[string]any{
				"id":         request.id,
				"name":       "overwrite-attempt",
				"backend_id": fixture.attackerBackend.ID,
				"target": map[string]any{
					"type":   model.BackupTargetAppDatabase,
					"app_id": fixture.attackerApp.ID,
				},
			})
			if recorder.Code != http.StatusNotFound {
				t.Fatalf("expected unauthorized supplied policy id to be hidden, code=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
	attackerPolicy := createBackupAuthorizationPolicy(t, fixture.stateStore, fixture.attackerApp, fixture.attackerBackend, "attacker-policy")
	recorder := performJSONRequest(t, fixture.server, http.MethodPost, "/v1/backups/policies", fixture.attackerKey, map[string]any{
		"id":         attackerPolicy.ID,
		"name":       "attacker-policy-updated",
		"backend_id": fixture.attackerBackend.ID,
		"target": map[string]any{
			"type":   model.BackupTargetAppDatabase,
			"app_id": fixture.attackerApp.ID,
		},
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected authorized policy id upsert to succeed, code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var upsertResponse struct {
		Policy model.BackupPolicy `json:"policy"`
	}
	mustDecodeJSON(t, recorder, &upsertResponse)
	if upsertResponse.Policy.ID != attackerPolicy.ID || upsertResponse.Policy.Name != "attacker-policy-updated" {
		t.Fatalf("expected authorized upsert to retain policy identity, got %+v", upsertResponse.Policy)
	}

	recorder = performJSONRequest(t, fixture.server, http.MethodPatch, "/v1/backups/policies/"+attackerPolicy.ID, fixture.attackerKey, map[string]any{
		"id":      victimPolicy.ID,
		"enabled": false,
	})
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "id is read-only") {
		t.Fatalf("expected policy id in PATCH body to be rejected, code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	attackerAfter, err := fixture.stateStore.GetBackupPolicy(attackerPolicy.ID, fixture.attacker.ID, false)
	if err != nil {
		t.Fatalf("get attacker policy after rejected PATCH: %v", err)
	}
	if !attackerAfter.Enabled {
		t.Fatalf("attacker policy was disabled despite rejected PATCH: %+v", attackerAfter)
	}

	victimAfter, err := fixture.stateStore.GetBackupPolicy(victimPolicy.ID, "", true)
	if err != nil {
		t.Fatalf("get victim policy after overwrite attempts: %v", err)
	}
	if victimAfter.TenantID != victimPolicy.TenantID || victimAfter.AppID != victimPolicy.AppID || victimAfter.Target.AppID != victimPolicy.Target.AppID {
		t.Fatalf("victim policy changed after rejected overwrite: before=%+v after=%+v", victimPolicy, victimAfter)
	}
	defaultAfter, err := fixture.stateStore.GetBackupPolicy(defaultPolicyID, "", true)
	if err != nil {
		t.Fatalf("get default policy after overwrite attempts: %v", err)
	}
	if defaultAfter.TenantID != defaultBefore.TenantID || defaultAfter.Target.Type != defaultBefore.Target.Type {
		t.Fatalf("platform policy changed after rejected overwrite: before=%+v after=%+v", defaultBefore, defaultAfter)
	}
}

func TestAppBackupPolicyUpsertIsIdempotent(t *testing.T) {
	t.Parallel()

	fixture := newBackupAuthorizationFixture(t)
	path := "/v1/apps/" + fixture.attackerApp.ID + "/backups/policies"
	var firstID string
	for idx, schedule := range []string{"0 */6 * * *", "15 */6 * * *"} {
		recorder := performJSONRequest(t, fixture.server, http.MethodPost, path, fixture.attackerKey, map[string]any{
			"backend_id": fixture.attackerBackend.ID,
			"schedule":   schedule,
			"target":     map[string]any{"type": model.BackupTargetAppDatabase},
		})
		if recorder.Code != http.StatusOK {
			t.Fatalf("app policy upsert %d failed, code=%d body=%s", idx+1, recorder.Code, recorder.Body.String())
		}
		var response struct {
			Policy model.BackupPolicy `json:"policy"`
		}
		mustDecodeJSON(t, recorder, &response)
		if idx == 0 {
			firstID = response.Policy.ID
		} else if response.Policy.ID != firstID {
			t.Fatalf("expected repeated enable to update policy %q, got %+v", firstID, response.Policy)
		}
		if response.Policy.Schedule != schedule {
			t.Fatalf("expected schedule %q, got %+v", schedule, response.Policy)
		}
	}
	policies, err := fixture.stateStore.ListBackupPolicies(store.BackupPolicyFilter{
		TenantID:        fixture.attacker.ID,
		AppID:           fixture.attackerApp.ID,
		IncludeDisabled: true,
	})
	if err != nil {
		t.Fatalf("list app backup policies: %v", err)
	}
	if len(policies) != 1 || policies[0].ID != firstID {
		t.Fatalf("expected one idempotently updated app policy, got %+v", policies)
	}
}

func TestAdminBackupPolicyUpsertUpdatesDefaultPolicy(t *testing.T) {
	clearDefaultDataBackendEnv(t)

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		ID:       stateStore.DefaultBackupBackendID(),
		Name:     "fugue-default-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
		Status:   "active",
	})
	if err != nil {
		t.Fatalf("create default backend: %v", err)
	}
	if err := stateStore.EnsureDefaultBackupPolicy(); err != nil {
		t.Fatalf("ensure default policy: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	defaultID := stateStore.DefaultControlPlaneBackupPolicyID()
	for idx, schedule := range []string{"0 */6 * * *", model.BackupDefaultSchedule} {
		recorder := performJSONRequest(t, server, http.MethodPost, "/v1/backups/policies", "bootstrap-secret", map[string]any{
			"backend_id": backend.ID,
			"enabled":    true,
			"schedule":   schedule,
			"target":     map[string]any{"type": model.BackupTargetControlPlaneDatabase},
		})
		if recorder.Code != http.StatusOK {
			t.Fatalf("default policy upsert %d failed, code=%d body=%s", idx+1, recorder.Code, recorder.Body.String())
		}
		var response struct {
			Policy model.BackupPolicy `json:"policy"`
		}
		mustDecodeJSON(t, recorder, &response)
		if response.Policy.ID != defaultID || response.Policy.Schedule != schedule || response.Policy.BackendID != backend.ID {
			t.Fatalf("expected default policy %q to be updated in place, got %+v", defaultID, response.Policy)
		}
	}
	policies, err := stateStore.ListBackupPolicies(store.BackupPolicyFilter{IncludeDisabled: true, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list platform backup policies: %v", err)
	}
	count := 0
	for _, policy := range policies {
		if policy.Target.Type == model.BackupTargetControlPlaneDatabase {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected one control-plane backup policy, got %+v", policies)
	}
}

func TestGenericBackupRunCanonicalizesAuthorizedPolicyAlias(t *testing.T) {
	t.Parallel()

	fixture := newBackupAuthorizationFixture(t)
	victimPolicy := createBackupAuthorizationPolicy(t, fixture.stateStore, fixture.victimApp, fixture.victimBackend, "victim-policy")
	aliasPolicy := createBackupAuthorizationPolicy(t, fixture.stateStore, fixture.attackerApp, fixture.attackerBackend, victimPolicy.ID)
	seenRun := make(chan model.BackupRun, 1)
	fixture.server.backupRunner = func(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
		seenRun <- run
		return nil, nil
	}

	recorder := performJSONRequest(t, fixture.server, http.MethodPost, "/v1/backups/runs", fixture.attackerKey, map[string]any{
		"policy_id": victimPolicy.ID,
		"target":    map[string]any{"type": model.BackupTargetControlPlaneDatabase},
		"wait":      true,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected authorized alias policy run, code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Run model.BackupRun `json:"run"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Run.PolicyID != aliasPolicy.ID || response.Run.AppID != fixture.attackerApp.ID || response.Run.Target.AppID != fixture.attackerApp.ID {
		t.Fatalf("expected run to use canonical authorized policy %q, got %+v", aliasPolicy.ID, response.Run)
	}
	select {
	case run := <-seenRun:
		if run.PolicyID != aliasPolicy.ID {
			t.Fatalf("worker received non-canonical policy id: %+v", run)
		}
	default:
		t.Fatal("expected backup runner to execute canonical policy")
	}
	victimAfter, err := fixture.stateStore.GetBackupPolicy(victimPolicy.ID, "", true)
	if err != nil {
		t.Fatalf("get victim policy after alias run: %v", err)
	}
	if victimAfter.LastRunID != "" || victimAfter.NextRunAt == nil || !victimAfter.NextRunAt.Equal(*victimPolicy.NextRunAt) {
		t.Fatalf("victim policy schedule changed through alias confusion: before=%+v after=%+v", victimPolicy, victimAfter)
	}
}

func TestAppBackupRunRejectsPolicyForAnotherAppOrTenant(t *testing.T) {
	t.Parallel()

	fixture := newBackupAuthorizationFixture(t)
	victimPolicy := createBackupAuthorizationPolicy(t, fixture.stateStore, fixture.victimApp, fixture.victimBackend, "victim-policy")
	otherApp, err := fixture.stateStore.CreateApp(fixture.attacker.ID, fixture.attackerProject.ID, "other-attacker-app", "", model.AppSpec{
		Image:     "ghcr.io/example/other:latest",
		RuntimeID: fixture.attackerApp.Spec.RuntimeID,
		Replicas:  1,
		Postgres:  &model.AppPostgresSpec{Database: "otherdb"},
	})
	if err != nil {
		t.Fatalf("create second attacker app: %v", err)
	}
	otherAppPolicy := createBackupAuthorizationPolicy(t, fixture.stateStore, otherApp, fixture.attackerBackend, "other-app-policy")

	for _, policyID := range []string{victimPolicy.ID, otherAppPolicy.ID} {
		recorder := performJSONRequest(t, fixture.server, http.MethodPost, "/v1/apps/"+fixture.attackerApp.ID+"/backups/runs", fixture.attackerKey, map[string]any{
			"policy_id": policyID,
		})
		if recorder.Code != http.StatusForbidden && recorder.Code != http.StatusNotFound {
			t.Fatalf("expected app route to reject policy %q, code=%d body=%s", policyID, recorder.Code, recorder.Body.String())
		}
	}
	runs, err := fixture.stateStore.ListBackupRuns(store.BackupRunFilter{PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list backup runs: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("expected rejected app policy runs to create no records, got %+v", runs)
	}
}

func TestAppBackupRunSelectsRequestedTargetPolicy(t *testing.T) {
	t.Parallel()

	fixture := newBackupAuthorizationFixture(t)
	_ = createBackupAuthorizationPolicy(t, fixture.stateStore, fixture.attackerApp, fixture.attackerBackend, "app-database-policy")
	persistentPolicy, err := fixture.stateStore.UpsertBackupPolicy(model.BackupPolicy{
		TenantID:  fixture.attacker.ID,
		ProjectID: fixture.attackerApp.ProjectID,
		AppID:     fixture.attackerApp.ID,
		Name:      "persistent-storage-policy",
		Target: model.BackupTarget{
			Type:      model.BackupTargetPersistentStorage,
			TenantID:  fixture.attacker.ID,
			ProjectID: fixture.attackerApp.ProjectID,
			AppID:     fixture.attackerApp.ID,
			RuntimeID: fixture.attackerApp.Spec.RuntimeID,
		},
		BackendID: fixture.attackerBackend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
		Schedule:  model.BackupDefaultSchedule,
	})
	if err != nil {
		t.Fatalf("create persistent storage policy: %v", err)
	}
	seenRun := make(chan model.BackupRun, 1)
	fixture.server.backupRunner = func(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
		seenRun <- run
		return nil, nil
	}
	recorder := performJSONRequest(t, fixture.server, http.MethodPost, "/v1/apps/"+fixture.attackerApp.ID+"/backups/runs", fixture.attackerKey, map[string]any{
		"target": map[string]any{"type": model.BackupTargetPersistentStorage},
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("run requested app backup target, code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Run model.BackupRun `json:"run"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Run.PolicyID != persistentPolicy.ID || response.Run.Target.Type != model.BackupTargetPersistentStorage {
		t.Fatalf("expected persistent storage policy %q, got %+v", persistentPolicy.ID, response.Run)
	}
	select {
	case run := <-seenRun:
		if run.PolicyID != persistentPolicy.ID || run.Target.Type != model.BackupTargetPersistentStorage {
			t.Fatalf("worker received wrong app backup policy: %+v", run)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for selected app backup worker")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		finalRun, err := fixture.stateStore.GetBackupRun(response.Run.ID, fixture.attacker.ID, false)
		if err != nil {
			t.Fatalf("get selected app backup run: %v", err)
		}
		if backupRunTerminalStatus(finalRun.Status) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("selected app backup run did not finish: %+v", finalRun)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTenantCannotUseAnotherTenantsBackupBackend(t *testing.T) {
	t.Parallel()

	fixture := newBackupAuthorizationFixture(t)
	requests := []struct {
		name string
		path string
		body map[string]any
	}{
		{
			name: "policy",
			path: "/v1/backups/policies",
			body: map[string]any{
				"name":       "victim-backend-policy",
				"backend_id": fixture.victimBackend.ID,
				"target":     map[string]any{"type": model.BackupTargetAppDatabase, "app_id": fixture.attackerApp.ID},
			},
		},
		{
			name: "direct run",
			path: "/v1/backups/runs",
			body: map[string]any{
				"backend_id": fixture.victimBackend.ID,
				"target":     map[string]any{"type": model.BackupTargetAppDatabase, "app_id": fixture.attackerApp.ID},
			},
		},
	}
	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			recorder := performJSONRequest(t, fixture.server, http.MethodPost, request.path, fixture.attackerKey, request.body)
			if recorder.Code != http.StatusForbidden {
				t.Fatalf("expected victim backend to be forbidden, code=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}

	legacyPolicy := createBackupAuthorizationPolicy(t, fixture.stateStore, fixture.attackerApp, fixture.victimBackend, "legacy-victim-backend")
	for _, path := range []string{"/v1/backups/runs", "/v1/apps/" + fixture.attackerApp.ID + "/backups/runs"} {
		recorder := performJSONRequest(t, fixture.server, http.MethodPost, path, fixture.attackerKey, map[string]any{"policy_id": legacyPolicy.ID})
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("expected legacy victim backend policy on %s to be forbidden, code=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
	safePolicy := createBackupAuthorizationPolicy(t, fixture.stateStore, fixture.attackerApp, fixture.attackerBackend, "safe-backend-policy")
	recorder := performJSONRequest(t, fixture.server, http.MethodPost, "/v1/apps/"+fixture.attackerApp.ID+"/backups/runs", fixture.attackerKey, map[string]any{
		"policy_id":  safePolicy.ID,
		"backend_id": fixture.victimBackend.ID,
	})
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("expected app run backend override to reject victim backend, code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	_, err := fixture.server.runBackup(context.Background(), model.BackupRun{
		TenantID:  fixture.attacker.ID,
		ProjectID: fixture.attackerApp.ProjectID,
		AppID:     fixture.attackerApp.ID,
		Target: model.BackupTarget{
			Type:      model.BackupTargetAppDatabase,
			TenantID:  fixture.attacker.ID,
			ProjectID: fixture.attackerApp.ProjectID,
			AppID:     fixture.attackerApp.ID,
		},
		BackendID: fixture.victimBackend.ID,
	})
	if !errors.Is(err, errBackupBackendNotAuthorized) {
		t.Fatalf("expected worker boundary to reject victim backend, got %v", err)
	}

	globalBackend, err := fixture.stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "shared-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "shared-bucket",
		Endpoint: "https://shared.example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create shared backend: %v", err)
	}
	recorder = performJSONRequest(t, fixture.server, http.MethodPost, "/v1/backups/policies", fixture.attackerKey, map[string]any{
		"name":       "shared-backend-policy",
		"backend_id": globalBackend.Name,
		"target":     map[string]any{"type": model.BackupTargetAppDatabase, "app_id": fixture.attackerApp.ID},
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected platform-shared backend to remain available, code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Policy model.BackupPolicy `json:"policy"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Policy.BackendID != globalBackend.ID {
		t.Fatalf("expected shared backend to be canonicalized to %q, got %+v", globalBackend.ID, response.Policy)
	}

	adminServer := NewServer(fixture.stateStore, auth.New(fixture.stateStore, "bootstrap-secret"), nil, ServerConfig{})
	for _, request := range []struct {
		path string
		body map[string]any
	}{
		{
			path: "/v1/apps/" + fixture.victimApp.ID + "/backups/policies",
			body: map[string]any{
				"backend_id": fixture.attackerBackend.ID,
				"target":     map[string]any{"type": model.BackupTargetAppDatabase},
			},
		},
		{
			path: "/v1/backups/runs",
			body: map[string]any{
				"backend_id": fixture.attackerBackend.ID,
				"target": map[string]any{
					"type":       model.BackupTargetAppDatabase,
					"tenant_id":  fixture.victimApp.TenantID,
					"project_id": fixture.victimApp.ProjectID,
					"app_id":     fixture.victimApp.ID,
				},
			},
		},
	} {
		recorder := performJSONRequest(t, adminServer, http.MethodPost, request.path, "bootstrap-secret", request.body)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("expected platform admin cross-tenant backend selection to fail fast, path=%s code=%d body=%s", request.path, recorder.Code, recorder.Body.String())
		}
	}
}

func TestTenantCannotMutatePlatformBackupBackend(t *testing.T) {
	t.Parallel()

	fixture := newBackupAuthorizationFixture(t)
	globalBackend, err := fixture.stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "platform-shared-mutation-test",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "platform-bucket",
		Endpoint: "https://platform.example.r2.cloudflarestorage.com",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "platform-access-key",
			SecretAccessKey: "platform-secret-key",
		},
	})
	if err != nil {
		t.Fatalf("create platform backup backend: %v", err)
	}
	before, err := fixture.stateStore.GetBackupBackendForUse(globalBackend.ID, "", true)
	if err != nil {
		t.Fatalf("get platform backend before mutation attempts: %v", err)
	}

	requests := []struct {
		method string
		path   string
		body   map[string]any
	}{
		{
			method: http.MethodPost,
			path:   "/v1/backups/backends",
			body: map[string]any{
				"name":        globalBackend.ID,
				"rotate_only": true,
				"credentials": map[string]any{"access_key_id": "attacker-key", "secret_access_key": "attacker-secret"},
			},
		},
		{method: http.MethodPost, path: "/v1/backups/backends/" + globalBackend.ID + "/test"},
		{method: http.MethodDelete, path: "/v1/backups/backends/" + globalBackend.ID},
	}
	for _, request := range requests {
		recorder := performJSONRequest(t, fixture.server, request.method, request.path, fixture.attackerKey, request.body)
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("expected platform backend mutation to be forbidden, method=%s path=%s code=%d body=%s", request.method, request.path, recorder.Code, recorder.Body.String())
		}
	}
	if _, err := fixture.stateStore.RotateBackupBackendCredentials(globalBackend.ID, fixture.attacker.ID, false, model.DataBackendCredentials{AccessKeyID: "x", SecretAccessKey: "y"}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("store must reject tenant rotation of platform backend, got %v", err)
	}
	if _, err := fixture.stateStore.RecordBackupBackendTest(globalBackend.ID, fixture.attacker.ID, false, false, "attacker"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("store must reject tenant test mutation of platform backend, got %v", err)
	}
	if _, err := fixture.stateStore.DeleteBackupBackend(globalBackend.ID, fixture.attacker.ID, false); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("store must reject tenant deletion of platform backend, got %v", err)
	}
	after, err := fixture.stateStore.GetBackupBackendForUse(globalBackend.ID, "", true)
	if err != nil {
		t.Fatalf("platform backend disappeared after rejected mutation: %v", err)
	}
	if after.Credentials.AccessKeyID != before.Credentials.AccessKeyID || after.Credentials.SecretAccessKey != before.Credentials.SecretAccessKey || after.LastTestedAt != nil {
		t.Fatalf("platform backend changed after rejected tenant mutation: before=%+v after=%+v", before, after)
	}
}

func TestTenantCannotDeleteAnotherTenantsOrPlatformBackupArtifact(t *testing.T) {
	t.Parallel()

	fixture := newBackupAuthorizationFixture(t)
	victimArtifact := createBackupAuthorizationArtifact(t, fixture.stateStore, fixture.victimApp, fixture.victimBackend, "victim.dump")
	globalArtifact, err := fixture.stateStore.CreateBackupArtifact(model.BackupArtifact{
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: fixture.victimBackend.ID,
		Kind:      model.BackupArtifactKindControlPlanePGDump,
		ObjectKey: "platform.dump",
		Status:    model.BackupArtifactStatusActive,
	})
	if err != nil {
		t.Fatalf("create platform artifact: %v", err)
	}
	for _, artifactID := range []string{victimArtifact.ID, globalArtifact.ID} {
		recorder := performJSONRequest(t, fixture.server, http.MethodDelete, "/v1/backups/artifacts/"+artifactID, fixture.attackerKey, nil)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("expected artifact %q deletion to be hidden, code=%d body=%s", artifactID, recorder.Code, recorder.Body.String())
		}
		artifact, err := fixture.stateStore.GetBackupArtifact(artifactID, "", true)
		if err != nil {
			t.Fatalf("get artifact %q after rejected deletion: %v", artifactID, err)
		}
		if artifact.Status != model.BackupArtifactStatusActive || artifact.DeletedAt != nil {
			t.Fatalf("artifact %q changed after rejected deletion: %+v", artifactID, artifact)
		}
	}

	attackerArtifact := createBackupAuthorizationArtifact(t, fixture.stateStore, fixture.attackerApp, fixture.attackerBackend, "attacker.dump")
	recorder := performJSONRequest(t, fixture.server, http.MethodDelete, "/v1/backups/artifacts/"+attackerArtifact.ID, fixture.attackerKey, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected tenant to delete its own artifact, code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	deleted, err := fixture.stateStore.GetBackupArtifact(attackerArtifact.ID, fixture.attacker.ID, false)
	if err != nil {
		t.Fatalf("get deleted attacker artifact: %v", err)
	}
	if deleted.Status != model.BackupArtifactStatusDeleted || deleted.DeletedAt == nil {
		t.Fatalf("expected own artifact to be marked deleted, got %+v", deleted)
	}
}

func TestTenantCannotPersistCrossTenantBackupRestoreTarget(t *testing.T) {
	t.Parallel()

	fixture := newBackupAuthorizationFixture(t)
	attackerArtifact := createBackupAuthorizationArtifact(t, fixture.stateStore, fixture.attackerApp, fixture.attackerBackend, "attacker-restore.dump")
	legacyMismatchArtifact, err := fixture.stateStore.CreateBackupArtifact(model.BackupArtifact{
		TenantID:  fixture.attacker.ID,
		ProjectID: fixture.attackerProject.ID,
		AppID:     fixture.attackerApp.ID,
		Target: model.BackupTarget{
			Type:      model.BackupTargetAppDatabase,
			TenantID:  fixture.victimApp.TenantID,
			ProjectID: fixture.victimApp.ProjectID,
			AppID:     fixture.victimApp.ID,
		},
		BackendID: fixture.attackerBackend.ID,
		Kind:      model.BackupArtifactKindAppPGDump,
		ObjectKey: "legacy-mismatch.dump",
		Status:    model.BackupArtifactStatusActive,
	})
	if err != nil {
		t.Fatalf("create legacy mismatch artifact: %v", err)
	}
	platformArtifact, err := fixture.stateStore.CreateBackupArtifact(model.BackupArtifact{
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		Kind:      model.BackupArtifactKindControlPlanePGDump,
		ObjectKey: "platform-restore.dump",
		Status:    model.BackupArtifactStatusActive,
	})
	if err != nil {
		t.Fatalf("create platform restore artifact: %v", err)
	}
	inactiveArtifact := createBackupAuthorizationArtifact(t, fixture.stateStore, fixture.attackerApp, fixture.attackerBackend, "inactive-restore.dump")
	if _, err := fixture.stateStore.MarkBackupArtifactDeleted(inactiveArtifact.ID, fixture.attacker.ID, false); err != nil {
		t.Fatalf("mark restore artifact inactive: %v", err)
	}

	requests := []map[string]any{
		{
			"artifact_id": attackerArtifact.ID,
			"target": map[string]any{
				"type":       model.BackupTargetAppDatabase,
				"tenant_id":  fixture.victimApp.TenantID,
				"project_id": fixture.victimApp.ProjectID,
				"app_id":     fixture.victimApp.ID,
			},
		},
		{
			"artifact_id": attackerArtifact.ID,
			"target": map[string]any{
				"type":         model.BackupTargetDataWorkspace,
				"tenant_id":    fixture.victimWorkspace.TenantID,
				"project_id":   fixture.victimWorkspace.ProjectID,
				"workspace_id": fixture.victimWorkspace.ID,
			},
		},
		{
			"artifact_id": attackerArtifact.ID,
			"target": map[string]any{
				"type":       model.BackupTargetPersistentStorage,
				"runtime_id": fixture.victimApp.Spec.RuntimeID,
			},
		},
		{"artifact_id": legacyMismatchArtifact.ID},
	}
	for _, body := range requests {
		recorder := performJSONRequest(t, fixture.server, http.MethodPost, "/v1/backups/restore-plans", fixture.attackerKey, body)
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("expected cross-tenant restore target to be forbidden, code=%d body=%s request=%+v", recorder.Code, recorder.Body.String(), body)
		}
	}
	plans, err := fixture.stateStore.ListBackupRestorePlans("", true, 100)
	if err != nil {
		t.Fatalf("list restore plans after rejected requests: %v", err)
	}
	if len(plans) != 0 {
		t.Fatalf("expected no cross-tenant restore plans to be persisted, got %+v", plans)
	}

	recorder := performJSONRequest(t, fixture.server, http.MethodPost, "/v1/backups/restore-plans", fixture.attackerKey, map[string]any{
		"artifact_id": platformArtifact.ID,
		"target":      map[string]any{"type": model.BackupTargetAppDatabase, "app_id": fixture.attackerApp.ID},
	})
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected platform artifact to be hidden from tenant restore, code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = performJSONRequest(t, fixture.server, http.MethodPost, "/v1/backups/restore-plans", fixture.attackerKey, map[string]any{
		"artifact_id": attackerArtifact.ID,
		"target": map[string]any{
			"type":       model.BackupTargetPersistentStorage,
			"app_id":     fixture.attackerApp.ID,
			"runtime_id": fixture.attackerApp.Spec.RuntimeID,
		},
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected mismatched restore target type to be rejected, code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = performJSONRequest(t, fixture.server, http.MethodPost, "/v1/backups/restore-plans", fixture.attackerKey, map[string]any{
		"artifact_id": inactiveArtifact.ID,
	})
	if recorder.Code != http.StatusConflict {
		t.Fatalf("expected inactive artifact restore to conflict, code=%d body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = performJSONRequest(t, fixture.server, http.MethodPost, "/v1/backups/restore-plans", fixture.attackerKey, map[string]any{
		"artifact_id": attackerArtifact.ID,
		"target": map[string]any{
			"type":       model.BackupTargetAppDatabase,
			"project_id": fixture.attackerApp.ProjectID,
			"app_id":     fixture.attackerApp.ID,
		},
	})
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected own-tenant restore target to remain allowed, code=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Plan model.BackupRestorePlan `json:"plan"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Plan.Target.TenantID != fixture.attacker.ID || response.Plan.Target.AppID != fixture.attackerApp.ID {
		t.Fatalf("expected restore target to be tenant-canonicalized, got %+v", response.Plan)
	}
}

func TestTenantCannotReadOrTriggerPlatformBackupResources(t *testing.T) {
	t.Parallel()

	fixture := newBackupAuthorizationFixture(t)
	platformRun, err := fixture.stateStore.CreateBackupRun(model.BackupRun{
		Target:  model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		Trigger: model.BackupRunTriggerManual,
		Status:  model.BackupRunStatusFailed,
	})
	if err != nil {
		t.Fatalf("create platform backup run: %v", err)
	}
	platformArtifact, err := fixture.stateStore.CreateBackupArtifact(model.BackupArtifact{
		RunID:     platformRun.ID,
		Target:    platformRun.Target,
		Kind:      model.BackupArtifactKindControlPlanePGDump,
		ObjectKey: "platform/control-plane.dump",
		Status:    model.BackupArtifactStatusActive,
	})
	if err != nil {
		t.Fatalf("create platform backup artifact: %v", err)
	}
	platformPlan, err := fixture.stateStore.CreateBackupRestorePlan(model.BackupRestorePlan{
		ArtifactID: platformArtifact.ID,
		Target:     platformArtifact.Target,
		Status:     model.BackupRestoreStatusPlanned,
	})
	if err != nil {
		t.Fatalf("create platform restore plan: %v", err)
	}
	platformRestoreRun, err := fixture.stateStore.CreateBackupRestoreRun(model.BackupRestoreRun{
		PlanID: platformPlan.ID,
		Status: model.BackupRestoreStatusPlanned,
	})
	if err != nil {
		t.Fatalf("create platform restore run: %v", err)
	}

	for _, path := range []string{
		"/v1/backups/policies/" + fixture.stateStore.DefaultControlPlaneBackupPolicyID(),
		"/v1/backups/runs/" + platformRun.ID,
		"/v1/backups/artifacts/" + platformArtifact.ID,
	} {
		recorder := performJSONRequest(t, fixture.server, http.MethodGet, path, fixture.attackerKey, nil)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("expected platform resource %s to be hidden, code=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}

	recorder := performJSONRequest(t, fixture.server, http.MethodPost, "/v1/backups/restore-runs", fixture.attackerKey, map[string]any{"plan_id": platformPlan.ID})
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected platform restore plan trigger to be hidden, code=%d body=%s", recorder.Code, recorder.Body.String())
	}

	for _, listing := range []struct {
		path string
		key  string
		id   string
	}{
		{path: "/v1/backups/policies?include_disabled=true", key: "policies", id: fixture.stateStore.DefaultControlPlaneBackupPolicyID()},
		{path: "/v1/backups/runs", key: "runs", id: platformRun.ID},
		{path: "/v1/backups/artifacts?include_deleted=true", key: "artifacts", id: platformArtifact.ID},
		{path: "/v1/backups/restore-plans", key: "plans", id: platformPlan.ID},
		{path: "/v1/backups/restore-runs", key: "runs", id: platformRestoreRun.ID},
	} {
		recorder := performJSONRequest(t, fixture.server, http.MethodGet, listing.path, fixture.attackerKey, nil)
		if recorder.Code != http.StatusOK {
			t.Fatalf("list tenant backup resources %s, code=%d body=%s", listing.path, recorder.Code, recorder.Body.String())
		}
		if strings.Contains(recorder.Body.String(), listing.id) {
			t.Fatalf("platform resource %q leaked through %s response=%s", listing.id, listing.path, recorder.Body.String())
		}
	}
}

type backupAuthorizationFixture struct {
	stateStore      *store.Store
	server          *Server
	attacker        model.Tenant
	attackerKey     string
	attackerProject model.Project
	attackerApp     model.App
	attackerBackend model.BackupBackend
	victimApp       model.App
	victimWorkspace model.DataWorkspace
	victimBackend   model.BackupBackend
}

func newBackupAuthorizationFixture(t *testing.T) backupAuthorizationFixture {
	t.Helper()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	attacker, err := stateStore.CreateTenant("Backup Attacker")
	if err != nil {
		t.Fatalf("create attacker tenant: %v", err)
	}
	victim, err := stateStore.CreateTenant("Backup Victim")
	if err != nil {
		t.Fatalf("create victim tenant: %v", err)
	}
	_, attackerKey, err := stateStore.CreateAPIKey(attacker.ID, "backup-writer", []string{"backup.read", "backup.write", "backup.restore"})
	if err != nil {
		t.Fatalf("create attacker api key: %v", err)
	}
	attackerProject, err := stateStore.CreateProject(attacker.ID, "attacker-project", "")
	if err != nil {
		t.Fatalf("create attacker project: %v", err)
	}
	attackerRuntime, _, err := stateStore.CreateRuntime(attacker.ID, "attacker-runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create attacker runtime: %v", err)
	}
	attackerApp, err := stateStore.CreateApp(attacker.ID, attackerProject.ID, "attacker-app", "", model.AppSpec{
		Image:     "ghcr.io/example/attacker:latest",
		RuntimeID: attackerRuntime.ID,
		Replicas:  1,
		Postgres:  &model.AppPostgresSpec{Database: "attackerdb"},
	})
	if err != nil {
		t.Fatalf("create attacker app: %v", err)
	}
	victimProject, err := stateStore.CreateProject(victim.ID, "victim-project", "")
	if err != nil {
		t.Fatalf("create victim project: %v", err)
	}
	victimRuntime, _, err := stateStore.CreateRuntime(victim.ID, "victim-runtime", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create victim runtime: %v", err)
	}
	victimApp, err := stateStore.CreateApp(victim.ID, victimProject.ID, "victim-app", "", model.AppSpec{
		Image:     "ghcr.io/example/victim:latest",
		RuntimeID: victimRuntime.ID,
		Replicas:  1,
		Postgres:  &model.AppPostgresSpec{Database: "victimdb"},
	})
	if err != nil {
		t.Fatalf("create victim app: %v", err)
	}
	victimWorkspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{
		TenantID:  victim.ID,
		ProjectID: victimProject.ID,
		Name:      "victim-workspace",
	})
	if err != nil {
		t.Fatalf("create victim workspace: %v", err)
	}
	attackerBackend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		TenantID: attacker.ID,
		Name:     "attacker-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "attacker-bucket",
		Endpoint: "https://attacker.example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create attacker backup backend: %v", err)
	}
	victimBackend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		TenantID: victim.ID,
		Name:     "victim-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "victim-bucket",
		Endpoint: "https://victim.example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create victim backup backend: %v", err)
	}
	return backupAuthorizationFixture{
		stateStore:      stateStore,
		server:          NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{}),
		attacker:        attacker,
		attackerKey:     attackerKey,
		attackerProject: attackerProject,
		attackerApp:     attackerApp,
		attackerBackend: attackerBackend,
		victimApp:       victimApp,
		victimWorkspace: victimWorkspace,
		victimBackend:   victimBackend,
	}
}

func createBackupAuthorizationPolicy(t *testing.T, stateStore *store.Store, app model.App, backend model.BackupBackend, name string) model.BackupPolicy {
	t.Helper()
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		TenantID:  app.TenantID,
		ProjectID: app.ProjectID,
		AppID:     app.ID,
		Name:      name,
		Target: model.BackupTarget{
			Type:      model.BackupTargetAppDatabase,
			TenantID:  app.TenantID,
			ProjectID: app.ProjectID,
			AppID:     app.ID,
		},
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
		Schedule:  model.BackupDefaultSchedule,
	})
	if err != nil {
		t.Fatalf("create backup authorization policy %q: %v", name, err)
	}
	return policy
}

func createBackupAuthorizationArtifact(t *testing.T, stateStore *store.Store, app model.App, backend model.BackupBackend, objectKey string) model.BackupArtifact {
	t.Helper()
	artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
		TenantID:  app.TenantID,
		ProjectID: app.ProjectID,
		AppID:     app.ID,
		Target: model.BackupTarget{
			Type:      model.BackupTargetAppDatabase,
			TenantID:  app.TenantID,
			ProjectID: app.ProjectID,
			AppID:     app.ID,
		},
		BackendID: backend.ID,
		Kind:      model.BackupArtifactKindAppPGDump,
		ObjectKey: objectKey,
		Status:    model.BackupArtifactStatusActive,
	})
	if err != nil {
		t.Fatalf("create backup authorization artifact %q: %v", objectKey, err)
	}
	return artifact
}

func TestAppBackupPosturePrefersOperationalPolicyAndUsesPolicyMessage(t *testing.T) {
	t.Parallel()

	server := &Server{}
	app := model.App{ID: "app_a", TenantID: "tenant_a", ProjectID: "project_a", Name: "app"}
	recentSuccess := time.Date(2026, 6, 13, 10, 0, 0, 0, time.UTC)
	policies := []model.BackupPolicy{
		{ID: "disabled", Name: "a-disabled", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase}, Enabled: false, Status: model.BackupPolicyStatusDisabled, DisabledReason: "disabled by user"},
		{ID: "blocked", Name: "b-blocked", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase}, Enabled: true, Status: model.BackupPolicyStatusBlockedNoBackend, DisabledReason: "backup backend is not configured"},
		{ID: "active-without-success", Name: "a-active", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase}, Enabled: true, Status: model.BackupPolicyStatusActive, DisabledReason: "stale message"},
		{ID: "active", Name: "z-active", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase}, Enabled: true, Status: model.BackupPolicyStatusActive, LastSuccessfulAt: &recentSuccess},
	}
	posture := server.appBackupPosture(app, policies, nil)
	if got := posture[0]; got.PolicyID != "active" || got.Status != model.BackupPolicyStatusActive || got.Message != "" {
		t.Fatalf("expected active policy to win and clear disabled messaging, got %+v", got)
	}

	posture = server.appBackupPosture(app, policies[:2], nil)
	if got := posture[0]; got.PolicyID != "blocked" || got.Status != model.BackupPolicyStatusBlockedNoBackend || got.Message != "backup backend is not configured" {
		t.Fatalf("expected blocked policy reason, got %+v", got)
	}
}

func TestPersistentStoragePostureDoesNotReportUnsupportedRuntimeActive(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Runtime Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	runtime, _, err := stateStore.CreateRuntime(tenant.ID, "external", model.RuntimeTypeExternalOwned, "", nil)
	if err != nil {
		t.Fatalf("create external runtime: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	app := model.App{
		ID:       "app_a",
		TenantID: tenant.ID,
		Spec: model.AppSpec{
			RuntimeID: runtime.ID,
			Workspace: &model.AppWorkspaceSpec{MountPath: "/workspace"},
		},
	}
	posture := server.appBackupPosture(app, []model.BackupPolicy{{
		ID:      "storage",
		Target:  model.BackupTarget{Type: model.BackupTargetPersistentStorage},
		Enabled: true,
		Status:  model.BackupPolicyStatusActive,
	}}, nil)
	if got := posture[1]; got.Status != "blocked" || !strings.Contains(got.Message, "does not support persistent workspaces") {
		t.Fatalf("expected unsupported runtime storage policy to be blocked, got %+v", got)
	}
}

func TestCreateAppBackupPolicyResolvesManagedPostgresTarget(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Backup Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtime, _, err := stateStore.CreateRuntime(tenant.ID, "tenant-owned", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "postgres-app", "", model.AppSpec{
		Image:     "ghcr.io/example/app:latest",
		RuntimeID: runtime.ID,
		Replicas:  1,
		Postgres: &model.AppPostgresSpec{
			Database: "appdb",
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.read", "backup.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/backups/policies", apiKey, map[string]any{
		"target": map[string]any{"type": model.BackupTargetAppDatabase},
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Policy model.BackupPolicy `json:"policy"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Policy.Target.Database != "appdb" {
		t.Fatalf("expected app database target to be resolved, got %+v", response.Policy.Target)
	}
	if response.Policy.Target.RuntimeID != runtime.ID {
		t.Fatalf("expected app database runtime %q, got %+v", runtime.ID, response.Policy.Target)
	}
	if response.Policy.Target.ServiceName == "" {
		t.Fatalf("expected app database service name to be resolved, got %+v", response.Policy.Target)
	}
	if response.Policy.Status != model.BackupPolicyStatusBlockedNoBackend {
		t.Fatalf("expected app backup policy without backend to be blocked, got %+v", response.Policy)
	}
}

func TestCreateAppBackupPolicyRejectsAppWithoutManagedPostgres(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Backup Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtime, _, err := stateStore.CreateRuntime(tenant.ID, "tenant-owned", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "stateless-app", "", model.AppSpec{
		Image:     "ghcr.io/example/app:latest",
		RuntimeID: runtime.ID,
		Replicas:  1,
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.read", "backup.write"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/backups/policies", apiKey, map[string]any{
		"target": map[string]any{"type": model.BackupTargetAppDatabase},
	})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
}

func TestControlPlaneBackupRunCreatesRestorePlan(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		Name:      "control-plane",
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
		Schedule:  model.BackupDefaultSchedule,
		Retention: model.BackupRetentionPolicy{RetainCount: model.BackupDefaultRetainCount},
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	artifactCh := make(chan model.BackupArtifact, 1)
	server.backupRunner = func(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
		artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
			RunID:             run.ID,
			PolicyID:          run.PolicyID,
			Target:            run.Target,
			BackendID:         run.BackendID,
			Kind:              model.BackupArtifactKindControlPlanePGDump,
			Version:           "test-control-plane",
			ObjectKey:         "control-plane/postgres.dump",
			ManifestObjectKey: "control-plane/manifest.json",
			SHA256:            strings.Repeat("a", 64),
			SizeBytes:         1024,
			LogicalBytes:      2048,
			Status:            model.BackupArtifactStatusActive,
		})
		if err != nil {
			return nil, err
		}
		artifactCh <- artifact
		return []model.BackupArtifact{artifact}, nil
	}

	runRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/backups/runs", "bootstrap-secret", map[string]any{
		"policy_id": policy.ID,
		"wait":      true,
	})
	if runRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, runRecorder.Code, runRecorder.Body.String())
	}
	var runResponse struct {
		Run model.BackupRun `json:"run"`
	}
	mustDecodeJSON(t, runRecorder, &runResponse)
	if runResponse.Run.Status != model.BackupRunStatusSucceeded {
		t.Fatalf("expected succeeded run, got %+v", runResponse.Run)
	}
	if runResponse.Run.BytesWritten != 1024 || runResponse.Run.ArtifactCount != 1 {
		t.Fatalf("expected backup run output counters, got %+v", runResponse.Run)
	}

	var artifact model.BackupArtifact
	select {
	case artifact = <-artifactCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for control-plane backup artifact")
	}
	planRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/backups/restore-plans", "bootstrap-secret", map[string]any{
		"artifact_id": artifact.ID,
		"mode":        "offline",
	})
	if planRecorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, planRecorder.Code, planRecorder.Body.String())
	}
	var planResponse struct {
		Plan model.BackupRestorePlan `json:"plan"`
	}
	mustDecodeJSON(t, planRecorder, &planResponse)
	if planResponse.Plan.ArtifactID != artifact.ID || planResponse.Plan.Target.Type != model.BackupTargetControlPlaneDatabase {
		t.Fatalf("unexpected control-plane restore plan: %+v", planResponse.Plan)
	}
	if planResponse.Plan.Mode != model.BackupRestoreModeOfflineControlPlane {
		t.Fatalf("expected offline control-plane restore mode, got %+v", planResponse.Plan)
	}
	if len(planResponse.Plan.Phases) == 0 || planResponse.Plan.Phases[0].Name != "download-artifact" {
		t.Fatalf("expected offline restore phases, got %+v", planResponse.Plan.Phases)
	}
}

func TestAdminBackupRunUsesDefaultControlPlanePolicy(t *testing.T) {
	clearDefaultDataBackendEnv(t)

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	_, err := stateStore.CreateBackupBackend(model.BackupBackend{
		ID:       stateStore.DefaultBackupBackendID(),
		Name:     "fugue-default-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
		Status:   "active",
	})
	if err != nil {
		t.Fatalf("create default backend: %v", err)
	}
	if err := stateStore.EnsureDefaultBackupPolicy(); err != nil {
		t.Fatalf("ensure default policy: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	seenRun := make(chan model.BackupRun, 1)
	server.backupRunner = func(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
		seenRun <- run
		return nil, nil
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/backups/runs", "bootstrap-secret", map[string]any{
		"wait": true,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Run model.BackupRun `json:"run"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Run.Status != model.BackupRunStatusSucceeded {
		t.Fatalf("expected succeeded default backup run, got %+v", response.Run)
	}
	if response.Run.PolicyID != stateStore.DefaultControlPlaneBackupPolicyID() {
		t.Fatalf("expected default control-plane policy, got %+v", response.Run)
	}
	if response.Run.BackendID != stateStore.DefaultBackupBackendID() {
		t.Fatalf("expected default backup backend, got %+v", response.Run)
	}

	select {
	case run := <-seenRun:
		if run.PolicyID != stateStore.DefaultControlPlaneBackupPolicyID() || run.BackendID != stateStore.DefaultBackupBackendID() {
			t.Fatalf("expected runner to receive default policy/backend, got %+v", run)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for default backup run execution")
	}
}

func TestBlockedBackupRunDoesNotStartWorker(t *testing.T) {
	clearDefaultDataBackendEnv(t)

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	workerCalled := make(chan struct{}, 1)
	server.backupRunner = func(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
		workerCalled <- struct{}{}
		return nil, nil
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/backups/runs", "bootstrap-secret", map[string]any{
		"wait": true,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Run model.BackupRun `json:"run"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.Run.Status != model.BackupRunStatusBlocked || response.Run.ErrorCode != "backup_backend_missing" {
		t.Fatalf("expected blocked missing-backend run, got %+v", response.Run)
	}
	select {
	case <-workerCalled:
		t.Fatal("blocked backup run should not start worker")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestAppDatabaseBackupRunCreatesCloneRestorePlan(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Backup Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtime, _, err := stateStore.CreateRuntime(tenant.ID, "tenant-owned", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "postgres-app", "", model.AppSpec{
		Image:     "ghcr.io/example/app:latest",
		RuntimeID: runtime.ID,
		Replicas:  1,
		Postgres:  &model.AppPostgresSpec{Database: "appdb"},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		TenantID: tenant.ID,
		Name:     "tenant-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "tenant-bucket",
		Endpoint: "https://tenant.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	_, apiKey, err := stateStore.CreateAPIKey(tenant.ID, "tenant-admin", []string{"app.read", "backup.read", "backup.write", "backup.restore"})
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	artifactCh := make(chan model.BackupArtifact, 1)
	server.backupRunner = func(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
		artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
			RunID:             run.ID,
			PolicyID:          run.PolicyID,
			TenantID:          run.TenantID,
			ProjectID:         run.ProjectID,
			AppID:             run.AppID,
			Target:            run.Target,
			BackendID:         run.BackendID,
			Kind:              model.BackupArtifactKindAppPGDump,
			Version:           "before-migration",
			ObjectKey:         "apps/app/database.dump",
			ManifestObjectKey: "apps/app/manifest.json",
			SHA256:            strings.Repeat("b", 64),
			SizeBytes:         2048,
			LogicalBytes:      4096,
			Status:            model.BackupArtifactStatusActive,
		})
		if err != nil {
			return nil, err
		}
		artifactCh <- artifact
		return []model.BackupArtifact{artifact}, nil
	}

	policyRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/backups/policies", apiKey, map[string]any{
		"target":     map[string]any{"type": model.BackupTargetAppDatabase},
		"backend_id": backend.ID,
		"version":    "before-migration",
	})
	if policyRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, policyRecorder.Code, policyRecorder.Body.String())
	}
	var policyResponse struct {
		Policy model.BackupPolicy `json:"policy"`
	}
	mustDecodeJSON(t, policyRecorder, &policyResponse)
	if policyResponse.Policy.Status != model.BackupPolicyStatusActive || policyResponse.Policy.Target.Database != "appdb" {
		t.Fatalf("expected active resolved app database policy, got %+v", policyResponse.Policy)
	}

	runRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps/"+app.ID+"/backups/runs", apiKey, nil)
	if runRecorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, runRecorder.Code, runRecorder.Body.String())
	}
	var runResponse struct {
		Run model.BackupRun `json:"run"`
	}
	mustDecodeJSON(t, runRecorder, &runResponse)
	if runResponse.Run.AppID != app.ID || runResponse.Run.Target.Type != model.BackupTargetAppDatabase {
		t.Fatalf("unexpected app backup run target: %+v", runResponse.Run)
	}

	var artifact model.BackupArtifact
	select {
	case artifact = <-artifactCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for app database backup artifact")
	}
	finalRun, err := stateStore.GetBackupRun(runResponse.Run.ID, tenant.ID, false)
	if err != nil {
		t.Fatalf("get final app backup run: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for finalRun.Status != model.BackupRunStatusSucceeded && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		finalRun, err = stateStore.GetBackupRun(runResponse.Run.ID, tenant.ID, false)
		if err != nil {
			t.Fatalf("get final app backup run: %v", err)
		}
	}
	if finalRun.Status != model.BackupRunStatusSucceeded || finalRun.ArtifactCount != 1 {
		t.Fatalf("expected succeeded app backup run, got %+v", finalRun)
	}

	planRecorder := performJSONRequest(t, server, http.MethodPost, "/v1/backups/restore-plans", apiKey, map[string]any{
		"artifact_id": artifact.ID,
		"mode":        model.BackupRestoreModeClone,
	})
	if planRecorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, planRecorder.Code, planRecorder.Body.String())
	}
	var planResponse struct {
		Plan model.BackupRestorePlan `json:"plan"`
	}
	mustDecodeJSON(t, planRecorder, &planResponse)
	if planResponse.Plan.TenantID != tenant.ID || planResponse.Plan.AppID != app.ID {
		t.Fatalf("expected tenant-scoped app restore plan, got %+v", planResponse.Plan)
	}
	if planResponse.Plan.Mode != model.BackupRestoreModeClone || planResponse.Plan.Target.Type != model.BackupTargetAppDatabase {
		t.Fatalf("expected app database clone restore plan, got %+v", planResponse.Plan)
	}
	if len(planResponse.Plan.Phases) == 0 || planResponse.Plan.Phases[0].Name != "provision-clone" {
		t.Fatalf("expected clone restore phases, got %+v", planResponse.Plan.Phases)
	}
}

func TestPersistentStorageBackupWorkerArchivesMountedRoot(t *testing.T) {
	s3URL, stored := newBackupFakeS3(t)
	sourceRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(sourceRoot, "data"), 0o755); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceRoot, "data", "hello.txt"), []byte("hello persistent storage\n"), 0o644); err != nil {
		t.Fatalf("write source file: %v", err)
	}
	t.Setenv("FUGUE_BACKUP_PERSISTENT_STORAGE_ROOT", sourceRoot)

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Storage Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	runtime, _, err := stateStore.CreateRuntime(tenant.ID, "tenant-owned", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	app, err := stateStore.CreateApp(tenant.ID, project.ID, "files-app", "", model.AppSpec{
		Image:     "ghcr.io/example/app:latest",
		RuntimeID: runtime.ID,
		Replicas:  1,
		PersistentStorage: &model.AppPersistentStorageSpec{
			Mode:             model.AppPersistentStorageModeMovableRWO,
			StorageClassName: "fugue-rwo",
			Mounts: []model.AppPersistentStorageMount{{
				Kind: model.AppPersistentStorageMountKindDirectory,
				Path: "/data",
			}},
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		TenantID: tenant.ID,
		Name:     "tenant-s3",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: s3URL,
		Region:   "us-east-1",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		TenantID:  tenant.ID,
		ProjectID: project.ID,
		AppID:     app.ID,
		Name:      "persistent-storage",
		Target:    model.BackupTarget{Type: model.BackupTargetPersistentStorage, TenantID: tenant.ID, ProjectID: project.ID, AppID: app.ID},
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		PolicyID:  policy.ID,
		TenantID:  tenant.ID,
		ProjectID: project.ID,
		AppID:     app.ID,
		Target:    policy.Target,
		BackendID: backend.ID,
		Status:    model.BackupRunStatusPending,
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	run, err = stateStore.ClaimBackupRun(run.ID, "persistent-storage-test-worker", time.Now().UTC(), backupRunLeaseTTL)
	if err != nil {
		t.Fatalf("claim run: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	artifacts, err := server.runPersistentStorageBackup(context.Background(), run)
	if err != nil {
		t.Fatalf("run persistent storage backup: %v", err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("expected one artifact, got %+v", artifacts)
	}
	artifact := artifacts[0]
	if artifact.Kind != model.BackupArtifactKindFileArchive || artifact.Target.Type != model.BackupTargetPersistentStorage {
		t.Fatalf("unexpected persistent storage artifact: %+v", artifact)
	}
	if artifact.Manifest.Metadata["restore_target"] != "new-pvc" || artifact.Manifest.Metadata["cutover"] != "normal-deploy-operation" {
		t.Fatalf("expected storage restore metadata, got %+v", artifact.Manifest.Metadata)
	}
	if len(artifact.Manifest.Files) == 0 {
		t.Fatalf("expected manifest files, got %+v", artifact.Manifest)
	}
	if _, ok := stored[artifact.ObjectKey]; !ok {
		t.Fatalf("expected archive object %q in fake s3 keys=%v", artifact.ObjectKey, stored)
	}
	if _, ok := stored[artifact.ManifestObjectKey]; !ok {
		t.Fatalf("expected manifest object %q in fake s3 keys=%v", artifact.ManifestObjectKey, stored)
	}
}

func TestDataWorkspaceBackupWorkerCreatesSnapshotArtifact(t *testing.T) {
	t.Parallel()

	s3URL, stored := newBackupFakeS3(t)
	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := stateStore.CreateTenant("Data Tenant")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := stateStore.CreateProject(tenant.ID, "data", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	workspace, err := stateStore.CreateDataWorkspace(model.DataWorkspace{
		TenantID:  tenant.ID,
		ProjectID: project.ID,
		Name:      "workspace",
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	snapshot, err := stateStore.CreateDataSnapshot(model.DataSnapshot{
		WorkspaceID: workspace.ID,
		Version:     "dataset-v1",
		Manifest: model.DataManifest{Entries: []model.DataManifestEntry{{
			AssetName:    "data",
			RelativePath: "rows.jsonl",
			Kind:         model.DataManifestEntryKindFile,
			Size:         128,
			SHA256:       strings.Repeat("c", 64),
		}}},
	})
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		TenantID: tenant.ID,
		Name:     "tenant-s3",
		Provider: model.DataBackendProviderS3,
		Bucket:   "bucket",
		Endpoint: s3URL,
		Region:   "us-east-1",
		Credentials: model.DataBackendCredentials{
			AccessKeyID:     "access",
			SecretAccessKey: "secret",
		},
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		TenantID:  tenant.ID,
		ProjectID: project.ID,
		Target:    model.BackupTarget{Type: model.BackupTargetDataWorkspace, TenantID: tenant.ID, ProjectID: project.ID, WorkspaceID: workspace.ID},
		BackendID: backend.ID,
		Status:    model.BackupRunStatusPending,
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	run, err = stateStore.ClaimBackupRun(run.ID, "data-workspace-test-worker", time.Now().UTC(), backupRunLeaseTTL)
	if err != nil {
		t.Fatalf("claim run: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	artifacts, err := server.runDataWorkspaceBackup(context.Background(), run)
	if err != nil {
		t.Fatalf("run data workspace backup: %v", err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("expected one artifact, got %+v", artifacts)
	}
	artifact := artifacts[0]
	if artifact.Kind != model.BackupArtifactKindDataSnapshot || artifact.Target.WorkspaceID != workspace.ID {
		t.Fatalf("unexpected data workspace artifact: %+v", artifact)
	}
	if !artifact.Protected {
		t.Fatalf("expected data workspace snapshot artifact to be retention protected, got %+v", artifact)
	}
	if artifact.Manifest.Metadata["snapshot_id"] != snapshot.ID || artifact.Manifest.Metadata["manifest_digest"] != snapshot.ManifestDigest {
		t.Fatalf("expected snapshot metadata, got artifact=%+v snapshot=%+v", artifact, snapshot)
	}
	if _, ok := stored[artifact.ManifestObjectKey]; !ok {
		t.Fatalf("expected data workspace manifest object %q in fake s3 keys=%v", artifact.ManifestObjectKey, stored)
	}
}

func TestReplaceRestoreRunQueuesProtectiveBackup(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	artifact, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: backend.ID,
		Kind:      model.BackupArtifactKindControlPlanePGDump,
		Status:    model.BackupArtifactStatusActive,
	})
	if err != nil {
		t.Fatalf("create artifact: %v", err)
	}
	plan, err := stateStore.CreateBackupRestorePlan(model.BackupRestorePlan{
		ArtifactID: artifact.ID,
		Mode:       model.BackupRestoreModeReplace,
	})
	if err != nil {
		t.Fatalf("create restore plan: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{})
	protectiveCh := make(chan string, 1)
	server.backupRunner = func(ctx context.Context, run model.BackupRun) ([]model.BackupArtifact, error) {
		protectiveCh <- run.ID
		return []model.BackupArtifact{}, nil
	}

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/backups/restore-runs", "bootstrap-secret", map[string]any{
		"plan_id": plan.ID,
		"mode":    model.BackupRestoreModeReplace,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusAccepted, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Run model.BackupRestoreRun `json:"run"`
	}
	mustDecodeJSON(t, recorder, &response)
	if len(response.Run.Phases) == 0 || !strings.Contains(response.Run.Phases[0].Message, "queued protective backup run") {
		t.Fatalf("expected protective backup phase, got %+v", response.Run.Phases)
	}
	runs, err := stateStore.ListBackupRuns(store.BackupRunFilter{PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list backup runs: %v", err)
	}
	if len(runs) != 1 || runs[0].Trigger != "pre-restore-protective" {
		t.Fatalf("expected protective backup run, got %+v", runs)
	}
	select {
	case <-protectiveCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for protective backup execution")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		latest, err := stateStore.GetBackupRun(runs[0].ID, "", true)
		if err != nil {
			t.Fatalf("get protective run: %v", err)
		}
		if latest.Status == model.BackupRunStatusSucceeded {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for protective backup run to succeed")
}

func TestPlatformBackupPostureReportsCNPGAndExternalizedComponents(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	server := NewServer(stateStore, auth.New(stateStore, "bootstrap-secret"), nil, ServerConfig{
		ControlPlaneCNPGBackupEnabled: true,
		ControlPlaneCNPGBackupName:    "fugue-postgres-backup",
		RegistryPushBase:              "registry.example.com",
		RegistryPullBase:              "registry.example.com",
		ClusterJoinRegistryEndpoint:   "registry.example.com",
		ClusterJoinMeshProvider:       "tailscale",
		ClusterJoinMeshLoginServer:    "https://mesh.example.com",
	})
	recorder := performJSONRequest(t, server, http.MethodGet, "/v1/admin/backups/status", "bootstrap-secret", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	var response struct {
		Posture []model.BackupPosture `json:"posture"`
	}
	mustDecodeJSON(t, recorder, &response)
	if !backupPostureHas(response.Posture, model.BackupTargetControlPlaneDatabase, "", func(posture model.BackupPosture) bool {
		return posture.CNPGBackupIntegrated
	}) {
		t.Fatalf("expected CNPG-integrated control-plane posture, got %+v", response.Posture)
	}
	if !backupPostureHas(response.Posture, model.BackupTargetRegistry, "registry", func(posture model.BackupPosture) bool {
		return posture.Externalized && posture.ExternallyBackedUp
	}) {
		t.Fatalf("expected externalized registry posture, got %+v", response.Posture)
	}
	if !backupPostureHas(response.Posture, model.BackupTargetPlatformComponent, "headscale", func(posture model.BackupPosture) bool {
		return posture.Externalized && posture.ExternallyBackedUp
	}) {
		t.Fatalf("expected externalized headscale posture, got %+v", response.Posture)
	}
}

func TestBackupMetricsExposePolicyAndBillableStorage(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	if _, err := stateStore.CreateBackupArtifact(model.BackupArtifact{
		TenantID:  "tenant_a",
		Target:    model.BackupTarget{Type: model.BackupTargetAppDatabase, TenantID: "tenant_a", AppID: "app_a"},
		Kind:      model.BackupArtifactKindAppPGDump,
		SizeBytes: 123,
		Status:    model.BackupArtifactStatusActive,
		Billable:  true,
	}); err != nil {
		t.Fatalf("create artifact: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	recorder := httptest.NewRecorder()
	server.MetricsHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, want := range []string{
		"fugue_backup_policies",
		`target_type="control-plane-db"`,
		"fugue_backup_artifact_bytes",
		"fugue_backup_billable_bytes",
		"123.000000",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected metrics to contain %q, got:\n%s", want, body)
		}
	}
}

func TestScheduleBackupRetryCreatesPendingRetryRun(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		Name:      "control-plane",
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
		Schedule:  model.BackupDefaultSchedule,
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	dueAt := time.Now().UTC().Add(-time.Minute)
	policy.NextRunAt = &dueAt
	policy, err = stateStore.UpsertBackupPolicy(policy)
	if err != nil {
		t.Fatalf("mark policy due: %v", err)
	}
	failedRun, err := stateStore.CreateBackupRun(model.BackupRun{
		PolicyID:   policy.ID,
		Target:     policy.Target,
		BackendID:  backend.ID,
		Trigger:    model.BackupRunTriggerScheduled,
		Status:     model.BackupRunStatusFailed,
		RetryCount: 0,
		Attempt:    1,
	})
	if err != nil {
		t.Fatalf("create failed run: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	before := time.Now().UTC()
	server.scheduleBackupRetry(ctx, failedRun)

	runs, err := stateStore.ListBackupRuns(store.BackupRunFilter{Status: model.BackupRunStatusPending, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list pending runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected one retry run, got %+v", runs)
	}
	retry := runs[0]
	if retry.Trigger != model.BackupRunTriggerRetry || retry.RetryCount != 1 || retry.Attempt != 2 {
		t.Fatalf("unexpected retry run: %+v", retry)
	}
	if retry.NextRetryAt == nil || retry.NextRetryAt.Before(before.Add(4*time.Minute)) || retry.NextRetryAt.After(before.Add(6*time.Minute)) {
		t.Fatalf("expected retry next run about five minutes out, got %+v", retry.NextRetryAt)
	}
}

func TestExecuteBackupRunClaimsPendingRunOnceAcrossReplicas(t *testing.T) {
	t.Parallel()

	storePath := filepath.Join(t.TempDir(), "store.json")
	primaryStore := store.New(storePath)
	if err := primaryStore.Init(); err != nil {
		t.Fatalf("init primary store: %v", err)
	}
	backend, err := primaryStore.CreateBackupBackend(model.BackupBackend{
		Name:     "claim-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "claim-bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backup backend: %v", err)
	}
	policy, err := primaryStore.UpsertBackupPolicy(model.BackupPolicy{
		Name:      "claim-policy",
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
		Schedule:  model.BackupDefaultSchedule,
	})
	if err != nil {
		t.Fatalf("create backup policy: %v", err)
	}
	run, err := primaryStore.CreateBackupRun(model.BackupRun{
		PolicyID:  policy.ID,
		Target:    policy.Target,
		BackendID: backend.ID,
		Trigger:   model.BackupRunTriggerManual,
		Status:    model.BackupRunStatusPending,
	})
	if err != nil {
		t.Fatalf("create pending backup run: %v", err)
	}

	replicaStore := store.New(storePath)
	if err := replicaStore.Init(); err != nil {
		t.Fatalf("init replica store: %v", err)
	}
	serverA := NewServer(primaryStore, auth.New(primaryStore, ""), nil, ServerConfig{})
	serverB := NewServer(replicaStore, auth.New(replicaStore, ""), nil, ServerConfig{})
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var callMu sync.Mutex
	runnerCalls := 0
	runner := func(ctx context.Context, claimed model.BackupRun) ([]model.BackupArtifact, error) {
		callMu.Lock()
		runnerCalls++
		call := runnerCalls
		callMu.Unlock()
		if claimed.Status != model.BackupRunStatusRunning || claimed.LeaseOwner == "" {
			return nil, errors.New("runner received an unclaimed backup run")
		}
		if call == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		return nil, nil
	}
	serverA.backupRunner = runner
	serverB.backupRunner = runner

	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		serverA.executeBackupRun(context.Background(), run.ID)
	}()
	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first replica to claim backup run")
	}

	secondDone := make(chan struct{})
	go func() {
		defer close(secondDone)
		serverB.executeBackupRun(context.Background(), run.ID)
	}()
	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		close(releaseFirst)
		t.Fatal("second replica did not reject the already claimed run")
	}
	close(releaseFirst)
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for claimed backup run to finish")
	}

	callMu.Lock()
	gotCalls := runnerCalls
	callMu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("expected one runner execution across replicas, got %d", gotCalls)
	}
	finalRun, err := primaryStore.GetBackupRun(run.ID, "", true)
	if err != nil {
		t.Fatalf("get final backup run: %v", err)
	}
	if finalRun.Status != model.BackupRunStatusSucceeded || finalRun.StartedAt == nil || finalRun.FinishedAt == nil {
		t.Fatalf("expected one successful claimed run, got %+v", finalRun)
	}
}

func TestExecuteBackupRunLostLeaseCannotFinalizeOrScheduleRetry(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "fenced-r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "fenced-bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backup backend: %v", err)
	}
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		Name:      "fenced-policy",
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
		Schedule:  model.BackupDefaultSchedule,
	})
	if err != nil {
		t.Fatalf("create backup policy: %v", err)
	}
	dueAt := time.Now().UTC().Add(-time.Minute)
	policy.NextRunAt = &dueAt
	policy, err = stateStore.UpsertBackupPolicy(policy)
	if err != nil {
		t.Fatalf("mark policy due: %v", err)
	}
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		PolicyID:        policy.ID,
		Target:          policy.Target,
		BackendID:       backend.ID,
		Trigger:         model.BackupRunTriggerScheduled,
		Status:          model.BackupRunStatusPending,
		RequestedByType: "system",
		RequestedByID:   "backup-scheduler",
	})
	if err != nil {
		t.Fatalf("create pending scheduled run: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	server.backupRunner = func(ctx context.Context, claimed model.BackupRun) ([]model.BackupArtifact, error) {
		status := model.BackupRunStatusFailed
		owner := "recovery-worker"
		code := "lease_recovered"
		message := "lease transferred after timeout"
		finishedAt := time.Now().UTC()
		if _, err := stateStore.UpdateBackupRun(claimed.ID, store.BackupRunUpdate{
			Status:       &status,
			LeaseOwner:   &owner,
			LockedUntil:  timePtrPtr(nil),
			ErrorCode:    &code,
			ErrorMessage: &message,
			FinishedAt:   timePtrPtr(&finishedAt),
		}); err != nil {
			return nil, err
		}
		return nil, errors.New("stale worker resumed")
	}
	server.executeBackupRun(context.Background(), run.ID)

	runs, err := stateStore.ListBackupRuns(store.BackupRunFilter{PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list backup runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected lost owner to create no retry, got %+v", runs)
	}
	if runs[0].Status != model.BackupRunStatusFailed || runs[0].LeaseOwner != "recovery-worker" || runs[0].ErrorCode != "lease_recovered" {
		t.Fatalf("expected recovery result to remain authoritative, got %+v", runs[0])
	}
}

func TestRecoverStaleBackupRunMarksFailedAndSchedulesRetry(t *testing.T) {
	t.Parallel()

	stateStore := store.New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	backend, err := stateStore.CreateBackupBackend(model.BackupBackend{
		Name:     "r2",
		Provider: model.DataBackendProviderCloudflareR2,
		Bucket:   "bucket",
		Endpoint: "https://example.r2.cloudflarestorage.com",
	})
	if err != nil {
		t.Fatalf("create backend: %v", err)
	}
	policy, err := stateStore.UpsertBackupPolicy(model.BackupPolicy{
		Name:      "control-plane",
		Target:    model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase},
		BackendID: backend.ID,
		Enabled:   true,
		Status:    model.BackupPolicyStatusActive,
		Schedule:  model.BackupDefaultSchedule,
	})
	if err != nil {
		t.Fatalf("create policy: %v", err)
	}
	dueAt := time.Now().UTC().Add(-time.Minute)
	policy.NextRunAt = &dueAt
	policy, err = stateStore.UpsertBackupPolicy(policy)
	if err != nil {
		t.Fatalf("mark policy due: %v", err)
	}
	staleHeartbeat := time.Now().UTC().Add(-10 * time.Minute)
	staleLock := time.Now().UTC().Add(-8 * time.Minute)
	run, err := stateStore.CreateBackupRun(model.BackupRun{
		PolicyID:    policy.ID,
		Target:      policy.Target,
		BackendID:   backend.ID,
		Trigger:     model.BackupRunTriggerScheduled,
		Status:      model.BackupRunStatusRunning,
		Attempt:     1,
		RetryCount:  0,
		LockedUntil: &staleLock,
		HeartbeatAt: &staleHeartbeat,
	})
	if err != nil {
		t.Fatalf("create stale run: %v", err)
	}

	server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
	server.recoverStaleBackupRuns(context.Background())

	recovered, err := stateStore.GetBackupRun(run.ID, "", true)
	if err != nil {
		t.Fatalf("get recovered run: %v", err)
	}
	if recovered.Status != model.BackupRunStatusFailed {
		t.Fatalf("expected stale run to fail, got %+v", recovered)
	}
	if recovered.ErrorCode != "backup_run_lost" || recovered.FinishedAt == nil {
		t.Fatalf("expected stale run recovery metadata, got %+v", recovered)
	}

	runs, err := stateStore.ListBackupRuns(store.BackupRunFilter{Status: model.BackupRunStatusPending, PlatformAdmin: true})
	if err != nil {
		t.Fatalf("list pending runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("expected one retry run, got %+v", runs)
	}
	retry := runs[0]
	if retry.Trigger != model.BackupRunTriggerRetry || retry.Attempt != 2 || retry.RetryCount != 1 {
		t.Fatalf("unexpected retry run: %+v", retry)
	}
	if retry.NextRetryAt == nil {
		t.Fatalf("expected retry next_retry_at, got %+v", retry)
	}
}

func TestBackupRunIsStaleDoesNotFailRecentlyDuePendingRetry(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	nextRetryAt := now.Add(-30 * time.Second)
	run := model.BackupRun{
		Trigger:     model.BackupRunTriggerRetry,
		Status:      model.BackupRunStatusPending,
		Attempt:     2,
		RetryCount:  1,
		NextRetryAt: &nextRetryAt,
		CreatedAt:   nextRetryAt.Add(-30 * time.Minute),
		UpdatedAt:   nextRetryAt.Add(-30 * time.Minute),
	}

	if backupRunIsStale(run, now) {
		t.Fatalf("recently due retry should not be stale: %+v", run)
	}
}

func TestBackupRunIsStaleFailsPendingRetryAfterDueGrace(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	nextRetryAt := now.Add(-10 * time.Minute)
	run := model.BackupRun{
		Trigger:     model.BackupRunTriggerRetry,
		Status:      model.BackupRunStatusPending,
		Attempt:     2,
		RetryCount:  1,
		NextRetryAt: &nextRetryAt,
		CreatedAt:   nextRetryAt.Add(-30 * time.Minute),
		UpdatedAt:   nextRetryAt.Add(-30 * time.Minute),
	}

	if !backupRunIsStale(run, now) {
		t.Fatalf("old due retry should be stale: %+v", run)
	}
}

func newBackupFakeS3(t *testing.T) (string, map[string][]byte) {
	t.Helper()
	var mu sync.Mutex
	objects := map[string][]byte{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/")
		key = strings.TrimPrefix(key, "bucket/")
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read fake s3 put body: %v", err)
			}
			mu.Lock()
			objects[key] = body
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			mu.Lock()
			body, ok := objects[key]
			mu.Unlock()
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = w.Write(body)
		default:
			t.Fatalf("unexpected fake s3 request %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(server.Close)
	return server.URL, objects
}

func backupPostureHas(postures []model.BackupPosture, targetType, component string, match func(model.BackupPosture) bool) bool {
	for _, posture := range postures {
		if posture.Target.Type != targetType {
			continue
		}
		if component != "" && posture.Target.Component != component {
			continue
		}
		if match(posture) {
			return true
		}
	}
	return false
}
