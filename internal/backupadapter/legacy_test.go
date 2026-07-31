package backupadapter

import (
	"encoding/json"
	"os/exec"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"fugue/internal/backupcontrol"
	"fugue/internal/model"
)

const (
	adapterBackendGeneration = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	adapterContentDigest     = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	adapterManifestDigest    = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func TestBuildShadowSpecUsesStableOwnershipCells(t *testing.T) {
	tests := []struct {
		name   string
		run    model.BackupRun
		prefix string
	}{
		{name: "control plane", run: model.BackupRun{ID: "run-control", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase}}, prefix: "backup/control-plane-db/"},
		{name: "app database", run: model.BackupRun{ID: "run-app", AppID: "app-1", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app-1", RuntimeID: "runtime-a"}}, prefix: "backup/app-database/"},
		{name: "persistent", run: model.BackupRun{ID: "run-storage", AppID: "app-1", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetPersistentStorage, AppID: "app-1"}}, prefix: "backup/persistent-storage/"},
		{name: "workspace", run: model.BackupRun{ID: "run-workspace", ProjectID: "project-1", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetDataWorkspace, ProjectID: "project-1", WorkspaceID: "workspace-1"}}, prefix: "backup/data-workspace/"},
		{name: "registry", run: model.BackupRun{ID: "run-registry", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetRegistry}}, prefix: "backup/registry/"},
		{name: "component", run: model.BackupRun{ID: "run-component", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetPlatformComponent, Component: "headscale"}}, prefix: "backup/platform-component/"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec, err := BuildShadowSpec(test.run, adapterBackendGeneration)
			if err != nil {
				t.Fatalf("build spec: %v", err)
			}
			if !strings.HasPrefix(spec.CellKey, test.prefix) || !spec.ObservationOnly || spec.ProductionWriteAllowed ||
				spec.RequestID != spec.RunID || spec.Digest == "" {
				t.Fatalf("spec crossed the shadow boundary: %+v", spec)
			}
			if err := backupcontrol.ValidateBackupRunSpec(spec); err != nil {
				t.Fatalf("validate spec: %v", err)
			}
		})
	}

	base := tests[1].run
	baseSpec, err := BuildShadowSpec(base, adapterBackendGeneration)
	if err != nil {
		t.Fatalf("build base app spec: %v", err)
	}
	drifted := base
	drifted.Target.RuntimeID = "runtime-b"
	drifted.Target.ServiceName = "postgres-new"
	drifted.Target.Database = "database-new"
	drifted.Target.Name = "renamed-display-only"
	driftedSpec, err := BuildShadowSpec(drifted, adapterBackendGeneration)
	if err != nil {
		t.Fatalf("build drifted app spec: %v", err)
	}
	if driftedSpec.CellKey != baseSpec.CellKey {
		t.Fatalf("volatile placement changed the ownership cell: base=%s drifted=%s", baseSpec.CellKey, driftedSpec.CellKey)
	}
}

func TestBackendGenerationExcludesSecretsAndHealthNoise(t *testing.T) {
	backend := model.BackupBackend{
		ID: "backend-1", TenantID: "tenant-1", Provider: model.DataBackendProviderS3,
		Bucket: "bucket-a", Region: "region-a", Endpoint: "https://s3.example.test",
		Prefix: "fugue", Status: "active", FugueManaged: false, Billable: true,
		Credentials: model.DataBackendCredentials{AccessKeyID: "public-id", SecretAccessKey: "never-hash-me", Token: "never-hash-me-either"},
		Name:        "display", UpdatedAt: time.Now().UTC(),
	}
	first, err := BackendGeneration(backend, "secret-rotation-1")
	if err != nil {
		t.Fatalf("first generation: %v", err)
	}
	changed := backend
	changed.Name = "renamed"
	changed.UpdatedAt = time.Now().UTC().Add(time.Hour)
	changed.LastTestResult = "failed"
	changed.ErrorMessage = "health probe changed"
	changed.Credentials.AccessKeyID = "different-public-id"
	changed.Credentials.SecretAccessKey = "different-secret"
	changed.Credentials.Token = "different-token"
	second, err := BackendGeneration(changed, "secret-rotation-1")
	if err != nil || second != first {
		t.Fatalf("health/secret noise changed generation: first=%q second=%q err=%v", first, second, err)
	}
	configChanged := changed
	configChanged.Bucket = "bucket-b"
	third, err := BackendGeneration(configChanged, "secret-rotation-1")
	if err != nil || third == first {
		t.Fatalf("backend config mutation did not change generation: first=%q third=%q err=%v", first, third, err)
	}
	rotationChanged, err := BackendGeneration(changed, "secret-rotation-2")
	if err != nil || rotationChanged == first {
		t.Fatalf("credential generation mutation did not change generation: first=%q rotated=%q err=%v", first, rotationChanged, err)
	}
}

func TestBuildShadowStatusMaintainsStrictStatesAndLKG(t *testing.T) {
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	run := model.BackupRun{ID: "run-1", AppID: "app-1", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app-1"}, Attempt: 1}
	spec, err := BuildShadowSpec(run, adapterBackendGeneration)
	if err != nil {
		t.Fatalf("build spec: %v", err)
	}
	tests := []struct {
		name       string
		status     string
		leaseOwner string
		errorCode  string
	}{
		{name: "pending", status: model.BackupRunStatusPending},
		{name: "running", status: model.BackupRunStatusRunning, leaseOwner: "fugue-api/host-a"},
		{name: "failed", status: model.BackupRunStatusFailed, errorCode: "backend unavailable"},
		{name: "blocked", status: model.BackupRunStatusBlocked, errorCode: "blocked_by_backup_lock"},
		{name: "canceled", status: model.BackupRunStatusCanceled},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := run
			candidate.Status = test.status
			candidate.LeaseOwner = test.leaseOwner
			candidate.ErrorCode = test.errorCode
			candidate.ErrorMessage = "sensitive backend detail must not cross the bridge"
			status, err := BuildShadowStatus(spec, candidate, nil, now)
			if err != nil {
				t.Fatalf("build status: %v", err)
			}
			if status.LastKnownGood != nil || status.ProductionWriteAllowed || !status.ObservationOnly {
				t.Fatalf("status boundary drifted: %+v", status)
			}
			if status.ObservedErrorCode != "" && strings.Contains(status.ObservedErrorCode, "sensitive") {
				t.Fatal("legacy error message crossed into status")
			}
			switch test.status {
			case model.BackupRunStatusRunning:
				if !strings.HasPrefix(status.ObservedWorkerID, "legacy-worker-") {
					t.Fatalf("non-canonical worker identity was not irreversibly bounded: %q", status.ObservedWorkerID)
				}
			case model.BackupRunStatusFailed:
				if status.ObservedErrorCode != "backend_unavailable" || !strings.HasPrefix(status.ObservedErrorDigest, "sha256:") {
					t.Fatalf("legacy failure was not safely represented: %+v", status)
				}
			}
			if err := backupcontrol.ValidateBackupRunStatus(spec, status); err != nil {
				t.Fatalf("validate status: %v", err)
			}
		})
	}

	succeeded := run
	succeeded.Status = model.BackupRunStatusSucceeded
	succeeded.ArtifactCount = 1
	artifacts := []model.BackupArtifact{{
		ID: "artifact-1", RunID: run.ID, BackendID: run.BackendID, Kind: model.BackupArtifactKindAppPGDump,
		AppID: run.AppID, Target: run.Target,
		SHA256: adapterContentDigest, ManifestDigest: strings.TrimPrefix(adapterManifestDigest, "sha256:"), Status: model.BackupArtifactStatusActive,
	}}
	status, err := BuildShadowStatus(spec, succeeded, artifacts, now)
	if err != nil {
		t.Fatalf("build succeeded status: %v", err)
	}
	if status.LastKnownGood == nil || status.LastKnownGood.ArtifactID != "artifact-1" || status.LastKnownGood.ContentDigest != "sha256:"+adapterContentDigest {
		t.Fatalf("successful artifact was not bound as LKG: %+v", status.LastKnownGood)
	}
	encoded, err := json.Marshal(status)
	if err != nil || strings.Contains(string(encoded), "sensitive") || strings.Contains(string(encoded), "object_key") {
		t.Fatalf("status leaked legacy detail: %s err=%v", encoded, err)
	}
}

func TestBuildShadowStatusFailsClosedOnDriftAndAmbiguousArtifacts(t *testing.T) {
	run := model.BackupRun{ID: "run-1", AppID: "app-1", BackendID: "backend-1", Target: model.BackupTarget{Type: model.BackupTargetAppDatabase, AppID: "app-1"}, Attempt: 1, ArtifactCount: 1, Status: model.BackupRunStatusSucceeded}
	spec, err := BuildShadowSpec(run, adapterBackendGeneration)
	if err != nil {
		t.Fatalf("build spec: %v", err)
	}
	valid := model.BackupArtifact{ID: "artifact-1", RunID: run.ID, AppID: run.AppID, Target: run.Target, BackendID: run.BackendID, Kind: model.BackupArtifactKindAppPGDump, SHA256: adapterContentDigest, ManifestDigest: strings.TrimPrefix(adapterManifestDigest, "sha256:"), Status: model.BackupArtifactStatusActive}
	mutations := []struct {
		name string
		edit func(*model.BackupRun, *[]model.BackupArtifact)
	}{
		{name: "wrong run", edit: func(candidate *model.BackupRun, _ *[]model.BackupArtifact) { candidate.ID = "run-other" }},
		{name: "attempt beyond contract", edit: func(candidate *model.BackupRun, _ *[]model.BackupArtifact) {
			candidate.Attempt = LegacyBackupAttemptLimit + 1
		}},
		{name: "missing lease for running", edit: func(candidate *model.BackupRun, _ *[]model.BackupArtifact) {
			candidate.Status = model.BackupRunStatusRunning
		}},
		{name: "artifact count drift", edit: func(candidate *model.BackupRun, _ *[]model.BackupArtifact) {
			candidate.ArtifactCount = 2
		}},
		{name: "ambiguous artifacts", edit: func(_ *model.BackupRun, items *[]model.BackupArtifact) { *items = append(*items, valid) }},
		{name: "bad content digest", edit: func(_ *model.BackupRun, items *[]model.BackupArtifact) { (*items)[0].SHA256 = "not-a-digest" }},
		{name: "wrong artifact kind", edit: func(_ *model.BackupRun, items *[]model.BackupArtifact) {
			(*items)[0].Kind = model.BackupArtifactKindFileArchive
		}},
		{name: "wrong artifact target", edit: func(_ *model.BackupRun, items *[]model.BackupArtifact) {
			(*items)[0].AppID = "app-other"
			(*items)[0].Target.AppID = "app-other"
		}},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			candidate := run
			items := []model.BackupArtifact{valid}
			mutation.edit(&candidate, &items)
			if _, err := BuildShadowStatus(spec, candidate, items, time.Now().UTC()); err == nil {
				t.Fatal("unsafe legacy snapshot was accepted")
			}
		})
	}
}

func TestBackupAdapterDependencyBoundary(t *testing.T) {
	command := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list adapter dependencies: %v", err)
	}
	local := make([]string, 0, 3)
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") {
			local = append(local, dependency)
		}
	}
	sort.Strings(local)
	if !reflect.DeepEqual(local, []string{"fugue/internal/backupcontrol", "fugue/internal/model"}) {
		t.Fatalf("adapter direct dependency boundary widened: %v", local)
	}
}
