package backupcontrol

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

const (
	testBackendGeneration = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testContentDigest     = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testManifestDigest    = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	testErrorDigest       = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
)

func TestNewShadowBackupRunSpecDerivesTargetArtifactCellAndDigest(t *testing.T) {
	tests := []struct {
		name       string
		target     BackupTarget
		artifact   string
		cellPrefix string
	}{
		{name: "control plane", target: BackupTarget{Type: TargetControlPlaneDatabase, ScopeKey: "platform/control-plane"}, artifact: ArtifactControlPlanePGDump, cellPrefix: "backup/control-plane-db/"},
		{name: "app database", target: BackupTarget{Type: TargetAppDatabase, ScopeKey: "app/app-1/database"}, artifact: ArtifactAppPGDump, cellPrefix: "backup/app-database/"},
		{name: "persistent", target: BackupTarget{Type: TargetPersistentStorage, ScopeKey: "app/app-1/storage"}, artifact: ArtifactFileArchive, cellPrefix: "backup/persistent-storage/"},
		{name: "workspace", target: BackupTarget{Type: TargetDataWorkspace, ScopeKey: "project/project-1/workspace"}, artifact: ArtifactDataSnapshot, cellPrefix: "backup/data-workspace/"},
		{name: "registry", target: BackupTarget{Type: TargetRegistry, ScopeKey: "platform/registry"}, artifact: ArtifactRegistryArchive, cellPrefix: "backup/registry/"},
		{name: "component", target: BackupTarget{Type: TargetPlatformComponent, ScopeKey: "platform/headscale"}, artifact: ArtifactFileArchive, cellPrefix: "backup/platform-component/"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec, err := NewShadowBackupRunSpec("run-1", "request-1", test.target, "backend-1", testBackendGeneration, 3, 120, 900)
			if err != nil {
				t.Fatalf("new spec: %v", err)
			}
			if spec.ArtifactKind != test.artifact || !strings.HasPrefix(spec.CellKey, test.cellPrefix) ||
				spec.Mode != BackupRunModeShadow || !spec.ObservationOnly || spec.ProductionWriteAllowed ||
				spec.IdempotencyKey != "backup-run/request-1" || spec.Digest != DigestBackupRunSpec(spec) {
				t.Fatalf("spec boundary drifted: %+v", spec)
			}
			if err := ValidateBackupRunSpec(spec); err != nil {
				t.Fatalf("validate spec: %v", err)
			}
			copySpec := spec
			copySpec.Target.ScopeKey = "app/other/scope"
			if copySpec.CellKey == BackupCellKey(copySpec.Target) {
				t.Fatal("different target scope reused the same cell key")
			}
		})
	}
}

func TestBackupRunSpecRejectsUnsafeOrUnboundedFields(t *testing.T) {
	valid := func(t *testing.T) BackupRunSpec {
		t.Helper()
		spec, err := NewShadowBackupRunSpec("run-1", "request-1", BackupTarget{Type: TargetAppDatabase, ScopeKey: "app/app-1/database"}, "backend-1", testBackendGeneration, 3, 120, 900)
		if err != nil {
			t.Fatalf("new valid spec: %v", err)
		}
		return spec
	}
	tests := []struct {
		name   string
		mutate func(*BackupRunSpec)
	}{
		{name: "production", mutate: func(spec *BackupRunSpec) { spec.ProductionWriteAllowed = true }},
		{name: "wrong mode", mutate: func(spec *BackupRunSpec) { spec.Mode = "full" }},
		{name: "bad target scope", mutate: func(spec *BackupRunSpec) { spec.Target.ScopeKey = "../../bucket" }},
		{name: "wrong artifact", mutate: func(spec *BackupRunSpec) { spec.ArtifactKind = ArtifactFileArchive }},
		{name: "attempt limit", mutate: func(spec *BackupRunSpec) { spec.AttemptLimit = 11 }},
		{name: "lease ttl", mutate: func(spec *BackupRunSpec) { spec.LeaseTTLSeconds = 10 }},
		{name: "operation timeout", mutate: func(spec *BackupRunSpec) { spec.OperationTimeoutSecs = 3601 }},
		{name: "digest", mutate: func(spec *BackupRunSpec) { spec.Digest = testContentDigest }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := valid(t)
			test.mutate(&spec)
			if err := ValidateBackupRunSpec(spec); err == nil {
				t.Fatal("unsafe spec was accepted")
			}
		})
	}
}

func TestObservedStatusMaintainsLKGAndExpiresBounded(t *testing.T) {
	spec := testSpec(t)
	artifact := &BackupArtifactRef{
		ArtifactID:        "artifact-previous",
		RunID:             "run-previous",
		Kind:              ArtifactAppPGDump,
		ContentDigest:     testContentDigest,
		ManifestDigest:    testManifestDigest,
		BackendGeneration: testBackendGeneration,
	}
	now := time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC)
	status, err := NewObservedBackupRunStatus(spec, LegacyRunObservation{State: ObservedStateRunning, Attempt: 1, Fence: 4, WorkerID: "worker-a"}, artifact, now, 2*time.Minute)
	if err != nil {
		t.Fatalf("new observed status: %v", err)
	}
	if status.ValidUntil.Sub(status.ObservedAt) != 2*time.Minute || status.CellKey != spec.CellKey ||
		status.SpecDigest != spec.Digest || status.Digest != DigestBackupRunStatus(status) ||
		status.LastKnownGood == nil || status.LastKnownGood.ArtifactID != artifact.ArtifactID {
		t.Fatalf("status/LKG boundary drifted: %+v", status)
	}
	if err := ValidateBackupRunStatus(spec, status); err != nil {
		t.Fatalf("validate observed status: %v", err)
	}
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	for _, forbidden := range []string{"token", "password", "secret", "bucket", "endpoint", "objectKey", "dsn"} {
		if bytes.Contains(bytes.ToLower(encoded), []byte(forbidden)) {
			t.Fatalf("status leaked forbidden material %q: %s", forbidden, encoded)
		}
	}
}

func TestObservedStatusStateMatrixAndTamperRejection(t *testing.T) {
	spec := testSpec(t)
	now := time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name          string
		observation   LegacyRunObservation
		lastKnownGood *BackupArtifactRef
		wantAccepted  bool
	}{
		{name: "pending", observation: LegacyRunObservation{State: ObservedStatePending}, wantAccepted: true},
		{name: "running", observation: LegacyRunObservation{State: ObservedStateRunning, Attempt: 1, Fence: 1, WorkerID: "worker-a"}, wantAccepted: true},
		{name: "succeeded without lkg", observation: LegacyRunObservation{State: ObservedStateSucceeded, Attempt: 1, Fence: 1}, wantAccepted: false},
		{name: "succeeded", observation: LegacyRunObservation{State: ObservedStateSucceeded, Attempt: 1, Fence: 1}, lastKnownGood: currentRunArtifact(spec), wantAccepted: true},
		{name: "failed with digest", observation: LegacyRunObservation{State: ObservedStateFailed, Attempt: 1, Fence: 1, ErrorCode: "backend_timeout", ErrorDigest: testErrorDigest}, wantAccepted: true},
		{name: "failed raw error", observation: LegacyRunObservation{State: ObservedStateFailed, Attempt: 1, Fence: 1, ErrorCode: "backend timeout", ErrorDigest: testErrorDigest}, wantAccepted: false},
		{name: "running no worker", observation: LegacyRunObservation{State: ObservedStateRunning, Attempt: 1, Fence: 1}, wantAccepted: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewObservedBackupRunStatus(spec, test.observation, test.lastKnownGood, now, time.Minute)
			if (err == nil) != test.wantAccepted {
				t.Fatalf("accepted=%v err=%v, want accepted=%v", err == nil, err, test.wantAccepted)
			}
		})
	}
	status, err := NewObservedBackupRunStatus(spec, LegacyRunObservation{State: ObservedStatePending}, nil, now, time.Minute)
	if err != nil {
		t.Fatalf("new pending status: %v", err)
	}
	status.ObservedState = ObservedStateSucceeded
	status.Digest = DigestBackupRunStatus(status)
	if err := ValidateBackupRunStatus(spec, status); err == nil {
		t.Fatal("tampered status with recomputed digest was accepted without LKG")
	}
}

func TestObservedStatusRejectsUnboundLKG(t *testing.T) {
	spec := testSpec(t)
	now := time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC)
	observation := LegacyRunObservation{State: ObservedStateSucceeded, Attempt: 1, Fence: 1}
	for name, mutate := range map[string]func(*BackupArtifactRef){
		"different run":        func(ref *BackupArtifactRef) { ref.RunID = "run-other" },
		"different kind":       func(ref *BackupArtifactRef) { ref.Kind = ArtifactFileArchive },
		"different generation": func(ref *BackupArtifactRef) { ref.BackendGeneration = testContentDigest },
	} {
		t.Run(name, func(t *testing.T) {
			artifact := currentRunArtifact(spec)
			mutate(artifact)
			if _, err := NewObservedBackupRunStatus(spec, observation, artifact, now, time.Minute); err == nil {
				t.Fatal("status accepted an LKG that is not bound to the completed run")
			}
		})
	}

	previous := currentRunArtifact(spec)
	previous.RunID = "run-previous"
	previous.BackendGeneration = testContentDigest
	if _, err := NewObservedBackupRunStatus(spec, LegacyRunObservation{State: ObservedStateRunning, Attempt: 1, Fence: 1, WorkerID: "worker-a"}, previous, now, time.Minute); err != nil {
		t.Fatalf("running observation rejected a valid previous-generation LKG: %v", err)
	}
}

func TestBackupStatusContractJSONIsStableAndIndependentOfLegacyModel(t *testing.T) {
	spec := testSpec(t)
	status, err := NewObservedBackupRunStatus(spec, LegacyRunObservation{State: ObservedStatePending}, nil, time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC), time.Minute)
	if err != nil {
		t.Fatalf("new status: %v", err)
	}
	first, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	decoded, err := DecodeBackupRunStatus(spec, first)
	if err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if !reflect.DeepEqual(status, decoded) || DigestBackupRunStatus(decoded) != status.Digest {
		t.Fatalf("status changed across JSON round trip: original=%+v decoded=%+v", status, decoded)
	}
	legacy := model.BackupRun{ID: spec.RunID, Target: model.BackupTarget{Type: model.BackupTargetAppDatabase}, Status: model.BackupRunStatusPending}
	if legacy.ID != status.RunID || legacy.Status != status.ObservedState || legacy.Target.Type != TargetAppDatabase {
		t.Fatalf("legacy compatibility mapping drifted: legacy=%+v status=%+v", legacy, status)
	}
}

func TestBackupContractDecodersRejectUnknownTrailingAndOversizedJSON(t *testing.T) {
	spec := testSpec(t)
	specJSON, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	if decoded, err := DecodeBackupRunSpec(specJSON); err != nil || !reflect.DeepEqual(decoded, spec) {
		t.Fatalf("decode valid spec: decoded=%+v err=%v", decoded, err)
	}

	unknown := append([]byte(nil), specJSON[:len(specJSON)-1]...)
	unknown = append(unknown, []byte(`,"credential":"forbidden"}`)...)
	for name, document := range map[string][]byte{
		"unknown field":  unknown,
		"trailing value": append(append([]byte(nil), specJSON...), []byte(` {}`)...),
		"oversized":      bytes.Repeat([]byte(" "), maxContractDocumentBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeBackupRunSpec(document); err == nil {
				t.Fatal("unsafe JSON document was accepted")
			}
		})
	}
}

func TestBackupControlProductionDependencyClosureIsLocal(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", ".")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("list backupcontrol production dependencies: %v", err)
	}
	forbiddenPrefixes := []string{
		"database/",
		"k8s.io/",
		"github.com/aws/",
		"github.com/google/go-containerregistry",
		"github.com/jackc/",
	}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.HasPrefix(dependency, "fugue/") && dependency != "fugue/internal/backupcontrol" {
			t.Fatalf("backupcontrol production closure crossed component boundary through %q", dependency)
		}
		if dependency == "net" || strings.HasPrefix(dependency, "net/") || dependency == "os/exec" {
			t.Fatalf("backupcontrol production closure imported forbidden capability %q", dependency)
		}
		for _, forbiddenPrefix := range forbiddenPrefixes {
			if strings.HasPrefix(dependency, forbiddenPrefix) {
				t.Fatalf("backupcontrol production closure imported forbidden capability %q", dependency)
			}
		}
	}
}

func currentRunArtifact(spec BackupRunSpec) *BackupArtifactRef {
	return &BackupArtifactRef{
		ArtifactID:        "artifact-1",
		RunID:             spec.RunID,
		Kind:              spec.ArtifactKind,
		ContentDigest:     testContentDigest,
		ManifestDigest:    testManifestDigest,
		BackendGeneration: spec.BackendGeneration,
	}
}

func testSpec(t *testing.T) BackupRunSpec {
	t.Helper()
	spec, err := NewShadowBackupRunSpec("run-1", "request-1", BackupTarget{Type: TargetAppDatabase, ScopeKey: "app/app-1/database"}, "backend-1", testBackendGeneration, 3, 120, 900)
	if err != nil {
		t.Fatalf("new test spec: %v", err)
	}
	return spec
}
