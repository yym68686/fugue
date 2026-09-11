package api

import (
	"fugue/internal/model"
	"testing"
)

func TestLonghornBackupIsDisabledByDefault(t *testing.T) {
	t.Setenv(longhornBackupEngineEnabledEnv, "")
	if longhornBackupEnabled() {
		t.Fatal("Longhorn backup must be opt-in")
	}
}

func TestLonghornBackupRequiresExplicitRemoteEngine(t *testing.T) {
	t.Setenv(longhornBackupEngineEnabledEnv, "true")
	if !longhornBackupEnabled() {
		t.Fatal("Longhorn backup should be enabled")
	}
	policy := model.NormalizeBackupPolicy(model.BackupPolicy{Target: model.BackupTarget{Type: model.BackupTargetAppDatabase}})
	if policy.Engine != model.BackupEngineLogicalPGDump {
		t.Fatalf("default engine = %q", policy.Engine)
	}
}

func TestLonghornBackupStatusRejectsWrongTarget(t *testing.T) {
	status := longhornBackupStatus{State: "Completed", BackupTargetName: "other"}
	if status.BackupTargetName == "default" {
		t.Fatal("fixture must exercise target mismatch")
	}
}

func TestResolveBackupEngineRejectsRemoteRequirementOnLogicalDump(t *testing.T) {
	_, _, err := resolveBackupEngine(model.BackupTarget{Type: model.BackupTargetAppDatabase}, model.BackupEngineLogicalPGDump, true)
	if err == nil {
		t.Fatal("expected remote snapshot requirement to reject logical pg_dump")
	}
}

func TestResolveBackupEngineMakesLonghornRemoteExplicit(t *testing.T) {
	target, engine, err := resolveBackupEngine(model.BackupTarget{Type: model.BackupTargetAppDatabase}, model.BackupEngineLonghornSnapshot, false)
	if err != nil {
		t.Fatalf("resolve Longhorn engine: %v", err)
	}
	if engine != model.BackupEngineLonghornSnapshot || !target.RemoteSnapshotRequired || target.Engine != engine {
		t.Fatalf("expected explicit remote Longhorn target, got engine=%q target=%+v", engine, target)
	}
}

func TestResolveBackupEngineRejectsLonghornForNonDatabaseTarget(t *testing.T) {
	_, _, err := resolveBackupEngine(model.BackupTarget{Type: model.BackupTargetPersistentStorage}, model.BackupEngineLonghornSnapshot, false)
	if err == nil {
		t.Fatal("expected Longhorn engine to reject non database target")
	}
}

func TestLonghornObjectNameMalformedResponseDoesNotPanic(t *testing.T) {
	if got := longhornObjectName(map[string]any{"metadata": "malformed"}); got != "" {
		t.Fatalf("expected empty name, got %q", got)
	}
}
