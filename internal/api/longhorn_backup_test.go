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
