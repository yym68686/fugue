package api

import (
	"context"
	"encoding/json"
	"fugue/internal/model"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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

func TestLonghornBackupLockConflictIsRetryable(t *testing.T) {
	for _, message := range []string{
		"Longhorn backup failed: failed to acquire lock backupstore/volumes/aa/bb when performing backup create/restore, please try again later",
		"proxy: failed to backup snapshot: failed to create backup: failed to acquire lock backupstore/volumes/aa/bb when performing backup create/restore",
	} {
		if !isLonghornBackupLockConflictMessage(message) {
			t.Fatalf("expected Longhorn lock conflict to be retryable: %q", message)
		}
	}
	if isLonghornBackupLockConflictMessage("Longhorn backup failed: repository is unavailable") || isLonghornBackupLockConflictMessage("failed to acquire lock when listing backups") {
		t.Fatal("unrelated backup failure must not use lock recovery")
	}
}

func TestLonghornLockRetryPolicyKeepsOtherFailuresBounded(t *testing.T) {
	lockFailure := model.BackupRun{Target: model.BackupTarget{Type: model.BackupTargetControlPlaneDatabase, Engine: model.BackupEngineLonghornSnapshot}, ErrorMessage: "failed to acquire lock when performing backup create/restore"}
	if backupRunRetryLimit(lockFailure) != 24 || backupRetryDelayFor(lockFailure, 4) != time.Hour {
		t.Fatal("Longhorn create lock conflicts must keep a durable hourly recovery window")
	}
	for _, mutate := range []func(*model.BackupRun){
		func(r *model.BackupRun) { r.Target.Engine = model.BackupEngineLogicalPGDump },
		func(r *model.BackupRun) { r.ErrorMessage = "failed to acquire lock when listing backups" },
	} {
		run := lockFailure
		mutate(&run)
		if backupRunRetryLimit(run) != 3 || backupRetryDelayFor(run, 4) != 30*time.Minute {
			t.Fatal("unrelated failure received extended retries")
		}
	}
}

func TestLonghornErrorBackupEligibilityIsConservative(t *testing.T) {
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	base := func(status map[string]any) map[string]any {
		return map[string]any{
			"metadata": map[string]any{
				"creationTimestamp": old,
				"labels":            map[string]any{"fugue.pro/backup-run": "run-1"},
			},
			"status": status,
		}
	}
	errorMessage := "failed to acquire lock backupstore/volumes/aa/bb when performing backup create/restore"
	if !longhornErrorBackupEligible(base(map[string]any{"state": "Error", "error": errorMessage}), now, 24*time.Hour) {
		t.Fatal("old Error CR without remote identity should be eligible")
	}
	for _, status := range []map[string]any{
		{"state": "Completed", "error": errorMessage},
		{"state": "Error", "error": errorMessage, "url": "s3://bucket?backup=b&volume=v"},
		{"state": "Error", "error": errorMessage, "volumeName": "volume"},
		{"state": "Error", "error": errorMessage, "size": "1048576"},
		{"state": "Error", "error": "backup target unavailable"},
	} {
		if longhornErrorBackupEligible(base(status), now, 24*time.Hour) {
			t.Fatalf("remote/active Longhorn object must not be eligible: %#v", status)
		}
	}
}

func TestDeleteLonghornErrorBackupUsesFreshIdentityAndPreconditions(t *testing.T) {
	now := time.Now().UTC()
	object := map[string]any{
		"metadata": map[string]any{
			"name": "backup-error", "uid": "uid-1", "resourceVersion": "42",
			"finalizers":        []any{"longhorn.io"},
			"creationTimestamp": now.Add(-48 * time.Hour).Format(time.RFC3339Nano),
			"labels":            map[string]any{"fugue.pro/backup-run": "run-1"},
		},
		"spec": map[string]any{"snapshotName": "snapshot-error"},
		"status": map[string]any{
			"state": "Error", "error": "failed to acquire lock backupstore/volumes/aa/bb when performing backup create/restore",
			"url": "", "volumeName": "", "size": "", "progress": 0,
		},
	}
	var deleteOptions map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/snapshots/") {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(object)
		case http.MethodDelete:
			if err := json.NewDecoder(r.Body).Decode(&deleteOptions); err != nil {
				t.Fatalf("decode DeleteOptions: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	client := &kubeLogsClient{baseURL: server.URL, client: server.Client()}
	if err := deleteLonghornErrorBackup(context.Background(), client, "longhorn-system", "backup-error", object, "run-1", now, 24*time.Hour); err != nil {
		t.Fatalf("delete eligible Error CR: %v", err)
	}
	preconditions, _ := deleteOptions["preconditions"].(map[string]any)
	if preconditions["uid"] != "uid-1" || preconditions["resourceVersion"] != "42" {
		t.Fatalf("expected UID/resourceVersion DeleteOptions preconditions, got %#v", deleteOptions)
	}

	object["status"].(map[string]any)["url"] = "s3://bucket?backup=backup-error&volume=volume-1"
	deleteOptions = nil
	if err := deleteLonghornErrorBackup(context.Background(), client, "longhorn-system", "backup-error", object, "run-1", now, 24*time.Hour); err == nil || !strings.Contains(err.Error(), "no longer eligible") {
		t.Fatalf("remote URL must block cleanup, got %v", err)
	}
	if deleteOptions != nil {
		t.Fatalf("unsafe object unexpectedly reached DELETE: %#v", deleteOptions)
	}
}
