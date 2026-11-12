package api

import (
	"context"
	"crypto/sha256"
	"fmt"
)

// A deterministic name closes the accepted-POST/lost-response restart window.
// Labels still discover old generateName objects; an ambiguous inventory must
// never be interpreted as permission to create another snapshot or backup.
func longhornRunObjectName(runID string) string {
	return fmt.Sprintf("fugue-backup-%x", sha256.Sum256([]byte(runID)))[:53]
}

func validateLonghornRunObject(obj map[string]any, runID string) error {
	meta, _ := obj["metadata"].(map[string]any)
	labels, _ := meta["labels"].(map[string]any)
	if longhornObjectName(obj) == "" || longhornStringValue(meta["uid"]) == "" || meta["deletionTimestamp"] != nil || longhornStringValue(labels["fugue.pro/backup-run"]) != runID {
		return fmt.Errorf("Longhorn run object identity or ownership changed")
	}
	return nil
}

func ensureLonghornBackupSource(ctx context.Context, client *kubeLogsClient, namespace, runID, volume string) (string, string, error) {
	backup, err := findLonghornBackup(ctx, client, namespace, runID)
	if err != nil {
		return "", "", fmt.Errorf("find existing Longhorn backup: %w", err)
	}
	if backup != nil {
		if err := validateLonghornRunObject(backup, runID); err != nil {
			return "", "", err
		}
		status, _ := backup["status"].(map[string]any)
		spec, _ := backup["spec"].(map[string]any)
		snapshot := firstNonEmptyString(longhornStringValue(status["snapshotName"]), longhornStringValue(spec["snapshotName"]))
		if snapshot == "" || (longhornStringValue(status["volumeName"]) != "" && longhornStringValue(status["volumeName"]) != volume) ||
			(longhornStringValue(spec["snapshotName"]) != "" && longhornStringValue(spec["snapshotName"]) != snapshot) {
			return "", "", fmt.Errorf("existing Longhorn backup source differs")
		}
		if longhornStringValue(status["state"]) == "Completed" && longhornStringValue(status["volumeName"]) != volume {
			return "", "", fmt.Errorf("completed Longhorn backup volume is unverified")
		}
		// A Completed remote backup remains reusable even if its old local
		// snapshot has already disappeared. Do not wait on that snapshot.
		return snapshot, longhornObjectName(backup), nil
	}
	snapshot, err := findLonghornRunResource(ctx, client, namespace, "snapshots", runID)
	if err != nil {
		return "", "", err
	}
	if snapshot == nil {
		body := map[string]any{
			"apiVersion": "longhorn.io/v1beta2", "kind": "Snapshot",
			"metadata": map[string]any{"name": longhornRunObjectName(runID), "namespace": namespace, "labels": map[string]string{"fugue.pro/backup-run": runID}},
			"spec":     map[string]any{"volume": volume, "createSnapshot": true},
		}
		if err := longhornCreate(ctx, client, namespace, "snapshots", body, &snapshot); err != nil {
			// A subsequent worker re-reads the inventory. Never blindly replay
			// an ambiguous create or replace an object on a name conflict.
			return "", "", fmt.Errorf("create Longhorn snapshot: %w", err)
		}
	}
	if err := validateLonghornRunObject(snapshot, runID); err != nil {
		return "", "", err
	}
	spec, _ := snapshot["spec"].(map[string]any)
	status, _ := snapshot["status"].(map[string]any)
	if longhornStringValue(spec["volume"]) != volume || boolValue(status["markRemoved"]) {
		return "", "", fmt.Errorf("existing Longhorn snapshot volume or removal state differs")
	}
	return longhornObjectName(snapshot), "", nil
}
