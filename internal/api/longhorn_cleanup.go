package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"fugue/internal/model"
)

var errLonghornCleanupPending = errors.New("Longhorn remote backup deletion is pending; awaiting engine finalizer")
var longhornResourceName = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$`)

// One bounded, restart-safe reconciliation step. Never deletes repository blocks.
// Longhorn's finalizer owns remote deletion, restore interlocks and shared-block GC.
func cleanupLonghornBackup(ctx context.Context, client *kubeLogsClient, artifact model.BackupArtifact, backend model.DataBackend) error {
	if artifact.Kind != model.BackupArtifactKindLonghornSnapshot || artifact.Protected ||
		(artifact.Status != model.BackupArtifactStatusDeleted && artifact.Status != model.BackupArtifactStatusExpired) {
		return errors.New("Longhorn cleanup requires an unprotected terminal artifact")
	}
	m := artifact.Manifest.Metadata
	namespace := firstNonEmptyString(m["longhorn_namespace"], getenv(longhornBackupNamespaceEnv), "longhorn-system")
	name, volume, target := m["longhorn_backup_name"], m["volume_name"], m["backup_target_name"]
	for _, value := range []string{namespace, name, volume, target} {
		if len(value) > 253 || !longhornResourceName.MatchString(value) {
			return errors.New("invalid Longhorn cleanup identity")
		}
	}
	if artifact.RunID == "" || backupRepositoryPrefix(artifact, backend) == "" {
		return errors.New("Longhorn repository does not belong to backup backend")
	}
	u, err := url.Parse(m["longhorn_backup_url"])
	if err != nil || u.User == nil {
		return errors.New("invalid Longhorn backup URL")
	}
	query := u.Query()
	if len(query) != 2 || len(query["backup"]) != 1 || len(query["volume"]) != 1 || query.Get("backup") != name || query.Get("volume") != volume {
		return errors.New("Longhorn backup URL identity mismatch")
	}
	u.RawQuery = ""
	if u.String() != m["backup_target_url"] {
		return errors.New("Longhorn backup URL repository mismatch")
	}
	liveTarget, err := longhornGet(ctx, client, namespace, "backuptargets", target)
	if err != nil {
		return fmt.Errorf("verify Longhorn backup target: %w", err)
	}
	spec, _ := liveTarget["spec"].(map[string]any)
	if longhornStringValue(spec["backupTargetURL"]) != m["backup_target_url"] {
		return errors.New("Longhorn live backup target changed")
	}
	obj, err := longhornGet(ctx, client, namespace, "backups", name)
	if isKubeNotFound(err) {
		return nil
	} // Finalizer completion is the engine's deletion receipt.
	if err != nil {
		return fmt.Errorf("inspect Longhorn backup: %w", err)
	}
	metadata, _ := obj["metadata"].(map[string]any)
	labels, _ := metadata["labels"].(map[string]any)
	status, _ := obj["status"].(map[string]any)
	if longhornStringValue(labels["fugue.pro/backup-run"]) != artifact.RunID ||
		longhornStringValue(status["volumeName"]) != volume || longhornStringValue(status["backupTargetName"]) != target ||
		longhornStringValue(status["url"]) != m["longhorn_backup_url"] {
		return errors.New("Longhorn live backup ownership mismatch")
	}
	if longhornStringValue(metadata["deletionTimestamp"]) != "" {
		return errLonghornCleanupPending
	}
	finalizers, _ := metadata["finalizers"].([]any)
	engineFinalizer := false
	for _, f := range finalizers {
		if f == "longhorn.io" {
			engineFinalizer = true
		}
	}
	uid, revision := longhornStringValue(metadata["uid"]), longhornStringValue(metadata["resourceVersion"])
	if !engineFinalizer || uid == "" || revision == "" {
		return errors.New("Longhorn backup lacks finalizer or deletion preconditions")
	}
	if !strings.EqualFold(longhornStringValue(status["state"]), "Completed") {
		return errors.New("Longhorn backup is not completed; cleanup deferred")
	}
	apiPath := "/apis/longhorn.io/v1beta2/namespaces/" + url.PathEscape(namespace) + "/backups/" + url.PathEscape(name)
	err = client.doJSONBody(ctx, http.MethodDelete, apiPath, map[string]any{
		"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": map[string]string{"uid": uid, "resourceVersion": revision},
	}, nil)
	if err != nil && !isKubeNotFound(err) {
		return fmt.Errorf("request Longhorn backup deletion: %w", err)
	}
	return errLonghornCleanupPending
}
