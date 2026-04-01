package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const legacyPostgresCleanupImage = "busybox:1.36"

func (s *Service) triggerLegacyPostgresCleanup(ctx context.Context, client *kubeClient, app model.App, cleanupKey string) error {
	storagePaths := store.LegacyPostgresStoragePaths(app)
	if len(storagePaths) == 0 {
		return nil
	}

	nodeNames, err := client.listNodeNames(ctx)
	if err != nil {
		return fmt.Errorf("list nodes for legacy postgres cleanup: %w", err)
	}
	if len(nodeNames) == 0 {
		return fmt.Errorf("list nodes for legacy postgres cleanup: no nodes found")
	}

	namespace := client.effectiveNamespace("")
	for _, nodeName := range nodeNames {
		job := buildLegacyPostgresCleanupJob(namespace, nodeName, app, cleanupKey, storagePaths)
		if err := client.createJob(ctx, namespace, job); err != nil {
			return fmt.Errorf("create legacy postgres cleanup job on node %s: %w", nodeName, err)
		}
	}
	return nil
}

func (s *Service) clearLegacyPostgresMetadataAndSyncManagedApp(ctx context.Context, client *kubeClient, appID string, scheduling runtime.SchedulingConstraints) error {
	if _, err := s.Store.ClearLegacyPostgresStoragePaths(appID); err != nil {
		return fmt.Errorf("clear legacy postgres storage paths in store: %w", err)
	}

	app, err := s.Store.GetApp(appID)
	if err != nil {
		return fmt.Errorf("reload app after clearing legacy postgres storage paths: %w", err)
	}
	if err := client.applyObject(ctx, runtime.BuildManagedAppObject(app, scheduling), nil); err != nil {
		return fmt.Errorf("sync managed app without legacy postgres storage metadata: %w", err)
	}
	return nil
}

func buildLegacyPostgresCleanupJob(namespace, nodeName string, app model.App, cleanupKey string, storagePaths []string) map[string]any {
	jobName := legacyPostgresCleanupJobName(nodeName, cleanupKey)
	appID := strings.TrimSpace(app.ID)
	tenantID := strings.TrimSpace(app.TenantID)
	appName := strings.TrimSpace(app.Name)

	script := strings.TrimSpace(`
set -eu
host_root="/host/var/lib/fugue"
tenant_root="/host/var/lib/fugue/tenant-data"

printf '%s\n' "$LEGACY_PATHS" | while IFS= read -r target; do
  [ -n "$target" ] || continue
  case "$target" in
    /var/lib/fugue/tenant-data/*) ;;
    *)
      echo "refusing unsafe path: $target" >&2
      exit 1
      ;;
  esac
  rel="${target#/var/lib/fugue/}"
  host_path="${host_root}/${rel}"
  if [ -e "$host_path" ]; then
    rm -rf -- "$host_path"
    echo "removed ${target}"
    parent="$(dirname "$host_path")"
    while [ "$parent" != "$tenant_root" ] && [ "$parent" != "$host_root" ] && [ "$parent" != "/" ]; do
      rmdir "$parent" 2>/dev/null || break
      parent="$(dirname "$parent")"
    done
  else
    echo "missing ${target}"
  fi
done
`)

	labels := map[string]string{
		"app.kubernetes.io/managed-by": "fugue",
		"app.kubernetes.io/component":  "legacy-postgres-cleanup",
		"fugue.pro/app-id":             appID,
		"fugue.pro/tenant-id":          tenantID,
	}
	if strings.TrimSpace(cleanupKey) != "" {
		labels["fugue.pro/cleanup-key"] = cleanupKey
	}

	return map[string]any{
		"apiVersion": "batch/v1",
		"kind":       "Job",
		"metadata": map[string]any{
			"name":      jobName,
			"namespace": namespace,
			"labels":    labels,
		},
		"spec": map[string]any{
			"backoffLimit":            0,
			"ttlSecondsAfterFinished": 600,
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": labels,
				},
				"spec": map[string]any{
					"nodeName":                      strings.TrimSpace(nodeName),
					"restartPolicy":                 "Never",
					"terminationGracePeriodSeconds": 10,
					"tolerations": []map[string]any{
						{"operator": "Exists"},
					},
					"containers": []map[string]any{
						{
							"name":            "legacy-postgres-cleanup",
							"image":           legacyPostgresCleanupImage,
							"imagePullPolicy": "IfNotPresent",
							"command":         []string{"/bin/sh", "-lc", script},
							"env": []map[string]any{
								{"name": "LEGACY_PATHS", "value": strings.Join(storagePaths, "\n")},
								{"name": "APP_ID", "value": appID},
								{"name": "APP_NAME", "value": appName},
							},
							"securityContext": map[string]any{
								"runAsUser": 0,
							},
							"volumeMounts": []map[string]any{
								{
									"name":      "host-var-lib-fugue",
									"mountPath": "/host/var/lib/fugue",
								},
							},
						},
					},
					"volumes": []map[string]any{
						{
							"name": "host-var-lib-fugue",
							"hostPath": map[string]any{
								"path": "/var/lib/fugue",
								"type": "DirectoryOrCreate",
							},
						},
					},
				},
			},
		},
	}
}

func legacyPostgresCleanupJobName(nodeName, cleanupKey string) string {
	nodeName = model.Slugify(nodeName)
	if nodeName == "" {
		nodeName = "node"
	}
	if len(nodeName) > 20 {
		nodeName = nodeName[:20]
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(cleanupKey) + "\n" + nodeName))
	return fmt.Sprintf("legacy-pg-cleanup-%s-%s", nodeName, hex.EncodeToString(sum[:])[:10])
}
