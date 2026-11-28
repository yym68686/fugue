package backupusage

import (
	"fmt"
	"path"
	"slices"
	"strings"

	"fugue/internal/model"
)

// OwnedObjectKeys returns directly owned logical keys, excluding snapshot and
// repository objects whose lifetime is managed elsewhere.
func OwnedObjectKeys(artifact model.BackupArtifact) ([]string, error) {
	artifact = model.NormalizeBackupArtifact(artifact)
	runID := strings.TrimSpace(artifact.RunID)
	if runID == "" {
		return nil, fmt.Errorf("artifact has no run id")
	}
	keys := make([]string, 0, 2)
	if artifact.Kind != model.BackupArtifactKindDataSnapshot && artifact.ObjectKey != "" {
		keys = append(keys, artifact.ObjectKey)
	}
	if artifact.ManifestObjectKey != "" {
		keys = append(keys, artifact.ManifestObjectKey)
	}
	if len(keys) == 0 {
		// Reconciled Longhorn manifests can live only in the database. Their
		// shared blocks are owned by the engine, whose cleanup is confirmed
		// separately before deleting an artifact's directly owned objects.
		if artifact.Kind == model.BackupArtifactKindLonghornSnapshot {
			return keys, nil
		}
		return nil, fmt.Errorf("artifact has no deletable object keys")
	}
	seen := map[string]struct{}{}
	validated := make([]string, 0, len(keys))
	for _, key := range keys {
		key = strings.Trim(strings.TrimSpace(key), "/")
		if key == "" || path.Clean(key) != key || strings.ContainsRune(key, '\x00') {
			return nil, fmt.Errorf("artifact contains an unsafe object key")
		}
		if !slices.Contains(strings.Split(key, "/"), runID) {
			return nil, fmt.Errorf("artifact object key is not scoped to its run")
		}
		if key == strings.Trim(strings.TrimSpace(artifact.ManifestObjectKey), "/") {
			if path.Base(key) != "manifest.json" {
				return nil, fmt.Errorf("artifact manifest key has an unexpected filename")
			}
		} else if !artifactDataFilenameAllowed(artifact.Kind, path.Base(key)) {
			return nil, fmt.Errorf("artifact data object key has an unexpected filename")
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		validated = append(validated, key)
	}
	return validated, nil
}

func artifactDataFilenameAllowed(kind, filename string) bool {
	switch model.NormalizeBackupArtifactKind(kind) {
	case model.BackupArtifactKindControlPlanePGDump:
		return filename == "control-plane.dump"
	case model.BackupArtifactKindAppPGDump:
		return filename == "database.dump"
	case model.BackupArtifactKindFileArchive:
		return filename == "persistent-storage.tar.gz"
	case model.BackupArtifactKindRegistryArchive:
		return filename == "registry.tar.gz"
	case model.BackupArtifactKindManifest:
		return false
	default:
		return false
	}
}
