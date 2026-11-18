package runtime

import (
	"path"

	"fugue/internal/model"
)

const (
	FugueStorageUnmountReadyCondition      = "fugue.pro/StorageUnmountBusinessReady"
	FugueStorageUnmountOperationAnnotation = "fugue.pro/storage-unmount-operation"
)

// Mount checks for an unmount rollout must never seed, reset, chmod or
// otherwise modify the live PVC while another Pod is using it.
func buildExistingPersistentStorageCheck(spec model.AppPersistentStorageSpec) map[string]any {
	args := []string{"sh", "-ec", `
while [ "$#" -gt 0 ]; do
  case "$1" in
    file) [ -f "$2" ] && [ ! -L "$2" ] ;;
    directory) [ -d "$2" ] && [ ! -L "$2" ] ;;
    *) exit 1 ;;
  esac || { printf 'existing persistent subpath missing or invalid\n' >&2; exit 1; }
  shift 2
done
`, "check-mounts"}
	for _, mount := range spec.Mounts {
		args = append(args, mount.Kind, path.Join(persistentStorageContainerRootPath(spec), model.AppPersistentStorageMountSubPath(mount)))
	}
	return map[string]any{
		"name": "init-persistent-storage", "image": defaultWaitImage,
		"command":         args,
		"securityContext": map[string]any{"runAsUser": 0},
		"resources":       runtimeHelperResourceRequirements(),
		"volumeMounts":    []map[string]any{{"name": workspaceVolumeName, "mountPath": persistentStorageRootPath, "readOnly": true}},
	}
}
