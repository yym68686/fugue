package cli

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"fugue/internal/model"
)

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneAppFiles(files []model.AppFile) []model.AppFile {
	if len(files) == 0 {
		return nil
	}
	out := make([]model.AppFile, len(files))
	copy(out, files)
	return out
}

func cloneAppPersistentStorageMounts(mounts []model.AppPersistentStorageMount) []model.AppPersistentStorageMount {
	if len(mounts) == 0 {
		return nil
	}
	out := make([]model.AppPersistentStorageMount, len(mounts))
	copy(out, mounts)
	return out
}

func cloneAppWorkspaceSpec(spec *model.AppWorkspaceSpec) *model.AppWorkspaceSpec {
	if spec == nil {
		return nil
	}
	out := *spec
	return &out
}

func cloneAppPersistentStorageSpec(spec *model.AppPersistentStorageSpec) *model.AppPersistentStorageSpec {
	if spec == nil {
		return nil
	}
	out := *spec
	out.Mounts = cloneAppPersistentStorageMounts(spec.Mounts)
	return &out
}

func cloneAppPostgresSpec(spec *model.AppPostgresSpec) *model.AppPostgresSpec {
	if spec == nil {
		return nil
	}
	out := *spec
	if spec.Resources != nil {
		resources := *spec.Resources
		out.Resources = &resources
	}
	return &out
}

func legacyWorkspacePersistentStorageSpec(workspace *model.AppWorkspaceSpec) *model.AppPersistentStorageSpec {
	if workspace == nil {
		return nil
	}
	mountPath, err := model.NormalizeAppWorkspaceMountPath(workspace.MountPath)
	if err != nil {
		mountPath = model.DefaultAppWorkspaceMountPath
	}
	return &model.AppPersistentStorageSpec{
		StoragePath:      strings.TrimSpace(workspace.StoragePath),
		StorageSize:      strings.TrimSpace(workspace.StorageSize),
		StorageClassName: strings.TrimSpace(workspace.StorageClassName),
		ResetToken:       strings.TrimSpace(workspace.ResetToken),
		Mounts: []model.AppPersistentStorageMount{
			{
				Kind: model.AppPersistentStorageMountKindDirectory,
				Path: mountPath,
				Mode: 0o755,
			},
		},
	}
}

func cloneAppSpec(spec model.AppSpec) model.AppSpec {
	out := spec
	if len(spec.Command) > 0 {
		out.Command = append([]string(nil), spec.Command...)
	}
	if len(spec.Args) > 0 {
		out.Args = append([]string(nil), spec.Args...)
	}
	if len(spec.Ports) > 0 {
		out.Ports = append([]int(nil), spec.Ports...)
	}
	out.Env = cloneStringMap(spec.Env)
	out.Files = cloneAppFiles(spec.Files)
	out.Workspace = cloneAppWorkspaceSpec(spec.Workspace)
	out.PersistentStorage = cloneAppPersistentStorageSpec(spec.PersistentStorage)
	if spec.VolumeReplication != nil {
		replication := *spec.VolumeReplication
		out.VolumeReplication = &replication
	}
	if spec.Failover != nil {
		failover := *spec.Failover
		out.Failover = &failover
	}
	if spec.Resources != nil {
		resources := *spec.Resources
		out.Resources = &resources
	}
	out.Postgres = cloneAppPostgresSpec(spec.Postgres)
	model.ApplyAppSpecDefaults(&out)
	return out
}

func startupCommandValue(spec model.AppSpec) string {
	if len(spec.Command) == 0 {
		return ""
	}
	if len(spec.Command) == 3 && spec.Command[0] == "sh" && spec.Command[1] == "-lc" {
		return strings.TrimSpace(spec.Command[2])
	}
	return strings.TrimSpace(strings.Join(spec.Command, " "))
}

func trimmedStringPointer(value string) *string {
	trimmed := strings.TrimSpace(value)
	return &trimmed
}

func randomResetToken() (string, error) {
	hexValue, err := randomHexString(8)
	if err != nil {
		return "", err
	}
	return "reset-" + hexValue, nil
}

func ensureManagedPostgresPassword(spec *model.AppPostgresSpec) error {
	if spec == nil || strings.TrimSpace(spec.Password) != "" {
		return nil
	}
	password, err := randomHexString(24)
	if err != nil {
		return fmt.Errorf("generate managed postgres password: %w", err)
	}
	spec.Password = password
	return nil
}

func randomHexString(numBytes int) (string, error) {
	buf := make([]byte, numBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
