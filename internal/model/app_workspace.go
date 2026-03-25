package model

import (
	"fmt"
	"path"
	"strings"
)

func NormalizeAbsolutePath(raw string) (string, error) {
	cleaned := path.Clean(strings.TrimSpace(raw))
	if !path.IsAbs(cleaned) {
		return "", fmt.Errorf("path must be absolute")
	}
	return cleaned, nil
}

func NormalizeAppWorkspaceMountPath(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return DefaultAppWorkspaceMountPath, nil
	}
	cleaned, err := NormalizeAbsolutePath(raw)
	if err != nil {
		return "", err
	}
	if cleaned == "/" {
		return "", fmt.Errorf("workspace mount_path must not be /")
	}
	return cleaned, nil
}

func NormalizeAppWorkspaceStoragePath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	return NormalizeAbsolutePath(raw)
}

func AppWorkspaceInternalPath(mountPath string) string {
	return path.Join(strings.TrimSpace(mountPath), AppWorkspaceInternalDirName)
}

func PathWithinBase(basePath, targetPath string) bool {
	basePath = path.Clean(strings.TrimSpace(basePath))
	targetPath = path.Clean(strings.TrimSpace(targetPath))
	if basePath == "." || targetPath == "." {
		return false
	}
	if basePath == targetPath {
		return true
	}
	return strings.HasPrefix(targetPath, basePath+"/")
}
