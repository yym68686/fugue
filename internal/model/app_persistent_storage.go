package model

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"strings"
)

const appPersistentStorageMountRootDirName = "mounts"

func NormalizeAppPersistentStorageMountKind(raw string) (string, error) {
	switch strings.TrimSpace(strings.ToLower(raw)) {
	case "", AppPersistentStorageMountKindDirectory:
		return AppPersistentStorageMountKindDirectory, nil
	case AppPersistentStorageMountKindFile:
		return AppPersistentStorageMountKindFile, nil
	default:
		return "", fmt.Errorf("mount kind must be file or directory")
	}
}

func NormalizeAppPersistentStoragePath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	return NormalizeAbsolutePath(raw)
}

func NormalizeAppPersistentStorageMountPath(kind, raw string) (string, error) {
	normalizedKind, err := NormalizeAppPersistentStorageMountKind(kind)
	if err != nil {
		return "", err
	}
	cleaned, err := NormalizeAbsolutePath(raw)
	if err != nil {
		return "", err
	}
	if cleaned == "/" {
		return "", fmt.Errorf("persistent storage mount path must not be /")
	}
	if normalizedKind == AppPersistentStorageMountKindFile && strings.HasSuffix(cleaned, "/") {
		return "", fmt.Errorf("persistent storage file mount path must point to a file")
	}
	return cleaned, nil
}

func AppPersistentStorageInternalPath(rootPath string) string {
	return path.Join(strings.TrimSpace(rootPath), AppPersistentStorageInternalDirName)
}

func AppPersistentStorageMountRootPath(rootPath string) string {
	return path.Join(strings.TrimSpace(rootPath), appPersistentStorageMountRootDirName)
}

func AppPersistentStorageMountSubPath(mount AppPersistentStorageMount) string {
	return path.Join(appPersistentStorageMountRootDirName, AppPersistentStorageMountKey(mount))
}

func AppPersistentStorageMountKey(mount AppPersistentStorageMount) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(strings.ToLower(mount.Kind)) + "\x00" + path.Clean(strings.TrimSpace(mount.Path))))
	return "mount-" + hex.EncodeToString(sum[:8])
}

func AppPersistentStorageMountPathConflict(left, right AppPersistentStorageMount) bool {
	leftKind, err := NormalizeAppPersistentStorageMountKind(left.Kind)
	if err != nil {
		return false
	}
	rightKind, err := NormalizeAppPersistentStorageMountKind(right.Kind)
	if err != nil {
		return false
	}
	leftPath, err := NormalizeAppPersistentStorageMountPath(leftKind, left.Path)
	if err != nil {
		return false
	}
	rightPath, err := NormalizeAppPersistentStorageMountPath(rightKind, right.Path)
	if err != nil {
		return false
	}
	if leftPath == rightPath {
		return true
	}
	return (leftKind == AppPersistentStorageMountKindDirectory && PathWithinBase(leftPath, rightPath)) ||
		(rightKind == AppPersistentStorageMountKindDirectory && PathWithinBase(rightPath, leftPath))
}
