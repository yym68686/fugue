package objectstorage

import (
	"path"
	"strings"
)

// PhysicalKey applies the configured backend prefix to a logical object key.
func PhysicalKey(prefix, logicalKey string) string {
	logicalKey = strings.Trim(strings.TrimSpace(logicalKey), "/")
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return logicalKey
	}
	return path.Join(prefix, logicalKey)
}

// LogicalKey reverses PhysicalKey only for a canonical key inside the exact
// backend prefix. Unsafe or sibling keys must never become logical keys.
func LogicalKey(prefix, physicalKey string) (string, bool) {
	canonicalPhysicalKey := strings.Trim(strings.TrimSpace(physicalKey), "/")
	if physicalKey != canonicalPhysicalKey || physicalKey == "" || path.Clean(physicalKey) != physicalKey || strings.ContainsRune(physicalKey, '\x00') {
		return "", false
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return physicalKey, true
	}
	if path.Clean(prefix) != prefix || strings.ContainsRune(prefix, '\x00') {
		return "", false
	}
	prefix += "/"
	if !strings.HasPrefix(physicalKey, prefix) {
		return "", false
	}
	logical := strings.TrimPrefix(physicalKey, prefix)
	if logical == "" || path.Clean(logical) != logical || strings.ContainsRune(logical, '\x00') {
		return "", false
	}
	return logical, true
}
