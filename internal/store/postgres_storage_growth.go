package store

import (
	"fugue/internal/model"
	"strings"

	"k8s.io/apimachinery/pkg/api/resource"
)

// ManagedPostgresStorageGrowthInPlace separates capacity intent from placement.
// Retaining the existing node pin must not turn an expansion into a migration.
func ManagedPostgresStorageGrowthInPlace(current, desired *model.AppPostgresSpec, sourceRuntimeID, targetRuntimeID, targetNode string) bool {
	if current == nil || desired == nil || strings.TrimSpace(sourceRuntimeID) == "" ||
		strings.TrimSpace(sourceRuntimeID) != strings.TrimSpace(targetRuntimeID) {
		return false
	}
	if node := strings.TrimSpace(targetNode); node != "" && node != strings.TrimSpace(current.PrimaryNodeName) {
		return false
	}
	if strings.TrimSpace(current.StorageClassName) != strings.TrimSpace(desired.StorageClassName) {
		return false
	}
	before, err := resource.ParseQuantity(strings.TrimSpace(current.StorageSize))
	if err != nil || before.Sign() <= 0 {
		return false
	}
	after, err := resource.ParseQuantity(strings.TrimSpace(desired.StorageSize))
	return err == nil && after.Cmp(before) > 0
}
