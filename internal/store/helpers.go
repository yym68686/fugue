package store

import (
	"strings"

	"fugue/internal/model"
)

func isDeletedPhase(phase string) bool {
	return strings.EqualFold(strings.TrimSpace(phase), "deleted")
}

func isDeletedApp(app model.App) bool {
	return isDeletedPhase(app.Status.Phase)
}

func deletedAppName(name, operationID string) string {
	suffix := "-deleted"
	if short := shortSlugSuffix(operationID, 8); short != "" {
		suffix += "-" + short
	}

	base := model.Slugify(name)
	maxBaseLen := 50 - len(suffix)
	if maxBaseLen < 3 {
		maxBaseLen = 3
	}
	if len(base) > maxBaseLen {
		base = strings.Trim(base[:maxBaseLen], "-")
	}
	if base == "" {
		base = "app"
	}
	return base + suffix
}

func shortSlugSuffix(value string, maxLen int) string {
	value = model.Slugify(value)
	if maxLen <= 0 || len(value) <= maxLen {
		return value
	}
	return strings.Trim(value[len(value)-maxLen:], "-")
}

func findIdempotencyRecord(state *model.State, scope, tenantID, key string) int {
	for index := range state.Idempotency {
		record := state.Idempotency[index]
		if record.Scope == scope && record.TenantID == tenantID && record.Key == key {
			return index
		}
	}
	return -1
}

func deleteIdempotencyRecordsByTenant(records []model.IdempotencyRecord, tenantID string) []model.IdempotencyRecord {
	filtered := records[:0]
	for _, record := range records {
		if record.TenantID == tenantID {
			continue
		}
		filtered = append(filtered, record)
	}
	return filtered
}
