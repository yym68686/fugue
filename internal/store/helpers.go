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

func fallbackLiveAppPhase(app model.App) (string, bool) {
	if isDeletedApp(app) {
		return "", false
	}
	if strings.TrimSpace(app.Status.CurrentRuntimeID) == "" && app.Status.CurrentReplicas <= 0 {
		return "", false
	}
	if app.Status.CurrentReplicas <= 0 {
		return "disabled", true
	}
	return "deployed", true
}

func failedPhaseForApp(app model.App) string {
	if phase, ok := fallbackLiveAppPhase(app); ok {
		return phase
	}
	return "failed"
}

func repairFailedAppPhase(app *model.App) bool {
	if app == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(app.Status.Phase), "failed") {
		return false
	}
	phase, ok := fallbackLiveAppPhase(*app)
	if !ok {
		return false
	}
	if app.Status.Phase == phase {
		return false
	}
	app.Status.Phase = phase
	return true
}

func normalizeAppStatusForRead(app *model.App) {
	repairFailedAppPhase(app)
}

func repairAllAppStatuses(state *model.State) bool {
	changed := false
	for index := range state.Apps {
		if repairFailedAppPhase(&state.Apps[index]) {
			changed = true
		}
	}
	return changed
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
