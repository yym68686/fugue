package store

import (
	"strings"

	"fugue/internal/model"
)

func normalizeAPIKeyStatus(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "", model.APIKeyStatusActive:
		return model.APIKeyStatusActive
	case model.APIKeyStatusDisabled:
		return model.APIKeyStatusDisabled
	default:
		return model.APIKeyStatusActive
	}
}

func normalizeAPIKeyForRead(key *model.APIKey) {
	if key == nil {
		return
	}
	key.Status = normalizeAPIKeyStatus(key.Status)
	if key.Status != model.APIKeyStatusDisabled {
		key.DisabledAt = nil
	}
}

func repairAllAPIKeyStatuses(state *model.State) bool {
	changed := false
	for index := range state.APIKeys {
		originalStatus := state.APIKeys[index].Status
		normalizedStatus := normalizeAPIKeyStatus(originalStatus)
		if normalizedStatus != originalStatus {
			state.APIKeys[index].Status = normalizedStatus
			changed = true
		}
		if normalizedStatus != model.APIKeyStatusDisabled && state.APIKeys[index].DisabledAt != nil {
			state.APIKeys[index].DisabledAt = nil
			changed = true
		}
	}
	return changed
}

func isDeletedPhase(phase string) bool {
	return strings.EqualFold(strings.TrimSpace(phase), "deleted")
}

func hasDeletedAppTombstoneName(name string) bool {
	name = model.SlugifyOptional(name)
	if name == "" {
		return false
	}
	return strings.HasSuffix(name, "-deleted") || strings.Contains(name, "-deleted-")
}

func isDeletedApp(app model.App) bool {
	if isDeletedPhase(app.Status.Phase) {
		return true
	}
	if !hasDeletedAppTombstoneName(app.Name) {
		return false
	}
	if app.Spec.Replicas > 0 {
		return false
	}
	if app.Status.CurrentReplicas > 0 {
		return false
	}
	if strings.TrimSpace(app.Status.CurrentRuntimeID) != "" {
		return false
	}
	return true
}

func fallbackLiveAppPhase(app model.App) (string, bool) {
	if isDeletedApp(app) {
		return "", false
	}
	if app.Status.CurrentReleaseReadyAt != nil {
		return "deployed", true
	}
	if app.Status.CurrentReleaseStartedAt != nil {
		return "deploying", true
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
	// A failed operation must not resurrect a green/deployed phase from the
	// durable CurrentReplicas/ready timestamps. Until a fresh runtime observer
	// proves the old workload, the only truthful projection is unknown.
	if phase, ok := fallbackLiveAppPhase(app); ok {
		if phase == "disabled" {
			return phase
		}
		return "unknown"
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
	if phase == "disabled" {
		return false
	}
	if app.Status.Phase == "unknown" {
		return false
	}
	app.Status.Phase = "unknown"
	return true
}

func normalizeAppStatusForRead(app *model.App) {
	if app == nil {
		return
	}
	model.ApplyAppSpecDefaults(&app.Spec)
	repairFailedAppPhase(app)
	invalidateStoredPhaseAfterFailure(app)
}

func invalidateStoredPhaseAfterFailure(app *model.App) bool {
	if app == nil || app.Status.LastFailedOperation == nil {
		return false
	}
	if !model.AppHasCurrentFailedOperation(app.Status) {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(app.Status.Phase)) {
	case "deployed", "running", "ready", "active":
		// A historical failure does not prove that the previous workload is
		// still serving. Only the live observed-status calculator may restore a
		// green phase after a fresh cluster/generation/endpoint observation.
		app.Status.Phase = "unknown"
		return true
	default:
		return false
	}
}

func repairAllAppStatuses(state *model.State) bool {
	changed := false
	latestFailures := make(map[string]model.Operation)
	for _, op := range state.Operations {
		if op.Status != model.OperationStatusFailed || strings.TrimSpace(op.AppID) == "" {
			continue
		}
		if existing, ok := latestFailures[op.AppID]; ok && !operationIsNewerForStatusRepair(op, existing) {
			continue
		}
		latestFailures[op.AppID] = op
	}
	for index := range state.Apps {
		if repairFailedAppPhase(&state.Apps[index]) {
			changed = true
		}
		failure := model.AppOperationFailureFromOperation(latestFailures[state.Apps[index].ID])
		if !appOperationFailureEqual(state.Apps[index].Status.LastFailedOperation, failure) {
			state.Apps[index].Status.LastFailedOperation = failure
			changed = true
		}
		if invalidateStoredPhaseAfterFailure(&state.Apps[index]) {
			changed = true
		}
	}
	return changed
}

func operationIsNewerForStatusRepair(candidate, existing model.Operation) bool {
	if candidate.UpdatedAt.Equal(existing.UpdatedAt) {
		if candidate.CreatedAt.Equal(existing.CreatedAt) {
			return candidate.ID > existing.ID
		}
		return candidate.CreatedAt.After(existing.CreatedAt)
	}
	return candidate.UpdatedAt.After(existing.UpdatedAt)
}

func appOperationFailureEqual(left, right *model.AppOperationFailure) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if left.ID != right.ID || left.Type != right.Type || left.ErrorMessage != right.ErrorMessage || left.ResultMessage != right.ResultMessage || left.RequestedByType != right.RequestedByType || left.RequestedByID != right.RequestedByID || !left.CreatedAt.Equal(right.CreatedAt) || !left.UpdatedAt.Equal(right.UpdatedAt) {
		return false
	}
	if left.CompletedAt == nil || right.CompletedAt == nil {
		return left.CompletedAt == nil && right.CompletedAt == nil
	}
	return left.CompletedAt.Equal(*right.CompletedAt)
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
