package platformcontrol

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const PlatformLKGMinimumRetainedGenerations = 3

type PlatformLKGGCRequest struct {
	History                   []model.PlatformLKGSnapshot
	Current                   *model.PlatformLKGSnapshot
	PinnedRollbackGenerations []string
	EvaluatedAt               time.Time
	DeleteBefore              time.Time
	RetainGenerations         int
}

type PlatformLKGGCDecision struct {
	SnapshotID string `json:"snapshot_id"`
	Generation string `json:"generation"`
	Delete     bool   `json:"delete"`
	Reason     string `json:"reason"`
}

type PlatformLKGGCPlan struct {
	SafeToApply bool                    `json:"safe_to_apply"`
	Blockers    []string                `json:"blockers,omitempty"`
	Decisions   []PlatformLKGGCDecision `json:"decisions"`
	KeepCount   int                     `json:"keep_count"`
	DeleteCount int                     `json:"delete_count"`
}

// PlanPlatformLKGHistoryGC is deliberately side-effect free. Callers must use
// the returned snapshot IDs as a guarded delete set and refuse to execute a
// plan whose SafeToApply field is false.
func PlanPlatformLKGHistoryGC(req PlatformLKGGCRequest) PlatformLKGGCPlan {
	plan := PlatformLKGGCPlan{
		SafeToApply: true,
		Decisions:   make([]PlatformLKGGCDecision, len(req.History)),
	}
	for index, snapshot := range req.History {
		plan.Decisions[index] = PlatformLKGGCDecision{
			SnapshotID: strings.TrimSpace(snapshot.ID),
			Generation: strings.TrimSpace(snapshot.Generation),
			Reason:     "safety_hold",
		}
	}

	evaluatedAt := req.EvaluatedAt.UTC()
	if evaluatedAt.IsZero() {
		plan.Blockers = append(plan.Blockers, "evaluated_at is required")
	}
	deleteBefore := req.DeleteBefore.UTC()
	if deleteBefore.IsZero() {
		plan.Blockers = append(plan.Blockers, "delete_before is required")
	} else if !evaluatedAt.IsZero() && deleteBefore.After(evaluatedAt) {
		plan.Blockers = append(plan.Blockers, "delete_before cannot be after evaluated_at")
	}
	retainGenerations := req.RetainGenerations
	if retainGenerations < PlatformLKGMinimumRetainedGenerations {
		retainGenerations = PlatformLKGMinimumRetainedGenerations
	}

	currentGeneration, artifactKind, scopeKey := "", "", ""
	if req.Current != nil {
		currentGeneration = strings.TrimSpace(req.Current.Generation)
		artifactKind = strings.TrimSpace(req.Current.ArtifactKind)
		scopeKey = strings.TrimSpace(strings.ToLower(req.Current.ScopeKey))
		if strings.TrimSpace(req.Current.ID) == "" || currentGeneration == "" || artifactKind == "" || scopeKey == "" {
			plan.Blockers = append(plan.Blockers, "current verified LKG identity is incomplete")
		}
	}

	latestByGeneration := make(map[string]model.PlatformLKGSnapshot, len(req.History))
	historyBySnapshotID := make(map[string]model.PlatformLKGSnapshot, len(req.History))
	seenSnapshotIDs := make(map[string]struct{}, len(req.History))
	for _, snapshot := range req.History {
		id := strings.TrimSpace(snapshot.ID)
		generation := strings.TrimSpace(snapshot.Generation)
		kind := strings.TrimSpace(snapshot.ArtifactKind)
		snapshotScopeKey := strings.TrimSpace(strings.ToLower(snapshot.ScopeKey))
		if id == "" || generation == "" || kind == "" || snapshotScopeKey == "" {
			plan.Blockers = append(plan.Blockers, "LKG history contains an incomplete snapshot identity")
			continue
		}
		if _, exists := seenSnapshotIDs[id]; exists {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("LKG history contains duplicate snapshot id %s", id))
			continue
		}
		seenSnapshotIDs[id] = struct{}{}
		historyBySnapshotID[id] = snapshot
		if artifactKind == "" {
			artifactKind = kind
			scopeKey = snapshotScopeKey
		}
		if kind != artifactKind || snapshotScopeKey != scopeKey {
			plan.Blockers = append(plan.Blockers, "LKG history spans more than one artifact scope")
		}
		if lkgRetentionSnapshotTime(snapshot).IsZero() {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("LKG snapshot %s has no trusted retention timestamp", id))
		}
		if previous, exists := latestByGeneration[generation]; !exists || lkgRetentionSnapshotNewer(snapshot, previous) {
			latestByGeneration[generation] = snapshot
		}
	}

	if len(req.History) > 0 && req.Current == nil {
		plan.Blockers = append(plan.Blockers, "current verified LKG is missing")
	}
	if currentGeneration != "" {
		if _, exists := latestByGeneration[currentGeneration]; !exists {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("current verified LKG generation %s is absent from history", currentGeneration))
		}
		if _, exists := seenSnapshotIDs[strings.TrimSpace(req.Current.ID)]; !exists {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("current verified LKG snapshot %s is absent from history", strings.TrimSpace(req.Current.ID)))
		} else if historical := historyBySnapshotID[strings.TrimSpace(req.Current.ID)]; !lkgRetentionSnapshotIdentityMatches(*req.Current, historical) {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("current verified LKG snapshot %s does not match its history event", strings.TrimSpace(req.Current.ID)))
		}
	}

	pinned := make(map[string]struct{}, len(req.PinnedRollbackGenerations))
	for _, raw := range req.PinnedRollbackGenerations {
		generation := strings.TrimSpace(raw)
		if generation == "" {
			plan.Blockers = append(plan.Blockers, "pinned rollback generation is empty")
			continue
		}
		pinned[generation] = struct{}{}
		if _, exists := latestByGeneration[generation]; !exists {
			plan.Blockers = append(plan.Blockers, fmt.Sprintf("pinned rollback generation %s is absent from history", generation))
		}
	}

	if len(plan.Blockers) > 0 {
		plan.Blockers = sortedUniqueStrings(plan.Blockers)
		plan.SafeToApply = false
		plan.KeepCount = len(plan.Decisions)
		return plan
	}

	newestGenerations := make([]model.PlatformLKGSnapshot, 0, len(latestByGeneration))
	for _, snapshot := range latestByGeneration {
		newestGenerations = append(newestGenerations, snapshot)
	}
	sort.SliceStable(newestGenerations, func(i, j int) bool {
		return lkgRetentionSnapshotNewer(newestGenerations[i], newestGenerations[j])
	})
	if req.Current != nil && len(newestGenerations) > 0 &&
		strings.TrimSpace(newestGenerations[0].ID) != strings.TrimSpace(req.Current.ID) {
		plan.Blockers = append(plan.Blockers, fmt.Sprintf(
			"current verified LKG snapshot %s is not the latest verification event",
			strings.TrimSpace(req.Current.ID),
		))
		plan.Blockers = sortedUniqueStrings(plan.Blockers)
		plan.SafeToApply = false
		plan.KeepCount = len(plan.Decisions)
		return plan
	}
	minimumRetained := make(map[string]struct{}, retainGenerations)
	for _, snapshot := range newestGenerations {
		if len(minimumRetained) >= retainGenerations {
			break
		}
		minimumRetained[strings.TrimSpace(snapshot.Generation)] = struct{}{}
	}

	for index, snapshot := range req.History {
		generation := strings.TrimSpace(snapshot.Generation)
		decision := &plan.Decisions[index]
		switch {
		case generation == currentGeneration:
			decision.Reason = "current_verified_lkg"
		case containsStringKey(pinned, generation):
			decision.Reason = "pinned_rollback_generation"
		case containsStringKey(minimumRetained, generation):
			decision.Reason = "minimum_verified_history"
		case !lkgRetentionSnapshotTime(snapshot).Before(deleteBefore):
			decision.Reason = "retention_window"
		default:
			decision.Delete = true
			decision.Reason = "retention_expired"
		}
		if decision.Delete {
			plan.DeleteCount++
		} else {
			plan.KeepCount++
		}
	}
	return plan
}

func lkgRetentionSnapshotTime(snapshot model.PlatformLKGSnapshot) time.Time {
	if !snapshot.UpdatedAt.IsZero() {
		return snapshot.UpdatedAt.UTC()
	}
	return snapshot.CreatedAt.UTC()
}

func lkgRetentionSnapshotNewer(candidate, current model.PlatformLKGSnapshot) bool {
	if !candidate.UpdatedAt.Equal(current.UpdatedAt) {
		return candidate.UpdatedAt.After(current.UpdatedAt)
	}
	if !candidate.CreatedAt.Equal(current.CreatedAt) {
		return candidate.CreatedAt.After(current.CreatedAt)
	}
	if candidate.GenerationSequence != current.GenerationSequence {
		return candidate.GenerationSequence > current.GenerationSequence
	}
	return strings.TrimSpace(candidate.ID) > strings.TrimSpace(current.ID)
}

func lkgRetentionSnapshotIdentityMatches(current, historical model.PlatformLKGSnapshot) bool {
	return strings.TrimSpace(current.ID) == strings.TrimSpace(historical.ID) &&
		strings.TrimSpace(current.ArtifactID) == strings.TrimSpace(historical.ArtifactID) &&
		strings.TrimSpace(current.ArtifactKind) == strings.TrimSpace(historical.ArtifactKind) &&
		strings.TrimSpace(strings.ToLower(current.ScopeKey)) == strings.TrimSpace(strings.ToLower(historical.ScopeKey)) &&
		strings.TrimSpace(current.Generation) == strings.TrimSpace(historical.Generation) &&
		current.GenerationSequence == historical.GenerationSequence &&
		strings.TrimSpace(current.ContentHash) == strings.TrimSpace(historical.ContentHash)
}

func containsStringKey(values map[string]struct{}, value string) bool {
	_, ok := values[value]
	return ok
}

func sortedUniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
