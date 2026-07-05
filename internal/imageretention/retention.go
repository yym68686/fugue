package imageretention

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/imagecachekeys"
	"fugue/internal/model"
)

// Plan computes the desired keep/drop set for distributed app image generations.
// It is intentionally pure so admin dry-runs and controller reconciliation share
// the same retention semantics.
func Plan(app model.App, images []model.Image, ops []model.Operation, pins []model.ImagePin, liveRefs map[string]struct{}, now time.Time) model.DistributedImageRetentionPlan {
	limit := model.EffectiveAppImageMirrorLimit(app.Spec.ImageMirrorLimit)
	plan := model.DistributedImageRetentionPlan{
		TenantID:       strings.TrimSpace(app.TenantID),
		AppID:          strings.TrimSpace(app.ID),
		AppName:        strings.TrimSpace(app.Name),
		EffectiveLimit: limit,
		KeepImageIDs:   []string{},
		DropImageIDs:   []string{},
		ImageDecisions: []model.ImageRetentionDecision{},
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	opByID := map[string]model.Operation{}
	activeOperationIDs := map[string]struct{}{}
	for _, op := range ops {
		if strings.TrimSpace(op.ID) == "" {
			continue
		}
		opByID[op.ID] = op
		if OperationActive(op) {
			activeOperationIDs[op.ID] = struct{}{}
		}
	}

	userPinned := map[string]struct{}{}
	for _, pin := range pins {
		if pin.ExpiresAt != nil && !pin.ExpiresAt.After(now) {
			continue
		}
		switch strings.TrimSpace(pin.Reason) {
		case model.ImagePinReasonUserPin, model.ImagePinReasonRetention:
			userPinned[strings.TrimSpace(pin.ImageID)] = struct{}{}
		}
	}

	liveKeys := keySetFromRefs(liveRefs)
	decisions := make([]model.ImageRetentionDecision, 0, len(images))
	for _, image := range images {
		if strings.TrimSpace(image.ID) == "" {
			continue
		}
		decision := model.ImageRetentionDecision{
			ImageID:           strings.TrimSpace(image.ID),
			ImageRef:          strings.TrimSpace(image.ImageRef),
			SourceOperationID: strings.TrimSpace(image.SourceOperationID),
			LifecycleState:    strings.TrimSpace(image.LifecycleState),
		}
		if op, ok := opByID[decision.SourceOperationID]; ok && op.CompletedAt != nil {
			completed := op.CompletedAt.UTC()
			decision.LastDeployedAt = &completed
		} else if !image.UpdatedAt.IsZero() {
			updated := image.UpdatedAt.UTC()
			decision.LastDeployedAt = &updated
		} else if !image.CreatedAt.IsZero() {
			created := image.CreatedAt.UTC()
			decision.LastDeployedAt = &created
		}
		decision.CurrentWorkload = ImageMatchesKeySet(image, liveKeys)
		_, decision.ActiveOperation = activeOperationIDs[decision.SourceOperationID]
		_, decision.UserPinned = userPinned[decision.ImageID]
		if decision.CurrentWorkload {
			decision.Keep = true
			decision.Reason = "current_workload"
		} else if decision.ActiveOperation {
			decision.Keep = true
			decision.Reason = "active_operation"
		} else if decision.UserPinned {
			decision.Keep = true
			decision.Reason = "user_pin"
		} else if strings.TrimSpace(image.LifecycleState) == model.ImageLifecycleDeleted {
			decision.Reason = "deleted_app"
		} else if strings.TrimSpace(image.LifecycleState) == model.ImageLifecycleLost {
			decision.Reason = "lost_image"
		} else if decision.SourceOperationID != "" {
			if _, ok := opByID[decision.SourceOperationID]; !ok {
				decision.Reason = "missing_source_operation"
			}
		}
		decisions = append(decisions, decision)
	}

	sort.SliceStable(decisions, func(i, j int) bool {
		if decisions[i].Keep != decisions[j].Keep {
			return decisions[i].Keep
		}
		if decisions[i].CurrentWorkload != decisions[j].CurrentWorkload {
			return decisions[i].CurrentWorkload
		}
		if decisions[i].ActiveOperation != decisions[j].ActiveOperation {
			return decisions[i].ActiveOperation
		}
		if decisions[i].UserPinned != decisions[j].UserPinned {
			return decisions[i].UserPinned
		}
		left := DecisionTime(decisions[i])
		right := DecisionTime(decisions[j])
		if !left.Equal(right) {
			return left.After(right)
		}
		return decisions[i].ImageID < decisions[j].ImageID
	})

	keptByLimit := 0
	for _, decision := range decisions {
		if decision.CurrentWorkload {
			keptByLimit++
		}
	}
	for i := range decisions {
		decisions[i].Rank = i + 1
		if decisions[i].Keep {
			continue
		}
		if decisions[i].Reason == "deleted_app" || decisions[i].Reason == "lost_image" {
			continue
		}
		if keptByLimit < limit {
			decisions[i].Keep = true
			decisions[i].Reason = "retention_keep_latest_n"
			keptByLimit++
			continue
		}
		if decisions[i].Reason == "" || decisions[i].Reason == "missing_source_operation" {
			decisions[i].Reason = "retention_excess"
		}
	}

	sort.SliceStable(decisions, func(i, j int) bool {
		if decisions[i].Keep != decisions[j].Keep {
			return decisions[i].Keep
		}
		left := DecisionTime(decisions[i])
		right := DecisionTime(decisions[j])
		if !left.Equal(right) {
			return left.After(right)
		}
		return decisions[i].ImageID < decisions[j].ImageID
	})
	for i := range decisions {
		decisions[i].Rank = i + 1
		if decisions[i].Keep {
			plan.KeepImageIDs = append(plan.KeepImageIDs, decisions[i].ImageID)
		} else {
			plan.DropImageIDs = append(plan.DropImageIDs, decisions[i].ImageID)
		}
	}
	plan.ImageDecisions = decisions
	return plan
}

func OperationActive(op model.Operation) bool {
	switch strings.TrimSpace(op.Status) {
	case model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent:
		return true
	default:
		return false
	}
}

func DecisionTime(decision model.ImageRetentionDecision) time.Time {
	if decision.LastDeployedAt != nil {
		return decision.LastDeployedAt.UTC()
	}
	return time.Time{}
}

func KeySetFromRefs(refs map[string]struct{}) map[string]struct{} {
	return keySetFromRefs(refs)
}

func keySetFromRefs(refs map[string]struct{}) map[string]struct{} {
	out := map[string]struct{}{}
	for ref := range refs {
		for _, key := range imagecachekeys.ExactImageReferenceKeys(ref, "") {
			key = strings.ToLower(strings.TrimSpace(key))
			if key != "" {
				out[key] = struct{}{}
			}
		}
	}
	return out
}

func ImageMatchesKeySet(image model.Image, keys map[string]struct{}) bool {
	if len(keys) == 0 {
		return false
	}
	for _, key := range imagecachekeys.ExactImageReferenceKeys(image.ImageRef, image.CanonicalDigest) {
		if _, ok := keys[strings.ToLower(strings.TrimSpace(key))]; ok {
			return true
		}
	}
	return false
}
