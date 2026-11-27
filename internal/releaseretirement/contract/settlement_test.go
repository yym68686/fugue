package contract

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestSettleDrainedRequiresMatchingReleaseAndTrafficFacts(t *testing.T) {
	now := time.Unix(60, 123456789).UTC()
	current := Snapshot{
		Previous: model.AppRelease{ID: "previous", AppID: "app", TenantID: "tenant", Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusDraining},
		Stable:   model.AppRelease{ID: "stable", AppID: "app", TenantID: "tenant", Role: model.AppReleaseRoleStable, Status: model.AppReleaseStatusServing},
		Traffic:  model.AppTrafficPolicy{AppID: "app", TenantID: "tenant", StableReleaseID: "stable", Mode: model.AppTrafficModeSingle, StableWeight: 100},
	}
	if retired, ok := SettleDrained(current, current, now); !ok || retired.Role != model.AppReleaseRoleRetired ||
		retired.Status != model.AppReleaseStatusRetired || retired.RetiredAt == nil ||
		!retired.RetiredAt.Equal(now.Truncate(time.Microsecond)) || current.Previous.Status != model.AppReleaseStatusDraining {
		t.Fatalf("invalid settlement: %+v, accepted=%v", retired, ok)
	}

	stale := current
	stale.Previous.UpdatedAt = now
	if _, ok := SettleDrained(current, stale, now); ok {
		t.Fatal("stale release snapshot was accepted")
	}
	wrongTenant := current
	wrongTenant.Stable.TenantID = "other"
	if _, ok := SettleDrained(wrongTenant, wrongTenant, now); ok {
		t.Fatal("cross-tenant stable release was accepted")
	}
	referenced := current
	referenced.Traffic.CandidateReleaseID = current.Previous.ID
	if _, ok := SettleDrained(referenced, referenced, now); ok {
		t.Fatal("traffic still referenced the retiring release")
	}
	retained := current
	next := now.Add(time.Minute)
	retained.Previous.RetentionUntil = &next
	if _, ok := SettleDrained(retained, retained, now); ok {
		t.Fatal("retention period had not elapsed")
	}
}
