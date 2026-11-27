package appimages

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestUsageSnapshotsDetachValuesAndPartitionVisibility(t *testing.T) {
	now := time.Now()
	cache := NewUsageSnapshots(time.Minute, func() time.Time { return now })
	tenant := ScopeForPrincipal(model.Principal{TenantID: "one"})
	other := ScopeForPrincipal(model.Principal{TenantID: "two"})
	admin := ScopeForPrincipal(model.Principal{TenantID: "one", Scopes: map[string]struct{}{"platform.admin": {}}})
	observed := now.Add(-time.Hour)
	value := ProjectUsageResponse{ObservedAt: &observed, MeasurementReasons: []string{"top"}, Projects: []ProjectUsageSummary{{
		ProjectID: "p", MeasurementReasons: []string{"project"},
		Apps: []ProjectUsageAppSummary{{AppID: "a", MeasurementReasons: []string{"app"}}},
	}}}
	want := cloneUsageResponse(value)
	cache.Publish(tenant, value, time.Time{})
	value.ObservedAt = nil
	value.MeasurementReasons[0] = "changed"
	value.Projects[0].Apps[0].MeasurementReasons[0] = "changed"
	got, ok := cache.Fresh(tenant)
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("published value was changed: %+v", got)
	}
	*got.ObservedAt = now
	got.Projects[0].MeasurementReasons[0] = "changed"
	got.Projects[0].Apps[0].AppID = "changed"
	if got, ok = cache.Fresh(tenant); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("read changed cached value: %+v", got)
	}
	for _, scope := range []UsageScope{other, admin} {
		if _, ok := cache.Fresh(scope); ok {
			t.Fatal("observation leaked across visibility partitions")
		}
	}
	// The older background producer must not extend the newer snapshot's TTL.
	started := now.Add(-time.Second)
	now = now.Add(30 * time.Second)
	cache.Publish(tenant, ProjectUsageResponse{}, started)
	if got, ok = cache.Fresh(tenant); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("background overwrote foreground value: %+v", got)
	}
	now = now.Add(31 * time.Second)
	if _, ok := cache.Fresh(tenant); ok {
		t.Fatal("background publication extended freshness")
	}
}

func TestUsageSnapshotsColdRefreshCoalescesAndRetainsContext(t *testing.T) {
	cache := NewUsageSnapshots(time.Minute, nil)
	scope := ScopeForPrincipal(model.Principal{TenantID: "tenant"})
	type timingKey struct{}
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	loader := func(ctx context.Context) (ProjectUsageResponse, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		if ctx.Value(timingKey{}) != "timing" {
			return ProjectUsageResponse{}, errors.New("request context values lost")
		}
		<-release
		return ProjectUsageResponse{RegistryConfigured: true}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.WithValue(t.Context(), timingKey{}, "timing"))
	defer cancel()
	first := make(chan usageLoadResult, 1)
	go func() {
		value, err := cache.Load(ctx, scope, loader, nil)
		first <- usageLoadResult{value, err}
	}()
	<-entered
	select {
	case <-first:
		t.Fatal("cold request returned before observation")
	case <-time.After(2 * UsageSnapshotSoftWait):
	}
	cancel()
	if result := <-first; !errors.Is(result.err, context.Canceled) {
		t.Fatalf("cancelled request returned %+v", result)
	}
	second := make(chan usageLoadResult, 1)
	go func() {
		value, err := cache.Load(context.WithValue(t.Context(), timingKey{}, "timing"), scope, loader, nil)
		second <- usageLoadResult{value, err}
	}()
	close(release)
	select {
	case result := <-second:
		if result.err != nil || !result.value.RegistryConfigured || calls.Load() != 1 {
			t.Fatalf("shared refresh result = %+v, calls = %d", result, calls.Load())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shared refresh did not finish")
	}
}

func TestUsageSnapshotsStaleResponseAndBackgroundFailure(t *testing.T) {
	var clock atomic.Int64
	clock.Store(time.Now().UnixNano())
	cache := NewUsageSnapshots(time.Minute, func() time.Time { return time.Unix(0, clock.Load()) })
	scope := ScopeForPrincipal(model.Principal{TenantID: "tenant"})
	stale := ProjectUsageResponse{Projects: []ProjectUsageSummary{{ProjectID: "last observed"}}}
	cache.Publish(scope, stale, time.Time{})
	clock.Add(int64(2 * time.Minute))
	release := make(chan struct{})
	wantErr := errors.New("refresh failed")
	background := make(chan error, 1)
	value, err := cache.Load(t.Context(), scope, func(context.Context) (ProjectUsageResponse, error) {
		<-release
		return ProjectUsageResponse{}, wantErr
	}, func(err error) { background <- err })
	close(release)
	if err != nil || !reflect.DeepEqual(value, stale) {
		t.Fatalf("stale value = %+v, %v", value, err)
	}
	select {
	case err := <-background:
		if !errors.Is(err, wantErr) {
			t.Fatalf("background error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("background error was lost")
	}
	if _, ok := cache.Fresh(scope); ok {
		t.Fatal("failed refresh published a value or extended freshness")
	}
}
