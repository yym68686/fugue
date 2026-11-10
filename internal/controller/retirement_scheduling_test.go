package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestRetirementUnboundMissingWorkloadBacksOffWithoutRetiring(t *testing.T) {
	s, app, release, fixture := newDrainWorkloadFixtureWithOutcome(t, true, true)
	// This synthetic app has no runtime record; keep its historical placement
	// empty so the test reaches the actual missing Kubernetes workload.
	release.RuntimeID = ""
	release.SpecSnapshot.RuntimeID = ""
	var err error
	release, err = s.Store.UpdateAppRelease(release)
	if err != nil {
		t.Fatal(err)
	}
	var missing, lists atomic.Int64
	fixture.extra = func(w http.ResponseWriter, r *http.Request) bool {
		if strings.Contains(r.URL.Path, "/deployments/") {
			missing.Add(1)
			http.NotFound(w, r)
			return true
		}
		if strings.HasSuffix(r.URL.Path, "/pods") || strings.HasSuffix(r.URL.Path, "/replicasets") {
			lists.Add(1)
		}
		return false
	}
	now := time.Now()
	s.now = func() time.Time { return now }
	run := func() {
		t.Helper()
		if err := s.retryDrainingAppReleaseRetirement(t.Context(), app); err != nil {
			t.Fatal(err)
		}
	}
	run()
	if missing.Load() != 1 {
		t.Fatal("missing workload was not observed", missing.Load())
	}
	before := lists.Load()
	run()
	if missing.Load() != 1 || lists.Load() != before {
		t.Fatal("no-progress retry repeated expensive discovery")
	}
	now = now.Add(11 * time.Second)
	run()
	if missing.Load() != 2 {
		t.Fatal("retry did not resume after deadline")
	}
	// A changed durable release bypasses the scheduling hint immediately.
	release.StatusReason = "updated lifecycle evidence"
	if _, err := s.Store.UpdateAppRelease(release); err != nil {
		t.Fatal(err)
	}
	run()
	if missing.Load() != 3 {
		t.Fatal("state change did not bypass backoff")
	}
	got, err := s.Store.GetAppRelease(app.TenantID, true, release.ID)
	if err != nil || got.Status == model.AppReleaseStatusRetired || fixture.deleteCalls != 0 {
		t.Fatal("missing evidence authorized retirement/deletion", err)
	}
}

func TestRetirementBackoffBoundsAndStateChange(t *testing.T) {
	var retries retirementRetries
	now := time.Now()
	key := retirementRetryKey{"tenant", "app", "release"}
	app := model.App{Spec: model.AppSpec{Image: "image:old"}}
	release := model.AppRelease{ID: "release", UpdatedAt: now}
	intent := retirementIntentVersion(app, model.AppRelease{}, model.AppTrafficPolicy{})
	version := retirementReleaseVersion(intent, release)
	if !retries.ready(key, version, now) {
		t.Fatal("new work suppressed")
	}
	for _, bound := range []bool{false, true} {
		retries = retirementRetries{}
		for i := 0; i < 10; i++ {
			retries.attempted(key, version, now, bound)
			next := retries.entries[key].next
			if retries.ready(key, version, now) || !retries.ready(key, version, next) {
				t.Fatal("retry deadline ignored")
			}
			now = next
		}
		want := 2 * time.Minute
		if bound {
			want = 30 * time.Second
		}
		if retries.entries[key].delay != want {
			t.Fatal("unbounded or incorrect retry interval")
		}
		changed := release
		changed.UpdatedAt = now.Add(time.Second)
		if !retries.ready(key, retirementReleaseVersion(intent, changed), now) {
			t.Fatal("release progress hidden")
		}
		app.Spec.Image = "image:new"
		changedIntent := retirementIntentVersion(app, model.AppRelease{}, model.AppTrafficPolicy{})
		if !retries.ready(key, retirementReleaseVersion(changedIntent, release), now) {
			t.Fatal("new intent hidden")
		}
	}
	for i := 0; i < maxRetirementRetries+10; i++ {
		retries.attempted(retirementRetryKey{"tenant", "app", fmt.Sprint(i)}, version, now, false)
	}
	if len(retries.entries) != maxRetirementRetries {
		t.Fatal("retry memory is unbounded")
	}
}

func TestRetirementNamespaceDiscoverySharedButProofReadsFresh(t *testing.T) {
	calls := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls[r.URL.Path]++
		fmt.Fprint(w, `{"items":[]}`)
	}))
	defer server.Close()
	client := &kubeClient{client: server.Client(), baseURL: server.URL}
	ctx := withRetirementSweepInventory(context.Background())
	for i := 0; i < 3; i++ {
		// Independent apps get independent discovery contexts in the same namespace.
		if _, err := readReleaseDrainInventory(retirementInitialDiscoveryContext(ctx, "sample"), client, "sample"); err != nil {
			t.Fatal(err)
		}
		if _, err := readRetirementResourcePresence(ctx, client, "sample"); err != nil {
			t.Fatal(err)
		}
	}
	for path, count := range calls {
		if count != 1 {
			t.Fatalf("discovery %s repeated %d times", path, count)
		}
	}
	// The sweep context itself must NEVER carry initial discovery authority.
	for i := 0; i < 2; i++ {
		if _, err := readReleaseDrainInventory(ctx, client, "sample"); err != nil {
			t.Fatal(err)
		}
	}
	for path, count := range calls {
		want := 1
		if strings.HasSuffix(path, "/pods") || strings.HasSuffix(path, "/replicasets") {
			want = 3
		}
		if count != want {
			t.Fatalf("proof not fresh: %s count=%d want=%d", path, count, want)
		}
	}
	first := retirementNamespace(ctx, "sample")
	if retirementNamespace(ctx, "other") == first {
		t.Fatal("namespace isolation lost")
	}
	if retirementNamespace(withRetirementSweepInventory(ctx), "sample") == first {
		t.Fatal("discovery survived worker sweep")
	}
	first.at = time.Now().Add(-time.Minute)
	if retirementNamespace(ctx, "sample") == first {
		t.Fatal("expired discovery reused")
	}
	for i := 0; i < 20; i++ {
		retirementNamespace(ctx, fmt.Sprint(i))
	}
	sweep := ctx.Value(retirementSweepInventoryKey{}).(*retirementSweepInventory)
	if len(sweep.namespaces) > 8 {
		t.Fatal("namespace budget exceeded")
	}
}

func TestRetirementDiscoveryNeverCachesIncompleteInventory(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		fmt.Fprint(w, `{"metadata":{"continue":"next"},"items":[]}`)
	}))
	defer server.Close()
	client := &kubeClient{client: server.Client(), baseURL: server.URL}
	ctx := withRetirementSweepInventory(t.Context())
	for i := 0; i < 2; i++ {
		if _, err := readRetirementResourcePresence(ctx, client, "sample"); err == nil {
			t.Fatal("partial absence accepted")
		}
		if _, err := readReleaseDrainInventory(retirementInitialDiscoveryContext(ctx, "sample"), client, "sample"); err == nil {
			t.Fatal("partial ownership accepted")
		}
	}
	if calls != 8 {
		t.Fatal("incomplete discovery cached", calls)
	}
}
