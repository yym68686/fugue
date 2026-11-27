package releaseretirement

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestSweepOnlyBacksOffAttemptsWithoutProgress(t *testing.T) {
	var s Scheduler
	app := model.App{TenantID: "tenant", ID: "app"}
	at := time.Now()
	intent := IntentVersion(app, model.AppRelease{}, model.AppTrafficPolicy{})
	progressed := model.AppRelease{ID: "progressed", Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusDraining}
	unavailable := progressed
	unavailable.ID = "unavailable"
	sweep, ok := s.Begin(app)
	if !ok {
		t.Fatal("could not acquire scan")
	}
	if _, err := sweep.Run(t.Context(), intent, []model.AppRelease{progressed, unavailable}, nil, func() time.Time { return at }, func(ctx context.Context, release model.AppRelease) {
		if release.ID == progressed.ID {
			NoteProgress(ctx)
		}
	}); err != nil {
		t.Fatal(err)
	}
	sweep.Close()
	sweep, ok = s.Begin(app)
	if !ok {
		t.Fatal("could not reacquire scan")
	}
	defer sweep.Close()
	if !sweep.Due(intent, progressed, at) || sweep.Due(intent, unavailable, at) {
		t.Fatal("retry delay did not distinguish positive progress from an unavailable release")
	}
	other, ok := s.Begin(model.App{TenantID: "other-tenant", ID: app.ID})
	if !ok {
		t.Fatal("could not acquire unrelated scan")
	}
	defer other.Close()
	if !other.Due(intent, unavailable, at) {
		t.Fatal("backoff crossed tenant boundary")
	}
}

func TestAppReleaseRetirementScanOrderResumesAcrossQueueChanges(t *testing.T) {
	now := time.Now()
	previous := model.AppRelease{ID: "previous", CreatedAt: now, Role: model.AppReleaseRolePrevious, Status: model.AppReleaseStatusDraining}
	failed := model.AppRelease{ID: "failed", CreatedAt: now.Add(time.Hour), Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusFailed}
	retired := model.AppRelease{ID: "retired", CreatedAt: now.Add(-time.Hour), Status: model.AppReleaseStatusRetired, RevisionWorkload: &model.AppReleaseWorkload{}}
	stable := model.AppRelease{ID: "stable", CreatedAt: now.Add(-2 * time.Hour), Role: model.AppReleaseRoleStable, Status: model.AppReleaseStatusServing}
	ids := func(releases []model.AppRelease) []string {
		var out []string
		for _, release := range releases {
			out = append(out, release.ID)
		}
		return out
	}
	queue := []model.AppRelease{failed, stable, previous, retired}
	for _, tc := range []struct {
		name     string
		queue    []model.AppRelease
		after    position
		expected []string
	}{
		{"fresh_leader_oldest_first", queue, position{}, []string{"retired", "previous", "failed"}},
		{"resume_after_unavailable", queue, positionOf(previous), []string{"failed", "retired", "previous"}},
		{"removed_cursor_and_new_arrival", []model.AppRelease{failed, stable, retired}, positionOf(previous), []string{"failed", "retired"}},
		{"wrap_after_last", queue, positionOf(failed), []string{"retired", "previous", "failed"}},
		{"empty", nil, positionOf(failed), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ids(order(tc.queue, tc.after)); !reflect.DeepEqual(got, tc.expected) {
				t.Fatalf("queue=%v, want %v", got, tc.expected)
			}
		})
	}
	// Identical creation timestamps use a deterministic, role-independent key.
	failed.CreatedAt = previous.CreatedAt
	if got := ids(order([]model.AppRelease{previous, failed}, positionOf(failed))); !reflect.DeepEqual(got, []string{"previous", "failed"}) {
		t.Fatal("equal-time cursor did not progress", got)
	}
	if queue[0].ID != failed.ID || queue[1].ID != stable.ID {
		t.Fatal("ordering mutated the Store projection")
	}
}

func TestAppReleaseRetirementScansSerializePerTenantAppAndBoundMemory(t *testing.T) {
	var scans Scheduler
	app := model.App{TenantID: "tenant-a", ID: "app-a"}
	sweep, acquired := scans.Begin(app)
	if !acquired {
		t.Fatal("initial scan not acquired")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if duplicate, ok := scans.Begin(app); ok {
				duplicate.Close()
				t.Error("concurrent scan for the same tenant/app")
			}
		}()
	}
	wg.Wait()
	for _, other := range []model.App{{TenantID: "tenant-b", ID: app.ID}, {TenantID: app.TenantID, ID: "app-b"}} {
		otherSweep, ok := scans.Begin(other)
		if !ok || otherSweep.positions[0].releaseID != "" {
			t.Fatal("unrelated tenant/app scan interfered")
		}
		otherSweep.Close()
	}
	last := position{time.Now(), "last-attempted"}
	sweep.positions[0] = last
	sweep.Close()
	sweep, acquired = scans.Begin(app)
	if !acquired || sweep.positions[0] != last {
		t.Fatal("scan lost progress")
	}
	for i := 0; i < 2*maxScans; i++ {
		other, ok := scans.Begin(model.App{TenantID: app.TenantID, ID: fmt.Sprintf("other-%d", i)})
		if !ok {
			t.Fatal("idle progress entries prevented scheduling")
		}
		other.Close()
	}
	if len(scans.apps) != maxScans {
		t.Fatal("unbounded progress cache", len(scans.apps))
	}
	if conflict, ok := scans.Begin(app); ok {
		conflict.Close()
		t.Fatal("active scan was evicted")
	}
	sweep.Close()
	sweep, acquired = scans.Begin(app)
	if !acquired || sweep.positions[0] != last {
		t.Fatal("bounded cache lost active scan progress")
	}
	sweep.Close()
}

func TestRetirementPriorityKeepsIndependentFairResumePositions(t *testing.T) {
	var releases []model.AppRelease
	for i := 0; i < 12; i++ {
		r := model.AppRelease{ID: fmt.Sprint(i), CreatedAt: time.Unix(int64(i), 0), Role: model.AppReleaseRoleCandidate, Status: model.AppReleaseStatusFailed, RevisionWorkload: &model.AppReleaseWorkload{}}
		releases = append(releases, r)
	}
	scheduled := func(r model.AppRelease) bool { return len(r.ID) == 1 && r.ID[0] < '8' }
	queue := prioritize(releases, [3]position{}, scheduled)
	if len(queue) != 12 {
		t.Fatal("lost retirement candidates")
	}
	for i, want := range []string{"0", "1", "2", "3", "8", "4", "5", "6", "7", "9", "10", "11"} {
		if queue[i].release.ID != want {
			t.Fatalf("position %d: %s want %s", i, queue[i].release.ID, want)
		}
	}
	var progress [3]position
	progress[0], progress[1] = positionOf(releases[3]), positionOf(releases[8])
	next := prioritize(releases, progress, scheduled)
	if next[0].release.ID != "4" || next[4].release.ID != "9" {
		t.Fatal("one priority class reset another class's progress")
	}
}

func TestRetirementQueueRunsOneHundredAttemptsAndJoinsOnCancellation(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelEarly), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var queue []item
			for i := 0; i < 130; i++ {
				queue = append(queue, item{release: model.AppRelease{ID: fmt.Sprint(i)}})
			}
			started := make(chan struct{}, 130)
			gate := make(chan struct{})
			done := make(chan error, 1)
			var active, peak, completed atomic.Int32
			var positions [3]position
			go func() {
				done <- runQueue(ctx, queue, &positions, func(ctx context.Context, _ item) {
					n := active.Add(1)
					for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
					}
					started <- struct{}{}
					select {
					case <-gate:
					case <-ctx.Done():
					}
					active.Add(-1)
					completed.Add(1)
				})
			}()
			timer := time.NewTimer(3 * time.Second)
			defer timer.Stop()
			for i := 0; i < 100; i++ {
				select {
				case <-started:
				case <-timer.C:
					t.Fatal("did not launch requested 100 concurrent attempts")
				}
			}
			if active.Load() != 100 || peak.Load() != 100 {
				t.Fatal("incorrect concurrency", active.Load(), peak.Load())
			}
			if cancelEarly {
				cancel()
			} else {
				close(gate)
			}
			select {
			case err := <-done:
				if !cancelEarly && err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("worker did not join outstanding attempts")
			}
			if active.Load() != 0 || peak.Load() > 100 || !cancelEarly && completed.Load() != 130 {
				t.Fatal("leaked or dropped retirement attempts")
			}
		})
	}
}

func TestRetirementBackoffBoundsAndStateChange(t *testing.T) {
	var retries retirementRetries
	now := time.Now()
	key := retirementRetryKey{"tenant", "app", "release"}
	app := model.App{Spec: model.AppSpec{Image: "image:old"}}
	release := model.AppRelease{ID: "release", UpdatedAt: now}
	intent := IntentVersion(app, model.AppRelease{}, model.AppTrafficPolicy{})
	version := releaseVersion(intent, release)
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
		if !retries.ready(key, releaseVersion(intent, changed), now) {
			t.Fatal("release progress hidden")
		}
		app.Spec.Image = "image:new"
		changedIntent := IntentVersion(app, model.AppRelease{}, model.AppTrafficPolicy{})
		if !retries.ready(key, releaseVersion(changedIntent, release), now) {
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
