package controller

import (
	"context"
	"sort"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

func runAppReleaseRetirementQueue(ctx context.Context, queue []prioritizedReleaseRetirement, positions *[3]appReleaseRetirementPosition, attempt func(context.Context, prioritizedReleaseRetirement)) error {
	var mu sync.Mutex
	next := 0
	var wg sync.WaitGroup
	for worker := 0; worker < min(appReleaseRetirementConcurrency, len(queue)); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if next == len(queue) || ctx.Err() != nil {
					mu.Unlock()
					return
				}
				item := queue[next]
				next++
				positions[item.priority] = appReleaseRetirementPositionOf(item.release)
				mu.Unlock()
				itemCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
				attempt(itemCtx, item)
				cancel()
			}
		}()
	}
	wg.Wait()
	return ctx.Err()
}

// Scheduling progress is disposable runtime state, not retirement evidence or
// serving intent. Oldest-first also makes a fresh leader useful immediately;
// a cursor prevents an unavailable old release from starving later entries.
type appReleaseRetirementPosition struct {
	createdAt time.Time
	releaseID string
}

func appReleaseRetirementPositionOf(release model.AppRelease) appReleaseRetirementPosition {
	return appReleaseRetirementPosition{release.CreatedAt, release.ID}
}

func (p appReleaseRetirementPosition) before(other appReleaseRetirementPosition) bool {
	return p.createdAt.Before(other.createdAt) || p.createdAt.Equal(other.createdAt) && p.releaseID < other.releaseID
}

func orderAppReleaseRetirementScan(releases []model.AppRelease, after appReleaseRetirementPosition) []model.AppRelease {
	queue := make([]model.AppRelease, 0, len(releases))
	for _, release := range releases {
		if store.AppReleaseAwaitingDrain(release) || release.Status == model.AppReleaseStatusRetired && release.RevisionWorkload != nil {
			queue = append(queue, release)
		}
	}
	sort.Slice(queue, func(i, j int) bool {
		return appReleaseRetirementPositionOf(queue[i]).before(appReleaseRetirementPositionOf(queue[j]))
	})
	// Seek by immutable key rather than offset or role. Removed/retired cursor
	// rows and newly arriving failed candidates cannot restart the scan.
	start := sort.Search(len(queue), func(i int) bool { return after.before(appReleaseRetirementPositionOf(queue[i])) })
	ordered := make([]model.AppRelease, 0, len(queue))
	ordered = append(ordered, queue[start:]...)
	return append(ordered, queue[:start]...)
}

const maxAppReleaseRetirementScans = 1024

type appReleaseRetirementScanKey struct{ tenantID, appID string }

type appReleaseRetirementScan struct {
	positions [3]appReleaseRetirementPosition
	usedAt    time.Time
	running   bool
}

type appReleaseRetirementScans struct {
	mu   sync.Mutex
	apps map[appReleaseRetirementScanKey]*appReleaseRetirementScan
}

// One background scan per app, with bounded LRU progress storage. Eviction or
// leader failover only restarts ordering; every gate is always checked afresh.
func (scans *appReleaseRetirementScans) begin(app model.App) (appReleaseRetirementPosition, func(appReleaseRetirementPosition), bool) {
	positions, finish, ok := scans.beginPrioritized(app)
	if !ok {
		return appReleaseRetirementPosition{}, nil, false
	}
	return positions[0], func(p appReleaseRetirementPosition) { positions[0] = p; finish(positions) }, true
}

func (scans *appReleaseRetirementScans) beginPrioritized(app model.App) ([3]appReleaseRetirementPosition, func([3]appReleaseRetirementPosition), bool) {
	scans.mu.Lock()
	defer scans.mu.Unlock()
	key := appReleaseRetirementScanKey{app.TenantID, app.ID}
	if scans.apps == nil {
		scans.apps = make(map[appReleaseRetirementScanKey]*appReleaseRetirementScan)
	}
	scan := scans.apps[key]
	if scan == nil {
		if len(scans.apps) >= maxAppReleaseRetirementScans {
			var oldestKey appReleaseRetirementScanKey
			var oldest *appReleaseRetirementScan
			for k, entry := range scans.apps {
				if !entry.running && (oldest == nil || entry.usedAt.Before(oldest.usedAt)) {
					oldestKey, oldest = k, entry
				}
			}
			if oldest == nil {
				return [3]appReleaseRetirementPosition{}, nil, false
			}
			delete(scans.apps, oldestKey)
		}
		scan = &appReleaseRetirementScan{}
		scans.apps[key] = scan
	}
	if scan.running {
		return [3]appReleaseRetirementPosition{}, nil, false
	}
	scan.running, scan.usedAt = true, time.Now()
	return scan.positions, func(positions [3]appReleaseRetirementPosition) {
		scans.mu.Lock()
		defer scans.mu.Unlock()
		scan.positions, scan.usedAt, scan.running = positions, time.Now(), false
	}, true
}

type prioritizedReleaseRetirement struct {
	release  model.AppRelease
	priority int
}

// Weight resource recovery 4:1:1, with independent resume positions in each
// class. Pending cancellation and zero-replica garbage collection cannot starve
// even under a stream of newly failed, scheduled workloads.
func prioritizeAppReleaseRetirement(releases []model.AppRelease, positions [3]appReleaseRetirementPosition, inv *releaseDrainInventory) []prioritizedReleaseRetirement {
	var classes [3][]model.AppRelease
	for _, r := range releases {
		if !store.AppReleaseAwaitingDrain(r) && !(r.Status == model.AppReleaseStatusRetired && r.RevisionWorkload != nil) {
			continue
		}
		priority := 1
		if r.Status == model.AppReleaseStatusRetired {
			priority = 2
		}
		if inv != nil && r.RevisionWorkload != nil {
			b := r.RevisionWorkload
			owned := map[string]string{}
			for _, rs := range inv.replicasByDeployment[b.DeploymentUID] {
				m := objectMapField(rs, "metadata")
				owned[objectStringField(m, "name")] = objectStringField(m, "uid")
			}
			for _, pod := range inv.podsForRelease(r.ID, b.DeploymentName, false, owned) {
				if objectStringField(objectMapField(pod, "spec"), "nodeName") != "" {
					priority = 0
					break
				}
			}
		}
		classes[priority] = append(classes[priority], r)
	}
	for i := range classes {
		classes[i] = orderAppReleaseRetirementScan(classes[i], positions[i])
	}
	var next [3]int
	var result []prioritizedReleaseRetirement
	for len(result) < len(classes[0])+len(classes[1])+len(classes[2]) {
		for _, priority := range []int{0, 0, 0, 0, 1, 2} {
			if next[priority] < len(classes[priority]) {
				result = append(result, prioritizedReleaseRetirement{classes[priority][next[priority]], priority})
				next[priority]++
			}
		}
	}
	return result
}
