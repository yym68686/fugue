package controller

import (
	"sort"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

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
	position appReleaseRetirementPosition
	usedAt   time.Time
	running  bool
}

type appReleaseRetirementScans struct {
	mu   sync.Mutex
	apps map[appReleaseRetirementScanKey]*appReleaseRetirementScan
}

// One background scan per app, with bounded LRU progress storage. Eviction or
// leader failover only restarts ordering; every gate is always checked afresh.
func (scans *appReleaseRetirementScans) begin(app model.App) (appReleaseRetirementPosition, func(appReleaseRetirementPosition), bool) {
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
				return appReleaseRetirementPosition{}, nil, false
			}
			delete(scans.apps, oldestKey)
		}
		scan = &appReleaseRetirementScan{}
		scans.apps[key] = scan
	}
	if scan.running {
		return appReleaseRetirementPosition{}, nil, false
	}
	scan.running, scan.usedAt = true, time.Now()
	return scan.position, func(position appReleaseRetirementPosition) {
		scans.mu.Lock()
		defer scans.mu.Unlock()
		scan.position, scan.usedAt, scan.running = position, time.Now(), false
	}, true
}
