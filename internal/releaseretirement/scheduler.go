// Package releaseretirement schedules attempts to retire old app releases. It
// cannot grant retirement authority or modify Store or runtime state.
package releaseretirement

import (
	"context"
	"sort"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseretirement/contract"
)

const (
	concurrency = 100
	maxScans    = 1024
)

// Scheduler owns disposable coordination state. Losing this state cannot be
// used as evidence for retirement. Its zero value is ready for use.
type Scheduler struct {
	mu      sync.Mutex
	apps    map[appKey]*scan
	retries retirementRetries
}

type appKey struct{ tenant, app string }

type scan struct {
	positions [3]position
	usedAt    time.Time
	running   bool
}

// Sweep reserves scheduling for one tenant/app. The caller owns the sweep and
// must close it after the run, including when initial discovery fails. Run and
// Close are called serially by that owner.
type Sweep struct {
	scheduler *Scheduler
	key       appKey
	entry     *scan
	positions [3]position
}

func (s *Scheduler) Begin(app model.App) (*Sweep, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := appKey{app.TenantID, app.ID}
	if s.apps == nil {
		s.apps = make(map[appKey]*scan)
	}
	entry := s.apps[key]
	if entry == nil {
		if len(s.apps) >= maxScans {
			var oldestKey appKey
			var oldest *scan
			for k, candidate := range s.apps {
				if !candidate.running && (oldest == nil || candidate.usedAt.Before(oldest.usedAt)) {
					oldestKey, oldest = k, candidate
				}
			}
			if oldest == nil {
				return nil, false
			}
			delete(s.apps, oldestKey)
		}
		entry = &scan{}
		s.apps[key] = entry
	}
	if entry.running {
		return nil, false
	}
	entry.running, entry.usedAt = true, time.Now()
	return &Sweep{scheduler: s, key: key, entry: entry, positions: entry.positions}, true
}

func (s *Sweep) Close() {
	s.scheduler.mu.Lock()
	defer s.scheduler.mu.Unlock()
	if s.entry != nil {
		s.entry.positions, s.entry.usedAt, s.entry.running = s.positions, time.Now(), false
		s.entry = nil
	}
}

type Stats struct{ Queued, Concurrency int }

// Run joins all started attempts, even on cancellation. The scheduled hint
// only affects ordering; the attempt must obtain its own fresh proof.
func (s *Sweep) Run(ctx context.Context, intent Version, releases []model.AppRelease, scheduled func(model.AppRelease) bool, now func() time.Time, attempt func(context.Context, model.AppRelease)) (Stats, error) {
	queue := prioritize(releases, s.positions, scheduled)
	stats := Stats{Queued: len(queue), Concurrency: min(concurrency, len(queue))}
	err := runQueue(ctx, queue, &s.positions, func(ctxAttempt context.Context, next item) {
		progress := &retirementProgress{}
		ctxAttempt = context.WithValue(ctxAttempt, retirementProgressKey{}, progress)
		attempt(ctxAttempt, next.release)
		if ctx.Err() == nil && !progress.Load() {
			s.Defer(intent, next.release, now(), next.release.RevisionWorkload != nil)
		}
	})
	return stats, err
}

type position struct {
	createdAt time.Time
	releaseID string
}

func positionOf(release model.AppRelease) position {
	return position{release.CreatedAt, release.ID}
}

func (p position) before(other position) bool {
	return p.createdAt.Before(other.createdAt) || p.createdAt.Equal(other.createdAt) && p.releaseID < other.releaseID
}

func order(releases []model.AppRelease, after position) []model.AppRelease {
	queue := make([]model.AppRelease, 0, len(releases))
	for _, release := range releases {
		if contract.Candidate(release) {
			queue = append(queue, release)
		}
	}
	sort.Slice(queue, func(i, j int) bool { return positionOf(queue[i]).before(positionOf(queue[j])) })
	// Seek by immutable key even after deleting or changing the cursor row.
	start := sort.Search(len(queue), func(i int) bool { return after.before(positionOf(queue[i])) })
	ordered := make([]model.AppRelease, 0, len(queue))
	ordered = append(ordered, queue[start:]...)
	return append(ordered, queue[:start]...)
}

type item struct {
	release  model.AppRelease
	priority int
}

func prioritize(releases []model.AppRelease, positions [3]position, scheduled func(model.AppRelease) bool) []item {
	var classes [3][]model.AppRelease
	for _, r := range releases {
		if !contract.Candidate(r) {
			continue
		}
		priority := 1
		if r.Status == model.AppReleaseStatusRetired {
			priority = 2
		}
		if scheduled != nil && r.RevisionWorkload != nil && scheduled(r) {
			priority = 0
		}
		classes[priority] = append(classes[priority], r)
	}
	for i := range classes {
		classes[i] = order(classes[i], positions[i])
	}
	// Weight resource recovery 4:1:1 and preserve a cursor per class so
	// pending cancellation and zero-replica cleanup cannot starve.
	var next [3]int
	var result []item
	for len(result) < len(classes[0])+len(classes[1])+len(classes[2]) {
		for _, priority := range []int{0, 0, 0, 0, 1, 2} {
			if next[priority] < len(classes[priority]) {
				result = append(result, item{classes[priority][next[priority]], priority})
				next[priority]++
			}
		}
	}
	return result
}

func runQueue(ctx context.Context, queue []item, positions *[3]position, attempt func(context.Context, item)) error {
	var mu sync.Mutex
	next := 0
	var wg sync.WaitGroup
	for worker := 0; worker < min(concurrency, len(queue)); worker++ {
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
				positions[item.priority] = positionOf(item.release)
				mu.Unlock()
				// Heavy read backpressure is part of the deadline. Leave room
				// for fresh traffic and drain proof within the outer 60s sweep.
				itemCtx, cancel := context.WithTimeout(ctx, 50*time.Second)
				attempt(itemCtx, item)
				cancel()
			}
		}()
	}
	wg.Wait()
	return ctx.Err()
}
