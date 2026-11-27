package appimages

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"

	"golang.org/x/sync/singleflight"
)

const UsageSnapshotTTL = 5 * time.Minute
const UsageSnapshotSoftWait = 50 * time.Millisecond

type ProjectUsageAppSummary struct {
	AppID                string   `json:"app_id"`
	AppName              string   `json:"app_name"`
	VersionCount         int      `json:"version_count"`
	CurrentVersionCount  int      `json:"current_version_count"`
	StaleVersionCount    int      `json:"stale_version_count"`
	TotalSizeBytes       int64    `json:"total_size_bytes"`
	CurrentSizeBytes     int64    `json:"current_size_bytes"`
	StaleSizeBytes       int64    `json:"stale_size_bytes"`
	ReclaimableSizeBytes int64    `json:"reclaimable_size_bytes"`
	MeasurementStatus    string   `json:"measurement_status,omitempty"`
	MeasurementReasons   []string `json:"measurement_reasons,omitempty"`
}

type ProjectUsageSummary struct {
	ProjectID            string                   `json:"project_id"`
	VersionCount         int                      `json:"version_count"`
	CurrentVersionCount  int                      `json:"current_version_count"`
	StaleVersionCount    int                      `json:"stale_version_count"`
	TotalSizeBytes       int64                    `json:"total_size_bytes"`
	CurrentSizeBytes     int64                    `json:"current_size_bytes"`
	StaleSizeBytes       int64                    `json:"stale_size_bytes"`
	ReclaimableSizeBytes int64                    `json:"reclaimable_size_bytes"`
	Apps                 []ProjectUsageAppSummary `json:"apps"`
	MeasurementStatus    string                   `json:"measurement_status,omitempty"`
	MeasurementReasons   []string                 `json:"measurement_reasons,omitempty"`
}

type ProjectUsageResponse struct {
	RegistryConfigured bool                  `json:"registry_configured"`
	ReclaimRequiresGC  bool                  `json:"reclaim_requires_gc"`
	ReclaimNote        string                `json:"reclaim_note,omitempty"`
	ImageStoreMode     string                `json:"image_store_mode,omitempty"`
	MeasurementStatus  string                `json:"measurement_status,omitempty"`
	MeasurementNote    string                `json:"measurement_note,omitempty"`
	MeasurementReasons []string              `json:"measurement_reasons,omitempty"`
	ObservedAt         *time.Time            `json:"observed_at,omitempty"`
	Projects           []ProjectUsageSummary `json:"projects"`
}

func cloneUsageResponse(value ProjectUsageResponse) ProjectUsageResponse {
	value.MeasurementReasons = slices.Clone(value.MeasurementReasons)
	if value.ObservedAt != nil {
		at := *value.ObservedAt
		value.ObservedAt = &at
	}
	value.Projects = slices.Clone(value.Projects)
	for i := range value.Projects {
		project := &value.Projects[i]
		project.MeasurementReasons = slices.Clone(project.MeasurementReasons)
		project.Apps = slices.Clone(project.Apps)
		for j := range project.Apps {
			app := &project.Apps[j]
			app.MeasurementReasons = slices.Clone(app.MeasurementReasons)
		}
	}
	return value
}

// UsageScope is a cache partition, not an authorization credential. The
// caller must load observations using the same authorized principal.
type UsageScope struct {
	key string
}

func ScopeForPrincipal(principal model.Principal) UsageScope {
	tenantID := strings.TrimSpace(principal.TenantID)
	if principal.IsPlatformAdmin() {
		if tenantID == "" {
			return UsageScope{key: "platform"}
		}
		return UsageScope{key: "platform:" + tenantID}
	}
	return UsageScope{key: "tenant:" + tenantID}
}

type usageSnapshot struct {
	value       ProjectUsageResponse
	publishedAt time.Time
	expiresAt   time.Time
}

// UsageSnapshots owns freshness, coalescing, background refresh and the
// cached values. Neither readers nor publishers receive mutable cache state.
type UsageSnapshots struct {
	mu    sync.RWMutex
	byKey map[UsageScope]usageSnapshot
	group singleflight.Group
	now   func() time.Time
	ttl   time.Duration
}

func NewUsageSnapshots(ttl time.Duration, now func() time.Time) *UsageSnapshots {
	if now == nil {
		now = time.Now
	}
	return &UsageSnapshots{ttl: ttl, now: now, byKey: make(map[UsageScope]usageSnapshot)}
}

func (c *UsageSnapshots) entry(scope UsageScope) (usageSnapshot, bool) {
	if c == nil {
		return usageSnapshot{}, false
	}
	c.mu.RLock()
	entry, ok := c.byKey[scope]
	c.mu.RUnlock()
	return entry, ok
}

func (c *UsageSnapshots) Fresh(scope UsageScope) (ProjectUsageResponse, bool) {
	entry, ok := c.entry(scope)
	if !ok || c.now().After(entry.expiresAt) {
		return ProjectUsageResponse{}, false
	}
	return cloneUsageResponse(entry.value), true
}

// Publish records one complete observation. A background producer may protect
// any foreground snapshot published after it began by passing started.
func (c *UsageSnapshots) Publish(scope UsageScope, value ProjectUsageResponse, started time.Time) {
	if c == nil || scope.key == "" || c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, ok := c.byKey[scope]; ok && !started.IsZero() && current.publishedAt.After(started) {
		return
	}
	now := c.now()
	c.byKey[scope] = usageSnapshot{value: cloneUsageResponse(value), publishedAt: now, expiresAt: now.Add(c.ttl)}
}

func (c *UsageSnapshots) refresh(ctx context.Context, scope UsageScope, load func(context.Context) (ProjectUsageResponse, error)) (ProjectUsageResponse, error) {
	if value, ok := c.Fresh(scope); ok {
		return value, nil
	}
	raw, err, _ := c.group.Do(scope.key, func() (any, error) {
		if value, ok := c.Fresh(scope); ok {
			return value, nil
		}
		value, err := load(ctx)
		if err != nil {
			return ProjectUsageResponse{}, err
		}
		c.Publish(scope, value, time.Time{})
		return value, nil
	})
	if err != nil {
		return ProjectUsageResponse{}, err
	}
	return cloneUsageResponse(raw.(ProjectUsageResponse)), nil
}

type usageLoadResult struct {
	value ProjectUsageResponse
	err   error
}

// Load waits for a truthful first snapshot; only an expired observation can
// be returned on the soft deadline. Preserve context values (including
// timing), but do not cancel a shared refresh when one request leaves.
func (c *UsageSnapshots) Load(ctx context.Context, scope UsageScope, load func(context.Context) (ProjectUsageResponse, error), onBackgroundError func(error)) (ProjectUsageResponse, error) {
	if c == nil {
		c = NewUsageSnapshots(0, nil)
	}
	if value, ok := c.Fresh(scope); ok {
		return value, nil
	}
	stale, hasStale := c.entry(scope)
	results := make(chan usageLoadResult, 1)
	refreshCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
	go func() {
		defer cancel()
		value, err := c.refresh(refreshCtx, scope, load)
		results <- usageLoadResult{value, err}
	}()
	if !hasStale {
		select {
		case result := <-results:
			return result.value, result.err
		case <-ctx.Done():
			return ProjectUsageResponse{}, ctx.Err()
		}
	}
	timer := time.NewTimer(UsageSnapshotSoftWait)
	defer timer.Stop()
	select {
	case result := <-results:
		return result.value, result.err
	case <-ctx.Done():
		return ProjectUsageResponse{}, ctx.Err()
	case <-timer.C:
		go func() {
			if result := <-results; result.err != nil && onBackgroundError != nil {
				onBackgroundError(result.err)
			}
		}()
		return cloneUsageResponse(stale.value), nil
	}
}
