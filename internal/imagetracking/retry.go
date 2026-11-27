package imagetracking

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func (s *Poller) RetrySuppression(ctx context.Context, app model.App, tracking model.AppImageTracking, digest string) (string, error) {
	if reason, err := s.startupRetrySuppression(ctx, app, digest); err != nil || reason != "" {
		return reason, err
	}
	if strings.TrimSpace(digest) == strings.TrimSpace(tracking.LastQueuedDigest) {
		ops, err := s.recentDeploys(app)
		if err != nil {
			return "", err
		}
		failures, since := 0, tracking.LastTriggeredAt
		for i := len(ops) - 1; i >= 0; i-- {
			op := ops[i]
			if op.Status != model.OperationStatusFailed || op.RequestedByID != model.OperationRequestedByImageTracking {
				break
			}
			failures++
			if op.CompletedAt != nil && (since == nil || op.CompletedAt.After(*since)) {
				at := *op.CompletedAt
				since = &at
			}
		}
		delay := 5 * time.Minute
		for i := 1; i < failures && delay < time.Hour; i++ {
			delay = min(2*delay, time.Hour)
		}
		if since != nil && s.now().Before(since.Add(delay)) {
			return fmt.Sprintf("digest already queued; retry backoff %s has not elapsed", delay), nil
		}
	}
	// A new digest must not bypass cleanup either. Retired resources whose
	// deletion failed still hold capacity. List live metadata once per kind so
	// a long release history does not become two Kubernetes reads per release.
	releases, err := s.listReleases(ctx, model.AppReleaseFilter{TenantID: app.TenantID, AppID: app.ID, PlatformAdmin: true, IncludeRetired: true})
	if err != nil {
		return "", err
	}
	var deployments, services map[string]bool
	for _, release := range releases {
		failed := release.Role == model.AppReleaseRoleCandidate && release.Status == model.AppReleaseStatusFailed
		if !failed && !s.isRetired(release) {
			continue
		}
		binding := release.RevisionWorkload
		if !s.liveApply {
			if failed && binding != nil {
				return "failed candidate still awaits verified retirement: " + release.ID, nil
			}
			continue
		}
		if deployments == nil {
			deployments, services, err = s.listWorkloads(ctx, app.TenantID)
			if err != nil {
				return "", err
			}
		}
		deployment, service := release.DeploymentName, release.ServiceName
		if binding != nil {
			deployment, service = binding.DeploymentName, binding.ServiceName
		} else {
			// Legacy release rows may point to the shared canonical aliases.
			// Those aliases are intentional serving resources, not evidence of
			// an independent failed/retired revision still holding capacity.
			options := runtime.RenderOptions{Revision: s.candidateRevision(release.ID)}
			if deployment == "" || deployment == runtime.RuntimeAppResourceName(app) {
				deployment = runtime.RuntimeAppResourceNameWithOptions(app, options)
			}
			if service == "" || service == runtime.RuntimeAppServiceName(app) {
				service = runtime.RuntimeAppServiceNameWithOptions(app, options)
			}
		}
		if deployments[deployment] || services[service] {
			return "withdrawn release resources still await cleanup: " + release.ID, nil
		}
	}
	return "", nil
}
