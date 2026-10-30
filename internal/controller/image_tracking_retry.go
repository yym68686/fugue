package controller

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func (s *Service) imageTrackingRetrySuppression(ctx context.Context, app model.App, tracking model.AppImageTracking, digest string) (string, error) {
	if strings.TrimSpace(digest) == strings.TrimSpace(tracking.LastQueuedDigest) {
		ops, err := s.Store.ListOperationSummariesFiltered(app.TenantID, true, store.OperationListFilter{AppID: app.ID, Types: []string{model.OperationTypeDeploy}, Limit: 8})
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
		if since != nil && s.currentTime().Before(since.Add(delay)) {
			return fmt.Sprintf("digest already queued; retry backoff %s has not elapsed", delay), nil
		}
	}
	// A new digest must not bypass cleanup either. Retired resources whose
	// deletion failed still hold capacity. List live metadata once per kind so
	// a long release history does not become two Kubernetes reads per release.
	releases, err := s.Store.ListAppReleaseMetadataContext(ctx, model.AppReleaseFilter{TenantID: app.TenantID, AppID: app.ID, PlatformAdmin: true, IncludeRetired: true})
	if err != nil {
		return "", err
	}
	var deployments, services map[string]bool
	for _, release := range releases {
		failed := release.Role == model.AppReleaseRoleCandidate && release.Status == model.AppReleaseStatusFailed
		if !failed && !store.AppReleaseIsRetired(release) {
			continue
		}
		binding := release.RevisionWorkload
		if !s.Config.KubectlApply {
			if failed && binding != nil {
				return "failed candidate still awaits verified retirement: " + release.ID, nil
			}
			continue
		}
		if deployments == nil {
			client, err := s.kubeClient()
			if err != nil {
				return "", err
			}
			deployments, services = map[string]bool{}, map[string]bool{}
			ns := url.PathEscape(runtime.NamespaceForTenant(app.TenantID))
			for _, item := range []struct {
				path  string
				names map[string]bool
			}{{"/apis/apps/v1/namespaces/" + ns + "/deployments", deployments}, {"/api/v1/namespaces/" + ns + "/services", services}} {
				var list kubeObjectList
				if _, err := client.doMetadataList(ctx, item.path, &list); err != nil {
					return "", err
				}
				for _, object := range list.Items {
					item.names[objectStringField(objectMapField(object, "metadata"), "name")] = true
				}
			}
		}
		deployment, service := release.DeploymentName, release.ServiceName
		if binding != nil {
			deployment, service = binding.DeploymentName, binding.ServiceName
		} else {
			// Legacy release rows may point to the shared canonical aliases.
			// Those aliases are intentional serving resources, not evidence of
			// an independent failed/retired revision still holding capacity.
			options := runtime.RenderOptions{Revision: safeRolloutCandidateRevision(release.ID)}
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
