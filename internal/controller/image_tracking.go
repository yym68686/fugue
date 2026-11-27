package controller

import (
	"context"
	"net/url"
	"strings"
	"time"

	"fugue/internal/imagetracking"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

func (s *Service) imageTrackingPoller() *imagetracking.Poller {
	var repository imagetracking.PollRepository
	if s.Store != nil {
		repository = s.Store.ImageTracking()
	}
	return imagetracking.NewPoller(imagetracking.PollOptions{
		Repository: repository, Metrics: &s.imageTrackingMetrics, Logger: s.Logger,
		GetApp: s.Store.GetApp, ListReleases: s.Store.ListAppReleaseMetadataContext,
		Interval: s.Config.ImageTrackingInterval, Timeout: s.Config.ImageTrackingTimeout,
		LeaderIdentity: s.Config.LeaderElectionIdentity, LiveApply: s.Config.KubectlApply,
		Now: s.currentTime, ResolveDigest: s.resolveRemoteImageDigest,
		ActiveOperationID: s.latestImageTrackingActiveOperationID, RecentDeploys: s.recentImageTrackingDeploys,
		StartupRetrySuppression: s.candidateStartupRetrySuppression, ListWorkloads: s.listImageTrackingWorkloads,
		IsRetired: store.AppReleaseIsRetired, CandidateRevision: safeRolloutCandidateRevision,
		RecordRelease: s.createImageTrackingReleaseAttemptBestEffort,
		NotFound:      store.ErrNotFound, Conflict: store.ErrConflict,
	})
}

func (s *Service) latestImageTrackingActiveOperationID(app model.App) (string, error) {
	ops, err := s.Store.ListOperationsFiltered(app.TenantID, true, store.OperationListFilter{
		AppID:    app.ID,
		Statuses: []string{model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent},
		Limit:    1,
	})
	if err != nil || len(ops) == 0 {
		return "", err
	}
	return strings.TrimSpace(ops[len(ops)-1].ID), nil
}

func (s *Service) recentImageTrackingDeploys(app model.App) ([]model.Operation, error) {
	return s.Store.ListOperationSummariesFiltered(app.TenantID, true, store.OperationListFilter{
		AppID: app.ID, Types: []string{model.OperationTypeDeploy}, Limit: 8,
	})
}

func (s *Service) listImageTrackingWorkloads(ctx context.Context, tenantID string) (map[string]bool, map[string]bool, error) {
	client, err := s.kubeClient()
	if err != nil {
		return nil, nil, err
	}
	deployments, services := map[string]bool{}, map[string]bool{}
	ns := url.PathEscape(runtime.NamespaceForTenant(tenantID))
	for _, item := range []struct {
		path  string
		names map[string]bool
	}{{"/apis/apps/v1/namespaces/" + ns + "/deployments", deployments}, {"/api/v1/namespaces/" + ns + "/services", services}} {
		var list kubeObjectList
		if _, err := client.doMetadataList(ctx, item.path, &list); err != nil {
			return nil, nil, err
		}
		for _, object := range list.Items {
			item.names[objectStringField(objectMapField(object, "metadata"), "name")] = true
		}
	}
	return deployments, services, nil
}

func (s *Service) currentTime() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}
