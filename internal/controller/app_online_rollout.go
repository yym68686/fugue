package controller

import (
	"context"
	"strings"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

func (s *Service) onlineDurableRolloutScheduling(
	ctx context.Context,
	currentApp, desiredApp model.App,
	base runtimepkg.SchedulingConstraints,
) runtimepkg.SchedulingConstraints {
	if !appUsesOnlineDurableRolloutIntent(desiredApp) {
		return base
	}

	nodeName, ok := s.currentReadyAppNodeForOnlineRollout(ctx, currentApp)
	if !ok {
		return base
	}

	out := base
	out.NodeSelector = clonePlacementStringMap(base.NodeSelector)
	if out.NodeSelector == nil {
		out.NodeSelector = map[string]string{}
	}
	if existing := strings.TrimSpace(out.NodeSelector[kubeHostnameLabelKey]); existing != "" && existing != nodeName {
		if s.Logger != nil {
			s.Logger.Printf(
				"online durable rollout for app=%s kept existing hostname selector %s instead of current ready node %s",
				desiredApp.ID,
				existing,
				nodeName,
			)
		}
		return base
	}
	out.NodeSelector[kubeHostnameLabelKey] = nodeName
	return out
}

func appUsesOnlineDurableRolloutIntent(app model.App) bool {
	if !model.AppHasClusterService(app.Spec) || app.Spec.Replicas <= 0 {
		return false
	}
	if app.Spec.Workspace == nil && !appPersistentStorageRequiresSameNodeOnlineRollout(app.Spec.PersistentStorage) {
		return false
	}
	switch strings.TrimSpace(app.Spec.RolloutIntent) {
	case model.AppRolloutIntentOnlineLifecycleUpdate,
		model.AppRolloutIntentOnlineRestart,
		model.AppRolloutIntentOnlineResourceUpdate:
		return true
	default:
		return false
	}
}

func appPersistentStorageRequiresSameNodeOnlineRollout(spec *model.AppPersistentStorageSpec) bool {
	if spec == nil || len(spec.Mounts) == 0 {
		return false
	}
	if model.AppPersistentStorageSpecUsesSharedProjectRWX(spec) {
		return false
	}
	_, err := model.NormalizeAppPersistentStorageMode(spec.Mode)
	return err == nil
}

func (s *Service) currentReadyAppNodeForOnlineRollout(ctx context.Context, app model.App) (string, bool) {
	client, err := s.kubeClient()
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("resolve online rollout node for app=%s failed to initialize kube client: %v", app.ID, err)
		}
		return "", false
	}

	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	pods, err := client.listPodsBySelector(ctx, namespace, managedAppPodLabelSelector(app))
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("resolve online rollout node for app=%s failed to list pods: %v", app.ID, err)
		}
		return "", false
	}

	nodes := map[string]struct{}{}
	for _, pod := range pods {
		nodeName := strings.TrimSpace(pod.Spec.NodeName)
		if nodeName == "" || strings.TrimSpace(pod.Metadata.DeletionTimestamp) != "" {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "Running") || !kubePodReady(pod) {
			continue
		}
		nodes[nodeName] = struct{}{}
	}
	if len(nodes) != 1 {
		if s.Logger != nil && len(nodes) > 1 {
			s.Logger.Printf("resolve online rollout node for app=%s found ready pods on multiple nodes; keeping runtime scheduling", app.ID)
		}
		return "", false
	}
	for nodeName := range nodes {
		return nodeName, true
	}
	return "", false
}
