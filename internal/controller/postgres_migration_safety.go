package controller

import (
	"context"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// validateManagedPostgresSwitchoverStage runs before the first Kubernetes
// write. A durable runtime/node pin is configuration intent, not proof of the
// currently serving primary. Never evict a working local-volume instance to
// discover that the destination cannot be prepared.
func (s *Service) validateManagedPostgresSwitchoverStage(
	ctx context.Context, client *kubeClient, app model.App,
	namespace, clusterName string, staged, stable model.AppPostgresSpec,
) error {
	return s.validateManagedPostgresStage(ctx, client, app, namespace, clusterName, &staged, stable, false)
}

func (s *Service) validateManagedPostgresStage(
	ctx context.Context, client *kubeClient, app model.App,
	namespace, clusterName string, staged *model.AppPostgresSpec, stable model.AppPostgresSpec,
	localize bool,
) error {
	cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, clusterName)
	if err != nil {
		return fmt.Errorf("observe postgres source before staging: %w", err)
	}
	if !found || strings.TrimSpace(cluster.Status.CurrentPrimary) == "" {
		return fmt.Errorf("postgres source has no observed primary; refusing to stage a switchover, use explicit database recovery")
	}
	_, ready, detail, err := s.observeManagedPostgresPrimaryReadiness(ctx, client, namespace, clusterName, cluster.Status.CurrentPrimary, stable)
	if err != nil {
		return fmt.Errorf("verify postgres source before staging: %w", err)
	}
	if !ready {
		return fmt.Errorf("postgres source is not serving verified read-write SQL; refusing to stage a switchover: %s", detail)
	}
	if staged == nil || !staged.PrimaryPlacementPendingRebalance {
		return fmt.Errorf("postgres staging requires source PodSpec reconciliation hold")
	}
	if localize {
		// Localization ends with a single destination instance. Retain every
		// existing Pod under the hold and allocate one additional replica on the
		// destination, including when stale topology exceeds stored intent.
		staged.Instances = max(staged.Instances, max(cluster.Spec.Instances, cluster.Status.Instances)+1)
		staged.SynchronousReplicas = 0
	}
	planned, err := s.managedPostgresServicePlacements(ctx, app, clusterName, app.Spec.RuntimeID, *staged)
	if err != nil {
		return fmt.Errorf("resolve postgres staging placements: %w", err)
	}
	if len(planned) == 0 {
		return fmt.Errorf("postgres staging has no explicit placement; source protection cannot be verified")
	}
	pods, err := client.listPodsBySelector(ctx, namespace, fmt.Sprintf(managedPostgresPodSelectorTemplate, clusterName))
	if err != nil {
		return fmt.Errorf("observe existing postgres instances before staging: %w", err)
	}
	for _, pod := range pods {
		if managedPostgresPodFinished(pod) || pod.Metadata.DeletionTimestamp != "" || strings.TrimSpace(pod.Spec.NodeName) == "" {
			continue
		}
		node, found, err := client.getNode(ctx, pod.Spec.NodeName)
		if err != nil {
			return fmt.Errorf("observe postgres instance node %s: %w", pod.Spec.NodeName, err)
		}
		if !found || (!localize && !postgresNodeMatchesAnyPlacement(node.Metadata.Labels, planned)) {
			return fmt.Errorf("postgres staging would exclude existing instance %s on node %s; refusing to mutate the source cluster before a source-preserving replica migration is prepared", pod.Metadata.Name, pod.Spec.NodeName)
		}
	}
	return nil
}

func postgresNodeMatchesAnyPlacement(labels map[string]string, placements []runtime.SchedulingConstraints) bool {
	for _, placement := range placements {
		if len(placement.NodeSelector) == 0 {
			continue
		}
		matches := true
		for key, value := range placement.NodeSelector {
			if labels[key] != value {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

// Restoring a manifest is not restoring service. Readiness must include the
// primary Pod, its read-write service endpoint and a direct writable SQL
// session. A timeout remains explicitly recoverable, never "restored".
func (s *Service) verifyManagedPostgresRollback(ctx context.Context, op model.Operation, app model.App, postgres *model.AppPostgresSpec) error {
	if postgres == nil {
		return fmt.Errorf("rollback lacks a database identity")
	}
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	clusterName := model.NormalizePostgresServiceName(postgres.ServiceName, "")
	cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, clusterName)
	if err != nil {
		return err
	}
	if !found || strings.TrimSpace(cluster.Status.CurrentPrimary) == "" {
		return fmt.Errorf("restored configuration has no observed primary")
	}
	_, err = s.waitForManagedPostgresPrimary(ctx, client, namespace, clusterName, cluster.Status.CurrentPrimary, op.ID, *postgres)
	return err
}
