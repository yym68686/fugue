package controller

import (
	"context"
	"fmt"
	"net/http"
	"sort"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Preflight the actual destination size, including recovery headroom, while
// preserving the requested spec used as the durable retry intent.
func coldRecoveryPreflightSize(ctx context.Context, c *kubeClient, ns, serviceID string, source kubeCloudNativePGCluster, requested string) (string, error) {
	st, _, err := loadColdState(ctx, c, ns, serviceID)
	if err != nil {
		return "", err
	}
	if st != nil {
		return maximumRecoveryStorageSize(requested, st.TargetSize)
	}
	pvc, found, err := c.getPersistentVolumeClaim(ctx, ns, source.Status.CurrentPrimary)
	if err != nil {
		return "", err
	}
	q, parseErr := resource.ParseQuantity(pvc.Status.Capacity["storage"])
	if !found || parseErr != nil || q.Sign() <= 0 {
		return "", fmt.Errorf("source capacity unavailable for recovery preflight")
	}
	return maximumRecoveryStorageSize(requested, resource.NewQuantity(q.Value()+(2<<30), resource.BinarySI).String())
}

// Validate every HA failure domain before stopping a source, not just the
// destination primary. CSI registration alone is insufficient for Longhorn.
// Return exact, storage-compatible hosts so CNPG cannot choose an incompatible
// member of a broader shared-runtime selector after the preflight succeeds.
func (s *Service) coldRecoveryPlacements(ctx context.Context, c *kubeClient, app model.App, pg model.AppPostgresSpec, primaryNode string) ([]runtimepkg.SchedulingConstraints, error) {
	placements, err := s.managedPostgresServicePlacements(ctx, app, pg.ServiceName, app.Spec.RuntimeID, pg)
	if err != nil {
		return nil, err
	}
	if len(placements) == 0 || primaryNode == "" || (pg.Instances > 1 && len(placements) < 2) {
		return nil, fmt.Errorf("cold recovery requires explicit primary and distinct HA replica placement")
	}
	size, err := resource.ParseQuantity(pg.StorageSize)
	if err != nil || size.Sign() <= 0 {
		return nil, fmt.Errorf("cold recovery storage size is invalid")
	}
	// This claim is only an observation key; no preflight volume is created.
	storage, err := loadStoragePlacement(ctx, c, runtimepkg.NamespaceForTenant(app.TenantID), []storageClaimRequest{{Name: coldStateName(pg.ServiceName) + "-preflight", Class: pg.StorageClassName, RequestedBytes: size.Value()}})
	if err != nil {
		return nil, err
	}
	var nodes struct {
		Items []kubeNode `json:"items"`
	}
	if _, err := c.doJSON(ctx, http.MethodGet, "/api/v1/nodes", nil, &nodes); err != nil {
		return nil, err
	}
	sort.Slice(nodes.Items, func(i, j int) bool { return nodes.Items[i].Metadata.Name < nodes.Items[j].Metadata.Name })
	var primary *runtimepkg.SchedulingConstraints
	var replicas []runtimepkg.SchedulingConstraints
	for _, node := range nodes.Items {
		if !managedSharedNodeSchedulable(node) || storage.rejection(node) != "" {
			continue
		}
		for i, placement := range placements {
			if !nodeLabelsMatchSelector(node.Metadata.Labels, placement.NodeSelector) {
				continue
			}
			exact := runtimepkg.SchedulingConstraints{NodeSelector: map[string]string{kubeHostnameLabelKey: node.Metadata.Name}, Tolerations: placement.Tolerations}
			if node.Metadata.Name == primaryNode {
				if i == 0 {
					primary = &exact
				}
			} else if i > 0 {
				replicas = append(replicas, exact)
				break
			}
		}
	}
	if primary == nil || len(replicas) < pg.Instances-1 {
		return nil, fmt.Errorf("recovery storage class %s cannot serve the requested primary and %d distinct HA replicas within their runtimes; source and serving endpoint remain unchanged", pg.StorageClassName, pg.Instances-1)
	}
	return append([]runtimepkg.SchedulingConstraints{*primary}, replicas...), nil
}
