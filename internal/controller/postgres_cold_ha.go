package controller

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

// Expand only after the initial primary is writable on the requested target.
// Holding PodSpec reconciliation keeps that primary running while replicas gain
// the additional failure-domain placement. Never restore HA by restarting the
// old primary or by promoting its potentially stale standby.
func (s *Service) restoreColdTargetHA(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState, app model.App, pg *model.AppPostgresSpec, primaryPlacement runtimepkg.SchedulingConstraints, operationID string) error {
	if pg.Instances <= 1 {
		return nil
	}
	placements, err := s.coldRecoveryPlacements(ctx, c, app, *pg, st.TargetNode)
	if err != nil {
		return err
	}
	if len(placements) < 2 {
		return fmt.Errorf("HA recovery requires a distinct replica placement")
	}
	placements[0] = primaryPlacement
	pg.PrimaryPlacementPendingRebalance = true
	targetApp, err := appWithBackingServicePostgres(st.ServiceID, app, *pg)
	if err != nil {
		return err
	}
	var desired map[string]any
	for _, obj := range runtimepkg.BuildManagedAppChildObjectsWithPlacements(targetApp, runtimepkg.SchedulingConstraints{}, map[string][]runtimepkg.SchedulingConstraints{st.TargetName: placements}, nil) {
		if obj["kind"] == "Cluster" && normalizeKubeMap(obj["metadata"])["name"] == st.TargetName {
			desired = obj
		}
	}
	if desired == nil {
		return fmt.Errorf("HA recovery cluster was not rendered")
	}
	if err := patchColdTargetHA(ctx, c, ns, st, desired); err != nil {
		return err
	}
	s.updateManagedPostgresTransitionProgress(operationID, "target primary is writable; rebuilding and verifying requested HA replicas before endpoint cutover")
	var primary string
	var replicas []string
	for {
		if err := s.ensureOperationStillActive(operationID); err != nil {
			return err
		}
		cluster, found, err := c.getCloudNativePGCluster(ctx, ns, st.TargetName)
		if err != nil || !found || cluster.Metadata.UID != st.TargetUID {
			return fmt.Errorf("HA recovery target identity changed")
		}
		pods, err := c.listPodsBySelector(ctx, ns, "cnpg.io/cluster="+st.TargetName)
		if err != nil {
			return err
		}
		primary, replicas, err = coldReadyHAInstances(cluster, pods, st.TargetNode, pg.Instances)
		if err != nil {
			return err
		}
		if primary != "" {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("target HA instances did not become ready: %w", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	for _, name := range replicas {
		if err := s.waitForManagedPostgresReplicationCatchup(ctx, c, ns, st.TargetName, name, operationID, *pg); err != nil {
			return err
		}
	}
	if pg.SynchronousReplicas > 0 {
		// Use the instance's local administrative socket, not application role
		// visibility into pg_stat_replication. This query never changes data.
		for {
			var out bytes.Buffer
			err := coldExec(ctx, c, ns, primary, "postgres", []string{"psql", "-XAt", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c", `SELECT count(*) FROM pg_stat_replication WHERE state='streaming' AND sync_state IN ('sync','quorum')`}, &out)
			count, valid := coldNumericID(strings.TrimSpace(out.String()))
			if err == nil && valid && count >= int64(pg.SynchronousReplicas) {
				break
			}
			if err := s.ensureOperationStillActive(operationID); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("synchronous HA replication not verified: %w", ctx.Err())
			case <-time.After(2 * time.Second):
			}
		}
	}
	// Re-check primary identity and location after the replication gates.
	cluster, found, err := c.getCloudNativePGCluster(ctx, ns, st.TargetName)
	if err != nil || !found || cluster.Metadata.UID != st.TargetUID || cluster.Status.CurrentPrimary != primary {
		return fmt.Errorf("target primary changed during HA validation")
	}
	return nil
}

func patchColdTargetHA(ctx context.Context, c *kubeClient, ns string, st *coldPostgresState, desired map[string]any) error {
	current, found, err := c.getCloudNativePGCluster(ctx, ns, st.TargetName)
	if err != nil || !found || current.Metadata.UID != st.TargetUID || current.Metadata.ResourceVersion == "" || current.Metadata.Annotations[coldMigrationAnnotation] != coldStateName(st.ServiceID) {
		return fmt.Errorf("target ownership unavailable before HA restoration")
	}
	spec := normalizeKubeMap(desired["spec"])
	fields := map[string]any{}
	for _, key := range []string{"instances", "affinity", "minSyncReplicas", "maxSyncReplicas"} {
		if spec[key] == nil {
			return fmt.Errorf("HA recovery field %s missing", key)
		}
		fields[key] = spec[key]
	}
	body := map[string]any{"metadata": map[string]any{"uid": st.TargetUID, "resourceVersion": current.Metadata.ResourceVersion, "annotations": map[string]string{runtimepkg.CloudNativePGReconcilePodSpecAnno: runtimepkg.CloudNativePGReconcilePodSpecHold}}, "spec": fields}
	_, err = c.doRequest(ctx, http.MethodPatch, cloudNativePGClusterAPIPath(ns, st.TargetName), "application/merge-patch+json", body, nil)
	return err
}

func coldReadyHAInstances(cluster kubeCloudNativePGCluster, pods []kubePod, targetNode string, instances int) (string, []string, error) {
	if cluster.Status.CurrentPrimary == "" || cluster.Status.ReadyInstances < instances {
		return "", nil, nil
	}
	primary := ""
	var replicas []string
	nodes := map[string]bool{}
	for _, pod := range pods {
		if managedPostgresPodFinished(pod) {
			continue
		}
		if !coldClusterOwnsPod(cluster, pod) {
			return "", nil, fmt.Errorf("HA target contains a Pod not owned by its exact cluster UID")
		}
		if !managedPostgresPrimaryPodReady(pod) || pod.Status.Phase != "Running" || pod.Metadata.DeletionTimestamp != "" || pod.Spec.NodeName == "" {
			return "", nil, nil
		}
		if nodes[pod.Spec.NodeName] {
			return "", nil, fmt.Errorf("HA target instances share a failure domain")
		}
		nodes[pod.Spec.NodeName] = true
		if pod.Metadata.Name == cluster.Status.CurrentPrimary {
			if pod.Spec.NodeName != targetNode {
				return "", nil, fmt.Errorf("HA target primary moved off requested destination")
			}
			primary = pod.Metadata.Name
		} else {
			replicas = append(replicas, pod.Metadata.Name)
		}
	}
	if primary == "" || len(replicas) != instances-1 {
		return "", nil, nil
	}
	return primary, replicas, nil
}
