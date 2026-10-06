package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	runtimepkg "fugue/internal/runtime"
)

// executeManagedDatabaseColdMigration moves a stopped/disk-full PostgreSQL
// primary to a destination storage class without allocating space in the
// source LocalPV pool. The source cluster remains hibernated after the copy;
// its PVC is retained as the rollback/archive source.
func (s *Service) executeManagedDatabaseColdMigration(
	ctx context.Context,
	op model.Operation,
	app model.App,
	sourceCluster kubeCloudNativePGCluster,
	desiredPostgres model.AppPostgresSpec,
	targetRuntime model.Runtime,
) error {
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	namespace := runtimepkg.NamespaceForTenant(app.TenantID)
	sourceClusterName := strings.TrimSpace(sourceCluster.Metadata.Name)
	primary := strings.TrimSpace(sourceCluster.Status.CurrentPrimary)
	if sourceClusterName == "" || primary == "" {
		return fmt.Errorf("cold database migration requires an observed source cluster and primary")
	}
	sourcePVC, found, err := client.getPersistentVolumeClaim(ctx, namespace, primary)
	if err != nil {
		return err
	}
	if !found || sourcePVC.Spec.VolumeName == "" || sourcePVC.Metadata.Labels["cnpg.io/cluster"] != sourceClusterName {
		return fmt.Errorf("cold database migration source primary has no verified bound data claim")
	}
	if strings.TrimSpace(sourcePVC.Spec.StorageClassName) == strings.TrimSpace(desiredPostgres.StorageClassName) {
		return fmt.Errorf("cold database migration requires a distinct destination storage class")
	}
	sourceNode, found, err := managedPostgresPVCNode(ctx, client, namespace, primary)
	if err != nil {
		return err
	}
	if !found || strings.TrimSpace(sourceNode) == "" {
		return fmt.Errorf("cold database migration cannot identify the source PVC node")
	}
	targetNode, err := recoveryStorageTargetNode(ctx, client, targetRuntime, desiredPostgres.StorageClassName, desiredPostgres.PrimaryNodeName)
	if err != nil {
		return err
	}

	clusterName := coldPostgresClusterName(sourceClusterName, op.ID)
	stagingClaim := coldPostgresStagingClaimName(sourceClusterName, op.ID)
	desiredPostgres.ServiceName = clusterName
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}

	// Fence first, then prove that no live or terminating workload still mounts
	// the source claim. This is the cold-copy boundary.
	if err := patchRecoverySourceHibernation(ctx, client, namespace, sourceCluster, runtimepkg.CloudNativePGHibernationOn); err != nil {
		return err
	}
	if err := waitColdPostgresSourceQuiescent(ctx, client, namespace, sourcePVC.Metadata.Name, 10*time.Minute); err != nil {
		return err
	}

	stagingPVC := buildColdPostgresStagingPVC(namespace, stagingClaim, sourcePVC, desiredPostgres.StorageClassName, app, op.ID)
	if err := client.applyObject(ctx, stagingPVC, nil); err != nil {
		return fmt.Errorf("create cold migration staging pvc %s/%s: %w", namespace, stagingClaim, err)
	}
	if err := waitColdPostgresPVCBound(ctx, client, namespace, stagingClaim, 10*time.Minute); err != nil {
		return err
	}

	sourceScheduling := runtimepkg.SchedulingConstraints{NodeSelector: map[string]string{kubeHostnameLabelKey: sourceNode}}
	targetScheduling := runtimepkg.SchedulingForRuntime(targetRuntime)
	if targetScheduling.NodeSelector == nil {
		targetScheduling.NodeSelector = map[string]string{}
	}
	targetScheduling.NodeSelector[kubeHostnameLabelKey] = targetNode
	copyApp := model.App{ID: "cold-" + op.ID, Name: "postgres-cold-migration"}
	names := movableRWOMigrationResourceNames(copyApp, stagingClaim)
	plan := movableRWOCopyPlan{sourceClaimName: primary, targetClaimName: stagingClaim, sourceCopyPath: ".", targetCopyPath: "."}
	if err := s.copyMovableRWOVolumeViaTransferPods(ctx, client, namespace, names, plan, sourceScheduling, targetScheduling); err != nil {
		return fmt.Errorf("copy fenced postgres source pvc to target storage: %w", err)
	}
	if err := verifyColdPostgresStagingClaim(ctx, client, namespace, stagingClaim, desiredPostgres.StorageClassName); err != nil {
		return err
	}
	if _, err := s.Store.RecordOperationEvidence(model.OperationEvidence{
		TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: op.ID,
		Type: model.OperationEvidenceTypePostgresResizePreflight, Source: model.OperationEvidenceSourceKubernetesAPI,
		Severity: model.OperationEvidenceSeverityInfo, Confidence: model.OperationEvidenceConfidenceConfirmed,
		SubjectKind: "PersistentVolumeClaim", SubjectName: stagingClaim, SubjectUID: sourcePVC.Metadata.UID,
		Reason: "cold_postgres_staging_copy_verified", Summary: "fenced source copied to target storage and transfer digest matched", Payload: map[string]any{
			"source_claim": primary, "source_node": sourceNode, "target_claim": stagingClaim, "target_node": targetNode, "target_storage_class": desiredPostgres.StorageClassName,
		}, PayloadVersion: 1,
	}); err != nil {
		return err
	}

	targetApp, err := appWithBackingServicePostgres(op.ServiceID, app, desiredPostgres)
	if err != nil {
		return err
	}
	objects := runtimepkg.BuildManagedAppChildObjectsWithPlacements(targetApp, runtimepkg.SchedulingForRuntime(targetRuntime), nil, nil)
	if err := applyColdPostgresClusterObjects(ctx, client, objects, stagingClaim, clusterName, desiredPostgres.StorageClassName); err != nil {
		return err
	}

	targetPrimary, err := s.waitColdPostgresPrimary(ctx, client, namespace, clusterName, op.ID, desiredPostgres)
	if err != nil {
		return err
	}
	targetCluster, found, err := client.getCloudNativePGCluster(ctx, namespace, clusterName)
	if err != nil {
		return err
	}
	if !found || targetCluster.Status.SystemID == "" || sourceCluster.Status.SystemID == "" || targetCluster.Status.SystemID != sourceCluster.Status.SystemID {
		return fmt.Errorf("cold database migration refused: target system identifier does not match the fenced source")
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	if strings.TrimSpace(targetPrimary) == "" {
		return fmt.Errorf("cold database migration produced no target primary")
	}

	finalApp, err := s.updateAppBackingServicePostgres(op.ServiceID, app, desiredPostgres)
	if err != nil {
		return err
	}
	bundle, err := s.applyManagedDesiredAppState(ctx, op.ID, finalApp, finalApp.Spec)
	if err != nil {
		return fmt.Errorf("apply cold database cutover state: %w", err)
	}
	if err := s.ensureOperationStillActive(op.ID); err != nil {
		return err
	}
	_, err = s.Store.CompleteManagedOperationWithResult(op.ID, bundle.ManifestPath, fmt.Sprintf("cold database migrated to runtime %s on target storage", desiredPostgres.RuntimeID), &finalApp.Spec, nil)
	return err
}

func patchRecoverySourceHibernation(ctx context.Context, client *kubeClient, namespace string, cluster kubeCloudNativePGCluster, value string) error {
	return client.patchCloudNativePGHibernation(ctx, namespace, cluster.Metadata.Name, value, cluster.Metadata.UID, cluster.Metadata.ResourceVersion)
}

func waitColdPostgresSourceQuiescent(ctx context.Context, client *kubeClient, namespace, claim string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		if err := ensureMovableRWOSourceQuiescent(waitCtx, client, namespace, claim); err == nil {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for fenced postgres source pvc %s/%s to become quiescent: %w", namespace, claim, waitCtx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func buildColdPostgresStagingPVC(namespace, name string, source kubePersistentVolumeClaim, storageClass string, app model.App, operationID string) map[string]any {
	labels := map[string]string{
		runtimepkg.FugueLabelManagedBy:      runtimepkg.FugueLabelManagedByValue,
		runtimepkg.FugueLabelAppID:          app.ID,
		runtimepkg.FugueLabelTenantID:       app.TenantID,
		"cnpg.io/pvcRole":                   "PG_DATA",
		"fugue.pro/cold-postgres-operation": operationID,
	}
	for k, v := range source.Metadata.Labels {
		if strings.HasPrefix(k, "cnpg.io/") && strings.TrimSpace(v) != "" {
			labels[k] = v
		}
	}
	size := strings.TrimSpace(source.Status.Capacity["storage"])
	if size == "" {
		size = strings.TrimSpace(source.Spec.Resources.Requests["storage"])
	}
	return map[string]any{"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": map[string]any{"name": name, "namespace": namespace, "labels": labels}, "spec": map[string]any{"accessModes": []string{"ReadWriteOnce"}, "storageClassName": storageClass, "resources": map[string]any{"requests": map[string]any{"storage": size}}}}
}

func waitColdPostgresPVCBound(ctx context.Context, client *kubeClient, namespace, name string, timeout time.Duration) error {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		pvc, found, err := client.getPersistentVolumeClaim(waitCtx, namespace, name)
		if err != nil {
			return err
		}
		if found && strings.EqualFold(strings.TrimSpace(pvc.Status.Phase), "Bound") && pvc.Spec.VolumeName != "" {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for cold staging pvc %s/%s: %w", namespace, name, waitCtx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func verifyColdPostgresStagingClaim(ctx context.Context, client *kubeClient, namespace, name, storageClass string) error {
	pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, name)
	if err != nil {
		return err
	}
	if !found || pvc.Spec.StorageClassName != storageClass || pvc.Spec.VolumeName == "" || !strings.EqualFold(strings.TrimSpace(pvc.Status.Phase), "Bound") {
		return fmt.Errorf("cold staging pvc %s/%s failed target storage verification", namespace, name)
	}
	return nil
}

func applyColdPostgresClusterObjects(ctx context.Context, client *kubeClient, objects []map[string]any, stagingClaim, clusterName, storageClass string) error {
	var secret, service, cluster map[string]any
	for _, object := range objects {
		switch object["kind"] {
		case "Secret":
			secret = object
		case "Service":
			service = object
		case "Cluster":
			cluster = object
		}
	}
	if cluster == nil || service == nil || secret == nil {
		return fmt.Errorf("cold database migration could not render target postgres objects")
	}
	metadata := normalizeKubeMap(cluster["metadata"])
	metadata["name"] = clusterName
	cluster["metadata"] = metadata
	spec := normalizeKubeMap(cluster["spec"])
	spec["instances"] = 1
	spec["storage"] = map[string]any{"size": normalizeKubeMap(spec["storage"])["size"], "storageClass": storageClass, "resizeInUseVolumes": true}
	spec["bootstrap"] = map[string]any{"recovery": map[string]any{"volumeSnapshots": map[string]any{"storage": map[string]any{"kind": "PersistentVolumeClaim", "name": stagingClaim}}}}
	cluster["spec"] = spec
	if err := client.applyObject(ctx, secret, nil); err != nil {
		return err
	}
	if err := client.applyObject(ctx, service, nil); err != nil {
		return err
	}
	return client.applyObject(ctx, cluster, nil)
}

func (s *Service) waitColdPostgresPrimary(ctx context.Context, client *kubeClient, namespace, clusterName, opID string, pg model.AppPostgresSpec) (string, error) {
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	for {
		cluster, found, err := client.getCloudNativePGCluster(waitCtx, namespace, clusterName)
		if err != nil {
			return "", err
		}
		if found && cluster.Status.CurrentPrimary != "" && cluster.Status.ReadyInstances >= 1 {
			_, err := s.waitForManagedPostgresPrimary(waitCtx, client, namespace, clusterName, cluster.Status.CurrentPrimary, opID, pg)
			return cluster.Status.CurrentPrimary, err
		}
		select {
		case <-waitCtx.Done():
			return "", waitCtx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func coldPostgresClusterName(base, operationID string) string {
	return coldPostgresName(base, "cold", operationID)
}
func coldPostgresStagingClaimName(base, operationID string) string {
	return coldPostgresName(base, "cold-seed", operationID)
}
func coldPostgresName(base, suffix, operationID string) string {
	base = model.Slugify(strings.TrimSpace(base))
	tail := model.Slugify(operationID)
	if len(tail) > 12 {
		tail = tail[len(tail)-12:]
	}
	name := base + "-" + suffix + "-" + tail
	if len(name) > 63 {
		name = name[len(name)-63:]
	}
	return strings.Trim(name, "-")
}
