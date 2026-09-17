package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"fugue/internal/storagerecovery"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"fugue/internal/localpvsafety"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Recovery first makes the existing primary writable. Replication cannot rescue
// a stopped source. No source PVC is removed, and normal localization remains
// responsible for verifying catch-up, promotion, and the final write probe.
func (s *Service) executeManagedDatabaseRecoveryOperation(ctx context.Context, op model.Operation, app model.App) error {
	timeout := s.Config.ManagedAppRolloutTimeout
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	target, err := store.ManagedPostgresOperationTargetForApp(app, op.ServiceID)
	if err != nil {
		return err
	}
	if target == nil || !target.AppOwned {
		return fmt.Errorf("storage recovery requires an app-owned database")
	}
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	namespace, name := runtime.NamespaceForTenant(app.TenantID), target.Postgres.ServiceName
	cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, name)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("recovery cluster %s/%s not found", namespace, name)
	}
	if cluster.Status.CurrentPrimary == "" {
		return fmt.Errorf("recovery requires an observed primary; refusing to choose a data source")
	}
	pg := model.CloneAppPostgresSpec(&target.Postgres)
	if op.DesiredSpec != nil && op.DesiredSpec.Postgres != nil {
		pg = model.CloneAppPostgresSpec(op.DesiredSpec.Postgres)
	}
	// The CNPG admission webhook forbids shrinking even a failed staged intent.
	liveSize, err := managedPostgresLiveStorageSize(ctx, client, namespace, name)
	if err != nil {
		return err
	}
	pg.StorageSize, err = maximumRecoveryStorageSize(pg.StorageSize, cluster.Spec.Storage.Size, liveSize)
	if err != nil {
		return err
	}
	if cluster.Spec.Storage.StorageClass != "" {
		pg.StorageClassName = cluster.Spec.Storage.StorageClass
	}
	desired := app.Spec
	desired.Postgres = pg
	op.DesiredSpec = &desired

	if cluster.Status.ReadyInstances == 0 && strings.EqualFold(strings.TrimSpace(cluster.Status.Phase), "Not enough disk space") {
		primary := cluster.Status.CurrentPrimary
		pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, primary)
		if err != nil {
			return err
		}
		if !found || pvc.Spec.VolumeName == "" || pvc.Metadata.Labels["cnpg.io/cluster"] != name {
			return fmt.Errorf("recovery primary has no verified bound data claim")
		}
		sourceClass := pvc.Spec.StorageClassName
		if sourceClass == pg.StorageClassName {
			return fmt.Errorf("disk-pressure recovery requires a distinct declared destination storage class; configure a storage migration first")
		}
		// Record the rescue target before mutation. Retrying the operation must not
		// keep adding increments to an already expanded source.
		sourceSize, err := s.recoverySourceTarget(op, app, pvc)
		if err != nil {
			return err
		}
		if err := s.ensureRecoveryPoolCapacity(ctx, client, op, namespace, pvc, sourceSize); err != nil {
			return err
		}
		if err := s.ensureOperationStillActive(op.ID); err != nil {
			return err
		}
		// Freeze only this cluster's automatic growth while the small source rescue
		// completes. The final target size remains unchanged and is never shrunk.
		if err := freezeRecoverySourceExpansion(ctx, client, namespace, name, cluster.Metadata.UID, primary); err != nil {
			return err
		}
		rescue := managedPostgresStorageTarget{StorageClassName: sourceClass, StorageSize: sourceSize}
		if err := s.prepareManagedPostgresInPlaceStorageExpansionForExistingCluster(ctx, client, namespace, name, rescue); err != nil {
			return err
		}
		s.updateManagedPostgresTransitionProgress(op.ID, "waiting for rescued source filesystem and writable primary before migration")
		if err := s.waitRecoverySourceStorage(ctx, client, op, namespace, name, primary, pvc.Metadata.UID, rescue, target.Postgres); err != nil {
			return err
		}
	}
	return s.executeManagedDatabaseLocalizeOperation(ctx, op, app)
}

func freezeRecoverySourceExpansion(ctx context.Context, client *kubeClient, namespace, name, uid, primary string) error {
	observed, found, err := client.getCloudNativePGCluster(ctx, namespace, name)
	if err != nil {
		return err
	}
	if !found || uid == "" || observed.Metadata.UID != uid || observed.Metadata.ResourceVersion == "" || observed.Status.CurrentPrimary != primary {
		return fmt.Errorf("cluster identity or primary changed before source rescue")
	}
	raw, err := json.Marshal(map[string]any{"metadata": map[string]string{"uid": uid, "resourceVersion": observed.Metadata.ResourceVersion}, "spec": map[string]any{"storage": map[string]any{"resizeInUseVolumes": false}}})
	if err != nil {
		return err
	}
	status, _, err := client.doRaw(ctx, http.MethodPatch, "/apis/postgresql.cnpg.io/v1/namespaces/"+url.PathEscape(namespace)+"/clusters/"+url.PathEscape(name), bytes.NewReader(raw), "application/merge-patch+json")
	if err != nil {
		return err
	}
	if status >= 300 {
		return fmt.Errorf("guarded source resize freeze refused: status=%d", status)
	}
	return nil
}

func maximumRecoveryStorageSize(values ...string) (string, error) {
	var largest resource.Quantity
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		q, err := resource.ParseQuantity(value)
		if err != nil || q.Sign() <= 0 {
			return "", fmt.Errorf("invalid observed recovery storage size %q", value)
		}
		if q.Cmp(largest) > 0 {
			largest = q
		}
	}
	if largest.Sign() <= 0 {
		return "", fmt.Errorf("no observed recovery storage capacity")
	}
	return largest.String(), nil
}

func (s *Service) recoverySourceTarget(op model.Operation, app model.App, pvc kubePersistentVolumeClaim) (string, error) {
	evidence, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{OperationID: op.ID, PlatformAdmin: true, Limit: 1000})
	if err != nil {
		return "", err
	}
	for _, e := range evidence {
		if e.Reason == "storage_recovery_source_target" && e.SubjectUID == pvc.Metadata.UID {
			if size, ok := e.Payload["storage_size"].(string); ok {
				return size, nil
			}
		}
	}
	capacity, err := resource.ParseQuantity(pvc.Status.Capacity["storage"])
	if err != nil || capacity.Sign() <= 0 {
		return "", fmt.Errorf("source PVC lacks actual capacity")
	}
	requested, err := resource.ParseQuantity(pvc.Spec.Resources.Requests["storage"])
	if err != nil {
		return "", err
	}
	// At most one two-GiB rescue increment per durable recovery operation.
	size := resource.NewQuantity(capacity.Value()+(2<<30), resource.BinarySI)
	if requested.Cmp(capacity) > 0 {
		size = &requested
	}
	_, err = s.Store.RecordOperationEvidence(model.OperationEvidence{
		TenantID: app.TenantID, ProjectID: app.ProjectID, AppID: app.ID, OperationID: op.ID,
		Type: model.OperationEvidenceTypePostgresResizePreflight, Source: model.OperationEvidenceSourceKubernetesAPI,
		Severity: model.OperationEvidenceSeverityInfo, Confidence: model.OperationEvidenceConfidenceConfirmed,
		SubjectKind: "PersistentVolumeClaim", SubjectName: pvc.Metadata.Name, SubjectUID: pvc.Metadata.UID,
		Reason: "storage_recovery_source_target", Summary: "bounded source-volume rescue target recorded before mutation",
		Payload: map[string]any{"storage_size": size.String()}, PayloadVersion: 1,
	})
	return size.String(), err
}

func recoveryPoolTarget(inventory model.LocalPVInventory, growth int64) (int64, error) {
	if inventory.ImageSizeBytes <= 0 || inventory.PVSizeBytes <= 0 || inventory.PVFreeBytes < 0 || inventory.PVFreeBytes > inventory.PVSizeBytes || growth < 0 {
		return 0, fmt.Errorf("invalid LocalPV capacity observation")
	}
	if inventory.PVFreeBytes >= growth+localpvsafety.RequiredFreeBytes(inventory.PVSizeBytes) {
		return inventory.ImageSizeBytes, nil
	}
	// Account for the reserve growing with the pool. Round to GiB for auditable
	// host tasks and fail instead of exceeding the task's bounded growth policy.
	for extra := int64(1 << 30); extra <= 64<<30; extra += 1 << 30 {
		if inventory.PVFreeBytes+extra >= growth+localpvsafety.RequiredFreeBytes(inventory.PVSizeBytes+extra) {
			return inventory.ImageSizeBytes + extra, nil
		}
	}
	return 0, fmt.Errorf("required LocalPV pool growth exceeds 64 GiB recovery bound")
}

func (s *Service) ensureRecoveryPoolCapacity(ctx context.Context, client *kubeClient, op model.Operation, namespace string, pvc kubePersistentVolumeClaim, size string) error {
	sc, found, err := client.getStorageClass(ctx, pvc.Spec.StorageClassName)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("source storage class missing")
	}
	if !strings.EqualFold(sc.Provisioner, openEBSLocalLVMProvisioner) {
		return nil
	}
	pv, found, err := client.getPersistentVolume(ctx, pvc.Spec.VolumeName)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("source PV missing")
	}
	node, vg := persistentVolumeNodeName(pv), managedPostgresLocalPVVolumeGroup(sc, pv)
	inventories, err := s.Store.ListLocalPVInventories(model.LocalPVInventoryFilter{})
	if err != nil {
		return err
	}
	inventory, found := newestLocalPVInventoryForNodeAndVG(inventories, node, vg)
	if !found || !localpvsafety.IsFresh(inventory.ObservedAt, time.Now(), localpvsafety.DefaultInventoryTTL) {
		return fmt.Errorf("fresh LocalPV inventory required for node %s volume group %s", node, vg)
	}
	target, err := resource.ParseQuantity(size)
	if err != nil {
		return err
	}
	capacity, err := resource.ParseQuantity(pvc.Status.Capacity["storage"])
	if err != nil {
		return err
	}
	growth := target.Value() - capacity.Value()
	if growth < 0 {
		growth = 0
	}
	imageSize, err := recoveryPoolTarget(inventory, growth)
	if err != nil {
		return err
	}
	if imageSize == inventory.ImageSizeBytes {
		return nil
	}
	principal := model.Principal{ActorType: model.ActorTypeSystem, ActorID: "fugue-controller/storage-recovery", Scopes: map[string]struct{}{"platform.admin": {}}}
	supported, err := s.recoveryUpdaterReady(inventory.ReportedByNodeUpdaterID, node)
	if err != nil {
		return err
	}
	if !supported {
		task, err := s.Store.CreateNodeUpdateTask(principal, inventory.ReportedByNodeUpdaterID, node, "", model.NodeUpdateTaskTypeUpgradeUpdater, map[string]string{"target_version": storagerecovery.NodeUpdaterVersion})
		if err != nil {
			return err
		}
		if err := s.waitRecoveryHostTask(ctx, op.ID, task); err != nil {
			return err
		}
		// The old updater process acknowledges its own replacement before the
		// new process can heartbeat its capabilities. Task completion alone is
		// not evidence that the control plane can deliver the new task type.
		s.updateManagedPostgresTransitionProgress(op.ID, "node updater upgraded; waiting for advertised storage recovery capability")
		if err := waitRecoveryCapability(ctx, func() (bool, error) {
			if err := s.ensureOperationStillActive(op.ID); err != nil {
				return false, err
			}
			return s.recoveryUpdaterReady(inventory.ReportedByNodeUpdaterID, node)
		}, func(ctx context.Context) error { return waitManagedPostgresResizePollInterval(ctx, 2*time.Second) }); err != nil {
			return err
		}
	}
	task, err := s.Store.CreateNodeUpdateTask(principal, inventory.ReportedByNodeUpdaterID, node, "", storagerecovery.ExpandPoolTask, map[string]string{
		"image_path": inventory.ImagePath, "vg_name": vg, "expected_image_size_bytes": strconv.FormatInt(inventory.ImageSizeBytes, 10), "target_image_size_bytes": strconv.FormatInt(imageSize, 10), "dry_run": "false",
	})
	if err != nil {
		return err
	}
	return s.waitRecoveryHostTask(ctx, op.ID, task)
}

func (s *Service) recoveryUpdaterReady(id, node string) (bool, error) {
	supported, err := s.Store.NodeUpdaterTargetSupportsTask(id, node, "", storagerecovery.ExpandPoolTask)
	if err != nil || !supported {
		return false, err
	}
	updaters, err := s.Store.ListNodeUpdaters("", true)
	if err != nil {
		return false, err
	}
	for _, updater := range updaters {
		if updater.ID == id {
			return !controllerNodeUpdaterNeedsUpgrade(updater.UpdaterVersion, storagerecovery.NodeUpdaterVersion), nil
		}
	}
	return false, fmt.Errorf("recovery node updater %s disappeared", id)
}

func waitRecoveryCapability(ctx context.Context, observe func() (bool, error), wait func(context.Context) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		supported, err := observe()
		if err != nil {
			return err
		}
		if supported {
			return nil
		}
		if err := wait(ctx); err != nil {
			return err
		}
	}
}

func (s *Service) waitRecoverySourceStorage(ctx context.Context, client *kubeClient, op model.Operation, namespace, name, primary, pvcUID string, target managedPostgresStorageTarget, postgres model.AppPostgresSpec) error {
	for {
		if err := s.ensureOperationStillActive(op.ID); err != nil {
			return err
		}
		converged, detail, err := inspectManagedPostgresStorageExpansion(ctx, client, namespace, name, target)
		if err != nil {
			return err
		}
		if converged {
			pvc, found, err := client.getPersistentVolumeClaim(ctx, namespace, primary)
			if err != nil {
				return err
			}
			if !found || pvc.Metadata.UID != pvcUID {
				return fmt.Errorf("source PVC identity changed during rescue")
			}
			cluster, found, err := client.getCloudNativePGCluster(ctx, namespace, name)
			if err != nil {
				return err
			}
			if !found || cluster.Status.CurrentPrimary != primary {
				return fmt.Errorf("source primary changed during rescue")
			}
			pod, found, err := client.getPod(ctx, namespace, primary)
			if err != nil {
				return err
			}
			if found && managedPostgresPrimaryPodReady(pod) {
				ip, found, err := client.getPodIP(ctx, namespace, primary)
				if err != nil {
					return err
				}
				if found && ip != "" {
					if err := s.probeManagedPostgresPrimarySQL(ctx, managedPostgresRWServiceHost(namespace, model.PostgresRWServiceName(name)), ip, postgres); err == nil {
						return nil
					}
				}
			}
			detail = "source storage has converged; waiting for the same primary to serve read-write SQL"
		}
		s.updateManagedPostgresTransitionProgress(op.ID, detail)
		if err := waitManagedPostgresResizePollInterval(ctx, 2*time.Second); err != nil {
			return err
		}
	}
}

func (s *Service) waitRecoveryHostTask(ctx context.Context, opID string, task model.NodeUpdateTask) error {
	s.updateManagedPostgresTransitionProgress(opID, fmt.Sprintf("waiting for guarded host task %s (%s)", task.ID, task.Type))
	for {
		if err := s.ensureOperationStillActive(opID); err != nil {
			return err
		}
		current, err := s.Store.GetNodeUpdateTaskForUpdater(task.ID, task.NodeUpdaterID)
		if err != nil {
			return err
		}
		switch current.Status {
		case model.NodeUpdateTaskStatusCompleted:
			return nil
		case model.NodeUpdateTaskStatusFailed, model.NodeUpdateTaskStatusCanceled:
			return fmt.Errorf("host recovery task %s %s: %s", task.ID, current.Status, current.ErrorMessage)
		}
		if err := waitManagedPostgresResizePollInterval(ctx, 2*time.Second); err != nil {
			return err
		}
	}
}
