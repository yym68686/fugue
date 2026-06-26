package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	registryGCRequestedAtAnnotation        = "fugue.pro/registry-gc-requested-at"
	registryGCRequestReasonAnnotation      = "fugue.pro/registry-gc-request-reason"
	registryGCRunningAnnotation            = "fugue.pro/registry-gc-running"
	registryGCLastCompletedAtAnnotation    = "fugue.pro/registry-gc-last-completed-at"
	registryGCLastStatusAnnotation         = "fugue.pro/registry-gc-last-status"
	registryStorageUsedBytesAnnotation     = "fugue.pro/registry-storage-used-bytes"
	registryStorageCapacityBytesAnnotation = "fugue.pro/registry-storage-capacity-bytes"
	registryUnreferencedBytesAnnotation    = "fugue.pro/registry-unreferenced-blob-bytes"
	registryUnreferencedCountAnnotation    = "fugue.pro/registry-unreferenced-blob-count"
	registryProtectedDigestsAnnotation     = "fugue.pro/registry-protected-workload-digests"
)

var errRegistryGCRunning = errors.New("registry garbage collection is running")

type registryMaintenanceStatus struct {
	JanitorPresent        bool
	GCCronJobPresent      bool
	GCRunning             bool
	GCRequested           bool
	LastGCStatus          string
	LastGCTimestamp       time.Time
	StorageUsedBytes      int64
	StorageCapacityBytes  int64
	UnreferencedBlobBytes int64
	UnreferencedBlobCount int64
	ProtectedDigestCount  int64
}

func (s *Service) markRegistryGCNeeded(ctx context.Context, reason string) {
	if s != nil && !s.imageStoreRegistryFallbackEnabled() {
		return
	}
	var err error
	if s.requestRegistryGC != nil {
		err = s.requestRegistryGC(ctx, reason)
	} else {
		err = s.recordRegistryGCRequest(ctx, reason)
	}
	if err != nil && s.Logger != nil {
		s.Logger.Printf("record registry GC request failed: %v", err)
	}
}

func (s *Service) recordRegistryGCRequest(ctx context.Context, reason string) error {
	if s == nil || strings.TrimSpace(s.Config.RegistryGCLeaseName) == "" {
		return nil
	}
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	namespace := strings.TrimSpace(s.Config.KubectlNamespace)
	name := strings.TrimSpace(s.Config.RegistryGCLeaseName)

	for attempt := 0; attempt < 4; attempt++ {
		lease, found, err := client.getLease(ctx, namespace, name)
		if err != nil {
			return err
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if !found {
			lease.APIVersion = "coordination.k8s.io/v1"
			lease.Kind = "Lease"
			lease.Metadata.Name = name
			lease.Metadata.Namespace = client.effectiveNamespace(namespace)
			lease.Metadata.Labels = map[string]string{
				"app.kubernetes.io/managed-by": "fugue",
				"app.kubernetes.io/component":  "registry-gc",
			}
			lease.Metadata.Annotations = map[string]string{
				registryGCRequestedAtAnnotation:   now,
				registryGCRequestReasonAnnotation: truncateRegistryMaintenanceValue(reason, 240),
			}
			lease.Spec.HolderIdentity = "controller"
			lease.Spec.RenewTime = formatKubeTimestamp(time.Now().UTC())
			if err := client.createLease(ctx, namespace, lease); err != nil {
				if errors.Is(err, errKubeConflict) {
					continue
				}
				return err
			}
			return nil
		}
		if lease.Metadata.Annotations == nil {
			lease.Metadata.Annotations = make(map[string]string)
		}
		lease.Metadata.Annotations[registryGCRequestedAtAnnotation] = now
		lease.Metadata.Annotations[registryGCRequestReasonAnnotation] = truncateRegistryMaintenanceValue(reason, 240)
		lease.Spec.HolderIdentity = "controller"
		lease.Spec.RenewTime = formatKubeTimestamp(time.Now().UTC())
		if err := client.updateLease(ctx, namespace, lease); err != nil {
			if errors.Is(err, errKubeConflict) || errors.Is(err, errKubeNotFound) {
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("record registry GC request after retries")
}

func (s *Service) registryGCInProgress(ctx context.Context) (bool, error) {
	if s == nil || strings.TrimSpace(s.Config.RegistryGCLeaseName) == "" {
		return false, nil
	}
	client, err := s.kubeClient()
	if err != nil {
		if strings.Contains(err.Error(), "kubernetes service host/port is not available") || strings.Contains(err.Error(), "resolve kubernetes namespace") {
			return false, nil
		}
		return false, err
	}
	lease, found, err := client.getLease(ctx, s.Config.KubectlNamespace, s.Config.RegistryGCLeaseName)
	if err != nil || !found {
		return false, err
	}
	return strings.EqualFold(strings.TrimSpace(lease.Metadata.Annotations[registryGCRunningAnnotation]), "true"), nil
}

func (s *Service) readRegistryMaintenanceStatus(ctx context.Context) registryMaintenanceStatus {
	status := registryMaintenanceStatus{}
	if s == nil {
		return status
	}
	client, err := s.kubeClient()
	if err != nil {
		return status
	}
	namespace := s.Config.KubectlNamespace
	if name := strings.TrimSpace(s.Config.RegistryJanitorCronJobName); name != "" {
		if cronJob, found, err := client.getCronJob(ctx, namespace, name); err == nil && found {
			status.JanitorPresent = cronJob.Spec.Suspend == nil || !*cronJob.Spec.Suspend
		}
	}
	if name := strings.TrimSpace(s.Config.RegistryGCCronJobName); name != "" {
		if cronJob, found, err := client.getCronJob(ctx, namespace, name); err == nil && found {
			status.GCCronJobPresent = cronJob.Spec.Suspend == nil || !*cronJob.Spec.Suspend
		}
	}
	leaseName := strings.TrimSpace(s.Config.RegistryGCLeaseName)
	if leaseName == "" {
		return status
	}
	lease, found, err := client.getLease(ctx, namespace, leaseName)
	if err != nil || !found {
		return status
	}
	annotations := lease.Metadata.Annotations
	status.GCRunning = strings.EqualFold(strings.TrimSpace(annotations[registryGCRunningAnnotation]), "true")
	status.LastGCStatus = strings.TrimSpace(annotations[registryGCLastStatusAnnotation])
	status.LastGCTimestamp = parseKubeTimestamp(annotations[registryGCLastCompletedAtAnnotation])
	requestedAt := parseKubeTimestamp(annotations[registryGCRequestedAtAnnotation])
	status.GCRequested = !requestedAt.IsZero() && (status.LastGCTimestamp.IsZero() || requestedAt.After(status.LastGCTimestamp))
	status.StorageUsedBytes = parseRegistryMetricInt(annotations[registryStorageUsedBytesAnnotation])
	status.StorageCapacityBytes = parseRegistryMetricInt(annotations[registryStorageCapacityBytesAnnotation])
	status.UnreferencedBlobBytes = parseRegistryMetricInt(annotations[registryUnreferencedBytesAnnotation])
	status.UnreferencedBlobCount = parseRegistryMetricInt(annotations[registryUnreferencedCountAnnotation])
	status.ProtectedDigestCount = parseRegistryMetricInt(annotations[registryProtectedDigestsAnnotation])
	return status
}

func parseRegistryMetricInt(value string) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}

func truncateRegistryMaintenanceValue(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit]
}
