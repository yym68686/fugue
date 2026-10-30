package controller

import (
	"context"
	"fmt"
	"net/http"
	"reflect"

	"fugue/internal/model"
)

// A never-scheduled Pod cannot supply a CRI receipt. Cancel its failed source
// Deployment instead, then require observed zero replicas, zero ReplicaSets and
// no owned Pods before accepting the ordinary retirement transaction.
func (s *Service) captureUnstartedReleaseWorkload(ctx context.Context, client *kubeClient, app model.App, release model.AppRelease) (releaseDrainWorkload, error) {
	if release.Role != model.AppReleaseRoleCandidate || release.Status != model.AppReleaseStatusFailed || release.PromotedAt != nil {
		return releaseDrainWorkload{}, fmt.Errorf("unstarted cancellation requires an unpromoted failed candidate")
	}
	return s.captureDrainWorkloadState(ctx, client, app, release, false, false, true)
}

func (s *Service) recaptureReleaseDrainWorkload(ctx context.Context, client *kubeClient, app model.App, release model.AppRelease, observed releaseDrainWorkload) (releaseDrainWorkload, error) {
	if observed.Unstarted {
		return s.captureUnstartedReleaseWorkload(ctx, client, app, release)
	}
	return s.captureReleaseDrainWorkloadMode(ctx, client, app, release, observed.RuntimeStopped)
}

func releasePodNeverScheduled(pod map[string]any) bool {
	if objectStringField(objectMapField(pod, "spec"), "nodeName") != "" ||
		nestedObjectValue(pod, "status", "phase") != "Pending" ||
		objectStringField(objectMapField(pod, "status"), "podIP") != "" ||
		len(mapSlice(nestedObjectValue(pod, "status", "podIPs"))) != 0 ||
		len(mapSlice(nestedObjectValue(pod, "status", "containerStatuses"))) != 0 ||
		len(mapSlice(nestedObjectValue(pod, "status", "initContainerStatuses"))) != 0 ||
		len(mapSlice(nestedObjectValue(pod, "status", "ephemeralContainerStatuses"))) != 0 ||
		len(mapSlice(nestedObjectValue(pod, "spec", "ephemeralContainers"))) != 0 ||
		nestedObjectValue(pod, "spec", "hostNetwork") == true {
		return false
	}
	for _, condition := range mapSlice(nestedObjectValue(pod, "status", "conditions")) {
		if condition["type"] == "PodScheduled" && condition["status"] != "False" {
			return false
		}
	}
	return true
}

func quiescedReleaseReplicaSet(rs, metadata map[string]any) bool {
	m := objectMapField(rs, "metadata")
	if objectStringField(m, "uid") != objectStringField(metadata, "uid") || objectStringField(m, "resourceVersion") != objectStringField(metadata, "resourceVersion") || nestedObjectValue(rs, "spec", "replicas") != float64(0) {
		return false
	}
	for _, field := range []string{"replicas", "readyReplicas", "availableReplicas", "fullyLabeledReplicas"} {
		value := nestedObjectValue(rs, "status", field)
		if value != nil && value != float64(0) {
			return false
		}
	}
	generation, gok := m["generation"].(float64)
	observed, ook := nestedObjectValue(rs, "status", "observedGeneration").(float64)
	return gok && ook && generation >= 1 && observed >= generation
}

func (s *Service) quiesceUnstartedRelease(ctx context.Context, client *kubeClient, app model.App, expected model.AppRelease, policy model.AppTrafficPolicy, before releaseDrainWorkload) error {
	current, err := s.Store.GetAppRelease(app.TenantID, true, expected.ID)
	if err != nil || !sameReleaseDrainTarget(expected, current) || current.RevisionWorkload == nil {
		return fmt.Errorf("unstarted release changed before cancellation")
	}
	op, err := s.Store.GetOperation(current.RevisionWorkload.OperationID)
	if err != nil || op.AppID != app.ID || op.TenantID != app.TenantID || op.Type != model.OperationTypeDeploy || op.Status != model.OperationStatusFailed {
		return fmt.Errorf("unstarted release has no failed source deploy")
	}
	if referenced, err := s.canonicalServicesReferenceRelease(ctx, client, app, current); err != nil || referenced {
		return fmt.Errorf("unstarted release still has a canonical Service reference or its absence is unconfirmed")
	}
	target := current
	target.DeploymentName, target.ServiceName = current.RevisionWorkload.DeploymentName, current.RevisionWorkload.ServiceName
	after, err := s.captureUnstartedReleaseWorkload(ctx, client, app, target)
	if err != nil || !reflect.DeepEqual(before, after) {
		return fmt.Errorf("unstarted workload changed before cancellation")
	}
	freshPolicy, err := s.Store.GetAppTrafficPolicy(app.TenantID, true, app.ID)
	if err != nil || !reflect.DeepEqual(policy, freshPolicy) {
		return fmt.Errorf("traffic policy changed before unstarted cancellation")
	}
	// Do not retry a version conflict: even a status update could mean that a
	// scheduler won the race. Reobserve the complete workload on the next scan.
	path := deploymentAPIPath(current.RevisionWorkload.Namespace, target.DeploymentName)
	_, err = client.doRequest(ctx, http.MethodPatch, path, "application/merge-patch+json", map[string]any{
		"metadata": map[string]any{"uid": before.DeploymentUID, "resourceVersion": before.DeploymentVersion},
		"spec":     map[string]any{"replicas": 0},
	}, nil)
	if err != nil {
		return fmt.Errorf("cancel unstarted release: %w", err)
	}
	return fmt.Errorf("unstarted release cancelled; awaiting observed zero replicas and Pod removal")
}
