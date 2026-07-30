package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *AgentService) kubectlJSON(ctx context.Context, args ...string) (map[string]any, error) {
	runner := s.CommandRunner
	if runner == nil {
		runner = defaultCommandRunner
	}
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	output, err := runner(readCtx, "kubectl", args...)
	if err != nil {
		return nil, fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	var object map[string]any
	if err := json.Unmarshal(output, &object); err != nil {
		return nil, fmt.Errorf("decode kubectl %s response: %w", strings.Join(args, " "), err)
	}
	return object, nil
}

func (s *AgentService) agentClusterID(ctx context.Context) (string, error) {
	object, err := s.kubectlJSON(ctx, "get", "namespace", "kube-system", "--output", "json")
	if err != nil {
		return "", err
	}
	clusterID := strings.TrimSpace(agentNestedString(object, "metadata", "uid"))
	if clusterID == "" {
		return "", fmt.Errorf("kube-system namespace has no UID")
	}
	return clusterID, nil
}

func agentReadyEndpoint(ctx context.Context, s *AgentService, app model.App) (bool, error) {
	serviceName := RuntimeAppServiceName(app)
	namespace := NamespaceForTenant(app.TenantID)
	// EndpointSlice is the current Kubernetes API. Fall back to the legacy
	// Endpoints object only when the API group is genuinely unavailable; a
	// malformed/failed slice query must not be treated as proof of readiness.
	slices, sliceErr := s.kubectlJSON(ctx, "get", "endpointslices", "--selector", "kubernetes.io/service-name="+serviceName, "--namespace", namespace, "--output", "json")
	if sliceErr == nil {
		ready, present := agentReadyEndpointSlices(slices)
		if present {
			return ready, nil
		}
		return false, nil
	}
	if !agentKubernetesNotFound(sliceErr) {
		return false, sliceErr
	}
	object, err := s.kubectlJSON(ctx, "get", "endpoints", serviceName, "--namespace", namespace, "--output", "json")
	if err != nil {
		return false, err
	}
	for _, subset := range agentObjectSlice(agentNestedValue(object, "subsets")) {
		for _, address := range agentObjectSlice(subset["addresses"]) {
			if strings.TrimSpace(fmt.Sprint(address["ip"])) != "" {
				return true, nil
			}
		}
	}
	return false, nil
}

func agentReadyEndpointSlices(object map[string]any) (ready, present bool) {
	items := agentObjectSlice(agentNestedValue(object, "items"))
	for _, item := range items {
		present = true
		for _, endpoint := range agentObjectSlice(item["endpoints"]) {
			addresses := agentObjectSlice(endpoint["addresses"])
			conditions, _ := endpoint["conditions"].(map[string]any)
			isReady, hasReady := conditions["ready"].(bool)
			if hasReady && isReady && len(addresses) > 0 {
				ready = true
			}
		}
	}
	return ready, present
}

func agentKubernetesNotFound(err error) bool {
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "notfound") || strings.Contains(message, "not found") || strings.Contains(message, "404")
}

func (s *AgentService) collectAgentMigrationLedger(ctx context.Context, op model.Operation, desired model.App, sourceClusterID string, imagePreflightVerified bool) (model.AppMigrationLedger, error) {
	clusterID, err := s.agentClusterID(ctx)
	if err != nil {
		return model.AppMigrationLedger{}, err
	}
	namespace := NamespaceForTenant(desired.TenantID)
	if _, err := s.kubectlJSON(ctx, "get", "namespace", namespace, "--output", "json"); err != nil {
		return model.AppMigrationLedger{}, fmt.Errorf("target namespace: %w", err)
	}
	managed, err := s.kubectlJSON(ctx, "get", "managedapps.fugue.pro", ManagedAppResourceName(desired), "--namespace", namespace, "--output", "json")
	if err != nil {
		return model.AppMigrationLedger{}, fmt.Errorf("target ManagedApp: %w", err)
	}
	deployment, err := s.kubectlJSON(ctx, "get", "deployment", RuntimeAppResourceName(desired), "--namespace", namespace, "--output", "json")
	if err != nil {
		return model.AppMigrationLedger{}, fmt.Errorf("target Deployment: %w", err)
	}

	desiredReplicas := desired.Spec.Replicas
	// AvailableReplicas is not a readiness proof during termination; the
	// migration gate intentionally uses the explicit readyReplicas field.
	updatedReplicas := agentNestedInt(deployment, "status", "updatedReplicas")
	readyReplicas := agentNestedInt(deployment, "status", "readyReplicas")
	availableReplicas := agentNestedInt(deployment, "status", "availableReplicas")
	physical := minAgentMigrationReplicaCount(updatedReplicas, readyReplicas, availableReplicas)
	deploymentGeneration := int64(agentNestedInt(deployment, "metadata", "generation"))
	deploymentObservedGeneration := int64(agentNestedInt(deployment, "status", "observedGeneration"))
	managedGeneration := int64(agentNestedInt(managed, "metadata", "generation"))
	managedObservedGeneration := int64(agentNestedInt(managed, "status", "observedGeneration"))
	generation, observedGeneration, invariants := agentMigrationGenerationEvidence(
		managedGeneration,
		managedObservedGeneration,
		deploymentGeneration,
		deploymentObservedGeneration,
	)
	if observedAppID := strings.TrimSpace(agentNestedString(managed, "spec", "appID")); observedAppID == "" || observedAppID != strings.TrimSpace(desired.ID) {
		invariants = append(invariants, "managed_app_identity_mismatch")
	}
	if observedRuntimeID := strings.TrimSpace(agentNestedString(managed, "spec", "appSpec", "runtime_id")); observedRuntimeID == "" || observedRuntimeID != strings.TrimSpace(op.TargetRuntimeID) {
		invariants = append(invariants, "managed_app_runtime_mismatch")
	}
	if desiredReplicas > 0 && physical < desiredReplicas {
		invariants = append(invariants, "desired_replicas_unready")
	}
	if desiredReplicas > 0 && updatedReplicas < desiredReplicas {
		invariants = append(invariants, "updated_replicas_below_desired")
	}
	if desiredReplicas > 0 && availableReplicas < desiredReplicas {
		invariants = append(invariants, "available_replicas_below_desired")
	}
	if desiredReplicas > 0 && physical <= 0 {
		invariants = append(invariants, "physical_replicas_zero")
	}
	expectedImage := strings.TrimSpace(desired.Spec.Image)
	actualImage := ""
	for _, container := range agentObjectSlice(agentNestedValue(deployment, "spec", "template", "spec", "containers")) {
		if image := strings.TrimSpace(fmt.Sprint(container["image"])); image != "" {
			actualImage = image
			break
		}
	}
	imageStatus, imageResult, imageInvariants := agentMigrationImageReplicationEvidence(
		desiredReplicas, expectedImage, actualImage, imagePreflightVerified,
	)
	invariants = append(invariants, imageInvariants...)
	objectStatus := model.AppMigrationEvidenceVerified
	objectResult := "ManagedApp and Deployment exist with current generations observed"
	if !strings.EqualFold(strings.TrimSpace(agentNestedString(managed, "status", "phase")), ManagedAppPhaseReady) {
		objectStatus = model.AppMigrationEvidenceMissing
		objectResult = "ManagedApp is not Ready"
		invariants = append(invariants, "managed_app_unready")
	}
	endpointRequired := model.AppHasClusterService(desired.Spec) || model.AppSSHEnabled(desired.Spec)
	endpointStatus := model.AppMigrationEvidenceNotApplicable
	endpointResult := "app has no cluster Service endpoint"
	var endpointReady *bool
	if endpointRequired {
		if _, err := s.kubectlJSON(ctx, "get", "service", RuntimeAppServiceName(desired), "--namespace", namespace, "--output", "json"); err != nil {
			return model.AppMigrationLedger{}, fmt.Errorf("target Service: %w", err)
		}
		ready, err := agentReadyEndpoint(ctx, s, desired)
		if err != nil {
			return model.AppMigrationLedger{}, fmt.Errorf("target Endpoint: %w", err)
		}
		endpointReady = &ready
		if ready {
			endpointStatus = model.AppMigrationEvidenceReady
			endpointResult = "target Endpoint has ready addresses"
		} else {
			endpointStatus = model.AppMigrationEvidenceMissing
			endpointResult = "target Endpoint has no ready addresses"
			invariants = append(invariants, "endpoint_unready")
		}
	}
	oldClusterID := ""
	if strings.TrimSpace(op.SourceRuntimeID) == strings.TrimSpace(op.TargetRuntimeID) {
		oldClusterID = clusterID
	} else if strings.TrimSpace(sourceClusterID) != "" {
		oldClusterID = strings.TrimSpace(sourceClusterID)
	}
	if oldClusterID == "" {
		return model.AppMigrationLedger{}, fmt.Errorf("source cluster identity is not present in the migration task")
	}
	physicalCopy := physical
	ledger := model.AppMigrationLedger{
		TenantID:               op.TenantID,
		ProjectID:              desired.ProjectID,
		AppID:                  desired.ID,
		OperationID:            op.ID,
		OldRuntimeID:           op.SourceRuntimeID,
		NewRuntimeID:           op.TargetRuntimeID,
		OldClusterID:           oldClusterID,
		NewClusterID:           clusterID,
		ImageRef:               expectedImage,
		ImageReplicationStatus: imageStatus,
		ImageReplicationResult: imageResult,
		RuntimeObjectStatus:    objectStatus,
		RuntimeObjectResult:    objectResult,
		EndpointRequired:       endpointRequired,
		EndpointStatus:         endpointStatus,
		EndpointResult:         endpointResult,
		EndpointReady:          endpointReady,
		PhysicalReplicas:       &physicalCopy,
		DesiredReplicas:        desiredReplicas,
		Generation:             generation,
		ObservedGeneration:     observedGeneration,
		InvariantViolations:    invariants,
		CutoverStatus:          model.AppMigrationCutoverVerified,
		OldArtifactsProtected:  true,
		EvidenceSource:         "runtime_agent_kubernetes_api",
		OperatorType:           "runtime-agent",
		OperatorID:             firstNonEmpty(s.Config.RuntimeID, "runtime-agent"),
		ObservedAt:             time.Now().UTC(),
	}
	if len(invariants) > 0 {
		return ledger, fmt.Errorf("migration target invariants failed: %s", strings.Join(invariants, ", "))
	}
	return ledger, nil
}

func minAgentMigrationReplicaCount(values ...int) int {
	if len(values) == 0 {
		return 0
	}
	minimum := values[0]
	for _, value := range values[1:] {
		if value < minimum {
			minimum = value
		}
	}
	if minimum < 0 {
		return 0
	}
	return minimum
}

func agentMigrationGenerationEvidence(
	managedGeneration,
	managedObservedGeneration,
	deploymentGeneration,
	deploymentObservedGeneration int64,
) (int64, int64, []string) {
	invariants := []string{}
	if managedGeneration <= 0 || managedObservedGeneration < managedGeneration {
		invariants = append(invariants, "generation_not_observed")
	}
	if deploymentGeneration <= 0 || deploymentObservedGeneration < deploymentGeneration {
		invariants = append(invariants, "deployment_generation_not_observed")
	}
	// The ledger's generation fields describe the ManagedApp, matching the
	// public observed-status contract. Deployment generation is an independent
	// counter and is represented by its invariant instead of being compared to
	// the ManagedApp counter.
	return managedGeneration, managedObservedGeneration, invariants
}

func agentMigrationImageReplicationEvidence(desiredReplicas int, expectedImage, actualImage string, imagePreflightVerified bool) (string, string, []string) {
	if desiredReplicas <= 0 {
		return model.AppMigrationEvidenceVerified, "image replication is not required while desired replicas are zero", nil
	}
	status := model.AppMigrationEvidenceVerified
	result := "target runtime image pull succeeded and the Deployment template contains the requested image"
	invariants := []string{}
	if !imagePreflightVerified {
		status = model.AppMigrationEvidenceMissing
		result = "target runtime did not complete an image pull during this migration operation"
		invariants = append(invariants, "image_replication_unverified")
	}
	if actualImage == "" || (expectedImage != "" && actualImage != expectedImage) {
		status = model.AppMigrationEvidenceMissing
		result = fmt.Sprintf("target image mismatch expected=%q actual=%q", expectedImage, actualImage)
		invariants = append(invariants, "image_missing")
	}
	return status, result, invariants
}
