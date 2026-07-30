package controller

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/appimages"
	"fugue/internal/model"
	"fugue/internal/runtime"
	"fugue/internal/store"
)

const (
	runtimeClusterIDLabel       = "fugue.io/cell-cluster-id"
	runtimeClusterIDLegacyLabel = "fugue.io/cluster-id"
)

// getClusterID returns the UID of kube-system. Kubernetes has no portable
// cluster-UID endpoint; a system namespace UID is the stable identity used by
// the API observed-status calculator as well. A missing/failed read is an
// error, never an empty identity.
func (c *kubeClient) getClusterID(ctx context.Context) (string, error) {
	var raw struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	status, err := c.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/kube-system", nil, &raw)
	if err != nil {
		if status == http.StatusNotFound {
			return "", fmt.Errorf("kubernetes system namespace is missing")
		}
		return "", fmt.Errorf("read kubernetes cluster identity: %w", err)
	}
	clusterID := strings.TrimSpace(raw.Metadata.UID)
	if clusterID == "" {
		return "", fmt.Errorf("kubernetes system namespace has no UID")
	}
	return clusterID, nil
}

func (c *kubeClient) getNamespace(ctx context.Context, name string) (bool, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return false, fmt.Errorf("namespace name is empty")
	}
	status, err := c.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+url.PathEscape(name), nil, nil)
	if err != nil {
		if status == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *kubeClient) getService(ctx context.Context, namespace, name string) (bool, error) {
	namespace = c.effectiveNamespace(namespace)
	name = strings.TrimSpace(name)
	if name == "" {
		return false, fmt.Errorf("service name is empty")
	}
	status, err := c.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+url.PathEscape(namespace)+"/services/"+url.PathEscape(name), nil, nil)
	if err != nil {
		if status == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func migrationRuntimeClusterID(runtimeObj model.Runtime) string {
	for _, key := range []string{runtimeClusterIDLabel, runtimeClusterIDLegacyLabel, "cluster_id"} {
		if value := strings.TrimSpace(runtimeObj.Labels[key]); value != "" {
			return value
		}
	}
	return ""
}

func migrationEndpointReady(ctx context.Context, client *kubeClient, namespace, serviceName string) (bool, error) {
	slices, slicesAvailable, err := client.listEndpointSlicesForServiceWithAvailability(ctx, namespace, serviceName)
	if err != nil {
		return false, err
	}
	if slicesAvailable {
		ready, _ := countReadyEndpointAddresses(slices)
		// An available EndpointSlice API returning no ready addresses is an
		// authoritative not-ready result. Do not fall back to a potentially
		// stale legacy Endpoints object in that case.
		return ready > 0, nil
	}
	endpoints, found, _, err := client.getEndpointsForServiceWithAvailability(ctx, namespace, serviceName)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	ready, _ := countReadyLegacyEndpointAddresses(endpoints)
	return ready > 0, nil
}

func (s *Service) sourceClusterIDForMigration(op model.Operation, newClusterID string) (string, error) {
	if strings.TrimSpace(op.SourceRuntimeID) == "" {
		return "", fmt.Errorf("migration %s has no source runtime", op.ID)
	}
	if strings.TrimSpace(op.SourceRuntimeID) == strings.TrimSpace(op.TargetRuntimeID) {
		return strings.TrimSpace(newClusterID), nil
	}
	sourceRuntime, err := s.Store.GetRuntime(op.SourceRuntimeID)
	if err != nil {
		return "", fmt.Errorf("load source runtime %s: %w", op.SourceRuntimeID, err)
	}
	if clusterID := migrationRuntimeClusterID(sourceRuntime); clusterID != "" {
		return clusterID, nil
	}
	switch strings.TrimSpace(sourceRuntime.Type) {
	case model.RuntimeTypeManagedShared, model.RuntimeTypeManagedOwned:
		// Managed runtimes are nodes/slices of the Kubernetes cluster queried by
		// this controller. The kube-system UID read at the start of this same
		// target snapshot is therefore authoritative for both runtime identities.
		if strings.TrimSpace(newClusterID) != "" {
			return strings.TrimSpace(newClusterID), nil
		}
	}
	return "", fmt.Errorf("source runtime %s has no observed cluster identity", op.SourceRuntimeID)
}

// verifyManagedAppMigrationCutover reads all target-side evidence in one
// bounded snapshot. It does not delete or deactivate anything; callers may
// complete the operation only after this function records a verified ledger.
func (s *Service) verifyManagedAppMigrationCutover(ctx context.Context, op model.Operation, app model.App, imagePreflightVerified bool) (model.AppMigrationLedger, error) {
	client, err := s.kubeClient()
	if err != nil {
		return model.AppMigrationLedger{}, err
	}
	readCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	newClusterID, err := client.getClusterID(readCtx)
	if err != nil {
		return model.AppMigrationLedger{}, err
	}
	oldClusterID, err := s.sourceClusterIDForMigration(op, newClusterID)
	if err != nil {
		// The app observation may already come from the target cluster after the
		// target ManagedApp is created. It therefore cannot substitute for the
		// source runtime's authoritative cluster identity.
		return model.AppMigrationLedger{}, err
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	namespacePresent, err := client.getNamespace(readCtx, namespace)
	if err != nil {
		return model.AppMigrationLedger{}, fmt.Errorf("read target namespace: %w", err)
	}
	if !namespacePresent {
		return model.AppMigrationLedger{}, fmt.Errorf("target namespace %s is missing", namespace)
	}
	managed, managedFound, err := client.getManagedApp(readCtx, namespace, runtime.ManagedAppResourceName(app))
	if err != nil {
		return model.AppMigrationLedger{}, fmt.Errorf("read target ManagedApp: %w", err)
	}
	if !managedFound {
		return model.AppMigrationLedger{}, fmt.Errorf("target ManagedApp is missing")
	}
	deployment, deploymentFound, err := client.getDeployment(readCtx, namespace, runtime.RuntimeAppResourceName(app))
	if err != nil {
		return model.AppMigrationLedger{}, fmt.Errorf("read target deployment: %w", err)
	}
	if !deploymentFound {
		return model.AppMigrationLedger{}, fmt.Errorf("target deployment is missing")
	}

	desiredReplicas := app.Spec.Replicas
	// Cutover requires ReadyReplicas specifically. AvailableReplicas can lag
	// readiness during termination and must not be used as a green substitute.
	physicalReplicas := minMigrationReplicaCount(
		deployment.Status.UpdatedReplicas,
		deployment.Status.ReadyReplicas,
		deployment.Status.AvailableReplicas,
	)
	physicalCopy := physicalReplicas
	invariants := []string{}
	if strings.TrimSpace(managed.Spec.AppID) != strings.TrimSpace(app.ID) {
		invariants = append(invariants, "managed_app_identity_mismatch")
	}
	if observedRuntimeID := strings.TrimSpace(managed.Spec.AppSpec.RuntimeID); observedRuntimeID == "" || observedRuntimeID != strings.TrimSpace(op.TargetRuntimeID) {
		invariants = append(invariants, "managed_app_runtime_mismatch")
	}
	if desiredReplicas > 0 {
		if deployment.Status.UpdatedReplicas < desiredReplicas {
			invariants = append(invariants, "updated_replicas_below_desired")
		}
		if deployment.Status.AvailableReplicas < desiredReplicas {
			invariants = append(invariants, "available_replicas_below_desired")
		}
		if physicalReplicas <= 0 {
			invariants = append(invariants, "physical_replicas_zero")
		}
		if physicalReplicas < desiredReplicas {
			invariants = append(invariants, "desired_replicas_unready")
		}
	}
	if managed.Metadata.Generation <= 0 || managed.Status.ObservedGeneration < managed.Metadata.Generation {
		invariants = append(invariants, "generation_not_observed")
	}
	if deployment.Metadata.Generation <= 0 || deployment.Status.ObservedGeneration < deployment.Metadata.Generation {
		invariants = append(invariants, "deployment_generation_not_observed")
	}
	expectedImage := strings.TrimSpace(app.Spec.Image)
	actualImage := deploymentPrimaryContainerImage(deployment)
	imageStatus := model.AppMigrationEvidenceVerified
	imageResult := "target deployment template and ready replicas reference the requested image"
	managedImageRef := strings.TrimSpace(s.managedDeployImageRef(app))
	imageMatches := actualImage != "" && (expectedImage == "" || s.migrationImageRefsEquivalent(app, expectedImage, actualImage))
	if desiredReplicas <= 0 {
		imageResult = "image replication is not required while desired replicas are zero"
	} else if !imageMatches {
		imageStatus = model.AppMigrationEvidenceMissing
		imageResult = fmt.Sprintf("target image mismatch expected=%q actual=%q", expectedImage, actualImage)
		invariants = append(invariants, "image_missing")
	} else if desiredReplicas > 0 && managedImageRef != "" {
		// A matching Deployment template is not, by itself, a durable copy
		// proof: an old pod can still be running from a node-local cache. The
		// migration operation must have passed the target-image preflight, and
		// strict distributed mode additionally requires a fresh Present report
		// scoped to the target runtime/node.
		targetVerified, targetResult, targetErr := s.verifyMigrationTargetImageReplication(
			readCtx, app, op, managedImageRef, imagePreflightVerified,
		)
		if targetErr != nil {
			return model.AppMigrationLedger{}, fmt.Errorf("verify target image replication: %w", targetErr)
		}
		if !targetVerified {
			imageStatus = model.AppMigrationEvidenceMissing
			imageResult = targetResult
			invariants = append(invariants, "image_replication_unverified")
		} else {
			imageResult = targetResult
		}
	}
	objectStatus := model.AppMigrationEvidenceVerified
	objectResult := "ManagedApp and Deployment exist with current generations observed"
	if !strings.EqualFold(strings.TrimSpace(managed.Status.Phase), runtime.ManagedAppPhaseReady) {
		objectStatus = model.AppMigrationEvidenceMissing
		objectResult = "ManagedApp is not ready: " + strings.TrimSpace(managed.Status.Phase)
		invariants = append(invariants, "managed_app_unready")
	}
	endpointRequired := model.AppHasClusterService(app.Spec) || model.AppSSHEnabled(app.Spec)
	endpointStatus := model.AppMigrationEvidenceNotApplicable
	endpointResult := "app has no cluster Service endpoint"
	var endpointReady *bool
	if endpointRequired {
		serviceName := runtime.RuntimeAppServiceName(app)
		servicePresent, serviceErr := client.getService(readCtx, namespace, serviceName)
		if serviceErr != nil {
			return model.AppMigrationLedger{}, fmt.Errorf("read target service: %w", serviceErr)
		}
		if !servicePresent {
			endpointStatus = model.AppMigrationEvidenceMissing
			endpointResult = "target Service is missing"
			falseValue := false
			endpointReady = &falseValue
			invariants = append(invariants, "endpoint_missing")
		} else {
			ready, readyErr := migrationEndpointReady(readCtx, client, namespace, serviceName)
			if readyErr != nil {
				return model.AppMigrationLedger{}, fmt.Errorf("read target endpoint: %w", readyErr)
			}
			endpointReady = &ready
			if ready {
				endpointStatus = model.AppMigrationEvidenceReady
				endpointResult = "target endpoint has ready addresses"
			} else {
				endpointStatus = model.AppMigrationEvidenceMissing
				endpointResult = "target endpoint has no ready addresses"
				invariants = append(invariants, "endpoint_unready")
			}
		}
	}
	ledger := model.AppMigrationLedger{
		TenantID:               op.TenantID,
		ProjectID:              app.ProjectID,
		AppID:                  app.ID,
		OperationID:            op.ID,
		OldRuntimeID:           op.SourceRuntimeID,
		NewRuntimeID:           op.TargetRuntimeID,
		OldClusterID:           oldClusterID,
		NewClusterID:           newClusterID,
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
		Generation:             managed.Metadata.Generation,
		ObservedGeneration:     managed.Status.ObservedGeneration,
		InvariantViolations:    invariants,
		CutoverStatus:          model.AppMigrationCutoverVerified,
		OldArtifactsProtected:  true,
		EvidenceSource:         model.OperationEvidenceSourceKubernetesAPI,
		OperatorType:           "controller",
		OperatorID:             firstNonEmptyControllerString(s.leaderIdentity, "fugue-controller"),
		ObservedAt:             time.Now().UTC(),
	}
	if err := store.ValidateAppMigrationCutover(ledger); err != nil {
		ledger.CutoverStatus = model.AppMigrationCutoverBlocked
		ledger.FailureReason = err.Error()
		_, _ = s.Store.RecordAppMigrationLedger(ledger)
		return ledger, err
	}
	if _, err := s.Store.RecordAppMigrationLedger(ledger); err != nil {
		return ledger, fmt.Errorf("record migration cutover ledger: %w", err)
	}
	return ledger, nil
}

func minMigrationReplicaCount(values ...int) int {
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

// verifyMigrationTargetImageReplication separates image identity from image
// placement. A current Deployment proves which image is declared/running; the
// migration ledger also needs proof that the requested artifact is available
// on the target runtime before source cleanup is permitted.
func (s *Service) verifyMigrationTargetImageReplication(
	ctx context.Context,
	app model.App,
	op model.Operation,
	managedImageRef string,
	imagePreflightVerified bool,
) (bool, string, error) {
	if s == nil || s.Store == nil {
		return false, "target image replication evidence store is unavailable", nil
	}
	target := deployImageTarget{RuntimeID: strings.TrimSpace(op.TargetRuntimeID)}
	if target.RuntimeID != "" {
		if runtimeObj, err := s.Store.GetRuntime(target.RuntimeID); err == nil {
			target.ClusterNodeName = strings.TrimSpace(runtimeObj.ClusterNodeName)
		}
	}
	refs := []string{managedImageRef, strings.TrimSpace(app.Spec.Image)}
	locations, err := s.presentImageLocations(app, refs...)
	if err != nil {
		return false, "", err
	}
	lastCacheVerificationError := ""
	for _, location := range locations {
		if !imageLocationPresentOnTarget([]model.ImageLocation{location}, target) {
			continue
		}
		if endpoint := strings.TrimSpace(location.CacheEndpoint); endpoint != "" && s.verifyDestinationImageCache != nil {
			verification, verifyErr := s.verifyDestinationImageCache(ctx, endpoint, managedImageRef)
			if verifyErr != nil {
				lastCacheVerificationError = verifyErr.Error()
				continue
			}
			if err := validateDestinationImageCacheVerification(verification, managedImageRef); err != nil {
				lastCacheVerificationError = err.Error()
				continue
			}
			return true, "target image-cache local graph verified for the target runtime", nil
		}
		return true, "fresh target runtime image-location evidence is present", nil
	}
	if s.imageStoreStrictDistributedMode() {
		if lastCacheVerificationError != "" {
			return false, "target image-cache graph verification failed: " + lastCacheVerificationError, nil
		}
		return false, "strict distributed image store has no fresh Present replica on the target runtime", nil
	}
	if !imagePreflightVerified {
		return false, "target image preflight did not produce a verified replication result", nil
	}
	return true, "target image preflight succeeded and the current ready Deployment uses the requested image", nil
}

func (s *Service) migrationImageRefsEquivalent(app model.App, expected, actual string) bool {
	expected = strings.TrimSpace(expected)
	actual = strings.TrimSpace(actual)
	if expected == "" || actual == "" {
		return false
	}
	if expected == actual {
		return true
	}
	// Push and pull registry bases are aliases for the same managed artifact.
	// Compare their normalized identities so a migration is not blocked by a
	// legitimate controller/runtime registry hostname change.
	normalizedExpected := appimages.NormalizeRuntimeImageRefForSource(expected, model.AppBuildSource(app), s.registryPushBase, s.registryPullBase)
	normalizedActual := appimages.NormalizeRuntimeImageRefForSource(actual, model.AppBuildSource(app), s.registryPushBase, s.registryPullBase)
	return normalizedExpected != "" && normalizedExpected == normalizedActual
}

// recordManagedMigrationCompletionFailure closes the small window after a
// verified target snapshot is recorded but before the operation transaction
// commits. A failed completion must re-protect old artifacts rather than leave
// the verified snapshot as the latest retirement permission.
func (s *Service) recordManagedMigrationCompletionFailure(op model.Operation, app model.App, reason string) {
	if s == nil || s.Store == nil || op.Type != model.OperationTypeMigrate {
		return
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "migration operation completion failed"
	}
	_, err := s.Store.RecordAppMigrationLedger(model.AppMigrationLedger{
		TenantID:              op.TenantID,
		ProjectID:             app.ProjectID,
		AppID:                 app.ID,
		OperationID:           op.ID,
		OldRuntimeID:          op.SourceRuntimeID,
		NewRuntimeID:          op.TargetRuntimeID,
		ImageRef:              strings.TrimSpace(app.Spec.Image),
		DesiredReplicas:       app.Spec.Replicas,
		CutoverStatus:         model.AppMigrationCutoverFailed,
		OldArtifactsProtected: true,
		FailureReason:         reason,
		EvidenceSource:        model.OperationEvidenceSourceController,
		OperatorType:          op.RequestedByType,
		OperatorID:            op.RequestedByID,
		ObservedAt:            time.Now().UTC(),
	})
	if err != nil && s.Logger != nil {
		s.Logger.Printf("record migration completion failure for operation %s: %v", op.ID, err)
	}
}
