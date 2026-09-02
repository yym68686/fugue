package runtime

import (
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	AppObservationSourceKubernetesAPI = "kubernetes_api"

	AppObservationReasonManagedAppReady             = "managed_app_ready"
	AppObservationReasonManagedAppProgressing       = "managed_app_progressing"
	AppObservationReasonManagedAppDisabled          = "managed_app_disabled"
	AppObservationReasonManagedAppDeleting          = "managed_app_deleting"
	AppObservationReasonManagedAppError             = "managed_app_error"
	AppObservationReasonManagedAppErrorLKGServing   = "managed_app_error_lkg_serving"
	AppObservationReasonManagedAppStatusEmpty       = "managed_app_status_empty"
	AppObservationReasonManagedAppNotFound          = "managed_app_not_found"
	AppObservationReasonGenerationNotObserved       = "generation_not_observed"
	AppObservationReasonClusterIdentityMissing      = "cluster_identity_missing"
	AppObservationReasonKubernetesQueryFailed       = "kubernetes_query_failed"
	AppObservationReasonObservationStale            = "observation_stale"
	AppObservationReasonObservationTimestampMissing = "observation_timestamp_missing"
	AppObservationReasonDesiredReplicasZero         = "desired_replicas_zero"
	AppObservationReasonRuntimeObservationNotReady  = "runtime_observation_not_ready"
)

// AppRuntimeObservation is the complete evidence envelope used by the single
// app observed-status calculator. Complete means the Kubernetes request
// succeeded authoritatively; Found then differentiates an object from a 404 or
// an item absent from a complete list. A failed request must set Complete=false
// and never masquerades as an absent runtime object.
type AppRuntimeObservation struct {
	ManagedApp              ManagedAppObject
	Found                   bool
	Complete                bool
	Fresh                   bool
	ObservedAt              time.Time
	ClusterID               string
	EvidenceSource          string
	EvidenceSources         []string
	NamespacePresent        *bool
	ServicePresent          *bool
	EndpointPresent         *bool
	EndpointReady           *bool
	PhysicalReplicas        *int
	PhysicalDesiredReplicas *int
	ImagePresent            *bool
	ImageRef                string
	InvariantViolations     []string
	ErrorMessage            string
}

func CalculateAppObservedStatus(app model.App, evidence AppRuntimeObservation) model.AppObservedStatus {
	observedAt := evidence.ObservedAt.UTC()
	source := strings.TrimSpace(evidence.EvidenceSource)
	if source == "" {
		source = AppObservationSourceKubernetesAPI
	}
	status := model.AppObservedStatus{
		Phase:           "unknown",
		RuntimeID:       strings.TrimSpace(app.Spec.RuntimeID),
		DesiredReplicas: app.Spec.Replicas,
		Fresh:           evidence.Complete && evidence.Fresh,
		ObservedAt:      observedAt,
		ClusterID:       strings.TrimSpace(evidence.ClusterID),
		EvidenceSource:  source,
		EvidenceSources: append([]string(nil), evidence.EvidenceSources...),
		Reason:          AppObservationReasonRuntimeObservationNotReady,
	}
	if len(status.EvidenceSources) == 0 {
		status.EvidenceSources = []string{source}
	}

	if !evidence.Complete {
		status.Fresh = false
		status.Reason = AppObservationReasonKubernetesQueryFailed
		status.Message = firstObservedStatusMessage(evidence.ErrorMessage, "live runtime observation is unavailable")
		return status
	}
	if evidence.ObservedAt.IsZero() {
		status.Fresh = false
		status.Reason = AppObservationReasonObservationTimestampMissing
		status.Message = firstObservedStatusMessage(evidence.ErrorMessage, "live runtime observation has no timestamp")
		return status
	}
	if strings.TrimSpace(evidence.ClusterID) == "" {
		status.Fresh = false
		status.Reason = AppObservationReasonClusterIdentityMissing
		status.Message = firstObservedStatusMessage(evidence.ErrorMessage, "live runtime observation has no cluster identity")
		return status
	}
	if !evidence.Fresh {
		status.Phase = "unknown"
		status.Fresh = false
		status.Reason = AppObservationReasonObservationStale
		status.Message = firstObservedStatusMessage(evidence.ErrorMessage, "runtime observation is stale")
		return status
	}

	present := evidence.Found
	status.RuntimeObjectPresent = &present
	status.NamespacePresent = cloneBoolPointer(evidence.NamespacePresent)
	status.ServicePresent = cloneBoolPointer(evidence.ServicePresent)
	status.EndpointPresent = cloneBoolPointer(evidence.EndpointPresent)
	status.EndpointReady = cloneBoolPointer(evidence.EndpointReady)
	status.PhysicalReplicas = cloneIntPointer(evidence.PhysicalReplicas)
	status.PhysicalDesired = cloneIntPointer(evidence.PhysicalDesiredReplicas)
	status.ImagePresent = cloneBoolPointer(evidence.ImagePresent)
	status.ImageRef = strings.TrimSpace(evidence.ImageRef)
	status.InvariantViolations = append([]string(nil), evidence.InvariantViolations...)
	if !evidence.Found {
		ready := 0
		status.Phase = "unavailable"
		status.ReadyReplicas = &ready
		status.Reason = AppObservationReasonManagedAppNotFound
		status.Message = "managed app runtime object not found"
		status.InvariantViolations = observedStatusInvariantViolations(status, app)
		return status
	}
	if app.Spec.Replicas == 0 {
		ready := 0
		status.Phase = "disabled"
		status.ReadyReplicas = &ready
		status.Reason = AppObservationReasonDesiredReplicasZero
		status.Message = "desired replicas is 0"
		return status
	}

	managed := evidence.ManagedApp
	// Once the object is found, its runtime identity is the observed value. The
	// durable app spec may still describe the source runtime while a migration
	// is in flight, so projecting the stored runtime here would mix desired and
	// observed state and make cluster evidence look authoritative for the wrong
	// runtime.
	if runtimeID := strings.TrimSpace(managed.Spec.AppSpec.RuntimeID); runtimeID != "" {
		status.RuntimeID = runtimeID
	}
	status.Generation = managed.Metadata.Generation
	status.ObservedGeneration = managed.Status.ObservedGeneration
	ready := managed.Status.ReadyReplicas
	if evidence.PhysicalReplicas != nil {
		ready = *evidence.PhysicalReplicas
	}
	status.ReadyReplicas = &ready
	// desired_replicas belongs to the durable app spec. The Kubernetes
	// controller's own desired count is exposed separately as
	// physical_desired_replicas so a lagging/foreign ManagedApp cannot rewrite
	// the control-plane desired state in the observed contract.
	status.Message = strings.TrimSpace(managed.Status.Message)
	// Record independently observed invariant failures even when the
	// controller has not yet acknowledged the current generation. Generation
	// lag makes the overall phase unknown, but must not hide a zero-replica,
	// missing-image, or missing-endpoint signal from operators.
	status.InvariantViolations = observedStatusInvariantViolations(status, app)

	if managed.Metadata.Generation <= 0 || managed.Status.ObservedGeneration < managed.Metadata.Generation {
		status.Phase = "unknown"
		status.Fresh = false
		status.Reason = AppObservationReasonGenerationNotObserved
		if managed.Metadata.Generation <= 0 {
			status.Message = "runtime object generation is unavailable"
		} else {
			status.Message = firstObservedStatusMessage(status.Message, "runtime controller has not observed the current generation")
		}
		return status
	}
	switch strings.TrimSpace(managed.Status.Phase) {
	case ManagedAppPhaseReady:
		status.Phase = "deployed"
		status.Reason = AppObservationReasonManagedAppReady
	case ManagedAppPhaseDisabled:
		status.Phase = "disabled"
		status.Reason = AppObservationReasonManagedAppDisabled
	case ManagedAppPhaseDeleting:
		status.Phase = "deleting"
		status.Reason = AppObservationReasonManagedAppDeleting
	case ManagedAppPhaseError:
		status.Phase = "failed"
		status.Reason = AppObservationReasonManagedAppError
	case ManagedAppPhasePending, ManagedAppPhaseProgressing:
		status.Phase = "deploying"
		status.Reason = AppObservationReasonManagedAppProgressing
	default:
		status.Phase = "unknown"
		status.Fresh = false
		status.Reason = AppObservationReasonManagedAppStatusEmpty
		status.Message = firstObservedStatusMessage(status.Message, "managed app status is empty")
	}
	// A failed preflight or automatic maintenance operation can leave the
	// previous release serving while the ManagedApp status records the failed
	// attempt. Preserve that LKG for runtime consumers only when the durable
	// status was deployed and this observation independently proves that the
	// current cohort is still healthy. The failure reason/message remains
	// visible in the observed contract and durable last-failure history.
	if status.Phase == "failed" && appObservedServingLKG(app, status) {
		status.Phase = "deployed"
		status.Reason = AppObservationReasonManagedAppErrorLKGServing
	}
	// A ready ManagedApp is publishable only when the independent physical,
	// image, namespace, and endpoint evidence also passes. Preserve explicit
	// controller error/progress phases for diagnosis while still making a
	// previously green/deployed state fail closed.
	if len(status.InvariantViolations) > 0 && (status.Phase == "deployed" || (status.Phase == "disabled" && app.Spec.Replicas > 0)) {
		status.Phase = "unavailable"
		status.Reason = AppObservationReasonRuntimeObservationNotReady
		status.Message = firstObservedStatusMessage(status.Message, strings.Join(status.InvariantViolations, "; "))
	}
	return status
}

func appObservedServingLKG(app model.App, observed model.AppObservedStatus) bool {
	stored := app.Status
	if app.StoredStatus != nil {
		stored = *app.StoredStatus
	}
	if !appHasDurableServingLKG(stored, app.Spec.Replicas) || app.Spec.Replicas <= 0 ||
		!observed.Fresh || observed.DesiredReplicas != app.Spec.Replicas ||
		observed.Generation <= 0 || observed.ObservedGeneration < observed.Generation ||
		len(observed.InvariantViolations) > 0 {
		return false
	}
	if !boolPointerIsTrue(observed.RuntimeObjectPresent) ||
		!boolPointerIsTrue(observed.NamespacePresent) ||
		!boolPointerIsTrue(observed.ImagePresent) ||
		!intPointerAtLeast(observed.ReadyReplicas, app.Spec.Replicas) ||
		!intPointerAtLeast(observed.PhysicalReplicas, app.Spec.Replicas) ||
		!intPointerAtLeast(observed.PhysicalDesired, app.Spec.Replicas) {
		return false
	}
	if model.AppHasClusterService(app.Spec) &&
		(!boolPointerIsTrue(observed.ServicePresent) || !boolPointerIsTrue(observed.EndpointPresent) || !boolPointerIsTrue(observed.EndpointReady)) {
		return false
	}
	return true
}

func appHasDurableServingLKG(status model.AppStatus, desiredReplicas int) bool {
	if strings.EqualFold(strings.TrimSpace(status.Phase), "deployed") {
		return true
	}
	// A failed operation intentionally invalidates the durable green phase
	// until the runtime observer proves what is still serving. The retained
	// ready timestamp and replica count identify an earlier completed release;
	// they are never sufficient without the fresh physical checks above.
	return strings.EqualFold(strings.TrimSpace(status.Phase), "unknown") &&
		desiredReplicas > 0 &&
		status.CurrentReleaseReadyAt != nil &&
		status.CurrentReplicas >= desiredReplicas &&
		model.AppHasCurrentFailedOperation(status)
}

func cloneBoolPointer(in *bool) *bool {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneIntPointer(in *int) *int {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func observedStatusInvariantViolations(status model.AppObservedStatus, app model.App) []string {
	violations := append([]string(nil), status.InvariantViolations...)
	appendUnique := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		for _, existing := range violations {
			if existing == value {
				return
			}
		}
		violations = append(violations, value)
	}
	if status.NamespacePresent != nil && !*status.NamespacePresent {
		appendUnique("namespace_missing")
	}
	if status.RuntimeObjectPresent != nil && !*status.RuntimeObjectPresent {
		appendUnique("managed_app_missing")
	}
	if status.ServicePresent != nil && !*status.ServicePresent {
		appendUnique("service_missing")
	}
	if status.EndpointPresent != nil && !*status.EndpointPresent {
		appendUnique("endpoint_missing")
	}
	if status.EndpointReady != nil && !*status.EndpointReady {
		appendUnique("endpoint_unready")
	}
	storedPhase := app.Status.Phase
	if app.StoredStatus != nil {
		storedPhase = app.StoredStatus.Phase
	}
	if strings.EqualFold(strings.TrimSpace(storedPhase), "deployed") &&
		(boolPointerIsFalse(status.RuntimeObjectPresent) ||
			boolPointerIsFalse(status.NamespacePresent) ||
			boolPointerIsFalse(status.ServicePresent) ||
			boolPointerIsFalse(status.EndpointPresent) ||
			boolPointerIsFalse(status.EndpointReady)) {
		appendUnique("stored_deployed_without_runtime_evidence")
	}
	if app.Spec.Replicas <= 0 {
		return violations
	}
	if status.ImagePresent != nil && !*status.ImagePresent {
		appendUnique("image_missing")
	}
	if status.PhysicalReplicas != nil && *status.PhysicalReplicas <= 0 {
		appendUnique("physical_replicas_zero")
	}
	if status.PhysicalDesired != nil && *status.PhysicalDesired < status.DesiredReplicas {
		appendUnique("physical_desired_replicas_below_desired")
	}
	if status.DesiredReplicas > 0 && status.ReadyReplicas != nil && *status.ReadyReplicas == 0 {
		appendUnique("desired_replicas_unready")
	}
	if strings.EqualFold(strings.TrimSpace(storedPhase), "deployed") &&
		((status.ReadyReplicas != nil && *status.ReadyReplicas == 0) ||
			(status.PhysicalReplicas != nil && *status.PhysicalReplicas == 0)) {
		appendUnique("stored_deployed_without_runtime_evidence")
	}
	return violations
}

func boolPointerIsFalse(value *bool) bool {
	return value != nil && !*value
}

func boolPointerIsTrue(value *bool) bool {
	return value != nil && *value
}

func intPointerAtLeast(value *int, minimum int) bool {
	return value != nil && *value >= minimum
}

// ApplyAppObservedStatus keeps the durable status available to API consumers
// while projecting the observation into the legacy status field for backward
// compatibility. Unknown evidence never reuses a historical replica count as
// if it were a current observation.
func ApplyAppObservedStatus(app model.App, observed model.AppObservedStatus) model.App {
	out := app
	stored := cloneObservedAppStatus(app.Status)
	if app.StoredStatus != nil {
		// Re-overlaying an already projected app must not promote the previous
		// observed projection into durable stored state.
		stored = cloneObservedAppStatus(*app.StoredStatus)
	}
	if stored.LastFailedOperation != nil {
		failure := *stored.LastFailedOperation
		if stored.LastFailedOperation.CompletedAt != nil {
			completedAt := stored.LastFailedOperation.CompletedAt.UTC()
			failure.CompletedAt = &completedAt
		}
		stored.LastFailedOperation = &failure
	}
	out.StoredStatus = &stored
	out.ObservedStatus = &observed
	if out.Status.LastFailedOperation == nil && stored.LastFailedOperation != nil {
		failure := *stored.LastFailedOperation
		if stored.LastFailedOperation.CompletedAt != nil {
			completedAt := stored.LastFailedOperation.CompletedAt.UTC()
			failure.CompletedAt = &completedAt
		}
		out.Status.LastFailedOperation = &failure
	}
	out.Status.Phase = observed.Phase
	out.Status.CurrentRuntimeID = observed.RuntimeID
	if observed.ReadyReplicas != nil {
		out.Status.CurrentReplicas = *observed.ReadyReplicas
	} else {
		out.Status.CurrentReplicas = 0
	}
	out.Status.UpdatedAt = observed.ObservedAt
	if strings.TrimSpace(observed.Message) != "" {
		out.Status.LastMessage = strings.TrimSpace(observed.Message)
	}
	return out
}

func cloneObservedAppStatus(in model.AppStatus) model.AppStatus {
	out := in
	out.SourceSync = model.CloneAppSourceSyncStatus(in.SourceSync)
	if in.LastFailedOperation != nil {
		failure := *in.LastFailedOperation
		if in.LastFailedOperation.CompletedAt != nil {
			completedAt := in.LastFailedOperation.CompletedAt.UTC()
			failure.CompletedAt = &completedAt
		}
		out.LastFailedOperation = &failure
	}
	return out
}

func firstObservedStatusMessage(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
