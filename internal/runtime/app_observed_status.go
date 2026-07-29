package runtime

import (
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	AppObservationSourceKubernetesAPI = "kubernetes_api"

	AppObservationReasonManagedAppReady            = "managed_app_ready"
	AppObservationReasonManagedAppProgressing      = "managed_app_progressing"
	AppObservationReasonManagedAppDisabled         = "managed_app_disabled"
	AppObservationReasonManagedAppDeleting         = "managed_app_deleting"
	AppObservationReasonManagedAppError            = "managed_app_error"
	AppObservationReasonManagedAppStatusEmpty      = "managed_app_status_empty"
	AppObservationReasonManagedAppNotFound         = "managed_app_not_found"
	AppObservationReasonGenerationNotObserved      = "generation_not_observed"
	AppObservationReasonClusterIdentityMissing     = "cluster_identity_missing"
	AppObservationReasonKubernetesQueryFailed      = "kubernetes_query_failed"
	AppObservationReasonObservationStale           = "observation_stale"
	AppObservationReasonDesiredReplicasZero        = "desired_replicas_zero"
	AppObservationReasonRuntimeObservationNotReady = "runtime_observation_not_ready"
)

// AppRuntimeObservation is the complete evidence envelope used by the single
// app observed-status calculator. Complete means the Kubernetes request
// succeeded authoritatively; Found then differentiates an object from a 404 or
// an item absent from a complete list. A failed request must set Complete=false
// and never masquerades as an absent runtime object.
type AppRuntimeObservation struct {
	ManagedApp     ManagedAppObject
	Found          bool
	Complete       bool
	Fresh          bool
	ObservedAt     time.Time
	ClusterID      string
	EvidenceSource string
	ErrorMessage   string
}

func CalculateAppObservedStatus(app model.App, evidence AppRuntimeObservation) model.AppObservedStatus {
	observedAt := evidence.ObservedAt.UTC()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
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
		Reason:          AppObservationReasonRuntimeObservationNotReady,
	}

	if !evidence.Complete {
		status.Fresh = false
		status.Reason = AppObservationReasonKubernetesQueryFailed
		status.Message = firstObservedStatusMessage(evidence.ErrorMessage, "live runtime observation is unavailable")
		return status
	}
	if strings.TrimSpace(evidence.ClusterID) == "" {
		status.Fresh = false
		status.Reason = AppObservationReasonClusterIdentityMissing
		status.Message = firstObservedStatusMessage(evidence.ErrorMessage, "live runtime observation has no cluster identity")
		return status
	}

	present := evidence.Found
	status.RuntimeObjectPresent = &present
	if app.Spec.Replicas == 0 {
		ready := 0
		status.Phase = "disabled"
		status.ReadyReplicas = &ready
		status.Reason = AppObservationReasonDesiredReplicasZero
		status.Message = "desired replicas is 0"
		return status
	}
	if !evidence.Found {
		ready := 0
		status.Phase = "unavailable"
		status.ReadyReplicas = &ready
		status.Reason = AppObservationReasonManagedAppNotFound
		status.Message = "managed app runtime object not found"
		return status
	}

	managed := evidence.ManagedApp
	status.Generation = managed.Metadata.Generation
	status.ObservedGeneration = managed.Status.ObservedGeneration
	ready := managed.Status.ReadyReplicas
	status.ReadyReplicas = &ready
	if managed.Status.DesiredReplicas > 0 {
		status.DesiredReplicas = managed.Status.DesiredReplicas
	}
	status.Message = strings.TrimSpace(managed.Status.Message)

	if !evidence.Fresh {
		status.Phase = "unknown"
		status.Fresh = false
		status.Reason = AppObservationReasonObservationStale
		status.Message = firstObservedStatusMessage(evidence.ErrorMessage, "runtime observation is stale")
		return status
	}
	if managed.Metadata.Generation > 0 && managed.Status.ObservedGeneration < managed.Metadata.Generation {
		status.Phase = "unknown"
		status.Fresh = false
		status.Reason = AppObservationReasonGenerationNotObserved
		status.Message = firstObservedStatusMessage(status.Message, "runtime controller has not observed the current generation")
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
	return status
}

// ApplyAppObservedStatus keeps the durable status available to API consumers
// while projecting the observation into the legacy status field for backward
// compatibility. Unknown evidence never reuses a historical replica count as
// if it were a current observation.
func ApplyAppObservedStatus(app model.App, observed model.AppObservedStatus) model.App {
	out := app
	stored := app.Status
	stored.SourceSync = model.CloneAppSourceSyncStatus(app.Status.SourceSync)
	out.StoredStatus = &stored
	out.ObservedStatus = &observed
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

func firstObservedStatusMessage(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
