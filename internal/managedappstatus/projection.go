package managedappstatus

import (
	"encoding/json"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// Project applies one observation and reports whether its revision requires a refresh.
func Project(app model.App, entry AppSnapshot, runtimeByID map[string]model.Runtime, fresh bool, errorMessage string) (model.App, bool) {
	if !entry.Found && !mayUseManagedRuntime(app, runtimeByID) {
		return app, false
	}
	complete := entry.Ok
	revisionChanged := complete && entry.Evidence.AppObservationKey != "" && entry.Evidence.AppObservationKey != ObservationKey(app)
	if revisionChanged {
		complete = false
		fresh = false
		errorMessage = "cached runtime observation belongs to a different app revision"
	}
	if !complete && strings.TrimSpace(errorMessage) == "" {
		errorMessage = "live runtime observation is unavailable"
	}
	managed := entry.Managed
	if complete && fresh && entry.Evidence.ServingReleaseReady {
		if managed.Metadata.Generation <= 0 {
			managed.Metadata.Generation = 1
		}
		managed.Status.ObservedGeneration = managed.Metadata.Generation
		managed.Status.Phase = runtime.ManagedAppPhaseReady
		managed.Status.DesiredReplicas = app.Spec.Replicas
		managed.Status.ReadyReplicas = app.Spec.Replicas
		managed.Status.Message = "stable release " + entry.Evidence.ServingReleaseID + " is serving the desired image"
	}
	observed := runtime.CalculateAppObservedStatus(app, runtime.AppRuntimeObservation{
		ManagedApp:              managed,
		Found:                   entry.Found,
		Complete:                complete,
		Fresh:                   fresh && complete,
		ObservedAt:              entry.RefreshedAt,
		ClusterID:               entry.ClusterID,
		EvidenceSource:          runtime.AppObservationSourceKubernetesAPI,
		EvidenceSources:         entry.Evidence.EvidenceSources,
		NamespacePresent:        entry.Evidence.NamespacePresent,
		ServicePresent:          entry.Evidence.ServicePresent,
		EndpointPresent:         entry.Evidence.EndpointPresent,
		EndpointReady:           entry.Evidence.EndpointReady,
		PhysicalReplicas:        entry.Evidence.PhysicalReplicas,
		ServingReplicas:         entry.Evidence.ServingReplicas,
		PhysicalDesiredReplicas: entry.Evidence.PhysicalDesiredReplicas,
		ImagePresent:            entry.Evidence.ImagePresent,
		ImageRef:                entry.Evidence.ImageRef,
		CurrentRuntimeID:        entry.Evidence.CurrentRuntimeID,
		ServingReleaseID:        entry.Evidence.ServingReleaseID,
		InvariantViolations:     entry.Evidence.InvariantViolations,
		ErrorMessage:            errorMessage,
	})
	out := runtime.ApplyAppObservedStatus(app, observed)
	if observed.ServingReleaseID != "" {
		// The effective status describes this specific observed release. The
		// ManagedApp can still carry readiness history from its old canonical pod.
		out.Status.CurrentReleaseStartedAt = nil
		if at := entry.Evidence.ServingReleaseCreatedAt; !at.IsZero() {
			started := at.UTC()
			out.Status.CurrentReleaseStartedAt = &started
		}
		out.Status.CurrentReleaseReadyAt = nil
		if at := entry.Evidence.ServingReleaseReadyAt; at != nil && !at.IsZero() {
			ready := at.UTC()
			out.Status.CurrentReleaseReadyAt = &ready
		}
	}
	return out, revisionChanged
}

func appUsesManagedRuntime(app model.App, runtimeByID map[string]model.Runtime) bool {
	runtimeObj, ok := runtimeByID[ObservedRuntimeID(app)]
	if !ok {
		return false
	}
	switch strings.TrimSpace(runtimeObj.Type) {
	case "", model.RuntimeTypeManagedShared, model.RuntimeTypeManagedOwned:
		return true
	default:
		return false
	}
}

func mayUseManagedRuntime(app model.App, runtimeByID map[string]model.Runtime) bool {
	observedID := ObservedRuntimeID(app)
	if strings.TrimSpace(app.Spec.RuntimeID) == model.DefaultManagedRuntimeID {
		return true
	}
	if appUsesManagedRuntime(app, runtimeByID) {
		return true
	}
	// During a migration the durable desired runtime can already be managed
	// while Status.CurrentRuntimeID still names the source runtime (or vice
	// versa). Inspect both identities; choosing only the current one would
	// skip the live observer and leave a stale deployed projection in place.
	for _, runtimeID := range []string{app.Status.CurrentRuntimeID, app.Spec.RuntimeID} {
		runtimeID = strings.TrimSpace(runtimeID)
		if runtimeID == "" || runtimeID == observedID {
			continue
		}
		runtimeObj, ok := runtimeByID[runtimeID]
		if !ok {
			continue
		}
		switch strings.TrimSpace(runtimeObj.Type) {
		case "", model.RuntimeTypeManagedShared, model.RuntimeTypeManagedOwned:
			return true
		}
	}
	// A nil/empty runtime inventory is an observation failure, not evidence
	// that an app uses an external runtime. Treat a non-empty runtime id as
	// eligible so the caller reports unknown instead of reusing stale state.
	return len(runtimeByID) == 0 && observedID != ""
}

// ObservedRuntimeID prefers observed placement over durable and desired placement.
func ObservedRuntimeID(app model.App) string {
	if app.ObservedStatus != nil {
		if runtimeID := strings.TrimSpace(app.ObservedStatus.RuntimeID); runtimeID != "" {
			return runtimeID
		}
	}
	if runtimeID := strings.TrimSpace(app.Status.CurrentRuntimeID); runtimeID != "" {
		return runtimeID
	}
	return strings.TrimSpace(app.Spec.RuntimeID)
}

// ObservationKey binds cached runtime evidence to the app revision it describes.
func ObservationKey(app model.App) string {
	type imageSource struct {
		Type             string `json:"type,omitempty"`
		ImageRef         string `json:"image_ref,omitempty"`
		ResolvedImageRef string `json:"resolved_image_ref,omitempty"`
	}
	type observationInput struct {
		ID               string      `json:"id"`
		TenantID         string      `json:"tenant_id"`
		Name             string      `json:"name"`
		Image            string      `json:"image"`
		Ports            []int       `json:"ports,omitempty"`
		Replicas         int         `json:"replicas"`
		RuntimeID        string      `json:"runtime_id"`
		CurrentRuntimeID string      `json:"current_runtime_id"`
		NetworkMode      string      `json:"network_mode,omitempty"`
		SSHEnabled       bool        `json:"ssh_enabled"`
		StoredPhase      string      `json:"stored_phase,omitempty"`
		Source           imageSource `json:"source"`
		BuildSource      imageSource `json:"build_source"`
	}

	storedPhase := app.Status.Phase
	if app.StoredStatus != nil {
		storedPhase = app.StoredStatus.Phase
	}
	source := imageSource{}
	if app.Source != nil {
		source = imageSource{
			Type:             strings.TrimSpace(app.Source.Type),
			ImageRef:         strings.TrimSpace(app.Source.ImageRef),
			ResolvedImageRef: strings.TrimSpace(app.Source.ResolvedImageRef),
		}
	}
	buildSource := imageSource{}
	if build := model.AppBuildSource(app); build != nil {
		buildSource = imageSource{
			Type:             strings.TrimSpace(build.Type),
			ImageRef:         strings.TrimSpace(build.ImageRef),
			ResolvedImageRef: strings.TrimSpace(build.ResolvedImageRef),
		}
	}
	payload, _ := json.Marshal(observationInput{
		ID:               strings.TrimSpace(app.ID),
		TenantID:         strings.TrimSpace(app.TenantID),
		Name:             strings.TrimSpace(app.Name),
		Image:            strings.TrimSpace(app.Spec.Image),
		Ports:            append([]int(nil), app.Spec.Ports...),
		Replicas:         app.Spec.Replicas,
		RuntimeID:        strings.TrimSpace(app.Spec.RuntimeID),
		CurrentRuntimeID: strings.TrimSpace(app.Status.CurrentRuntimeID),
		NetworkMode:      model.NormalizeAppNetworkMode(app.Spec.NetworkMode),
		SSHEnabled:       model.AppSSHEnabled(app.Spec),
		StoredPhase:      strings.TrimSpace(storedPhase),
		Source:           source,
		BuildSource:      buildSource,
	})
	return string(payload)
}
