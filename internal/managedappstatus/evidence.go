package managedappstatus

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/releaseproof"
	"fugue/internal/runtime"
)

// RuntimeSnapshot is the read-only Kubernetes evidence from one observation cycle.
type RuntimeSnapshot struct {
	Namespaces              map[string]struct{}
	Deployments             map[string]releaseproof.Deployment
	Services                map[string]struct{}
	Endpoints               map[string]Endpoint
	EndpointSlices          map[string]Endpoint
	EndpointSlicesAvailable bool
}

type Endpoint struct {
	Present           bool
	ReadyAddresses    int
	NotReadyAddresses int
}

type ImageLocationObservation struct {
	Status, Source string
	ObservedAt     time.Time
}

type EvidenceInputs struct {
	ServingRelease    model.AppRelease
	HasServingRelease bool
	ImagesEquivalent  func(expected, actual string) bool
	ReadImagePresence func(imageRef, runtimeID string) (*bool, ImageLocationObservation, error)
}

// EvaluateEvidence checks identities, generations and readiness in the observation.
func EvaluateEvidence(app model.App, managed runtime.ManagedAppObject, found bool, snapshot RuntimeSnapshot, inputs EvidenceInputs) (Evidence, error) {
	if inputs.ImagesEquivalent == nil {
		inputs.ImagesEquivalent = func(expected, actual string) bool {
			return strings.TrimSpace(expected) != "" && strings.TrimSpace(expected) == strings.TrimSpace(actual)
		}
	}
	evidence := Evidence{
		AppObservationKey: ObservationKey(app),
		EvidenceSources:   []string{runtime.AppObservationSourceKubernetesAPI},
	}
	if found {
		evidence.ManagedGeneration = managed.Metadata.Generation
		evidence.ManagedObservedGeneration = managed.Status.ObservedGeneration
		evidence.ManagedImageDigest = observationDigest(managed.Spec.AppSpec.Image)
		evidence.ManagedDesiredReplicas = managed.Status.DesiredReplicas
		evidence.ManagedReadyReplicas = managed.Status.ReadyReplicas
		expectedName := strings.TrimSpace(runtime.ManagedAppResourceName(app))
		expectedNamespace := strings.TrimSpace(runtime.NamespaceForTenant(app.TenantID))
		if observedName := strings.TrimSpace(managed.Metadata.Name); observedName != expectedName {
			evidence.InvariantViolations = append(evidence.InvariantViolations, "managed_app_name_mismatch")
		}
		if observedNamespace := strings.TrimSpace(managed.Metadata.Namespace); observedNamespace != expectedNamespace {
			evidence.InvariantViolations = append(evidence.InvariantViolations, "managed_app_namespace_mismatch")
		}
		if observedAppID := strings.TrimSpace(managed.Spec.AppID); observedAppID != "" && observedAppID != strings.TrimSpace(app.ID) {
			evidence.InvariantViolations = append(evidence.InvariantViolations, "managed_app_identity_mismatch")
		}
		if observedTenantID := strings.TrimSpace(managed.Spec.TenantID); observedTenantID != "" && observedTenantID != strings.TrimSpace(app.TenantID) {
			evidence.InvariantViolations = append(evidence.InvariantViolations, "managed_app_tenant_mismatch")
		}
		observedRuntimeID := strings.TrimSpace(managed.Spec.AppSpec.RuntimeID)
		evidence.CurrentRuntimeID = observedRuntimeID
		if observedRuntimeID != "" {
			expectedRuntimeIDs := map[string]struct{}{}
			for _, runtimeID := range []string{app.Spec.RuntimeID, app.Status.CurrentRuntimeID} {
				if runtimeID = strings.TrimSpace(runtimeID); runtimeID != "" {
					expectedRuntimeIDs[runtimeID] = struct{}{}
				}
			}
			if len(expectedRuntimeIDs) > 0 {
				if _, matches := expectedRuntimeIDs[observedRuntimeID]; !matches {
					evidence.InvariantViolations = append(evidence.InvariantViolations, "managed_app_runtime_mismatch")
				}
			}
		}
	}
	namespace := runtime.NamespaceForTenant(app.TenantID)
	_, namespaceExists := snapshot.Namespaces[strings.TrimSpace(namespace)]
	namespacePresent := namespaceExists
	evidence.NamespacePresent = &namespacePresent

	// Use the durable spec as the identity source. A found ManagedApp can
	// carry a stale/foreign spec during a migration; its metadata identity is
	// still authoritative for generation, while child names must remain tied
	// to the app being observed.
	serviceRequired := model.AppHasClusterService(app.Spec) || model.AppSSHEnabled(app.Spec)
	deploymentRequired := app.Spec.Replicas > 0 || strings.TrimSpace(app.Spec.Image) != ""
	canonicalServiceName := runtime.RuntimeAppServiceName(app)
	canonicalDeploymentName := runtime.RuntimeAppResourceName(app)
	serviceName := canonicalServiceName
	deploymentName := canonicalDeploymentName
	servingRelease, servingReleaseFound := inputs.ServingRelease, inputs.HasServingRelease
	if servingReleaseFound {
		if name := strings.TrimSpace(servingRelease.ServiceName); name != "" {
			serviceName = name
		}
		if name := strings.TrimSpace(servingRelease.DeploymentName); name != "" {
			deploymentName = name
		}
		releaseDeployment, releaseDeploymentFound := snapshot.Deployments[namespacedKey(namespace, deploymentName)]
		releaseDeploymentReady := releaseDeploymentFound && releaseproof.DeploymentCurrentCohortComplete(releaseDeployment) &&
			inputs.ImagesEquivalent(servingRelease.ResolvedImageRef, firstDeploymentContainerImage(releaseDeployment))
		if !releaseDeploymentReady {
			canonicalDeployment, canonicalDeploymentFound := snapshot.Deployments[namespacedKey(namespace, canonicalDeploymentName)]
			canonicalDeploymentReady := canonicalDeploymentFound && releaseproof.DeploymentCurrentCohortComplete(canonicalDeployment) &&
				inputs.ImagesEquivalent(servingRelease.ResolvedImageRef, firstDeploymentContainerImage(canonicalDeployment))
			if canonicalDeploymentReady {
				deploymentName = canonicalDeploymentName
				serviceName = canonicalServiceName
			}
		}
	}
	serviceKey := namespacedKey(namespace, serviceName)
	deploymentKey := namespacedKey(namespace, deploymentName)

	if serviceRequired {
		_, serviceExists := snapshot.Services[serviceKey]
		servicePresent := serviceExists
		evidence.ServicePresent = &servicePresent
		endpoint, endpointExists := snapshot.Endpoints[serviceKey]
		slice := snapshot.EndpointSlices[serviceKey]
		// Prefer EndpointSlice whenever that API is available, even when this
		// service has no slice. Falling back to legacy Endpoints merely because
		// the slice map lacks a key could advertise a stale endpoint from the
		// older API after the current slice inventory has gone empty.
		// Combining the two sources with OR could advertise a ready route from a
		// stale legacy Endpoints object while the current slices report zero
		// ready addresses. The fallback is used only when the EndpointSlice API
		// itself is unavailable.
		endpointPresent := endpointExists && endpoint.Present
		endpointReady := endpointPresent && endpoint.ReadyAddresses > 0
		if snapshot.EndpointSlicesAvailable {
			endpointPresent = slice.Present
			endpointReady = slice.Present && slice.ReadyAddresses > 0
		}
		evidence.EndpointPresent = &endpointPresent
		evidence.EndpointReady = &endpointReady
	}
	deploymentImageMatches := false
	if deploymentRequired {
		deployment, deploymentExists := snapshot.Deployments[deploymentKey]
		physicalDesired := 0
		physicalReady := 0
		servingReady := 0
		if deploymentExists {
			servingReady = model.MinObservedReplicaCount(deployment.Status.ReadyReplicas, deployment.Status.AvailableReplicas)
			evidence.DeploymentGeneration = deployment.Metadata.Generation
			evidence.DeploymentObservedGeneration = deployment.Status.ObservedGeneration
			evidence.DeploymentImageDigest = observationDigest(firstDeploymentContainerImage(deployment))
			evidence.DeploymentReplicas = deployment.Status.Replicas
			evidence.DeploymentUpdatedReplicas = deployment.Status.UpdatedReplicas
			evidence.DeploymentReadyReplicas = deployment.Status.ReadyReplicas
			evidence.DeploymentAvailableReplicas = deployment.Status.AvailableReplicas
			if deployment.Spec.Replicas != nil {
				physicalDesired = *deployment.Spec.Replicas
			}
			// Deployment readyReplicas can still belong to the previous
			// ReplicaSet after the template/generation changed. Only replicas
			// that are simultaneously updated, ready, and available are current
			// physical proof for this generation.
			physicalReady = model.MinObservedReplicaCount(
				deployment.Status.UpdatedReplicas,
				deployment.Status.ReadyReplicas,
				deployment.Status.AvailableReplicas,
			)
			if servingReleaseFound {
				deploymentImageMatches = releaseproof.DeploymentCurrentCohortComplete(deployment) && inputs.ImagesEquivalent(servingRelease.ResolvedImageRef, firstDeploymentContainerImage(deployment))
			}
		}
		if !deploymentExists || !releaseproof.DeploymentCurrentCohortComplete(deployment) {
			storedStatus := app.Status
			if app.StoredStatus != nil {
				storedStatus = *app.StoredStatus
			}
			if runtimeID := strings.TrimSpace(storedStatus.CurrentRuntimeID); runtimeID != "" {
				evidence.CurrentRuntimeID = runtimeID
			}
		}
		evidence.PhysicalDesiredReplicas = &physicalDesired
		evidence.PhysicalReplicas = &physicalReady
		evidence.ServingReplicas = &servingReady
		if deploymentExists && (deployment.Metadata.Generation <= 0 || deployment.Status.ObservedGeneration < deployment.Metadata.Generation) {
			evidence.InvariantViolations = append(evidence.InvariantViolations, "deployment_generation_unobserved")
		}
	}

	imageRef := strings.TrimSpace(app.Spec.Image)
	if found && !servingReleaseFound {
		imageRef = strings.TrimSpace(managed.Spec.AppSpec.Image)
	}
	if servingReleaseFound {
		imageRef = strings.TrimSpace(servingRelease.ResolvedImageRef)
	}
	if imageRef != "" {
		evidence.ImageRef = imageRef
		managedImage := strings.Contains(imageRef, "/fugue-apps/")
		if managedImage && inputs.ReadImagePresence != nil {
			observedRuntimeID := strings.TrimSpace(app.Spec.RuntimeID)
			if found {
				// Runtime identity from the object in this Kubernetes snapshot is
				// the only image-location scope that can be associated with the
				// current target during a migration. Do not use the durable source
				// runtime when the target object has already been created.
				if runtimeID := strings.TrimSpace(managed.Spec.AppSpec.RuntimeID); runtimeID != "" {
					observedRuntimeID = runtimeID
				}
			}
			present, locationObservation, err := inputs.ReadImagePresence(imageRef, observedRuntimeID)
			if err != nil {
				return Evidence{}, err
			}
			evidence.ImagePresent = present
			evidence.ImageLocationStatus = locationObservation.Status
			evidence.ImageLocationSource = locationObservation.Source
			evidence.ImageLocationObservedAt = locationObservation.ObservedAt
			evidence.EvidenceSources = append(evidence.EvidenceSources, "image_location_store")
		}
		if deployment, deploymentExists := snapshot.Deployments[deploymentKey]; deploymentExists &&
			releaseproof.DeploymentCurrentCohortComplete(deployment) {
			deployedImage := firstDeploymentContainerImage(deployment)
			if deployedImage != "" {
				matches := inputs.ImagesEquivalent(imageRef, deployedImage)
				if matches {
					if evidence.ImagePresent == nil {
						evidence.ImagePresent = &matches
					}
				} else {
					evidence.InvariantViolations = append(evidence.InvariantViolations, "current_image_mismatch")
				}
			}
		}
	}
	if servingReleaseFound && found && app.Spec.Replicas > 0 && namespacePresent && deploymentImageMatches &&
		evidence.PhysicalDesiredReplicas != nil && *evidence.PhysicalDesiredReplicas >= app.Spec.Replicas &&
		evidence.PhysicalReplicas != nil && *evidence.PhysicalReplicas >= app.Spec.Replicas &&
		(!serviceRequired || (boolPointerTrue(evidence.ServicePresent) && boolPointerTrue(evidence.EndpointReady))) {
		evidence.ServingReleaseReady = true
		evidence.ServingReleaseID = strings.TrimSpace(servingRelease.ID)
		evidence.ServingReleaseCreatedAt = servingRelease.CreatedAt
		evidence.ServingReleaseReadyAt = servingRelease.ReadyAt
		evidence.EvidenceSources = append(evidence.EvidenceSources, "app_release_traffic_policy")
	}

	// Record contradictions as explicit invariant evidence. The calculator
	// decides the observed phase; this list is never inferred from stored
	// replicas alone.
	if found && app.Spec.Replicas > 0 {
		storedPhase := app.Status.Phase
		if app.StoredStatus != nil {
			storedPhase = app.StoredStatus.Phase
		}
		if strings.EqualFold(strings.TrimSpace(storedPhase), "deployed") && managed.Status.ReadyReplicas == 0 {
			evidence.InvariantViolations = append(evidence.InvariantViolations, "stored_deployed_but_observed_ready_zero")
		}
	}
	return evidence, nil
}

func firstDeploymentContainerImage(deployment releaseproof.Deployment) string {
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if image := strings.TrimSpace(container.Image); image != "" {
			return image
		}
	}
	return ""
}

func boolPointerTrue(value *bool) bool {
	return value != nil && *value
}

func namespacedKey(namespace, name string) string {
	return strings.TrimSpace(namespace) + "/" + strings.TrimSpace(name)
}

func observationDigest(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])[:24]
}
