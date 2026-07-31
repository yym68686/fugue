package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

type managedAppObservationProvenance struct {
	cacheLayer             string
	cacheHit               bool
	cacheExpired           bool
	observationFresh       bool
	clusterIDDigest        string
	durableRevisionDigest  string
	durablePhase           string
	durableOperationDigest string
	managedFound           bool
	refreshedAt            time.Time
	expiresAt              time.Time
	sequence               managedAppObservationSequence
	evidence               managedAppRuntimeEvidence
}

func managedAppListObservationProvenance(apps []model.App, entry managedAppStatusListCacheEntry, expired, fresh bool) map[string]managedAppObservationProvenance {
	out := make(map[string]managedAppObservationProvenance, len(apps))
	for _, app := range apps {
		appID := strings.TrimSpace(app.ID)
		_, found := entry.items[appID]
		durablePhase := strings.TrimSpace(app.Status.Phase)
		if app.StoredStatus != nil {
			durablePhase = strings.TrimSpace(app.StoredStatus.Phase)
		}
		out[appID] = managedAppObservationProvenance{
			cacheLayer:             "list",
			cacheHit:               true,
			cacheExpired:           expired,
			observationFresh:       fresh,
			clusterIDDigest:        edgeObservationDigest(entry.clusterID),
			durableRevisionDigest:  edgeObservationDigest(managedAppRuntimeEvidenceObservationKey(app)),
			durablePhase:           durablePhase,
			durableOperationDigest: edgeObservationDigest(app.Status.LastOperationID),
			managedFound:           found,
			refreshedAt:            entry.refreshedAt,
			expiresAt:              entry.expiresAt,
			sequence:               entry.sequence,
			evidence:               entry.evidence[appID],
		}
	}
	return out
}

func managedAppUnknownObservationProvenance(apps []model.App) map[string]managedAppObservationProvenance {
	out := make(map[string]managedAppObservationProvenance, len(apps))
	for _, app := range apps {
		durablePhase := strings.TrimSpace(app.Status.Phase)
		if app.StoredStatus != nil {
			durablePhase = strings.TrimSpace(app.StoredStatus.Phase)
		}
		out[strings.TrimSpace(app.ID)] = managedAppObservationProvenance{
			cacheLayer:             "list",
			durableRevisionDigest:  edgeObservationDigest(managedAppRuntimeEvidenceObservationKey(app)),
			durablePhase:           durablePhase,
			durableOperationDigest: edgeObservationDigest(app.Status.LastOperationID),
		}
	}
	return out
}

type edgeRouteDecisionMaterial struct {
	RouteGeneration                string   `json:"route_generation"`
	DeploymentGenerationDigest     string   `json:"deployment_generation_digest,omitempty"`
	Status                         string   `json:"status"`
	StatusReason                   string   `json:"status_reason,omitempty"`
	DurableRevisionDigest          string   `json:"durable_revision_digest,omitempty"`
	DurablePhase                   string   `json:"durable_phase,omitempty"`
	DurableOperationDigest         string   `json:"durable_operation_digest,omitempty"`
	AppObservationKeyDigest        string   `json:"app_observation_key_digest,omitempty"`
	CacheLayer                     string   `json:"cache_layer,omitempty"`
	CacheHit                       bool     `json:"cache_hit"`
	CacheExpired                   bool     `json:"cache_expired"`
	ObservationFresh               bool     `json:"observation_fresh"`
	ClusterIDDigest                string   `json:"cluster_id_digest,omitempty"`
	ManagedFound                   bool     `json:"managed_found"`
	ManagedGeneration              int64    `json:"managed_generation,omitempty"`
	ManagedObservedGeneration      int64    `json:"managed_observed_generation,omitempty"`
	ManagedImageDigest             string   `json:"managed_image_digest,omitempty"`
	ManagedDesiredReplicas         int      `json:"managed_desired_replicas"`
	ManagedReadyReplicas           int      `json:"managed_ready_replicas"`
	DeploymentObservedGeneration   int64    `json:"deployment_observed_generation,omitempty"`
	DeploymentKubernetesGeneration int64    `json:"deployment_kubernetes_generation,omitempty"`
	DeploymentImageDigest          string   `json:"deployment_image_digest,omitempty"`
	DeploymentReplicas             int      `json:"deployment_replicas"`
	DeploymentUpdatedReplicas      int      `json:"deployment_updated_replicas"`
	DeploymentReadyReplicas        int      `json:"deployment_ready_replicas"`
	DeploymentAvailableReplicas    int      `json:"deployment_available_replicas"`
	NamespacePresent               *bool    `json:"namespace_present,omitempty"`
	ServicePresent                 *bool    `json:"service_present,omitempty"`
	EndpointPresent                *bool    `json:"endpoint_present,omitempty"`
	EndpointReady                  *bool    `json:"endpoint_ready,omitempty"`
	ImageLocationStatus            string   `json:"image_location_status,omitempty"`
	ImageLocationSource            string   `json:"image_location_source,omitempty"`
	ImageLocationObservedAt        string   `json:"image_location_observed_at,omitempty"`
	InvariantViolations            []string `json:"invariant_violations,omitempty"`
}

func edgeRouteDecisionMaterialFor(app model.App, route model.EdgeRouteBinding, provenance managedAppObservationProvenance) edgeRouteDecisionMaterial {
	storedPhase := strings.TrimSpace(app.Status.Phase)
	if app.StoredStatus != nil {
		storedPhase = strings.TrimSpace(app.StoredStatus.Phase)
	}
	evidence := provenance.evidence
	durableRevisionDigest := provenance.durableRevisionDigest
	durableOperationDigest := provenance.durableOperationDigest
	if strings.TrimSpace(app.ID) != "" {
		if durableRevisionDigest == "" {
			durableRevisionDigest = edgeObservationDigest(managedAppRuntimeEvidenceObservationKey(app))
		}
		if durableOperationDigest == "" {
			durableOperationDigest = edgeObservationDigest(app.Status.LastOperationID)
		}
	}
	if provenance.durablePhase != "" {
		storedPhase = provenance.durablePhase
	}
	return edgeRouteDecisionMaterial{
		RouteGeneration:                strings.TrimSpace(route.RouteGeneration),
		DeploymentGenerationDigest:     edgeObservationDigest(route.DeploymentGeneration),
		Status:                         strings.TrimSpace(route.Status),
		StatusReason:                   strings.TrimSpace(route.StatusReason),
		DurableRevisionDigest:          durableRevisionDigest,
		DurablePhase:                   storedPhase,
		DurableOperationDigest:         durableOperationDigest,
		AppObservationKeyDigest:        edgeObservationDigest(evidence.appObservationKey),
		CacheLayer:                     strings.TrimSpace(provenance.cacheLayer),
		CacheHit:                       provenance.cacheHit,
		CacheExpired:                   provenance.cacheExpired,
		ObservationFresh:               provenance.observationFresh,
		ClusterIDDigest:                provenance.clusterIDDigest,
		ManagedFound:                   provenance.managedFound,
		ManagedGeneration:              evidence.managedGeneration,
		ManagedObservedGeneration:      evidence.managedObservedGeneration,
		ManagedImageDigest:             evidence.managedImageDigest,
		ManagedDesiredReplicas:         evidence.managedDesiredReplicas,
		ManagedReadyReplicas:           evidence.managedReadyReplicas,
		DeploymentObservedGeneration:   evidence.deploymentObservedGeneration,
		DeploymentKubernetesGeneration: evidence.deploymentGeneration,
		DeploymentImageDigest:          evidence.deploymentImageDigest,
		DeploymentReplicas:             evidence.deploymentReplicas,
		DeploymentUpdatedReplicas:      evidence.deploymentUpdatedReplicas,
		DeploymentReadyReplicas:        evidence.deploymentReadyReplicas,
		DeploymentAvailableReplicas:    evidence.deploymentAvailableReplicas,
		NamespacePresent:               evidence.namespacePresent,
		ServicePresent:                 evidence.servicePresent,
		EndpointPresent:                evidence.endpointPresent,
		EndpointReady:                  evidence.endpointReady,
		ImageLocationStatus:            strings.TrimSpace(evidence.imageLocationStatus),
		ImageLocationSource:            strings.TrimSpace(evidence.imageLocationSource),
		ImageLocationObservedAt:        formatObservationTime(evidence.imageLocationObservedAt),
		InvariantViolations:            sortedInvariantViolations(evidence.invariantViolations),
	}
}

func edgeRouteDecisionID(material edgeRouteDecisionMaterial) string {
	payload, _ := json.Marshal(material)
	sum := sha256.Sum256(payload)
	return "decision_" + hex.EncodeToString(sum[:])[:24]
}

func edgeObservationDigest(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])[:24]
}

func formatObservationTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func finalizeEdgeRouteDecisions(routes []model.EdgeRouteBinding, appsByID map[string]model.App, provenanceByAppID map[string]managedAppObservationProvenance) []model.EdgeRouteBinding {
	for index := range routes {
		appID := strings.TrimSpace(routes[index].AppID)
		material := edgeRouteDecisionMaterialFor(appsByID[appID], routes[index], provenanceByAppID[appID])
		routes[index].DecisionID = edgeRouteDecisionID(material)
	}
	return routes
}

func (s *Server) logEdgeRouteDecisionChanges(bundle model.EdgeRouteBundle, appsByID map[string]model.App, provenanceByAppID map[string]managedAppObservationProvenance) {
	if s == nil || s.log == nil {
		return
	}
	for _, route := range bundle.Routes {
		if !s.edgeRouteDecisionChanged(route) {
			continue
		}
		app := appsByID[strings.TrimSpace(route.AppID)]
		provenance := provenanceByAppID[strings.TrimSpace(route.AppID)]
		material := edgeRouteDecisionMaterialFor(app, route, provenance)
		event := map[string]any{
			"event_type":                       "edge_route_decision",
			"decision_id":                      strings.TrimSpace(route.DecisionID),
			"bundle_version":                   strings.TrimSpace(bundle.Version),
			"route_generation":                 material.RouteGeneration,
			"deployment_generation_digest":     material.DeploymentGenerationDigest,
			"app_id":                           strings.TrimSpace(route.AppID),
			"hostname":                         strings.TrimSpace(route.Hostname),
			"path_prefix":                      model.NormalizeAppRoutePathPrefix(route.PathPrefix),
			"edge_group_id":                    strings.TrimSpace(route.EdgeGroupID),
			"final_status":                     material.Status,
			"final_reason":                     material.StatusReason,
			"durable_revision_digest":          material.DurableRevisionDigest,
			"durable_phase":                    material.DurablePhase,
			"durable_operation_digest":         material.DurableOperationDigest,
			"app_observation_key_digest":       material.AppObservationKeyDigest,
			"cache_layer":                      material.CacheLayer,
			"cache_hit":                        material.CacheHit,
			"cache_expired":                    material.CacheExpired,
			"cache_refreshed_at":               formatObservationTime(provenance.refreshedAt),
			"cache_expires_at":                 formatObservationTime(provenance.expiresAt),
			"observation_fresh":                material.ObservationFresh,
			"cluster_id_digest":                material.ClusterIDDigest,
			"managed_found":                    material.ManagedFound,
			"managed_generation":               material.ManagedGeneration,
			"managed_observed_generation":      material.ManagedObservedGeneration,
			"managed_image_digest":             material.ManagedImageDigest,
			"managed_desired_replicas":         material.ManagedDesiredReplicas,
			"managed_ready_replicas":           material.ManagedReadyReplicas,
			"deployment_kubernetes_generation": material.DeploymentKubernetesGeneration,
			"deployment_observed_generation":   material.DeploymentObservedGeneration,
			"deployment_image_digest":          material.DeploymentImageDigest,
			"deployment_replicas":              material.DeploymentReplicas,
			"deployment_updated_replicas":      material.DeploymentUpdatedReplicas,
			"deployment_ready_replicas":        material.DeploymentReadyReplicas,
			"deployment_available_replicas":    material.DeploymentAvailableReplicas,
			"namespace_present":                material.NamespacePresent,
			"service_present":                  material.ServicePresent,
			"endpoint_present":                 material.EndpointPresent,
			"endpoint_ready":                   material.EndpointReady,
			"image_location_status":            material.ImageLocationStatus,
			"image_location_source":            material.ImageLocationSource,
			"image_location_observed_at":       material.ImageLocationObservedAt,
			"invariant_violations":             material.InvariantViolations,
			"refresh_started_sequence":         provenance.sequence.refreshStarted,
			"managed_apps_read_sequence":       provenance.sequence.managedAppsRead,
			"kube_snapshot_read_sequence":      provenance.sequence.kubeSnapshotRead,
			"durable_apps_read_sequence":       provenance.sequence.durableAppsRead,
			"refresh_completed_sequence":       provenance.sequence.refreshCompleted,
		}
		s.logStructuredEvent(event)
	}
}

func (s *Server) edgeRouteDecisionChanged(route model.EdgeRouteBinding) bool {
	keyParts := []string{
		normalizeExternalAppDomain(route.Hostname),
		model.NormalizeAppRoutePathPrefix(route.PathPrefix),
		strings.TrimSpace(route.RouteKind),
		strings.TrimSpace(route.EdgeGroupID),
	}
	key := strings.Join(keyParts, "\x00")
	decisionID := strings.TrimSpace(route.DecisionID)
	s.edgeRouteDecisionMu.Lock()
	defer s.edgeRouteDecisionMu.Unlock()
	if s.edgeRouteDecisionLast == nil {
		s.edgeRouteDecisionLast = make(map[string]string)
	}
	if s.edgeRouteDecisionLast[key] == decisionID {
		return false
	}
	if len(s.edgeRouteDecisionLast) >= 4096 {
		s.edgeRouteDecisionLast = make(map[string]string)
	}
	s.edgeRouteDecisionLast[key] = decisionID
	return true
}

func (s *Server) logManagedAppRefreshEvent(cacheLayer, phase, result string, sequence uint64, err error) {
	if s == nil || s.log == nil {
		return
	}
	event := map[string]any{
		"event_type":  "managed_app_observation_refresh",
		"cache_layer": strings.TrimSpace(cacheLayer),
		"phase":       strings.TrimSpace(phase),
		"result":      strings.TrimSpace(result),
		"sequence":    sequence,
	}
	if err != nil {
		event["error_digest"] = edgeObservationDigest(err.Error())
	}
	s.logStructuredEvent(event)
}

func (s *Server) logStructuredEvent(event map[string]any) {
	if s == nil || s.log == nil || len(event) == 0 {
		return
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return
	}
	s.log.Print(string(payload))
}

func sortedInvariantViolations(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
