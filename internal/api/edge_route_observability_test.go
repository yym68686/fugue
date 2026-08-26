package api

import (
	"bytes"
	"log"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/trafficepoch"
)

func TestFinalizeEdgeRouteDecisionIsBehaviorNeutralAndStable(t *testing.T) {
	t.Parallel()

	app := edgeRouteObservationTestApp("deploying", "op_deploy")
	route := edgeRouteObservationTestRoute()
	before := route
	beforeGeneration := trafficepoch.EdgeRouteGeneration(route)
	provenance := edgeRouteObservationTestProvenance()

	routes := finalizeEdgeRouteDecisions(
		[]model.EdgeRouteBinding{route},
		map[string]model.App{app.ID: app},
		map[string]managedAppObservationProvenance{app.ID: provenance},
	)
	if len(routes) != 1 || !strings.HasPrefix(routes[0].DecisionID, "decision_") {
		t.Fatalf("expected decision id, got %+v", routes)
	}
	decisionID := routes[0].DecisionID
	routes[0].DecisionID = ""
	if !reflect.DeepEqual(routes[0], before) {
		t.Fatalf("observability changed route behavior\nbefore=%+v\nafter=%+v", before, routes[0])
	}
	routes[0].DecisionID = decisionID
	if got := trafficepoch.EdgeRouteGeneration(routes[0]); got != beforeGeneration {
		t.Fatalf("decision metadata changed route generation: before=%s after=%s", beforeGeneration, got)
	}
	repeated := finalizeEdgeRouteDecisions(
		[]model.EdgeRouteBinding{route},
		map[string]model.App{app.ID: app},
		map[string]managedAppObservationProvenance{app.ID: provenance},
	)
	if repeated[0].DecisionID != decisionID {
		t.Fatalf("decision id is not stable: first=%s repeated=%s", decisionID, repeated[0].DecisionID)
	}
}

func TestEdgeRouteDecisionProvenanceDistinguishesIncidentClasses(t *testing.T) {
	t.Parallel()

	route := edgeRouteObservationTestRoute()
	base := edgeRouteObservationTestProvenance()

	staleApp := edgeRouteObservationTestApp("deployed", "op_completed")
	stale := base
	stale.cacheExpired = true
	stale.observationFresh = false

	crossResourceApp := edgeRouteObservationTestApp("deploying", "op_running")
	crossResource := base
	crossResource.evidence.managedImageDigest = edgeObservationDigest("registry/new")
	crossResource.evidence.deploymentImageDigest = edgeObservationDigest("registry/old")
	crossResource.evidence.invariantViolations = []string{"current_image_mismatch", "image_missing"}

	imageNegativeApp := edgeRouteObservationTestApp("deploying", "op_running")
	imageNegative := base
	imageNegative.evidence.imageLocationStatus = model.ImageLocationStatusMissing
	imageNegative.evidence.imageLocationSource = "image_location_store"
	imageNegative.evidence.imageLocationObservedAt = time.Date(2026, 7, 31, 11, 29, 25, 0, time.UTC)
	imageNegative.evidence.invariantViolations = []string{"image_missing"}

	driftApp := edgeRouteObservationTestApp("deployed", "op_completed")
	drift := base
	drift.evidence.managedImageDigest = edgeObservationDigest("registry/new")
	drift.evidence.deploymentImageDigest = edgeObservationDigest("registry/old")
	drift.evidence.invariantViolations = []string{"current_image_mismatch", "image_missing"}

	cases := []struct {
		name       string
		app        model.App
		provenance managedAppObservationProvenance
		assert     func(*testing.T, edgeRouteDecisionMaterial)
	}{
		{name: "stale-cache", app: staleApp, provenance: stale, assert: func(t *testing.T, material edgeRouteDecisionMaterial) {
			if !material.CacheExpired || material.ObservationFresh {
				t.Fatalf("stale cache provenance missing: %+v", material)
			}
		}},
		{name: "fresh-cross-resource", app: crossResourceApp, provenance: crossResource, assert: func(t *testing.T, material edgeRouteDecisionMaterial) {
			if material.CacheExpired || !material.ObservationFresh || material.DurablePhase != "deploying" || material.ManagedImageDigest == material.DeploymentImageDigest {
				t.Fatalf("fresh cross-resource provenance missing: %+v", material)
			}
		}},
		{name: "image-location-negative", app: imageNegativeApp, provenance: imageNegative, assert: func(t *testing.T, material edgeRouteDecisionMaterial) {
			if material.ImageLocationStatus != model.ImageLocationStatusMissing || material.ImageLocationSource != "image_location_store" || material.ImageLocationObservedAt == "" {
				t.Fatalf("image-location provenance missing: %+v", material)
			}
		}},
		{name: "post-completion-drift", app: driftApp, provenance: drift, assert: func(t *testing.T, material edgeRouteDecisionMaterial) {
			if material.DurablePhase != "deployed" || material.ManagedImageDigest == material.DeploymentImageDigest || material.DurableOperationDigest == "" {
				t.Fatalf("post-completion drift provenance missing: %+v", material)
			}
		}},
	}

	decisionIDs := map[string]string{}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			material := edgeRouteDecisionMaterialFor(testCase.app, route, testCase.provenance)
			testCase.assert(t, material)
			decisionID := edgeRouteDecisionID(material)
			if previous, exists := decisionIDs[decisionID]; exists {
				t.Fatalf("incident classes %s and %s shared decision id %s", previous, testCase.name, decisionID)
			}
			decisionIDs[decisionID] = testCase.name
		})
	}
}

func TestEdgeRouteDecisionEventBindsRefreshSequenceWithoutRawRefs(t *testing.T) {
	t.Parallel()

	app := edgeRouteObservationTestApp("deploying", "op_sensitive_value")
	provenance := edgeRouteObservationTestProvenance()
	provenance.evidence.appObservationKey = "registry.example/private/image:secret"
	provenance.sequence = managedAppObservationSequence{
		refreshStarted:   10,
		managedAppsRead:  11,
		kubeSnapshotRead: 12,
		durableAppsRead:  13,
		refreshCompleted: 14,
	}
	route := edgeRouteObservationTestRoute()
	route.DeploymentGeneration = "deployment_op_sensitive_value"
	route.DecisionID = edgeRouteDecisionID(edgeRouteDecisionMaterialFor(app, route, provenance))
	bundle := model.EdgeRouteBundle{Version: "routegen_bundle", Routes: []model.EdgeRouteBinding{route}}
	var output bytes.Buffer
	server := &Server{log: log.New(&output, "", 0)}
	server.logEdgeRouteDecisionChanges(bundle, map[string]model.App{app.ID: app}, map[string]managedAppObservationProvenance{app.ID: provenance})

	logLine := output.String()
	for _, want := range []string{
		`"event_type":"edge_route_decision"`,
		`"bundle_version":"routegen_bundle"`,
		`"decision_id":"` + route.DecisionID + `"`,
		`"correlation_key":"[\"` + route.DecisionID + `\",\"routegen_bundle\",\"routegen_observed\"]"`,
		`"tenant_id":"tenant_observation"`,
		`"project_id":"project_observation"`,
		`"deployment_generation_digest":"` + edgeObservationDigest(route.DeploymentGeneration) + `"`,
		`"durable_image_digest":"` + edgeObservationDigest(app.Spec.Image) + `"`,
		`"refresh_started_sequence":10`,
		`"managed_apps_read_sequence":11`,
		`"kube_snapshot_read_sequence":12`,
		`"durable_apps_read_sequence":13`,
		`"refresh_completed_sequence":14`,
	} {
		if !strings.Contains(logLine, want) {
			t.Fatalf("decision event missing %s: %s", want, logLine)
		}
	}
	for _, forbidden := range []string{"registry/new", "registry.example/private/image:secret", "op_sensitive_value", "deployment_op_sensitive_value"} {
		if strings.Contains(logLine, forbidden) {
			t.Fatalf("decision event leaked raw reference %q: %s", forbidden, logLine)
		}
	}
}

func TestEdgeRouteInvariantDecisionBypassesChangeLogDedup(t *testing.T) {
	t.Parallel()

	app := edgeRouteObservationTestApp("deploying", "op_running")
	provenance := edgeRouteObservationTestProvenance()
	provenance.evidence.invariantViolations = []string{"current_image_mismatch", "image_missing"}
	route := edgeRouteObservationTestRoute()
	route.DecisionID = edgeRouteDecisionID(edgeRouteDecisionMaterialFor(app, route, provenance))
	bundle := model.EdgeRouteBundle{Version: "routegen_bundle", Routes: []model.EdgeRouteBinding{route}}
	before := bundle
	var output bytes.Buffer
	server := &Server{log: log.New(&output, "", 0)}
	for range 2 {
		server.logEdgeRouteDecisionChanges(bundle, map[string]model.App{app.ID: app}, map[string]managedAppObservationProvenance{app.ID: provenance})
	}
	if got := strings.Count(output.String(), `"event_type":"edge_route_decision"`); got != 2 {
		t.Fatalf("invariant decision material was deduplicated: got %d events: %s", got, output.String())
	}
	if !strings.Contains(output.String(), `"invariant_violations_json":"[\"current_image_mismatch\",\"image_missing\"]"`) {
		t.Fatalf("missing persistable invariant material: %s", output.String())
	}
	if !reflect.DeepEqual(bundle, before) {
		t.Fatalf("persistence logging changed route/bundle behavior: before=%+v after=%+v", before, bundle)
	}
}

func TestEdgeRouteActiveDecisionStillUsesChangeLogDedup(t *testing.T) {
	t.Parallel()

	app := edgeRouteObservationTestApp("deployed", "op_complete")
	provenance := edgeRouteObservationTestProvenance()
	route := edgeRouteObservationTestRoute()
	route.Status = model.EdgeRouteStatusActive
	route.StatusReason = ""
	route.DecisionID = edgeRouteDecisionID(edgeRouteDecisionMaterialFor(app, route, provenance))
	bundle := model.EdgeRouteBundle{Version: "routegen_bundle", Routes: []model.EdgeRouteBinding{route}}
	var output bytes.Buffer
	server := &Server{log: log.New(&output, "", 0)}
	for range 2 {
		server.logEdgeRouteDecisionChanges(bundle, map[string]model.App{app.ID: app}, map[string]managedAppObservationProvenance{app.ID: provenance})
	}
	if got := strings.Count(output.String(), `"event_type":"edge_route_decision"`); got != 1 {
		t.Fatalf("active decision dedup changed: got %d events: %s", got, output.String())
	}
}

func TestManagedAppObservationSequenceIsMonotonic(t *testing.T) {
	t.Parallel()

	cache := newManagedAppStatusCache(time.Minute, time.Second)
	first := cache.nextObservationSequence()
	second := cache.nextObservationSequence()
	third := cache.nextObservationSequence()
	if !(first > 0 && first < second && second < third) {
		t.Fatalf("observation sequence is not monotonic: %d %d %d", first, second, third)
	}
}

func edgeRouteObservationTestApp(phase, operationID string) model.App {
	return model.App{
		ID:        "app_observation",
		TenantID:  "tenant_observation",
		ProjectID: "project_observation",
		Spec: model.AppSpec{
			Image:     "registry/new",
			Replicas:  1,
			RuntimeID: model.DefaultManagedRuntimeID,
		},
		Status: model.AppStatus{
			Phase:            phase,
			CurrentRuntimeID: model.DefaultManagedRuntimeID,
			CurrentReplicas:  1,
			LastOperationID:  operationID,
		},
	}
}

func edgeRouteObservationTestRoute() model.EdgeRouteBinding {
	return model.EdgeRouteBinding{
		Hostname:             "observed.example.test",
		PathPrefix:           "/",
		RouteKind:            model.EdgeRouteKindPlatform,
		AppID:                "app_observation",
		TenantID:             "tenant_observation",
		RuntimeID:            model.DefaultManagedRuntimeID,
		EdgeGroupID:          "edge-group-country-us",
		RoutePolicy:          model.EdgeRoutePolicyEnabled,
		UpstreamKind:         model.EdgeRouteUpstreamKindKubernetesService,
		UpstreamScope:        model.EdgeRouteUpstreamScopeLocalService,
		UpstreamURL:          "http://observed.default.svc.cluster.local:8080",
		ServicePort:          8080,
		TLSPolicy:            model.EdgeRouteTLSPolicyPlatform,
		DeploymentGeneration: "op_running",
		Streaming:            true,
		Status:               model.EdgeRouteStatusUnavailable,
		StatusReason:         "runtime invariant violation: current_image_mismatch, image_missing",
		RouteGeneration:      "routegen_observed",
	}
}

func edgeRouteObservationTestProvenance() managedAppObservationProvenance {
	now := time.Date(2026, 7, 31, 11, 29, 25, 0, time.UTC)
	present := true
	return managedAppObservationProvenance{
		cacheLayer:       "list",
		cacheHit:         true,
		observationFresh: true,
		clusterIDDigest:  edgeObservationDigest("cluster-observed"),
		managedFound:     true,
		refreshedAt:      now,
		expiresAt:        now.Add(15 * time.Second),
		evidence: managedAppRuntimeEvidence{
			appObservationKey:            "observation-key",
			managedGeneration:            2,
			managedObservedGeneration:    2,
			managedImageDigest:           edgeObservationDigest("registry/new"),
			managedDesiredReplicas:       1,
			managedReadyReplicas:         1,
			deploymentGeneration:         2,
			deploymentObservedGeneration: 2,
			deploymentImageDigest:        edgeObservationDigest("registry/new"),
			deploymentReplicas:           1,
			deploymentUpdatedReplicas:    1,
			deploymentReadyReplicas:      1,
			deploymentAvailableReplicas:  1,
			namespacePresent:             &present,
			servicePresent:               &present,
			endpointPresent:              &present,
			endpointReady:                &present,
		},
	}
}
