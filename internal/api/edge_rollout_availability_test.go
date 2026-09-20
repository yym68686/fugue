package api

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEdgeRetainsServingEndpointsWithoutDeclaringRolloutComplete(t *testing.T) {
	now := time.Now().UTC()
	fixture := func() model.App {
		yes, one, two := true, 1, 2
		return model.App{Spec: model.AppSpec{Replicas: 2, RuntimeID: "runtime-a"},
			StoredStatus: &model.AppStatus{CurrentReplicas: 2, CurrentRuntimeID: "runtime-a"},
			ObservedStatus: &model.AppObservedStatus{Phase: "deploying", RuntimeID: "runtime-a", DesiredReplicas: 2,
				ReadyReplicas: &one, PhysicalReplicas: &one, ServingReplicas: &two,
				RuntimeObjectPresent: &yes, NamespacePresent: &yes, ServicePresent: &yes, EndpointPresent: &yes, EndpointReady: &yes, ImagePresent: &yes,
				Fresh: true, ObservedAt: now, ClusterID: "cluster-a", Generation: 2, ObservedGeneration: 2, EvidenceSource: "kubernetes_api",
				InvariantViolations: []string{"desired_replicas_unready"}}}
	}
	app := fixture()
	if status, _ := edgeRouteStatus(app, app.Spec.RuntimeID, true); status != model.EdgeRouteStatusActive {
		t.Fatalf("serving rollout was withdrawn: %s", status)
	}
	if appObservedReadyForServing(app, now) {
		t.Fatal("availability was confused with deployment convergence")
	}
	for name, mutate := range map[string]func(*model.App){
		"missing serving proof": func(a *model.App) { a.ObservedStatus.ServingReplicas = nil },
		"no serving pods":       func(a *model.App) { zero := 0; a.ObservedStatus.ServingReplicas = &zero },
		"endpoint unavailable":  func(a *model.App) { no := false; a.ObservedStatus.EndpointReady = &no },
		"namespace missing":     func(a *model.App) { a.ObservedStatus.NamespacePresent = nil },
		"image mismatch": func(a *model.App) {
			a.ObservedStatus.InvariantViolations = append(a.ObservedStatus.InvariantViolations, "current_image_mismatch")
		},
		"unobserved generation": func(a *model.App) { a.ObservedStatus.ObservedGeneration = 1 },
		"stale snapshot":        func(a *model.App) { a.ObservedStatus.ObservedAt = now.Add(-2 * time.Minute) },
		"new route":             func(a *model.App) { a.StoredStatus = nil },
		"migration":             func(a *model.App) { a.Spec.RuntimeID = "runtime-b" },
		"disabled":              func(a *model.App) { a.Spec.Replicas = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			a := fixture()
			mutate(&a)
			if edgeRouteServingDuringRollout(a, now) {
				t.Fatal("unsafe rollout serving exception")
			}
		})
	}
}
