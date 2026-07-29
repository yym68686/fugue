package runtime

import (
	"slices"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestCalculateAppObservedStatusDistinguishesAbsentFromQueryFailure(t *testing.T) {
	now := time.Date(2026, 7, 29, 1, 2, 3, 0, time.UTC)
	app := model.App{
		Spec:   model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
		Status: model.AppStatus{Phase: "deployed", CurrentReplicas: 1},
	}

	absent := CalculateAppObservedStatus(app, AppRuntimeObservation{
		Complete:   true,
		Fresh:      true,
		ObservedAt: now,
		ClusterID:  "cluster-uid",
	})
	if absent.Phase != "unavailable" || absent.ReadyReplicas == nil || *absent.ReadyReplicas != 0 {
		t.Fatalf("authoritative absence must be unavailable with zero ready replicas: %+v", absent)
	}
	if absent.RuntimeObjectPresent == nil || *absent.RuntimeObjectPresent || !absent.Fresh {
		t.Fatalf("authoritative absence must record a fresh false presence value: %+v", absent)
	}

	unknown := CalculateAppObservedStatus(app, AppRuntimeObservation{
		Complete:     false,
		Fresh:        false,
		ObservedAt:   now,
		ClusterID:    "cluster-uid",
		ErrorMessage: "kubernetes timeout",
	})
	if unknown.Phase != "unknown" || unknown.ReadyReplicas != nil || unknown.RuntimeObjectPresent != nil || unknown.Fresh {
		t.Fatalf("query failure must remain unknown, not absent: %+v", unknown)
	}
	if unknown.Reason != AppObservationReasonKubernetesQueryFailed {
		t.Fatalf("unexpected unknown reason %q", unknown.Reason)
	}
}

func TestCalculateAppObservedStatusRequiresClusterIdentityForAuthoritativeAbsence(t *testing.T) {
	status := CalculateAppObservedStatus(model.App{
		Spec: model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
	}, AppRuntimeObservation{
		Complete:   true,
		Fresh:      true,
		ObservedAt: time.Now().UTC(),
	})
	if status.Phase != "unknown" || status.Fresh || status.ReadyReplicas != nil || status.RuntimeObjectPresent != nil {
		t.Fatalf("identity-free observation must not claim runtime absence: %+v", status)
	}
	if status.Reason != AppObservationReasonClusterIdentityMissing {
		t.Fatalf("unexpected identity-free reason %q", status.Reason)
	}
}

func TestCalculateAppObservedStatusTreatsStaleAbsenceAsUnknown(t *testing.T) {
	status := CalculateAppObservedStatus(model.App{
		Spec:   model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1},
		Status: model.AppStatus{Phase: "deployed", CurrentReplicas: 1},
	}, AppRuntimeObservation{
		Complete:     true,
		Fresh:        false,
		Found:        false,
		ObservedAt:   time.Now().UTC(),
		ClusterID:    "cluster-uid",
		ErrorMessage: "refresh timed out",
	})
	if status.Phase != "unknown" || status.Fresh || status.RuntimeObjectPresent != nil || status.ReadyReplicas != nil {
		t.Fatalf("stale absence must not be published as unavailable: %+v", status)
	}
	if status.Reason != AppObservationReasonObservationStale {
		t.Fatalf("unexpected stale reason: %+v", status)
	}
}

func TestCalculateAppObservedStatusRequiresCurrentGeneration(t *testing.T) {
	app := model.App{Spec: model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1}}
	managed := ManagedAppObject{
		Metadata: ManagedAppMeta{Generation: 4},
		Status: ManagedAppStatus{
			Phase:              runtimeManagedAppReadyForObservedStatusTest,
			ReadyReplicas:      1,
			ObservedGeneration: 3,
		},
	}
	status := CalculateAppObservedStatus(app, AppRuntimeObservation{
		ManagedApp: managed,
		Found:      true,
		Complete:   true,
		Fresh:      true,
		ObservedAt: time.Now(),
		ClusterID:  "cluster-uid",
	})
	if status.Phase != "unknown" || status.Fresh || status.Reason != AppObservationReasonGenerationNotObserved {
		t.Fatalf("unobserved generation must not be deployed: %+v", status)
	}

	managed.Metadata.Generation = 0
	managed.Status.ObservedGeneration = 0
	status = CalculateAppObservedStatus(app, AppRuntimeObservation{
		ManagedApp: managed,
		Found:      true,
		Complete:   true,
		Fresh:      true,
		ObservedAt: time.Now(),
		ClusterID:  "cluster-uid",
	})
	if status.Phase != "unknown" || status.Fresh || status.Reason != AppObservationReasonGenerationNotObserved {
		t.Fatalf("missing generation evidence must not be deployed: %+v", status)
	}

	managed.Metadata.Generation = 4
	managed.Status.ObservedGeneration = managed.Metadata.Generation
	status = CalculateAppObservedStatus(app, AppRuntimeObservation{
		ManagedApp: managed,
		Found:      true,
		Complete:   true,
		Fresh:      true,
		ObservedAt: time.Now(),
		ClusterID:  "cluster-uid",
	})
	if status.Phase != "deployed" || !status.Fresh || status.ReadyReplicas == nil || *status.ReadyReplicas != 1 {
		t.Fatalf("current generation ready status must be deployed: %+v", status)
	}
}

func TestApplyAppObservedStatusPreservesStoredStateAndClearsUnknownReplicaClaim(t *testing.T) {
	storedUpdatedAt := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	observedAt := time.Date(2026, 7, 29, 0, 0, 0, 0, time.UTC)
	app := model.App{Status: model.AppStatus{
		Phase: "deployed", CurrentRuntimeID: model.DefaultManagedRuntimeID,
		CurrentReplicas: 1, UpdatedAt: storedUpdatedAt,
	}}
	updated := ApplyAppObservedStatus(app, model.AppObservedStatus{
		Phase: "unknown", RuntimeID: model.DefaultManagedRuntimeID,
		ObservedAt: observedAt, EvidenceSource: AppObservationSourceKubernetesAPI,
	})
	if updated.StoredStatus == nil || updated.StoredStatus.Phase != "deployed" || updated.StoredStatus.CurrentReplicas != 1 {
		t.Fatalf("durable status was not preserved: %+v", updated.StoredStatus)
	}
	if updated.Status.Phase != "unknown" || updated.Status.CurrentReplicas != 0 || !updated.Status.UpdatedAt.Equal(observedAt) {
		t.Fatalf("legacy projection reused stale runtime state: %+v", updated.Status)
	}
}

func TestCalculateAppObservedStatusFailsClosedOnRuntimeInvariants(t *testing.T) {
	app := model.App{
		Spec:   model.AppSpec{RuntimeID: model.DefaultManagedRuntimeID, Replicas: 1, Ports: []int{8080}},
		Status: model.AppStatus{Phase: "deployed", CurrentReplicas: 1},
	}
	base := func() AppRuntimeObservation {
		return AppRuntimeObservation{
			ManagedApp: ManagedAppObject{
				Metadata: ManagedAppMeta{Generation: 7},
				Status: ManagedAppStatus{
					Phase:              ManagedAppPhaseReady,
					DesiredReplicas:    1,
					ReadyReplicas:      1,
					ObservedGeneration: 7,
				},
			},
			Found:                   true,
			Complete:                true,
			Fresh:                   true,
			ObservedAt:              time.Now().UTC(),
			ClusterID:               "cluster-uid",
			NamespacePresent:        boolPointer(true),
			ServicePresent:          boolPointer(true),
			EndpointPresent:         boolPointer(true),
			EndpointReady:           boolPointer(true),
			PhysicalReplicas:        intPointer(1),
			PhysicalDesiredReplicas: intPointer(1),
			ImagePresent:            boolPointer(true),
		}
	}
	tests := []struct {
		name      string
		violation string
		mutate    func(*AppRuntimeObservation)
	}{
		{name: "namespace missing", violation: "namespace_missing", mutate: func(in *AppRuntimeObservation) { in.NamespacePresent = boolPointer(false) }},
		{name: "service missing", violation: "service_missing", mutate: func(in *AppRuntimeObservation) { in.ServicePresent = boolPointer(false) }},
		{name: "endpoint missing", violation: "endpoint_missing", mutate: func(in *AppRuntimeObservation) { in.EndpointPresent = boolPointer(false) }},
		{name: "endpoint unready", violation: "endpoint_unready", mutate: func(in *AppRuntimeObservation) { in.EndpointReady = boolPointer(false) }},
		{name: "image missing", violation: "image_missing", mutate: func(in *AppRuntimeObservation) { in.ImagePresent = boolPointer(false) }},
		{name: "physical replicas zero", violation: "physical_replicas_zero", mutate: func(in *AppRuntimeObservation) { in.PhysicalReplicas = intPointer(0) }},
		{name: "desired replicas unready", violation: "desired_replicas_unready", mutate: func(in *AppRuntimeObservation) {
			in.ManagedApp.Status.ReadyReplicas = 0
			in.PhysicalReplicas = nil
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evidence := base()
			test.mutate(&evidence)
			status := CalculateAppObservedStatus(app, evidence)
			if status.Phase != "unavailable" {
				t.Fatalf("invariant %s must fail a ready state closed, got %+v", test.violation, status)
			}
			if !slices.Contains(status.InvariantViolations, test.violation) {
				t.Fatalf("missing invariant %s in %+v", test.violation, status.InvariantViolations)
			}
		})
	}
}

func boolPointer(value bool) *bool { return &value }

func intPointer(value int) *int { return &value }

const runtimeManagedAppReadyForObservedStatusTest = ManagedAppPhaseReady
