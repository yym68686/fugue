package runtime

import (
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

const runtimeManagedAppReadyForObservedStatusTest = ManagedAppPhaseReady
