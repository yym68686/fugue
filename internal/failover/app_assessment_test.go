package failover

import (
	"testing"

	"fugue/internal/model"
)

func TestAssessAppBlockedByManagedState(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:   "app_123",
		Name: "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_managed_shared",
			Replicas:  1,
			Workspace: &model.AppWorkspaceSpec{MountPath: "/workspace"},
		},
		Status: model.AppStatus{
			CurrentReplicas: 1,
		},
		Bindings: []model.ServiceBinding{
			{
				ServiceID: "svc_pg",
			},
		},
		BackingServices: []model.BackingService{
			{
				ID:          "svc_pg",
				Type:        model.BackingServiceTypePostgres,
				Provisioner: model.BackingServiceProvisionerManaged,
				Status:      model.BackingServiceStatusActive,
			},
		},
	}

	assessment := AssessApp(app, &model.Runtime{
		ID:     "runtime_managed_shared",
		Type:   model.RuntimeTypeManagedShared,
		Status: model.RuntimeStatusActive,
	})

	if assessment.Classification != AppClassificationBlocked {
		t.Fatalf("expected classification %q, got %q", AppClassificationBlocked, assessment.Classification)
	}
	if got, want := assessment.Summary, "blocked by managed backing services and persistent storage"; got != want {
		t.Fatalf("expected summary %q, got %q", want, got)
	}
	if len(assessment.Blockers) != 2 {
		t.Fatalf("expected 2 blockers, got %#v", assessment.Blockers)
	}
	if len(assessment.Warnings) == 0 || assessment.Warnings[0] != "desired replicas are below 2" {
		t.Fatalf("expected replica warning, got %#v", assessment.Warnings)
	}
}

func TestAssessAppReadyForStatelessFailover(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:   "app_123",
		Name: "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_managed_shared",
			Replicas:  2,
		},
		Status: model.AppStatus{
			CurrentRuntimeID: "runtime_managed_shared",
			CurrentReplicas:  2,
		},
	}

	assessment := AssessApp(app, &model.Runtime{
		ID:     "runtime_managed_shared",
		Type:   model.RuntimeTypeManagedShared,
		Status: model.RuntimeStatusActive,
	})

	if assessment.Classification != AppClassificationReady {
		t.Fatalf("expected classification %q, got %q", AppClassificationReady, assessment.Classification)
	}
	if got, want := assessment.Summary, "eligible for stateless failover"; got != want {
		t.Fatalf("expected summary %q, got %q", want, got)
	}
	if len(assessment.Blockers) != 0 {
		t.Fatalf("expected no blockers, got %#v", assessment.Blockers)
	}
	if len(assessment.Warnings) != 0 {
		t.Fatalf("expected no warnings, got %#v", assessment.Warnings)
	}
}

func TestAssessAppCautionsForDedicatedRuntime(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:   "app_123",
		Name: "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_owned_1",
			Replicas:  1,
		},
		Status: model.AppStatus{
			CurrentRuntimeID: "runtime_owned_1",
			CurrentReplicas:  1,
		},
	}

	assessment := AssessApp(app, &model.Runtime{
		ID:     "runtime_owned_1",
		Type:   model.RuntimeTypeExternalOwned,
		Status: model.RuntimeStatusOffline,
	})

	if assessment.Classification != AppClassificationCaution {
		t.Fatalf("expected classification %q, got %q", AppClassificationCaution, assessment.Classification)
	}
	for _, want := range []string{
		"desired replicas are below 2",
		"runtime status is offline",
		"runtime is dedicated; Fugue does not infer redundant node placement",
	} {
		found := false
		for _, warning := range assessment.Warnings {
			if warning == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected warning %q, got %#v", want, assessment.Warnings)
		}
	}
}
