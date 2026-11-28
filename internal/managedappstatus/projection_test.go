package managedappstatus

import (
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestAppMayUseManagedRuntimeChecksDesiredAndCurrentRuntimeDuringMigration(t *testing.T) {
	app := model.App{
		Spec:   model.AppSpec{RuntimeID: "runtime_target_managed"},
		Status: model.AppStatus{CurrentRuntimeID: "runtime_source_external"},
	}
	runtimes := map[string]model.Runtime{
		"runtime_source_external": {ID: "runtime_source_external", Type: model.RuntimeTypeExternalOwned},
		"runtime_target_managed":  {ID: "runtime_target_managed", Type: model.RuntimeTypeManagedOwned},
	}
	if !mayUseManagedRuntime(app, runtimes) {
		t.Fatal("migration with a managed desired runtime must still receive live status observation")
	}
}

func TestProjectRequestsRefreshForPreviousRevision(t *testing.T) {
	live := model.App{ID: "app", TenantID: "tenant", Spec: model.AppSpec{Image: "example/current", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID}}
	previous := live
	previous.Spec.Image = "example/previous"
	out, refresh := Project(live, AppSnapshot{
		Ok: true, Found: true, ClusterID: "cluster", RefreshedAt: time.Now(),
		Managed:  runtime.ManagedAppObject{Status: runtime.ManagedAppStatus{Phase: runtime.ManagedAppPhaseReady}},
		Evidence: Evidence{AppObservationKey: ObservationKey(previous)},
	}, nil, true, "")
	if !refresh || out.ObservedStatus == nil || out.ObservedStatus.Fresh || out.ObservedStatus.Phase != "unknown" {
		t.Fatalf("previous revision should remain unknown pending refresh: refresh=%v, status=%+v", refresh, out.ObservedStatus)
	}
}
