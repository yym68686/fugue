package managedappstatus

import (
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestAppObservedReadyForServingRequiresDesiredReplicaAgreement(t *testing.T) {
	t.Parallel()
	ready := 1
	present := true
	app := model.App{
		Spec: model.AppSpec{Image: "registry.example/app:v1", Ports: []int{8080}, Replicas: 1},
		ObservedStatus: &model.AppObservedStatus{
			Phase: "deployed", DesiredReplicas: 1, ReadyReplicas: &ready,
			RuntimeObjectPresent: &present, NamespacePresent: &present, ServicePresent: &present,
			EndpointPresent: &present, EndpointReady: &present, PhysicalReplicas: &ready,
			PhysicalDesired: &ready, ImagePresent: &present, Fresh: true, ObservedAt: time.Now().UTC(),
			ClusterID: "cluster-current", Generation: 2, ObservedGeneration: 2,
			EvidenceSource: runtime.AppObservationSourceKubernetesAPI,
		},
	}
	if !ReadyForServing(app, time.Now().UTC()) {
		t.Fatal("complete matching runtime evidence should be publishable")
	}
	app.ObservedStatus.DesiredReplicas = 0
	if ReadyForServing(app, time.Now().UTC()) {
		t.Fatal("observed desired replicas below the durable spec were accepted")
	}
}
