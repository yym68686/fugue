package controller

import (
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestManagedPostgresCPULimitPressure(t *testing.T) {
	for _, tc := range []struct {
		input string
		ok    bool
	}{
		{"10000 100000 1000 800 1030 827", true},
		{"10000 100000 1000 800 1030 815", true},
		{"10000 100000 1000 800 1030 814", false},
		{"10000 100000 1000 800 1010 810", false},
		{"10000 100000 1000 800 30 27", false},
		{"10000 100000 1000 800 1030 831", false},
		{"20000 100000 1000 800 1030 827", false},
		{"max 100000 1000 800 1030 827", false},
		{"10000 100000 1000 800 1030", false},
	} {
		if got := managedPostgresCPUWindowThrottled(tc.input, 100); got != tc.ok {
			t.Errorf("%q: throttled=%t, want %t", tc.input, got, tc.ok)
		}
	}
}

func TestManagedPostgresCPULimitUsageFreshAndSustained(t *testing.T) {
	now := time.Now()
	at := func(minutes int, cpu int64) model.ResourceUsageSample {
		return model.ResourceUsageSample{ObservedAt: now.Add(-time.Duration(minutes) * time.Minute), CPUMilliCores: &cpu}
	}
	for _, tc := range []struct {
		samples []model.ResourceUsageSample
		ok      bool
	}{
		{[]model.ResourceUsageSample{at(1, 91), at(11, 98), at(6, 90)}, true},
		{[]model.ResourceUsageSample{at(1, 96), at(2, 95), at(6, 97), at(7, 98)}, true},
		{[]model.ResourceUsageSample{at(1, 96), at(2, 10), at(6, 97), at(7, 98)}, false},
		{[]model.ResourceUsageSample{at(1, 96), at(2, 96), at(6, 97), at(7, 10)}, true},
		{[]model.ResourceUsageSample{at(9, 100), at(14, 100)}, false},
		{[]model.ResourceUsageSample{at(1, 100), at(2, 100)}, false},
		{[]model.ResourceUsageSample{at(1, 100), at(1, 100)}, false},
		{[]model.ResourceUsageSample{at(1, 89), at(6, 100), at(11, 100)}, false},
		{[]model.ResourceUsageSample{at(1, 100), at(6, 89), at(11, 100)}, false},
		{[]model.ResourceUsageSample{at(-1, 100), at(5, 100)}, false},
	} {
		if got := managedPostgresCPURecentlySaturated(tc.samples, now, 100); got != tc.ok {
			t.Errorf("samples=%+v: saturated=%t, want %t", tc.samples, got, tc.ok)
		}
	}
}

func TestManagedPostgresCPULimitTargetFence(t *testing.T) {
	enrollment := config.ManagedPostgresCPULimitTarget{
		ManagedPostgresCPURequestTarget: config.ManagedPostgresCPURequestTarget{ServiceID: "svc", Namespace: "test", Cluster: "db", ClusterUID: "cluster-uid", SystemID: "123"},
		MaxLimitMilliCores:              500,
	}
	var snapshot managedPostgresResizeSnapshot
	snapshot.Cluster.Metadata.Name, snapshot.Cluster.Metadata.UID = enrollment.Cluster, enrollment.ClusterUID
	snapshot.Cluster.Status.SystemID, snapshot.Cluster.Spec.Instances = enrollment.SystemID, 1
	o := &snapshot.Observation
	o.Namespace, o.PodUID, o.ContainerStartedAt = enrollment.Namespace, "pod", "2026-01-01T00:00:00Z"
	o.DesiredResources, _ = managedPostgresKubeResources(model.ResourceSpec{CPUMilliCores: 100, CPULimitMilliCores: 100, MemoryMebibytes: 256, MemoryLimitMebibytes: 768})
	target := cloneKubeResourceRequirements(o.DesiredResources)
	target.Limits["cpu"] = "200m"
	actor := managedPostgresCPULimitActor(snapshot, 100)
	if err := validateManagedPostgresAutoCPULimit(actor, enrollment, snapshot, target); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*managedPostgresResizeSnapshot, *config.ManagedPostgresCPULimitTarget, *kubeResourceRequirements){
		func(s *managedPostgresResizeSnapshot, _ *config.ManagedPostgresCPULimitTarget, _ *kubeResourceRequirements) {
			s.Observation.RestartCount++
		},
		func(s *managedPostgresResizeSnapshot, _ *config.ManagedPostgresCPULimitTarget, _ *kubeResourceRequirements) {
			s.Cluster.Status.SystemID = "456"
		},
		func(s *managedPostgresResizeSnapshot, _ *config.ManagedPostgresCPULimitTarget, _ *kubeResourceRequirements) {
			s.Cluster.Spec.Instances = 2
		},
		func(_ *managedPostgresResizeSnapshot, e *config.ManagedPostgresCPULimitTarget, _ *kubeResourceRequirements) {
			e.MaxLimitMilliCores = 150
		},
		func(_ *managedPostgresResizeSnapshot, _ *config.ManagedPostgresCPULimitTarget, r *kubeResourceRequirements) {
			r.Requests["cpu"] = "150m"
		},
		func(_ *managedPostgresResizeSnapshot, _ *config.ManagedPostgresCPULimitTarget, r *kubeResourceRequirements) {
			r.Limits["cpu"] = "250m"
		},
		func(_ *managedPostgresResizeSnapshot, _ *config.ManagedPostgresCPULimitTarget, r *kubeResourceRequirements) {
			r.Requests["memory"] = "300Mi"
		},
	} {
		s, e, r := snapshot, enrollment, cloneKubeResourceRequirements(target)
		change(&s, &e, &r)
		if err := validateManagedPostgresAutoCPULimit(actor, e, s, r); err == nil {
			t.Fatalf("accepted drift: %+v %+v %+v", s, e, r)
		}
	}
	o.DesiredResources = target
	if err := validateManagedPostgresAutoCPULimit(actor, enrollment, snapshot, target); err != nil {
		t.Fatalf("must permit own accepted target to converge: %v", err)
	}
}

func TestManagedPostgresCPULimitBoundAndCooldown(t *testing.T) {
	for _, tc := range []struct{ current, cap, want int64 }{{100, 500, 200}, {200, 500, 400}, {400, 500, 500}, {500, 500, 0}, {0, 500, 0}, {100, 64001, 0}} {
		if got := managedPostgresNextCPULimit(tc.current, tc.cap); got != tc.want {
			t.Fatalf("current=%d cap=%d: got %d, want %d", tc.current, tc.cap, got, tc.want)
		}
	}
	now := time.Now()
	recent := model.Operation{ServiceID: "svc", Type: model.OperationTypeDatabaseResize, RequestedByID: managedPostgresAutoCPULimitActor + "/digest/100", Status: model.OperationStatusCompleted, UpdatedAt: now.Add(-7 * time.Minute)}
	ops := []model.Operation{recent}
	if !managedPostgresCPULimitCoolingDown(ops, "svc", now) || managedPostgresCPULimitCoolingDown(ops, "other", now) || managedPostgresCPULimitCoolingDown(ops, "svc", now.Add(6*time.Minute)) {
		t.Fatal("wrong success cool down")
	}
	ops[0].Status = model.OperationStatusFailed
	if !managedPostgresCPULimitCoolingDown(ops, "svc", now.Add(6*time.Minute)) || managedPostgresCPULimitCoolingDown(ops, "svc", now.Add(time.Hour)) {
		t.Fatal("wrong failure backoff")
	}
}
