package controller

import (
	"testing"
	"time"

	"fugue/internal/runtime"
)

func TestManagedRolloutCapacityDomainKeyIgnoresMapAndTolerationOrder(t *testing.T) {
	t.Parallel()

	left := runtime.SchedulingConstraints{
		NodeSelector: map[string]string{"node": "ns101351", "pool": "shared"},
		Tolerations: []runtime.Toleration{
			{Key: "dedicated", Operator: "Equal", Value: "apps", Effect: "NoSchedule"},
			{Key: "maintenance", Operator: "Exists", Effect: "NoExecute"},
		},
	}
	right := runtime.SchedulingConstraints{
		NodeSelector: map[string]string{"pool": "shared", "node": "ns101351"},
		Tolerations: []runtime.Toleration{
			{Key: "maintenance", Operator: "Exists", Effect: "NoExecute"},
			{Key: "dedicated", Operator: "Equal", Value: "apps", Effect: "NoSchedule"},
		},
	}
	if got, want := managedRolloutCapacityDomainKey(left), managedRolloutCapacityDomainKey(right); got != want {
		t.Fatalf("equivalent scheduling constraints produced different capacity domains: %q != %q", got, want)
	}
}

func TestManagedRolloutCapacityDomainLockSerializesSameDomain(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	scheduling := runtime.SchedulingConstraints{NodeSelector: map[string]string{"kubernetes.io/hostname": "ns101351"}}
	unlockFirst := svc.lockManagedRolloutCapacityDomain(scheduling)
	acquired := make(chan func(), 1)
	go func() {
		acquired <- svc.lockManagedRolloutCapacityDomain(scheduling)
	}()

	select {
	case unlockSecond := <-acquired:
		unlockSecond()
		t.Fatal("second rollout acquired the same capacity domain concurrently")
	case <-time.After(25 * time.Millisecond):
	}
	unlockFirst()
	unlockSecond := <-acquired
	unlockSecond()
}
