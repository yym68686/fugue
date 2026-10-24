package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
)

func TestDeclaredHealthBudgetPreservesDefaultAndOuterCancellation(t *testing.T) {
	release := declarativerelease.PlanRelease{}
	if got := healthWaitTimeout(release, 120*time.Second); got != 120*time.Second {
		t.Fatal("default component budget changed", got)
	}
	release.Workload.HealthTimeoutSeconds = 240
	if got := healthWaitTimeout(release, 120*time.Second); got != 240*time.Second {
		t.Fatal("explicit cold recovery budget ignored", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cluster := &kubectlCluster{timeout: time.Millisecond}
	if observation, err := cluster.WaitHealthy(ctx, release, declarativerelease.TargetIdentity{Present: true}, nil); err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) || observation.HealthDigest != "" {
		t.Fatal("longer budget bypassed outer cancellation or manufactured success", err)
	}
}
