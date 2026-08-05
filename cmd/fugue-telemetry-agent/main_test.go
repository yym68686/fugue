package main

import (
	"testing"

	"fugue/internal/observability"
)

func TestBoundTelemetryAgentMemoryCapsRetainedQueueAndBatchRisk(t *testing.T) {
	cfg := boundTelemetryAgentMemory(observability.Config{
		QueueSize:                     32768,
		BatchSize:                     512,
		MemoryLimitBytes:              128 << 20,
		KubernetesLogTailLines:        2000,
		KubernetesLogMaxPods:          500,
		KubernetesLogMaxLinesPerCycle: 20000,
	})
	if cfg.MemoryLimitBytes != telemetryAgentMemoryLimitBytes || cfg.QueueSize != telemetryAgentQueueSize || cfg.BatchSize != telemetryAgentBatchSize ||
		cfg.KubernetesLogTailLines != telemetryAgentKubernetesLogTailLines || cfg.KubernetesLogMaxPods != telemetryAgentKubernetesLogMaxPods ||
		cfg.KubernetesLogMaxLinesPerCycle != telemetryAgentKubernetesLogMaxLines {
		t.Fatalf("telemetry memory bounds were not applied: %+v", cfg)
	}
}

func TestBoundTelemetryAgentMemoryPreservesStricterConfiguration(t *testing.T) {
	cfg := boundTelemetryAgentMemory(observability.Config{
		QueueSize:                     16,
		BatchSize:                     8,
		MemoryLimitBytes:              8 << 20,
		KubernetesLogTailLines:        20,
		KubernetesLogMaxPods:          10,
		KubernetesLogMaxLinesPerCycle: 100,
	})
	if cfg.MemoryLimitBytes != 8<<20 || cfg.BatchSize != 8 || cfg.QueueSize != 16 || cfg.KubernetesLogTailLines != 20 ||
		cfg.KubernetesLogMaxPods != 10 || cfg.KubernetesLogMaxLinesPerCycle != 100 {
		t.Fatalf("stricter telemetry memory bounds changed: %+v", cfg)
	}
}
