package main

import (
	"strings"
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
	if cfg.KubernetesLogMaxPods != observability.DefaultKubernetesLogMaxPods {
		t.Fatalf("telemetry pod coverage drifted below the configured default: %+v", cfg)
	}
}

func TestConfiguredTelemetryBurstFitsWithoutIncreasingPayloadBudget(t *testing.T) {
	cfg := boundTelemetryAgentMemory(observability.Config{Enabled: true, QueueSize: 8192, KubernetesLogMaxLinesPerCycle: 8000, MemoryLimitBytes: 128 << 20})
	if cfg.QueueSize != 8192 || cfg.KubernetesLogMaxLinesPerCycle != 8000 {
		t.Fatalf("configured bounded throughput was silently reduced: queue=%d cycle=%d", cfg.QueueSize, cfg.KubernetesLogMaxLinesPerCycle)
	}
	p := observability.NewPipeline(cfg, nil)
	for i := 0; i < 6000; i++ {
		if !p.IngestLogLine(t.Context(), "synthetic-burst", "bounded request completed") {
			t.Fatalf("small-event burst blocked at %d: %+v", i, p.Snapshot())
		}
	}
	if s := p.Snapshot(); s.Received != 6000 || s.Dropped != 0 || s.QueuedBytes > 16<<20 {
		t.Fatalf("burst violated memory/admission bounds: %+v", s)
	}
	large := strings.Repeat("x", 1<<20)
	rejected := false
	for i := 0; i < 32; i++ {
		if !p.IngestLogLine(t.Context(), "synthetic-burst", large) {
			rejected = true
			break
		}
	}
	if !rejected || p.Snapshot().QueuedBytes > 16<<20 || cfg.MemoryLimitBytes != 16<<20 || cfg.BatchSize != 128 {
		t.Fatalf("larger count capacity bypassed payload/batch protection: %+v", p.Snapshot())
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
