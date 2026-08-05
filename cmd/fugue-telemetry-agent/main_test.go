package main

import (
	"testing"

	"fugue/internal/observability"
)

func TestBoundTelemetryAgentMemoryCapsRetainedQueueAndBatchRisk(t *testing.T) {
	cfg := boundTelemetryAgentMemory(observability.Config{
		QueueSize:        32768,
		BatchSize:        512,
		MemoryLimitBytes: 128 << 20,
	})
	if cfg.MemoryLimitBytes != telemetryAgentMemoryLimitBytes || cfg.BatchSize != telemetryAgentBatchSize {
		t.Fatalf("telemetry memory bounds were not applied: %+v", cfg)
	}
}

func TestBoundTelemetryAgentMemoryPreservesStricterConfiguration(t *testing.T) {
	cfg := boundTelemetryAgentMemory(observability.Config{
		QueueSize:        16,
		BatchSize:        8,
		MemoryLimitBytes: 8 << 20,
	})
	if cfg.MemoryLimitBytes != 8<<20 || cfg.BatchSize != 8 || cfg.QueueSize != 16 {
		t.Fatalf("stricter telemetry memory bounds changed: %+v", cfg)
	}
}
