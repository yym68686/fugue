package oomrightsizing

import (
	"testing"

	"fugue/internal/model"
)

func TestOOMExpandedResourceSpecGrowsRequestAndLimitWithoutChangingCPU(t *testing.T) {
	t.Parallel()

	got := oomExpandedResourceSpec(&model.ResourceSpec{
		CPUMilliCores:        500,
		CPULimitMilliCores:   750,
		MemoryMebibytes:      1024,
		MemoryLimitMebibytes: 1024,
	}, model.BackingServiceTypePostgres)

	if got.CPUMilliCores != 500 || got.CPULimitMilliCores != 750 {
		t.Fatalf("expected CPU resources to remain unchanged, got %+v", got)
	}
	if got.MemoryMebibytes != 1536 || got.MemoryLimitMebibytes != 1664 {
		t.Fatalf("expected OOM growth to 1536Mi request and 1664Mi limit, got %+v", got)
	}
}
