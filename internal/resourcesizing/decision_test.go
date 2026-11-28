package resourcesizing

import (
	"testing"

	"fugue/internal/model"
)

func TestAutoRightSizingMixedDirectionPrioritizesMaterialUpscale(t *testing.T) {
	t.Parallel()

	current := &model.ResourceSpec{
		CPUMilliCores:        1030,
		MemoryMebibytes:      2288,
		MemoryLimitMebibytes: 4576,
	}
	recommended := &model.ResourceSpec{
		CPUMilliCores:        1515,
		MemoryMebibytes:      2256,
		MemoryLimitMebibytes: 4512,
	}

	decision := AutomaticChange(current, recommended)
	if !decision.Allowed {
		t.Fatal("expected a material CPU increase to be allowed even when memory recommendations decrease")
	}
	if decision.Downscale {
		t.Fatal("expected mixed-direction recommendation to be treated as an upscale")
	}
	if decision.RequestedByID != model.OperationRequestedByRightSizing {
		t.Fatalf("expected upscale requester %q, got %q", model.OperationRequestedByRightSizing, decision.RequestedByID)
	}
	if decision.Resources == nil {
		t.Fatal("expected an upscale resource target")
	}
	if got := decision.Resources.CPUMilliCores; got != recommended.CPUMilliCores {
		t.Fatalf("expected CPU request to increase to %dm, got %dm", recommended.CPUMilliCores, got)
	}
	if got := decision.Resources.MemoryMebibytes; got != current.MemoryMebibytes {
		t.Fatalf("expected memory request to remain at %dMi, got %dMi", current.MemoryMebibytes, got)
	}
	if got := decision.Resources.MemoryLimitMebibytes; got != current.MemoryLimitMebibytes {
		t.Fatalf("expected memory limit to remain at %dMi, got %dMi", current.MemoryLimitMebibytes, got)
	}
}

func TestAutoRightSizingCPURequestDownscaleConvergesDirectly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		current     *model.ResourceSpec
		recommended *model.ResourceSpec
		wantCPU     int64
	}{
		{
			name: "below managed default",
			current: &model.ResourceSpec{
				CPUMilliCores:   150,
				MemoryMebibytes: 512,
			},
			recommended: &model.ResourceSpec{
				CPUMilliCores:   25,
				MemoryMebibytes: 512,
			},
			wantCPU: 25,
		},
		{
			name: "low CPU service",
			current: &model.ResourceSpec{
				CPUMilliCores:   50,
				MemoryMebibytes: 512,
			},
			recommended: &model.ResourceSpec{
				CPUMilliCores:   25,
				MemoryMebibytes: 512,
			},
			wantCPU: 25,
		},
		{
			name: "low CPU rounding boundary",
			current: &model.ResourceSpec{
				CPUMilliCores:   15,
				MemoryMebibytes: 128,
			},
			recommended: &model.ResourceSpec{
				CPUMilliCores:   10,
				MemoryMebibytes: 128,
			},
			wantCPU: 10,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			decision := AutomaticChange(test.current, test.recommended)
			if !decision.Allowed || !decision.Downscale || decision.Resources == nil {
				t.Fatalf("expected CPU downscale, got %+v", decision)
			}
			if got := decision.Resources.CPUMilliCores; got != test.wantCPU {
				t.Fatalf("expected CPU target %dm, got %dm", test.wantCPU, got)
			}
			current := *test.current
			if decision.Resources.CPUMilliCores >= current.CPUMilliCores {
				t.Fatalf("downscale must lower CPU from %dm, got %dm", current.CPUMilliCores, decision.Resources.CPUMilliCores)
			}
			if decision.Resources.MemoryMebibytes != current.MemoryMebibytes {
				t.Fatalf("CPU-only downscale must preserve memory at %dMi, got %dMi", current.MemoryMebibytes, decision.Resources.MemoryMebibytes)
			}
		})
	}
}

func TestAutoRightSizingNilResourcesAreZeroRequestUpscale(t *testing.T) {
	t.Parallel()
	recommended := &model.ResourceSpec{CPUMilliCores: 25, MemoryMebibytes: 64}
	decision := AutomaticChange(nil, recommended)
	if !decision.Allowed || decision.Downscale || decision.Resources == nil {
		t.Fatalf("expected nil resources to be an allowed upscale, got %+v", decision)
	}
	if decision.Resources.CPUMilliCores != 25 || decision.Resources.MemoryMebibytes != 64 {
		t.Fatalf("unexpected upscale target: %+v", decision.Resources)
	}
}

func TestAutoRightSizingSmallCPURequestIncreaseUsesGuaranteeHysteresis(t *testing.T) {
	t.Parallel()

	current := &model.ResourceSpec{CPUMilliCores: 25, MemoryMebibytes: 128}
	recommended := &model.ResourceSpec{CPUMilliCores: 40, MemoryMebibytes: 128}
	decision := AutomaticChange(current, recommended)
	if !decision.Allowed || decision.Downscale || decision.Resources == nil || decision.Resources.CPUMilliCores != 40 {
		t.Fatalf("expected material small CPU guarantee increase to apply, got %+v", decision)
	}
}

func TestAutoRightSizingLowCPUDownscaleKeepsRatioHysteresis(t *testing.T) {
	t.Parallel()

	current := &model.ResourceSpec{CPUMilliCores: 200, MemoryMebibytes: 128}
	recommended := &model.ResourceSpec{CPUMilliCores: 195, MemoryMebibytes: 128}
	decision := AutomaticChange(current, recommended)
	if decision.Allowed {
		t.Fatalf("expected a CPU change below 5%% to remain blocked, got %+v", decision)
	}
}

func TestAutoRightSizingDownscaleDimensionsAreIndependent(t *testing.T) {
	t.Parallel()

	t.Run("CPU-only downscale preserves memory", func(t *testing.T) {
		t.Parallel()

		current := &model.ResourceSpec{
			CPUMilliCores:        50,
			MemoryMebibytes:      288,
			MemoryLimitMebibytes: 576,
		}
		recommended := &model.ResourceSpec{
			CPUMilliCores:        25,
			MemoryMebibytes:      280,
			MemoryLimitMebibytes: 560,
		}
		decision := AutomaticChange(current, recommended)
		if !decision.Allowed || decision.Resources == nil {
			t.Fatalf("expected CPU downscale, got %+v", decision)
		}
		if got := decision.Resources; got.CPUMilliCores != 25 || got.MemoryMebibytes != 288 || got.MemoryLimitMebibytes != 576 {
			t.Fatalf("expected only CPU to downscale, got %+v", got)
		}
	})

	t.Run("memory-only downscale preserves CPU", func(t *testing.T) {
		t.Parallel()

		current := &model.ResourceSpec{
			CPUMilliCores:        100,
			MemoryMebibytes:      1024,
			MemoryLimitMebibytes: 2048,
		}
		recommended := &model.ResourceSpec{
			CPUMilliCores:        96,
			MemoryMebibytes:      512,
			MemoryLimitMebibytes: 1024,
		}
		decision := AutomaticChange(current, recommended)
		if !decision.Allowed || decision.Resources == nil {
			t.Fatalf("expected memory downscale, got %+v", decision)
		}
		if got := decision.Resources; got.CPUMilliCores != 100 || got.MemoryMebibytes != 768 || got.MemoryLimitMebibytes != 1536 {
			t.Fatalf("expected only memory to downscale, got %+v", got)
		}
	})

	t.Run("minor memory increase does not block CPU downscale", func(t *testing.T) {
		t.Parallel()

		current := &model.ResourceSpec{CPUMilliCores: 50, MemoryMebibytes: 256}
		recommended := &model.ResourceSpec{CPUMilliCores: 25, MemoryMebibytes: 264}
		decision := AutomaticChange(current, recommended)
		if !decision.Allowed || decision.Resources == nil {
			t.Fatalf("expected CPU downscale, got %+v", decision)
		}
		if got := decision.Resources; got.CPUMilliCores != 25 || got.MemoryMebibytes != 256 {
			t.Fatalf("expected CPU downscale with memory preserved, got %+v", got)
		}
	})
}

func TestAutoRightSizingDownscaleContinuesBelowManagedCreationDefault(t *testing.T) {
	t.Parallel()

	current := &model.ResourceSpec{
		CPUMilliCores:        150,
		MemoryMebibytes:      400,
		MemoryLimitMebibytes: 800,
	}
	recommended := &model.ResourceSpec{
		CPUMilliCores:        25,
		MemoryMebibytes:      64,
		MemoryLimitMebibytes: 128,
	}
	decision := AutomaticChange(current, recommended)
	if !decision.Allowed || decision.Resources == nil {
		t.Fatalf("expected CPU downscale, got %+v", decision)
	}
	if got := decision.Resources; got.CPUMilliCores != 25 || got.MemoryMebibytes != 304 || got.MemoryLimitMebibytes != 608 {
		t.Fatalf("downscale must keep reducing memory below managed creation defaults, got %+v", got)
	}
}

func TestAutoRightSizingMemoryChangesUseOnlyFivePercentThreshold(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name               string
		current, target    int64
		allowed, downscale bool
	}{
		{"exact five percent decrease", 320, 304, true, true},
		{"below five percent decrease", 320, 305, false, false},
		{"exact five percent increase", 320, 336, true, false},
		{"below five percent increase", 320, 335, false, false},
		{"small absolute decrease", 16, 15, true, true},
		{"small absolute increase", 16, 17, true, false},
		{"no change", 320, 320, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			current := &model.ResourceSpec{CPUMilliCores: 10, MemoryMebibytes: test.current}
			target := &model.ResourceSpec{CPUMilliCores: 10, MemoryMebibytes: test.target}
			decision := AutomaticChange(current, target)
			if decision.Allowed != test.allowed || decision.Downscale != test.downscale {
				t.Fatalf("unexpected decision for %dMi -> %dMi: %+v", test.current, test.target, decision)
			}
			if !test.allowed {
				return
			}
			if decision.Resources == nil || decision.Resources.CPUMilliCores != current.CPUMilliCores {
				t.Fatalf("memory-only change must preserve CPU: %+v", decision)
			}
			got := decision.Resources.MemoryMebibytes
			if test.downscale {
				if got >= test.current || got < test.target {
					t.Fatalf("memory downscale must progress without undershooting recommendation: %+v", decision)
				}
			} else if got != test.target {
				t.Fatalf("expected memory increase to %dMi, got %dMi", test.target, got)
			}
		})
	}
}

func TestAutoRightSizingMemoryDownscaleConvergesWithoutDefaultFloor(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name            string
		current, target int64
	}{
		{"below creation default", 512, 96},
		{"already below creation default", 400, 64},
		{"below recommendation policy floors", 64, 16},
		{"below rounding quantum", 16, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			current := &model.ResourceSpec{CPUMilliCores: 10, MemoryMebibytes: test.current, MemoryLimitMebibytes: 2 * test.current}
			target := &model.ResourceSpec{CPUMilliCores: 10, MemoryMebibytes: test.target, MemoryLimitMebibytes: 2 * test.target}
			for scan := 0; current.MemoryMebibytes != test.target; scan++ {
				if scan >= 64 {
					t.Fatalf("did not converge to %dMi: %+v", test.target, current)
				}
				decision := AutomaticChange(current, target)
				if !decision.Allowed || !decision.MemoryDownscale || decision.Resources == nil {
					t.Fatalf("memory downscale stalled above %dMi: current=%+v decision=%+v", test.target, current, decision)
				}
				next := decision.Resources
				if next.MemoryMebibytes >= current.MemoryMebibytes || next.MemoryMebibytes < test.target {
					t.Fatalf("invalid memory progression: current=%+v next=%+v", current, next)
				}
				if next.MemoryLimitMebibytes < next.MemoryMebibytes || next.MemoryLimitMebibytes > current.MemoryLimitMebibytes {
					t.Fatalf("invalid memory limit progression: current=%+v next=%+v", current, next)
				}
				current = next
			}
		})
	}
}

func TestAppCPURequestOnlyChangeDoesNotSwallowOtherDimensions(t *testing.T) {
	current := &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 128, MemoryLimitMebibytes: 256}
	target := *current
	target.CPUMilliCores = 75
	if !CPURequestOnlyChange(current, &target) {
		t.Fatal("CPU-only downscale not selected")
	}
	for _, change := range []string{"memory", "memory-limit", "cpu-limit"} {
		other := target
		switch change {
		case "memory":
			other.MemoryMebibytes++
		case "memory-limit":
			other.MemoryLimitMebibytes++
		case "cpu-limit":
			other.CPULimitMilliCores++
		}
		if CPURequestOnlyChange(current, &other) {
			t.Fatal("lost recommendation dimension", change)
		}
	}
	if CPURequestOnlyChange(nil, &target) || CPURequestOnlyChange(current, current) {
		t.Fatal("invalid/noop target selected")
	}
}

func TestAppCPUResizeAutoStagesCPUWithoutChangingMemoryOrLimits(t *testing.T) {
	current := &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 256, MemoryLimitMebibytes: 512}
	target := &model.ResourceSpec{CPUMilliCores: 25, MemoryMebibytes: 128, MemoryLimitMebibytes: 256}
	decision := AutomaticCPURequestChange(current, target)
	if !decision.Allowed || decision.Resources.CPUMilliCores != 25 || decision.Resources.MemoryMebibytes != 256 || decision.Resources.MemoryLimitMebibytes != 512 {
		t.Fatal("CPU stage lost independent resource intent", decision)
	}
	target.CPUMilliCores = 96
	if AutomaticCPURequestChange(current, target).Allowed {
		t.Fatal("CPU stage bypassed the change threshold")
	}
}

func TestAppCPUResizeAutoUsesFivePercentAndFiveMillicoreThresholds(t *testing.T) {
	for _, test := range []struct {
		current, target int64
		allowed         bool
	}{
		{120, 100, true}, {100, 95, true}, {100, 105, true},
		{100, 96, false}, {100, 104, false},
		{200, 191, false}, {200, 209, false},
		{200, 190, true}, {200, 210, true},
		{10, 6, false}, {10, 5, true},
	} {
		decision := AutomaticCPURequestChange(&model.ResourceSpec{CPUMilliCores: test.current}, &model.ResourceSpec{CPUMilliCores: test.target})
		if decision.Allowed != test.allowed {
			t.Fatalf("%dm -> %dm allowed=%v, want %v", test.current, test.target, decision.Allowed, test.allowed)
		}
	}
}

func TestAutoAppMemoryRequestChangePreservesOtherDimensionsAndThreshold(t *testing.T) {
	for _, tc := range []struct {
		current, target, want int64
		allowed               bool
	}{{320, 304, 304, true}, {320, 305, 0, false}, {10, 9, 9, true}, {128, 64, 96, true}, {128, 136, 136, true}} {
		current := &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: tc.current, MemoryLimitMebibytes: 2048}
		target := &model.ResourceSpec{CPUMilliCores: 25, MemoryMebibytes: tc.target, MemoryLimitMebibytes: 256}
		d := AutomaticMemoryRequestChange(current, target)
		if d.Allowed != tc.allowed || (tc.allowed && (d.Resources.MemoryMebibytes != tc.want || d.Resources.MemoryLimitMebibytes != 2048 || d.Resources.CPUMilliCores != 100)) {
			t.Fatalf("%d -> %d: %+v", tc.current, tc.target, d)
		}
	}
	if AutomaticMemoryRequestChange(&model.ResourceSpec{MemoryMebibytes: 128, MemoryLimitMebibytes: 256}, &model.ResourceSpec{MemoryMebibytes: 512}).Allowed {
		t.Fatal("request-only resize raised a limit")
	}
}
