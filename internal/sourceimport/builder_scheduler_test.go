package sourceimport

import (
	"reflect"
	"testing"
)

func TestSelectBuilderCandidatesLightPrefersSmallNodes(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileLight)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileLight, demand, []builderNodeSnapshot{
		builderTestNode("large-a", "large-a", policy, policy.LargeNodeLabelValue, "4000m", "16Gi", "32Gi", "2500m", "9Gi", "24Gi"),
		builderTestNode("small-a", "small-a", policy, policy.SmallNodeLabelValue, "2000m", "8Gi", "8Gi", "200m", "1Gi", "1Gi"),
	}, nil)

	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "small-a" {
		t.Fatalf("expected small node to rank first for light build, got %q", got)
	}
}

func TestSelectBuilderCandidatesHeavyPrefersLargeAndSkipsSmall(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileHeavy, demand, []builderNodeSnapshot{
		builderTestNode("small-a", "small-a", policy, policy.SmallNodeLabelValue, "4000m", "16Gi", "16Gi", "250m", "1Gi", "1Gi"),
		builderTestNode("medium-a", "medium-a", policy, policy.MediumNodeLabelValue, "4000m", "16Gi", "20Gi", "500m", "1Gi", "3Gi"),
		builderTestNode("large-a", "large-a", policy, policy.LargeNodeLabelValue, "4000m", "16Gi", "30Gi", "500m", "1Gi", "3Gi"),
	}, nil)

	if len(candidates) != 2 {
		t.Fatalf("expected 2 heavy candidates, got %d", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "large-a" {
		t.Fatalf("expected large node to rank first for heavy build, got %q", got)
	}
	for _, candidate := range candidates {
		if candidate.Node.Name == "small-a" {
			t.Fatalf("expected heavy build to exclude explicit small nodes")
		}
	}
}

func TestSelectBuilderCandidatesReservationsReduceHeadroom(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileLight)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileLight, demand, []builderNodeSnapshot{
		builderTestNode("medium-a", "medium-a", policy, policy.MediumNodeLabelValue, "3000m", "8Gi", "8Gi", "0", "0", "0"),
		builderTestNode("medium-b", "medium-b", policy, policy.MediumNodeLabelValue, "3000m", "8Gi", "8Gi", "0", "0", "0"),
	}, []builderReservation{
		{
			Name:     "reservation-a",
			NodeName: "medium-a",
			Demand: builderResourceDemand{
				EphemeralBytes: parseBuilderBytes("6Gi"),
			},
		},
	})

	if len(candidates) != 1 {
		t.Fatalf("expected reservation to disqualify one node, got %d candidates", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "medium-b" {
		t.Fatalf("expected medium-b to remain eligible, got %q", got)
	}
}

func TestSelectBuilderCandidatesFallsBackWhenNoBuildPoolLabelsExist(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileLight)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	snapshots := []builderNodeSnapshot{
		{
			Name:     "node-a",
			Hostname: "node-a",
			Labels: map[string]string{
				policy.LargeNodeLabelKey: policy.MediumNodeLabelValue,
			},
			Ready: true,
			Allocatable: builderResourceDemand{
				CPUMilli:       parseBuilderCPUMilli("2000m"),
				MemoryBytes:    parseBuilderBytes("8Gi"),
				EphemeralBytes: parseBuilderBytes("10Gi"),
			},
		},
		{
			Name:     "node-b",
			Hostname: "node-b",
			Labels: map[string]string{
				policy.LargeNodeLabelKey: policy.LargeNodeLabelValue,
			},
			Ready: true,
			Allocatable: builderResourceDemand{
				CPUMilli:       parseBuilderCPUMilli("3000m"),
				MemoryBytes:    parseBuilderBytes("12Gi"),
				EphemeralBytes: parseBuilderBytes("20Gi"),
			},
		},
	}

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileLight, demand, snapshots, nil)
	if len(candidates) != 2 {
		t.Fatalf("expected unlabeled healthy nodes to remain eligible, got %d", len(candidates))
	}
}

func TestBuildBuilderPlacementPrefersSelectedNode(t *testing.T) {
	t.Parallel()

	placement := buildBuilderPlacement([]builderCandidate{
		{Node: builderNodeSnapshot{Name: "node-a", Hostname: "host-a"}},
		{Node: builderNodeSnapshot{Name: "node-b", Hostname: "host-b"}},
		{Node: builderNodeSnapshot{Name: "node-c", Hostname: "host-c"}},
	}, "node-b", 2)

	if placement.PreferredHostname != "host-b" {
		t.Fatalf("expected preferred hostname host-b, got %q", placement.PreferredHostname)
	}
	expected := []string{"host-b", "host-a"}
	if !reflect.DeepEqual(placement.CandidateHostnames, expected) {
		t.Fatalf("expected ordered hostnames %v, got %v", expected, placement.CandidateHostnames)
	}
}

func TestBuildBuildpacksJobObjectPlacementOverridesGenericAffinity(t *testing.T) {
	t.Parallel()

	jobObject, err := buildBuildpacksJobObject("fugue-system", "build-demo", buildpacksBuildRequest{
		ArchiveDownloadURL: "https://example.com/archive.tar.gz",
		SourceDir:          ".",
		ImageRef:           "10.128.0.2:30500/fugue-apps/demo:git-abc123",
		WorkloadProfile:    builderWorkloadProfileHeavy,
		Placement: builderJobPlacement{
			CandidateHostnames: []string{"host-b", "host-a"},
			PreferredHostname:  "host-b",
		},
	})
	if err != nil {
		t.Fatalf("build buildpacks job object: %v", err)
	}

	podSpec := jobObject["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	nodeAffinity := podSpec["affinity"].(map[string]any)["nodeAffinity"].(map[string]any)

	required := nodeAffinity["requiredDuringSchedulingIgnoredDuringExecution"].(map[string]any)
	requiredTerms := required["nodeSelectorTerms"].([]map[string]any)
	requiredMatchExpressions := requiredTerms[0]["matchExpressions"].([]map[string]any)
	if got := requiredMatchExpressions[0]["key"]; got != builderHostnameLabelKey {
		t.Fatalf("expected required hostname key %q, got %#v", builderHostnameLabelKey, got)
	}
	if got := requiredMatchExpressions[0]["values"].([]string); !reflect.DeepEqual(got, []string{"host-b", "host-a"}) {
		t.Fatalf("expected required hostnames [host-b host-a], got %v", got)
	}

	preferred := nodeAffinity["preferredDuringSchedulingIgnoredDuringExecution"].([]map[string]any)
	preference := preferred[0]["preference"].(map[string]any)
	preferredMatchExpressions := preference["matchExpressions"].([]map[string]any)
	if got := preferredMatchExpressions[0]["key"]; got != builderHostnameLabelKey {
		t.Fatalf("expected preferred hostname key %q, got %#v", builderHostnameLabelKey, got)
	}
	if got := preferredMatchExpressions[0]["values"].([]string); !reflect.DeepEqual(got, []string{"host-b"}) {
		t.Fatalf("expected preferred hostname [host-b], got %v", got)
	}
}

func builderTestNode(name, hostname string, policy BuilderPodPolicy, sizeClass, cpu, memory, ephemeral, usedCPU, usedMemory, usedEphemeral string) builderNodeSnapshot {
	return builderNodeSnapshot{
		Name:     name,
		Hostname: hostname,
		Labels: map[string]string{
			policy.BuildNodeLabelKey: valueOrDefault(policy.BuildNodeLabelValue, "true"),
			policy.LargeNodeLabelKey: sizeClass,
		},
		Ready: true,
		Allocatable: builderResourceDemand{
			CPUMilli:       parseBuilderCPUMilli(cpu),
			MemoryBytes:    parseBuilderBytes(memory),
			EphemeralBytes: parseBuilderBytes(ephemeral),
		},
		Used: builderResourceDemand{
			CPUMilli:       parseBuilderCPUMilli(usedCPU),
			MemoryBytes:    parseBuilderBytes(usedMemory),
			EphemeralBytes: parseBuilderBytes(usedEphemeral),
		},
	}
}

func valueOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
