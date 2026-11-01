package sourceimport

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

var kubeTimestampPattern = regexp.MustCompile(`\.\d{6}Z$`)

func TestFormatKubeTimestampUsesMicrosecondPrecision(t *testing.T) {
	t.Parallel()

	value := time.Date(2026, time.March, 24, 12, 34, 56, 123456789, time.UTC)

	formatted := formatKubeTimestamp(value)
	expected := "2026-03-24T12:34:56.123456Z"
	if formatted != expected {
		t.Fatalf("expected %q, got %q", expected, formatted)
	}

	parsed, err := time.Parse("2006-01-02T15:04:05.000000Z07:00", formatted)
	if err != nil {
		t.Fatalf("parse formatted timestamp: %v", err)
	}
	if !parsed.UTC().Equal(time.Date(2026, time.March, 24, 12, 34, 56, 123456000, time.UTC)) {
		t.Fatalf("unexpected parsed time: %s", parsed.UTC().Format(time.RFC3339Nano))
	}
}

func TestNewBuilderLeaseUsesMicrosecondPrecision(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.March, 25, 18, 15, 34, 885326946, time.UTC)
	lease := newBuilderLease("lease-a", builderReservationComponentValue, "holder-a", 2*time.Minute, nil, now)

	if got := lease.Spec.AcquireTime; got != "2026-03-25T18:15:34.885326Z" {
		t.Fatalf("expected acquire time with microsecond precision, got %q", got)
	}
	if got := lease.Spec.RenewTime; got != "2026-03-25T18:15:34.885326Z" {
		t.Fatalf("expected renew time with microsecond precision, got %q", got)
	}
}

func TestSelectBuilderCandidatesLightPrefersHigherOverallHeadroom(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileLight)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileLight, demand, []builderNodeSnapshot{
		builderTestNode("higher-headroom", "higher-headroom", policy, "4000m", "16Gi", "20Gi", "200m", "12Gi", "2Gi"),
		builderTestNode("lower-headroom", "lower-headroom", policy, "2000m", "8Gi", "10Gi", "200m", "1Gi", "1Gi"),
	}, nil, nil)

	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates, got %d", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "higher-headroom" {
		t.Fatalf("expected highest overall headroom node to rank first for light build, got %q", got)
	}
}

func TestSelectBuilderCandidatesHeavyPrefersHigherOverallHeadroom(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileHeavy, demand, []builderNodeSnapshot{
		builderTestNode("higher-headroom", "higher-headroom", policy, "4000m", "16Gi", "20Gi", "200m", "11Gi", "2Gi"),
		builderTestNode("lower-headroom", "lower-headroom", policy, "2000m", "8Gi", "10Gi", "200m", "2Gi", "1Gi"),
	}, nil, nil)

	if len(candidates) != 2 {
		t.Fatalf("expected 2 heavy candidates, got %d", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "lower-headroom" {
		t.Fatalf("expected node with highest available memory to rank first for heavy build, got %q", got)
	}
}

func TestBuilderDemandForProfileUsesRequestedResourcesWhenPresent(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()

	lightDemand, err := builderDemandForProfile(policy, builderWorkloadProfileLight)
	if err != nil {
		t.Fatalf("light builder demand: %v", err)
	}
	if lightDemand.CPUMilli != parseBuilderCPUMilli("250m") {
		t.Fatalf("expected light cpu demand 250m, got %dm", lightDemand.CPUMilli)
	}
	if lightDemand.MemoryBytes != parseBuilderBytes("512Mi") {
		t.Fatalf("expected light memory demand 512Mi, got %d", lightDemand.MemoryBytes)
	}
	if lightDemand.EphemeralBytes != parseBuilderBytes("1Gi") {
		t.Fatalf("expected light ephemeral demand 1Gi, got %d", lightDemand.EphemeralBytes)
	}

	heavyDemand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if err != nil {
		t.Fatalf("heavy builder demand: %v", err)
	}
	if heavyDemand.CPUMilli != parseBuilderCPUMilli("250m") {
		t.Fatalf("expected heavy cpu demand 250m, got %dm", heavyDemand.CPUMilli)
	}
	if heavyDemand.MemoryBytes != parseBuilderBytes("512Mi") {
		t.Fatalf("expected heavy placement demand to use the 512Mi minimum admission floor, got %d", heavyDemand.MemoryBytes)
	}
	if heavyDemand.EphemeralBytes != parseBuilderBytes("3Gi") {
		t.Fatalf("expected heavy ephemeral demand 3Gi, got %d", heavyDemand.EphemeralBytes)
	}
}

func TestEscalatedBuilderDemandReservesOOMMemoryTier(t *testing.T) {
	t.Parallel()

	policy := builderPodPolicyForRetryCounts(defaultBuilderPodPolicy(), builderWorkloadProfileHeavy, 1, 0)
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}
	if demand.MemoryBytes != parseBuilderBytes("512Mi") {
		t.Fatalf("expected OOM escalation to retain the dynamic 512Mi admission floor, got %d bytes", demand.MemoryBytes)
	}
}

func TestBuilderAllocatedDemandUsesAvailableMemoryUpToCeiling(t *testing.T) {
	t.Parallel()

	policy := builderPodPolicyWithMemoryCeiling(defaultBuilderPodPolicy(), parseBuilderBytes("12Gi"))
	base := builderResourceDemand{
		CPUMilli:       parseBuilderCPUMilli("250m"),
		MemoryBytes:    parseBuilderBytes("6Gi"),
		EphemeralBytes: parseBuilderBytes("8Gi"),
	}
	for _, test := range []struct {
		name      string
		available string
		expected  string
	}{
		{name: "headroom below ceiling", available: "10Gi", expected: "10Gi"},
		{name: "ceiling caps allocation", available: "20Gi", expected: "12Gi"},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := builderCandidate{Available: builderResourceDemand{
				CPUMilli:       base.CPUMilli,
				MemoryBytes:    parseBuilderBytes(test.available),
				EphemeralBytes: base.EphemeralBytes,
			}}
			got := builderAllocatedDemand(policy, builderWorkloadProfileHeavy, candidate)
			if got.MemoryBytes != parseBuilderBytes(test.expected) {
				t.Fatalf("expected allocated memory %s, got %d", test.expected, got.MemoryBytes)
			}
		})
	}
}

func TestBuilderPodPolicyWithAllocatedDemandSetsMemoryRequestAndLimit(t *testing.T) {
	t.Parallel()

	policy := builderPodPolicyWithAllocatedDemand(defaultBuilderPodPolicy(), builderWorkloadProfileHeavy, builderResourceDemand{MemoryBytes: parseBuilderBytes("10Gi")})
	if got := policy.Heavy.Resources.Requests["memory"]; got != "10240Mi" {
		t.Fatalf("expected heavy memory request 10Gi, got %q", got)
	}
	if got := policy.Heavy.Resources.Limits["memory"]; got != "10240Mi" {
		t.Fatalf("expected heavy memory limit 10Gi, got %q", got)
	}
}

func TestEscalatedBuilderDemandReservesEphemeralTier(t *testing.T) {
	t.Parallel()

	policy := builderPodPolicyForRetryCounts(defaultBuilderPodPolicy(), builderWorkloadProfileHeavy, 0, 1)
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}
	if demand.EphemeralBytes != parseBuilderBytes("12Gi") {
		t.Fatalf("expected escalated builder to reserve 12Gi ephemeral storage, got %d bytes", demand.EphemeralBytes)
	}
}

func TestSelectBuilderCandidatesExcludesNodesBelowReservedDemand(t *testing.T) {
	t.Parallel()

	policy := builderPodPolicyForRetryCounts(defaultBuilderPodPolicy(), builderWorkloadProfileHeavy, 1, 0)
	policy.HeavyMemoryMinimum = "8Gi"
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}
	candidates := selectBuilderCandidates(policy, builderWorkloadProfileHeavy, demand, []builderNodeSnapshot{
		builderTestNode("too-small", "too-small", policy, "4000m", "10Gi", "20Gi", "0", "1Gi", "0"),
		builderTestNode("fits", "fits", policy, "4000m", "16Gi", "20Gi", "0", "1Gi", "0"),
	}, nil, nil)
	candidates = builderFittingCandidates(candidates, demand)
	if len(candidates) != 1 || candidates[0].Node.Name != "fits" {
		t.Fatalf("expected only node with 8Gi plus safety headroom, got %+v", candidates)
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
		builderTestNode("medium-a", "medium-a", policy, "3000m", "8Gi", "8Gi", "0", "0", "0"),
		builderTestNode("medium-b", "medium-b", policy, "3000m", "8Gi", "8Gi", "0", "0", "0"),
	}, []builderReservation{
		{
			Name:     "reservation-a",
			NodeName: "medium-a",
			Demand: builderResourceDemand{
				EphemeralBytes: parseBuilderBytes("6Gi"),
			},
		},
	}, nil)

	if len(candidates) != 2 {
		t.Fatalf("expected both nodes to remain candidates after reservation, got %d candidates", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "medium-b" {
		t.Fatalf("expected medium-b to rank ahead after reservation reduces medium-a headroom, got %q", got)
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
			Labels:   map[string]string{},
			Ready:    true,
			Allocatable: builderResourceDemand{
				CPUMilli:       parseBuilderCPUMilli("2000m"),
				MemoryBytes:    parseBuilderBytes("8Gi"),
				EphemeralBytes: parseBuilderBytes("10Gi"),
			},
		},
		{
			Name:     "node-b",
			Hostname: "node-b",
			Labels:   map[string]string{},
			Ready:    true,
			Allocatable: builderResourceDemand{
				CPUMilli:       parseBuilderCPUMilli("3000m"),
				MemoryBytes:    parseBuilderBytes("12Gi"),
				EphemeralBytes: parseBuilderBytes("20Gi"),
			},
		},
	}

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileLight, demand, snapshots, nil, nil)
	if len(candidates) != 2 {
		t.Fatalf("expected unlabeled healthy nodes to remain eligible, got %d", len(candidates))
	}
}

func TestSelectBuilderCandidatesFallbackExcludesTenantScopedNodes(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileLight)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	snapshots := []builderNodeSnapshot{
		builderTestNodeWithFS("shared-a", "shared-a", policy, "2000m", "8Gi", "10Gi", "18Gi", "0", "0", "0"),
		{
			Name:     "alicehk2",
			Hostname: "alicehk2",
			Labels: map[string]string{
				runtime.NodeModeLabelKey:  model.RuntimeTypeManagedOwned,
				runtime.TenantIDLabelKey:  "tenant_demo",
				runtime.RuntimeIDLabelKey: "runtime_demo",
			},
			Taints: []builderKubeNodeTaint{
				{Key: runtime.TenantTaintKey, Value: "tenant_demo", Effect: "NoSchedule"},
			},
			Ready: true,
			Allocatable: builderResourceDemand{
				CPUMilli:       parseBuilderCPUMilli("4000m"),
				MemoryBytes:    parseBuilderBytes("16Gi"),
				EphemeralBytes: parseBuilderBytes("30Gi"),
			},
			FilesystemAvailableBytes: parseBuilderBytes("28Gi"),
		},
	}

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileLight, demand, snapshots, nil, nil)
	if len(candidates) != 1 {
		t.Fatalf("expected tenant-scoped node to be excluded, got %d candidates", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "shared-a" {
		t.Fatalf("expected shared node to remain eligible, got %q", got)
	}
}

func TestSelectBuilderCandidatesIncludesExplicitBuildPoolTenantScopedNodes(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	shared := builderTestNode("shared-a", "shared-a", policy, "4000m", "16Gi", "24Gi", "250m", "1536Mi", "3Gi")
	alicehk2 := builderTestNode("alicehk2", "fortedrape8", policy, "4000m", "16Gi", "30Gi", "250m", "768Mi", "2Gi")
	alicehk2.Labels[runtime.NodeModeLabelKey] = model.RuntimeTypeManagedOwned
	alicehk2.Labels[runtime.TenantIDLabelKey] = "tenant_demo"
	alicehk2.Labels[runtime.RuntimeIDLabelKey] = "runtime_demo"

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileHeavy, demand, []builderNodeSnapshot{shared, alicehk2}, nil, nil)

	if len(candidates) != 2 {
		t.Fatalf("expected explicit build-pool tenant node to remain eligible, got %d candidates", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "alicehk2" {
		t.Fatalf("expected explicit build-pool tenant node to rank first, got %q", got)
	}
}

func TestSelectBuilderCandidatesIncludesInternalSharedPoolManagedOwnedNodesWithoutBuildLabel(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	sharedPoolNode := builderTestNode("alicehk2", "fortedrape8", policy, "4000m", "8Gi", "30Gi", "250m", "768Mi", "2Gi")
	delete(sharedPoolNode.Labels, policy.BuildNodeLabelKey)
	sharedPoolNode.Labels[runtime.NodeModeLabelKey] = model.RuntimeTypeManagedOwned
	sharedPoolNode.Labels[runtime.TenantIDLabelKey] = "tenant_demo"
	sharedPoolNode.Labels[runtime.SharedPoolLabelKey] = runtime.SharedPoolLabelValue

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileHeavy, demand, []builderNodeSnapshot{sharedPoolNode}, nil, nil)
	if len(candidates) != 1 {
		t.Fatalf("expected internal shared-pool managed-owned node to remain eligible, got %d candidates", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "alicehk2" {
		t.Fatalf("expected alicehk2 to remain eligible via shared-pool label, got %q", got)
	}
}

func TestSelectBuilderCandidatesRanksSharedNodesAheadOfExhaustedBuildPoolNodes(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	exhaustedBuilder := builderTestNode("alicehk2", "fortedrape8", policy, "2000m", "4Gi", "12Gi", "1800m", "3500Mi", "10Gi")
	sharedFallback := builderTestNode("gcp2", "instance-20260322-112431", policy, "4000m", "8Gi", "20Gi", "250m", "1Gi", "2Gi")
	delete(sharedFallback.Labels, policy.BuildNodeLabelKey)

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileHeavy, demand, []builderNodeSnapshot{exhaustedBuilder, sharedFallback}, nil, nil)
	if len(candidates) != 2 {
		t.Fatalf("expected exhausted explicit build node to remain in the ranked set, got %d candidates", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "gcp2" {
		t.Fatalf("expected gcp2 shared fallback to rank ahead of exhausted explicit build node, got %q", got)
	}
}

func TestSelectBuilderCandidatesFiltersByRequiredNodeLabels(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileLight)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	tokyo := builderTestNode("tokyo-a", "tokyo-a", policy, "2000m", "8Gi", "10Gi", "0", "0", "0")
	tokyo.Labels[runtime.RegionLabelKey] = "ap-northeast-1"
	hk := builderTestNode("hongkong-a", "hongkong-a", policy, "2000m", "8Gi", "10Gi", "0", "0", "0")
	hk.Labels[runtime.RegionLabelKey] = "ap-east-1"

	candidates := selectBuilderCandidates(
		policy,
		builderWorkloadProfileLight,
		demand,
		[]builderNodeSnapshot{tokyo, hk},
		nil,
		map[string]string{runtime.RegionLabelKey: "ap-east-1"},
	)

	if len(candidates) != 1 {
		t.Fatalf("expected 1 region-matched candidate, got %d", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "hongkong-a" {
		t.Fatalf("expected hongkong-a to match required region, got %q", got)
	}
}

func TestSelectBuilderCandidatesExcludesUntoleratedNoScheduleTaints(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileLight)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	node := builderTestNode("builder-a", "builder-a", policy, "2000m", "8Gi", "10Gi", "0", "0", "0")
	node.Taints = []builderKubeNodeTaint{
		{Key: "dedicated", Value: "builders", Effect: "NoSchedule"},
	}

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileLight, demand, []builderNodeSnapshot{node}, nil, nil)
	if len(candidates) != 0 {
		t.Fatalf("expected untolerated tainted node to be excluded, got %d candidates", len(candidates))
	}
}

func TestSelectBuilderCandidatesAllowsConfiguredTolerations(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	policy.Tolerations = []BuilderToleration{
		{
			Key:      "dedicated",
			Operator: "Equal",
			Value:    "builders",
			Effect:   "NoSchedule",
		},
	}
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileLight)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	node := builderTestNode("builder-a", "builder-a", policy, "2000m", "8Gi", "10Gi", "0", "0", "0")
	node.Taints = []builderKubeNodeTaint{
		{Key: "dedicated", Value: "builders", Effect: "NoSchedule"},
	}

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileLight, demand, []builderNodeSnapshot{node}, nil, nil)
	if len(candidates) != 1 {
		t.Fatalf("expected configured toleration to keep node eligible, got %d candidates", len(candidates))
	}
}

func TestSelectBuilderCandidatesUsesFilesystemAvailabilityForEphemeralHeadroom(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileLight)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileLight, demand, []builderNodeSnapshot{
		builderTestNodeWithFS("gcp1", "gcp1", policy, "2000m", "4Gi", "9140Mi", "18Gi", "484m", "1500Mi", "20Mi"),
		builderTestNodeWithFS("gcp3", "gcp3", policy, "2000m", "4Gi", "9140Mi", "2500Mi", "103m", "1200Mi", "56Ki"),
	}, nil, nil)

	if len(candidates) != 2 {
		t.Fatalf("expected low-disk node to stay in ranking instead of being filtered, got %d candidates", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "gcp1" {
		t.Fatalf("expected node with enough ephemeral headroom to rank first, got %q", got)
	}
}

func TestBuilderUsedResourcesUsesPodEphemeralUsage(t *testing.T) {
	t.Parallel()

	used := builderUsedResources(&builderKubeNodeSummary{
		Node: builderKubeSummaryNode{
			Memory: builderKubeSummaryMem{
				WorkingSetBytes: builderUint64Ptr(uint64(parseBuilderBytes("1536Mi"))),
			},
			FS: builderKubeSummaryFS{
				CapacityBytes:  builderUint64Ptr(uint64(parseBuilderBytes("30Gi"))),
				AvailableBytes: builderUint64Ptr(uint64(parseBuilderBytes("18Gi"))),
				UsedBytes:      builderUint64Ptr(uint64(parseBuilderBytes("11Gi"))),
			},
		},
		Pods: []builderKubeSummaryPod{
			{EphemeralStorage: builderKubeSummaryFS{UsedBytes: builderUint64Ptr(uint64(parseBuilderBytes("18Mi")))}},
			{EphemeralStorage: builderKubeSummaryFS{UsedBytes: builderUint64Ptr(uint64(parseBuilderBytes("2Mi")))}},
		},
	}, parseBuilderBytes("4Gi"))

	if got := used.EphemeralBytes; got != parseBuilderBytes("20Mi") {
		t.Fatalf("expected pod ephemeral usage 20Mi, got %d", got)
	}
}

func TestBuilderUsedResourcesPrefersAvailableBytesForMemoryHeadroom(t *testing.T) {
	t.Parallel()

	capacity := parseBuilderBytes("4Gi")
	used := builderUsedResources(&builderKubeNodeSummary{
		Node: builderKubeSummaryNode{
			Memory: builderKubeSummaryMem{
				WorkingSetBytes: builderUint64Ptr(uint64(parseBuilderBytes("3Gi"))),
				UsageBytes:      builderUint64Ptr(uint64(parseBuilderBytes("3Gi"))),
				AvailableBytes:  builderUint64Ptr(uint64(parseBuilderBytes("2Gi"))),
			},
		},
	}, capacity)

	if got := used.MemoryBytes; got != parseBuilderBytes("2Gi") {
		t.Fatalf("expected memory used to be derived from available bytes, got %d", got)
	}
}

func TestBuilderNodeFilesystemAvailableBytesUsesNodeSummaryAvailability(t *testing.T) {
	t.Parallel()

	available := builderNodeFilesystemAvailableBytes(&builderKubeNodeSummary{
		Node: builderKubeSummaryNode{
			FS: builderKubeSummaryFS{
				CapacityBytes:  builderUint64Ptr(uint64(parseBuilderBytes("30Gi"))),
				AvailableBytes: builderUint64Ptr(uint64(parseBuilderBytes("18Gi"))),
				UsedBytes:      builderUint64Ptr(uint64(parseBuilderBytes("11Gi"))),
			},
		},
	})
	if available != parseBuilderBytes("18Gi") {
		t.Fatalf("expected filesystem available bytes for 18Gi, got %d", available)
	}
}

func TestBuildBuilderPlacementPrefersSelectedNode(t *testing.T) {
	t.Parallel()

	placement := buildBuilderPlacement([]builderCandidate{
		{Node: builderNodeSnapshot{Name: "node-a", Hostname: "host-a"}},
		{Node: builderNodeSnapshot{Name: "node-b", Hostname: "host-b"}},
		{Node: builderNodeSnapshot{Name: "node-c", Hostname: "host-c"}},
	}, "node-b")

	if placement.PreferredHostname != "host-b" {
		t.Fatalf("expected preferred hostname host-b, got %q", placement.PreferredHostname)
	}
	expected := []string{"host-b"}
	if !reflect.DeepEqual(placement.CandidateHostnames, expected) {
		t.Fatalf("expected ordered hostnames %v, got %v", expected, placement.CandidateHostnames)
	}
}

func TestBuilderPlacementCandidatesCapsFallbackByProfile(t *testing.T) {
	t.Parallel()

	candidates := []builderCandidate{
		{Node: builderNodeSnapshot{Name: "node-a", Hostname: "host-a"}},
		{Node: builderNodeSnapshot{Name: "node-b", Hostname: "host-b"}},
		{Node: builderNodeSnapshot{Name: "node-c", Hostname: "host-c"}},
	}

	heavy := builderPlacementCandidates(candidates, builderWorkloadProfileHeavy, 3)
	if len(heavy) != 2 || heavy[0].Node.Name != "node-a" || heavy[1].Node.Name != "node-b" {
		t.Fatalf("expected heavy placement to keep the top two candidates, got %+v", heavy)
	}

	light := builderPlacementCandidates(candidates, builderWorkloadProfileLight, 3)
	if len(light) != 2 {
		t.Fatalf("expected light placement to keep only two fallback candidates, got %d", len(light))
	}
}

func TestSelectBuilderCandidatesHeavyAllowsSharedFourGiNodesWithModerateHeadroom(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	candidates := selectBuilderCandidates(policy, builderWorkloadProfileHeavy, demand, []builderNodeSnapshot{
		builderTestNode("fortedrape8", "fortedrape8", policy, "2", "16Gi", "40Gi", "162m", "2Gi", "0"),
		builderTestNode("gcp2", "instance-20260322-112431", policy, "2", "12Gi", "30Gi", "608m", "2Gi", "0"),
		builderTestNode("gcp3", "instance-20260322-112657", policy, "2", "10Gi", "20Gi", "298m", "2Gi", "0"),
	}, nil, nil)

	if len(candidates) < 2 {
		t.Fatalf("expected at least two eligible heavy builder nodes, got %d", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "fortedrape8" {
		t.Fatalf("expected fortedrape8-like node to rank first, got %q", got)
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
	if got := requiredMatchExpressions[0]["values"].([]string); !reflect.DeepEqual(got, []string{"host-b"}) {
		t.Fatalf("expected hard-bound hostname [host-b], got %v", got)
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

func TestLoadNodeSnapshotsSkipsUnavailableNodeSummaries(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/pods":
			if got := r.URL.Query().Get("fieldSelector"); got != "spec.nodeName=node-a" && got != "spec.nodeName=node-b" {
				t.Fatalf("expected node-scoped pod list, got fieldSelector=%q", got)
			}
			pods := builderKubePodList{}
			if r.URL.Query().Get("fieldSelector") == "spec.nodeName=node-b" {
				pods.Items = []builderKubePod{builderTestPod("node-b", "Running", "1500m", "4Gi", "2Gi")}
			}
			writeBuilderTestJSON(t, w, pods)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes":
			writeBuilderTestJSON(t, w, builderKubeNodeList{
				Items: []builderKubeNode{
					builderTestKubeNode("node-a", "host-a", policy),
					builderTestKubeNode("node-b", "host-b", policy),
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/node-a/proxy/stats/summary":
			http.Error(w, `{"message":"stats unavailable"}`, http.StatusServiceUnavailable)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/node-b/proxy/stats/summary":
			writeBuilderTestJSON(t, w, builderTestNodeSummary("18Gi"))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	scheduler := &builderScheduler{
		client: &builderKubeClient{
			client:    server.Client(),
			baseURL:   server.URL,
			namespace: "fugue-system",
		},
		policy: policy,
	}

	snapshots, err := scheduler.loadNodeSnapshots(context.Background())
	if err != nil {
		t.Fatalf("load node snapshots: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot after skipping unavailable summary, got %d", len(snapshots))
	}
	if got := snapshots[0].Name; got != "node-b" {
		t.Fatalf("expected node-b snapshot to remain, got %q", got)
	}
	if got := snapshots[0].Requested; got != (builderResourceDemand{
		CPUMilli:       parseBuilderCPUMilli("1500m"),
		MemoryBytes:    parseBuilderBytes("4Gi"),
		EphemeralBytes: parseBuilderBytes("2Gi"),
	}) {
		t.Fatalf("expected node pod requests to be included, got %+v", got)
	}
}

func TestBuilderAvailableResourcesRespectsKubernetesRequestsAndMeasuredUsage(t *testing.T) {
	t.Parallel()

	snapshot := builderNodeSnapshot{
		Allocatable: builderResourceDemand{
			CPUMilli:       8_000,
			MemoryBytes:    parseBuilderBytes("16Gi"),
			EphemeralBytes: parseBuilderBytes("100Gi"),
		},
		Used: builderResourceDemand{
			CPUMilli:       2_000,
			MemoryBytes:    parseBuilderBytes("10Gi"),
			EphemeralBytes: parseBuilderBytes("3Gi"),
		},
		Requested: builderResourceDemand{
			CPUMilli:       3_000,
			MemoryBytes:    parseBuilderBytes("12Gi"),
			EphemeralBytes: parseBuilderBytes("5Gi"),
		},
	}

	available := builderAvailableResources(snapshot, builderResourceDemand{}, builderResourceDemand{})
	if available.CPUMilli != 5_000 || available.MemoryBytes != parseBuilderBytes("4Gi") || available.EphemeralBytes != parseBuilderBytes("95Gi") {
		t.Fatalf("expected per-resource maximum of observed use and scheduler requests to be deducted, got %+v", available)
	}
}

func TestBuilderNodePodRequestsAccountsForInitSidecarsOverheadAndTerminalPods(t *testing.T) {
	t.Parallel()

	var active builderKubePod
	if err := json.Unmarshal([]byte(`{
		"spec": {
			"nodeName": "node-a",
			"containers": [{"resources": {"requests": {"memory": "2Gi"}}}, {"resources": {"requests": {"memory": "1Gi"}}}],
			"initContainers": [
				{"restartPolicy": "Always", "resources": {"requests": {"memory": "1Gi"}}},
				{"resources": {"requests": {"memory": "5Gi"}}}
			],
			"overhead": {"memory": "128Mi"}
		},
		"status": {"phase": "Running"}
	}`), &active); err != nil {
		t.Fatal(err)
	}
	requested := builderNodePodRequests([]builderKubePod{
		active,
		builderTestPod("node-a", "Succeeded", "0", "10Gi", "0"),
		builderTestPod("node-a", "Failed", "0", "10Gi", "0"),
		builderTestPod("", "Pending", "0", "10Gi", "0"),
	})
	if requested.MemoryBytes != parseBuilderBytes("6272Mi") {
		t.Fatalf("expected peak init plus restartable sidecar and overhead (6Gi + 128Mi), got %+v", requested)
	}
	active.Spec.InitContainers = active.Spec.InitContainers[:1]
	if got := builderPodRequestedResources(active).MemoryBytes; got != parseBuilderBytes("4224Mi") {
		t.Fatalf("expected steady app plus sidecar and overhead (4Gi + 128Mi), got %d", got)
	}
}

func TestHeavyBuilderBudgetUsesPhysicalSafetyAndRequestHeadroomAsSeparateCeilings(t *testing.T) {
	t.Parallel()

	policy := builderPodPolicyWithMemoryCeiling(defaultBuilderPodPolicy(), parseBuilderBytes("16Gi"))
	for _, test := range []struct {
		name, used, requested, reservation, expected string
		fits                                         bool
	}{
		{name: "requests constrain allocation without duplicate safety deduction", used: "14Gi", requested: "24Gi", reservation: "1Gi", expected: "7Gi", fits: true},
		{name: "real pressure preserves physical safety buffer", used: "27Gi", requested: "8Gi", reservation: "0", expected: "0", fits: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			node := builderTestNode("worker", "worker", policy, "8000m", "32Gi", "100Gi", "1000m", test.used, "1Gi")
			node.Requested.MemoryBytes = parseBuilderBytes(test.requested)
			reserved := builderResourceDemand{MemoryBytes: parseBuilderBytes(test.reservation)}
			available := builderAvailableResources(node, reserved, builderSafetyBuffer(node.Allocatable, builderWorkloadProfileHeavy))
			demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
			if err != nil {
				t.Fatal(err)
			}
			if got := builderDemandFits(available, demand); got != test.fits {
				t.Fatalf("expected fits=%t, got fits=%t for available=%+v", test.fits, got, available)
			}
			if test.fits && available.MemoryBytes != parseBuilderBytes(test.expected) {
				t.Fatalf("expected %s available, got %d", test.expected, available.MemoryBytes)
			}
			if !test.fits && available.MemoryBytes >= parseBuilderBytes("1Gi") {
				t.Fatalf("expected less than 1Gi after retaining safety buffer, got %d", available.MemoryBytes)
			}
		})
	}
}

func TestLoadNodeSnapshotsFailsWhenAllEligibleSummariesAreUnavailable(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/pods":
			writeBuilderTestJSON(t, w, builderKubePodList{})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes":
			writeBuilderTestJSON(t, w, builderKubeNodeList{
				Items: []builderKubeNode{
					builderTestKubeNode("node-a", "host-a", policy),
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/node-a/proxy/stats/summary":
			http.Error(w, `{"message":"stats unavailable"}`, http.StatusServiceUnavailable)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	scheduler := &builderScheduler{
		client: &builderKubeClient{
			client:    server.Client(),
			baseURL:   server.URL,
			namespace: "fugue-system",
		},
		policy: policy,
	}

	_, err := scheduler.loadNodeSnapshots(context.Background())
	if err == nil {
		t.Fatalf("expected load node snapshots to fail when all summaries are unavailable")
	}
	if !strings.Contains(err.Error(), "load builder node stats summaries") {
		t.Fatalf("expected summary error context, got %v", err)
	}
	if !strings.Contains(err.Error(), "node-a") {
		t.Fatalf("expected failing node name in error, got %v", err)
	}
}

func TestInspectPlacementReportsReservationsLocksAndNodeReasons(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}
	nodeA := builderTestKubeNode("node-a", "host-a", policy)
	nodeB := builderTestKubeNode("node-b", "host-b", policy)
	delete(nodeB.Metadata.Labels, policy.BuildNodeLabelKey)
	nodeB.Metadata.Labels[runtime.TenantIDLabelKey] = "tenant_demo"
	nodeB.Metadata.Labels[runtime.NodeModeLabelKey] = model.RuntimeTypeManagedOwned
	nodeC := builderTestKubeNode("node-c", "", policy)
	delete(nodeC.Metadata.Labels, builderHostnameLabelKey)
	nodeD := builderTestKubeNode("node-d", "host-d", policy)
	nodeE := builderTestKubeNode("node-e", "host-e", policy)
	nodeE.Metadata.Labels[runtime.SharedPoolLabelKey] = runtime.SharedPoolLabelValue
	nodeE.Metadata.Labels[runtime.BuildNodeLabelKey] = "false"
	nodeF := builderTestKubeNode("node-f", "host-f", policy)
	now := time.Now().UTC()
	reservationRenewedAt := formatKubeTimestamp(now.Add(-30 * time.Second))
	lockRenewedAt := formatKubeTimestamp(now.Add(-10 * time.Second))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/pods":
			name := strings.TrimPrefix(r.URL.Query().Get("fieldSelector"), "spec.nodeName=")
			memory := "4Gi"
			if name == "node-f" {
				memory = "7936Mi"
			}
			writeBuilderTestJSON(t, w, builderKubePodList{Items: []builderKubePod{builderTestPod(name, "Running", "0", memory, "0")}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes":
			writeBuilderTestJSON(t, w, builderKubeNodeList{
				Items: []builderKubeNode{nodeA, nodeB, nodeC, nodeD, nodeE, nodeF},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/node-a/proxy/stats/summary":
			writeBuilderTestJSON(t, w, builderTestNodeSummary("18Gi"))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/node-c/proxy/stats/summary":
			writeBuilderTestJSON(t, w, builderTestNodeSummary("12Gi"))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/node-d/proxy/stats/summary":
			http.Error(w, `{"message":"stats unavailable"}`, http.StatusServiceUnavailable)
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/nodes/node-f/proxy/stats/summary":
			writeBuilderTestJSON(t, w, builderTestNodeSummary("18Gi"))
		case r.Method == http.MethodGet && r.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/fugue-system/leases" && r.URL.Query().Get("labelSelector") == builderReservationLabelSelector:
			writeBuilderTestJSON(t, w, builderKubeLeaseList{
				Items: []builderKubeLease{
					builderTestLease("reservation-a", builderReservationComponentValue, "reservation-a", "node-a", reservationRenewedAt, 120, map[string]string{
						builderAnnotationCPUMilli:       "750",
						builderAnnotationMemoryBytes:    "1073741824",
						builderAnnotationEphemeralBytes: "3221225472",
					}),
				},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/fugue-system/leases" && r.URL.Query().Get("labelSelector") == builderNodeLockLabelSelector:
			writeBuilderTestJSON(t, w, builderKubeLeaseList{
				Items: []builderKubeLease{
					builderTestLease("lock-node-a", builderNodeLockComponentValue, "build-demo", "node-a", lockRenewedAt, 20, nil),
				},
			})
		default:
			t.Fatalf("unexpected request: %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer server.Close()

	scheduler := &builderScheduler{
		client: &builderKubeClient{
			client:    server.Client(),
			baseURL:   server.URL,
			namespace: "fugue-system",
		},
		namespace: "fugue-system",
		policy:    policy,
		profile:   builderWorkloadProfileHeavy,
		demand:    demand,
	}

	report, err := scheduler.inspectPlacement(context.Background(), model.AppBuildStrategyDockerfile)
	if err != nil {
		t.Fatalf("inspect placement: %v", err)
	}
	if report.Profile != "heavy" {
		t.Fatalf("expected heavy profile, got %+v", report)
	}
	if report.ObservedAt == nil || report.MemoryCeilingBytes != parseBuilderBytes("16Gi") {
		t.Fatal("missing inspection policy/time", report)
	}
	if len(report.Reservations) != 1 {
		t.Fatalf("expected 1 reservation, got %+v", report.Reservations)
	}
	if len(report.Locks) != 1 {
		t.Fatalf("expected 1 lock, got %+v", report.Locks)
	}

	nodesByName := make(map[string]model.BuilderPlacementNodeInspection, len(report.Nodes))
	for _, node := range report.Nodes {
		nodesByName[node.NodeName] = node
	}
	if node := nodesByName["node-a"]; !node.Eligible || node.Rank != 1 {
		t.Fatalf("expected node-a to be top eligible node, got %+v", node)
	}
	if got := nodesByName["node-a"].Available.MemoryBytes; got != parseBuilderBytes("3Gi") {
		t.Fatalf("expected diagnostics to match request-aware scheduling headroom (8Gi - 4Gi Pod requests - 1Gi reservation), got %d", got)
	}
	if node := nodesByName["node-a"]; node.Requested.MemoryBytes != parseBuilderBytes("4Gi") || node.Allocation.MemoryBytes != parseBuilderBytes("3Gi") {
		t.Fatal("missing dynamic allocation or requests", node)
	}
	if node := nodesByName["node-b"]; node.Eligible || !strings.Contains(strings.Join(node.Reasons, "\n"), runtime.TenantIDLabelKey) {
		t.Fatalf("expected node-b to be excluded by tenant policy, got %+v", node)
	}
	if node := nodesByName["node-c"]; node.Eligible || !strings.Contains(strings.Join(node.Reasons, "\n"), builderHostnameLabelKey) {
		t.Fatalf("expected node-c to be excluded for missing hostname, got %+v", node)
	}
	if node := nodesByName["node-d"]; node.Eligible || !strings.Contains(strings.Join(node.Reasons, "\n"), "stats summary unavailable") {
		t.Fatalf("expected node-d to report summary failure, got %+v", node)
	}
	if node := nodesByName["node-e"]; node.Eligible || node.Rank != 0 || !strings.Contains(strings.Join(node.Reasons, " "), "builds explicitly disabled") {
		t.Fatal("disabled node was misreported", node)
	}
	if node := nodesByName["node-f"]; node.Eligible || node.Rank != 0 || !strings.Contains(strings.Join(node.Reasons, " "), "insufficient memory_bytes") {
		t.Fatal("capacity failure was not explained", node)
	}
}

func TestTryAcquireNodeLockCreateUsesMicrosecondPrecision(t *testing.T) {
	t.Parallel()

	lockName := builderNodeLockLeaseName("node-a")
	var created builderKubeLease
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/fugue-system/leases/"+lockName:
			http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
		case r.Method == http.MethodPost && r.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/fugue-system/leases":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read request body: %v", err)
			}
			if err := json.Unmarshal(body, &created); err != nil {
				t.Fatalf("decode lease body: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	scheduler := &builderScheduler{
		client: &builderKubeClient{
			client:    server.Client(),
			baseURL:   server.URL,
			namespace: "fugue-system",
		},
	}

	acquired, err := scheduler.tryAcquireNodeLock(context.Background(), "node-a", "job-a")
	if err != nil {
		t.Fatalf("try acquire node lock: %v", err)
	}
	if !acquired {
		t.Fatalf("expected lock acquisition to succeed")
	}
	assertMicrosecondKubeTimestamp(t, created.Spec.AcquireTime)
	assertMicrosecondKubeTimestamp(t, created.Spec.RenewTime)
}

func TestUpsertReservationUpdateUsesMicrosecondPrecision(t *testing.T) {
	t.Parallel()

	var updated builderKubeLease
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/fugue-system/leases/reservation-a":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"apiVersion":"coordination.k8s.io/v1",
				"kind":"Lease",
				"metadata":{"name":"reservation-a","namespace":"fugue-system","resourceVersion":"1"},
				"spec":{
					"holderIdentity":"reservation-a",
					"leaseDurationSeconds":120,
					"acquireTime":"2026-03-25T18:10:34.123456Z",
					"renewTime":"2026-03-25T18:11:34.123456Z"
				}
			}`))
		case r.Method == http.MethodPut && r.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/fugue-system/leases/reservation-a":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read request body: %v", err)
			}
			if err := json.Unmarshal(body, &updated); err != nil {
				t.Fatalf("decode lease body: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	scheduler := &builderScheduler{
		client: &builderKubeClient{
			client:    server.Client(),
			baseURL:   server.URL,
			namespace: "fugue-system",
		},
		demand: builderResourceDemand{
			CPUMilli:       750,
			MemoryBytes:    parseBuilderBytes("1Gi"),
			EphemeralBytes: parseBuilderBytes("3Gi"),
		},
		reservationLeaseDuration: 2 * time.Minute,
	}

	if err := scheduler.upsertReservation(context.Background(), "reservation-a", "node-a", scheduler.demand); err != nil {
		t.Fatalf("upsert reservation: %v", err)
	}
	assertMicrosecondKubeTimestamp(t, updated.Spec.AcquireTime)
	assertMicrosecondKubeTimestamp(t, updated.Spec.RenewTime)
}

func builderTestNode(name, hostname string, policy BuilderPodPolicy, cpu, memory, ephemeral, usedCPU, usedMemory, usedEphemeral string) builderNodeSnapshot {
	return builderTestNodeWithFS(name, hostname, policy, cpu, memory, ephemeral, ephemeral, usedCPU, usedMemory, usedEphemeral)
}

func builderTestPod(nodeName, phase, cpu, memory, ephemeral string) builderKubePod {
	var pod builderKubePod
	pod.Spec.NodeName = nodeName
	pod.Status.Phase = phase
	container := builderKubeContainer{}
	container.Resources.Requests = map[string]string{
		"cpu":               cpu,
		"memory":            memory,
		"ephemeral-storage": ephemeral,
	}
	pod.Spec.Containers = []builderKubeContainer{container}
	return pod
}

func builderTestKubeNode(name, hostname string, policy BuilderPodPolicy) builderKubeNode {
	var node builderKubeNode
	node.Metadata.Name = name
	node.Metadata.Labels = map[string]string{
		builderHostnameLabelKey:  hostname,
		policy.BuildNodeLabelKey: valueOrDefault(policy.BuildNodeLabelValue, "true"),
	}
	node.Status.Conditions = []builderKubeNodeCondition{
		{Type: "Ready", Status: "True"},
	}
	node.Status.Allocatable = map[string]string{
		"cpu":               "2000m",
		"memory":            "8Gi",
		"ephemeral-storage": "10Gi",
	}
	return node
}

func builderTestNodeSummary(filesystemAvailable string) builderKubeNodeSummary {
	return builderKubeNodeSummary{
		Node: builderKubeSummaryNode{
			Memory: builderKubeSummaryMem{
				WorkingSetBytes: builderUint64Ptr(uint64(parseBuilderBytes("1Gi"))),
			},
			FS: builderKubeSummaryFS{
				AvailableBytes: builderUint64Ptr(uint64(parseBuilderBytes(filesystemAvailable))),
				CapacityBytes:  builderUint64Ptr(uint64(parseBuilderBytes("30Gi"))),
			},
		},
	}
}

func builderTestNodeWithFS(name, hostname string, policy BuilderPodPolicy, cpu, memory, ephemeral, filesystemAvailable, usedCPU, usedMemory, usedEphemeral string) builderNodeSnapshot {
	return builderNodeSnapshot{
		Name:     name,
		Hostname: hostname,
		Labels: map[string]string{
			policy.BuildNodeLabelKey: valueOrDefault(policy.BuildNodeLabelValue, "true"),
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
		FilesystemAvailableBytes: parseBuilderBytes(filesystemAvailable),
	}
}

func valueOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func assertMicrosecondKubeTimestamp(t *testing.T, value string) {
	t.Helper()

	if !kubeTimestampPattern.MatchString(strings.TrimSpace(value)) {
		t.Fatalf("expected microsecond kube timestamp, got %q", value)
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000000Z07:00", value); err != nil {
		t.Fatalf("parse kube timestamp %q: %v", value, err)
	}
}

func builderTestLease(name, component, holder, nodeName, renewTime string, durationSeconds int, annotations map[string]string) builderKubeLease {
	var lease builderKubeLease
	lease.Metadata.Name = name
	lease.Metadata.Namespace = "fugue-system"
	lease.Metadata.Labels = map[string]string{
		"app.kubernetes.io/managed-by": "fugue",
		"app.kubernetes.io/component":  component,
	}
	lease.Metadata.Annotations = map[string]string{
		builderAnnotationNodeName: nodeName,
	}
	for key, value := range annotations {
		lease.Metadata.Annotations[key] = value
	}
	lease.Spec.HolderIdentity = holder
	lease.Spec.LeaseDurationSeconds = durationSeconds
	lease.Spec.AcquireTime = renewTime
	lease.Spec.RenewTime = renewTime
	return lease
}

func builderUint64Ptr(value uint64) *uint64 {
	return &value
}

func writeBuilderTestJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
