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
	}, nil, nil)

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
	}, nil, nil)

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
	if heavyDemand.CPUMilli != parseBuilderCPUMilli("750m") {
		t.Fatalf("expected heavy cpu demand 750m, got %dm", heavyDemand.CPUMilli)
	}
	if heavyDemand.MemoryBytes != parseBuilderBytes("1Gi") {
		t.Fatalf("expected heavy memory demand 1Gi, got %d", heavyDemand.MemoryBytes)
	}
	if heavyDemand.EphemeralBytes != parseBuilderBytes("3Gi") {
		t.Fatalf("expected heavy ephemeral demand 3Gi, got %d", heavyDemand.EphemeralBytes)
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
	}, nil)

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
		builderTestNodeWithFS("shared-a", "shared-a", policy, policy.MediumNodeLabelValue, "2000m", "8Gi", "10Gi", "18Gi", "0", "0", "0"),
		{
			Name:     "alicehk2",
			Hostname: "alicehk2",
			Labels: map[string]string{
				policy.LargeNodeLabelKey:  policy.LargeNodeLabelValue,
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

	shared := builderTestNode("shared-a", "shared-a", policy, policy.MediumNodeLabelValue, "4000m", "4Gi", "24Gi", "250m", "1536Mi", "3Gi")
	alicehk2 := builderTestNode("alicehk2", "fortedrape8", policy, policy.LargeNodeLabelValue, "4000m", "4Gi", "30Gi", "250m", "768Mi", "2Gi")
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

func TestSelectBuilderCandidatesFiltersByRequiredNodeLabels(t *testing.T) {
	t.Parallel()

	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileLight)
	if err != nil {
		t.Fatalf("builder demand: %v", err)
	}

	tokyo := builderTestNode("tokyo-a", "tokyo-a", policy, policy.MediumNodeLabelValue, "2000m", "8Gi", "10Gi", "0", "0", "0")
	tokyo.Labels[runtime.RegionLabelKey] = "ap-northeast-1"
	hk := builderTestNode("hongkong-a", "hongkong-a", policy, policy.MediumNodeLabelValue, "2000m", "8Gi", "10Gi", "0", "0", "0")
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

	node := builderTestNode("builder-a", "builder-a", policy, policy.MediumNodeLabelValue, "2000m", "8Gi", "10Gi", "0", "0", "0")
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

	node := builderTestNode("builder-a", "builder-a", policy, policy.MediumNodeLabelValue, "2000m", "8Gi", "10Gi", "0", "0", "0")
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
		builderTestNodeWithFS("gcp1", "gcp1", policy, policy.MediumNodeLabelValue, "2000m", "4Gi", "9140Mi", "18Gi", "484m", "1500Mi", "20Mi"),
		builderTestNodeWithFS("gcp3", "gcp3", policy, policy.MediumNodeLabelValue, "2000m", "4Gi", "9140Mi", "2500Mi", "103m", "1200Mi", "56Ki"),
	}, nil, nil)

	if len(candidates) != 1 {
		t.Fatalf("expected filesystem headroom to reject low-disk node, got %d candidates", len(candidates))
	}
	if got := candidates[0].Node.Name; got != "gcp1" {
		t.Fatalf("expected gcp1-like node to remain eligible, got %q", got)
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

	if err := scheduler.upsertReservation(context.Background(), "reservation-a", "node-a"); err != nil {
		t.Fatalf("upsert reservation: %v", err)
	}
	assertMicrosecondKubeTimestamp(t, updated.Spec.AcquireTime)
	assertMicrosecondKubeTimestamp(t, updated.Spec.RenewTime)
}

func builderTestNode(name, hostname string, policy BuilderPodPolicy, sizeClass, cpu, memory, ephemeral, usedCPU, usedMemory, usedEphemeral string) builderNodeSnapshot {
	return builderTestNodeWithFS(name, hostname, policy, sizeClass, cpu, memory, ephemeral, ephemeral, usedCPU, usedMemory, usedEphemeral)
}

func builderTestNodeWithFS(name, hostname string, policy BuilderPodPolicy, sizeClass, cpu, memory, ephemeral, filesystemAvailable, usedCPU, usedMemory, usedEphemeral string) builderNodeSnapshot {
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

func builderUint64Ptr(value uint64) *uint64 {
	return &value
}
