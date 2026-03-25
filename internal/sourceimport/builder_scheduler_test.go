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

func assertMicrosecondKubeTimestamp(t *testing.T, value string) {
	t.Helper()

	if !kubeTimestampPattern.MatchString(strings.TrimSpace(value)) {
		t.Fatalf("expected microsecond kube timestamp, got %q", value)
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000000Z07:00", value); err != nil {
		t.Fatalf("parse kube timestamp %q: %v", value, err)
	}
}
