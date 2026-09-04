package livediagnostics

import (
	"strings"
	"testing"
)

func TestStartRequestUsesKindSpecificBounds(t *testing.T) {
	for _, tc := range []struct {
		kind     ProbeKind
		duration int
		wantErr  bool
	}{{ProbeCPUProfile, 120, false}, {ProbeCPUProfile, 121, true}, {ProbeMemoryProfile, 360, false}, {ProbeProcessSample, 360, false}, {ProbeMemoryProfile, 361, true}} {
		request := StartRequest{Kind: tc.kind, DurationSeconds: tc.duration}
		if err := request.Normalize(); (err != nil) != tc.wantErr {
			t.Fatalf("kind=%s duration=%d err=%v", tc.kind, tc.duration, err)
		}
	}
}

func TestBuildJobSupportsContainerAndHostTargets(t *testing.T) {
	image := "registry.example/fugue-api@sha256:" + strings.Repeat("a", 64)
	containerTarget := Target{Type: TargetPlatformComponent, Component: "api", Namespace: "fugue-system", Pod: "api-1", PodUID: "uid-1", Container: "api", ContainerID: "containerd://abcdef1234567890", Node: "node-1"}
	job, err := BuildJob(containerTarget, "diagnostic-123", "fugue-system", image, "api", StartRequest{Kind: ProbeMemoryProfile, DurationSeconds: 300})
	if err != nil {
		t.Fatal(err)
	}
	if !job.Spec.Template.Spec.HostPID || job.Spec.Template.Spec.NodeName != "node-1" || job.Annotations[TargetTypeLabel] != "" {
		t.Fatalf("unexpected job: %+v", job)
	}
	args := strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--container-id containerd://abcdef1234567890") || !strings.Contains(args, "--kind memory-profile") {
		t.Fatalf("unexpected container args %q", args)
	}
	hostTarget := Target{Type: TargetNodeProcess, Node: "node-2", ProcessName: "fugue-agent"}
	job, err = BuildJob(hostTarget, "diagnostic-456", "fugue-system", image, "direct-kubernetes", StartRequest{Kind: ProbeProcessSample, DurationSeconds: 30})
	if err != nil {
		t.Fatal(err)
	}
	args = strings.Join(job.Spec.Template.Spec.Containers[0].Args, " ")
	if !strings.Contains(args, "--process-name fugue-agent") || job.Labels[ControlPathLabel] != "direct-kubernetes" {
		t.Fatalf("unexpected host job: labels=%+v args=%q", job.Labels, args)
	}
}
