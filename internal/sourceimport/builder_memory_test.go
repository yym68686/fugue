package sourceimport

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func memoryTestPod() builderPod {
	pod := builderPod{}
	pod.Metadata.UID = "fixture-uid"
	pod.Metadata.CreationTimestamp = time.Now().UTC()
	pod.Metadata.Annotations = map[string]string{builderMemoryKeyAnnotation: strings.Repeat("a", 64), builderMemoryAnnotation: "cold_start"}
	pod.Status.Phase = "Succeeded"
	container := func(name string) builderEvidenceContainer {
		return builderEvidenceContainer{Name: name, Resources: BuilderResourceRequirements{Limits: map[string]string{"memory": "2Gi"}}}
	}
	status := func(name string, mib int64) builderContainerStatus {
		return builderContainerStatus{Name: name, State: builderRuntimeState{Terminated: &builderStateDetail{ExitCode: 0, Message: fmt.Sprintf(`{"fugue_memory_v":1,"peak_bytes":%d}`, mib<<20)}}}
	}
	pod.Spec.InitContainers = []builderEvidenceContainer{container("download"), container("compile")}
	pod.Spec.Containers = []builderEvidenceContainer{container("build"), container("sidecar")}
	pod.Status.InitContainerStatuses = []builderContainerStatus{status("download", 900), status("compile", 1300)}
	pod.Status.ContainerStatuses = []builderContainerStatus{status("build", 600), status("sidecar", 800)}
	return pod
}

func TestBuilderMemoryRequiresCompleteSuccessfulCurrentAttempt(t *testing.T) {
	for _, change := range []string{"none", "missing init", "bad measurement", "restarted", "OOM", "failed", "unknown uid", "sidecar init", "stale", "duplicate", "run error"} {
		t.Run(change, func(t *testing.T) {
			pod := memoryTestPod()
			started := pod.Metadata.CreationTimestamp.Add(-time.Second)
			succeeded := true
			switch change {
			case "missing init":
				pod.Status.InitContainerStatuses = pod.Status.InitContainerStatuses[:1]
			case "bad measurement":
				pod.Status.ContainerStatuses[0].State.Terminated.Message = `{"fugue_memory_v":1,"peak_bytes":999999999999}`
			case "restarted":
				pod.Status.ContainerStatuses[0].RestartCount = 1
			case "OOM":
				pod.Status.InitContainerStatuses[0].LastState.Terminated = &builderStateDetail{Reason: "OOMKilled", ExitCode: 137}
			case "failed":
				pod.Status.Phase = "Failed"
			case "unknown uid":
				pod.Metadata.UID = ""
			case "sidecar init":
				pod.Spec.InitContainers[0].RestartPolicy = "Always"
			case "stale":
				started = started.Add(time.Hour)
			case "run error":
				succeeded = false
			}
			pods := []builderPod{pod}
			if change == "duplicate" {
				pods = append(pods, pod)
			}
			recorded := 0
			ctx := WithBuilderMemoryHistory(t.Context(), nil, func(_ context.Context, key, uid string, peak int64) error {
				recorded++
				if key != strings.Repeat("a", 64) || uid != "fixture-uid" || peak != 1400<<20 {
					t.Fatalf("wrong observation: %s %s %d", key, uid, peak)
				}
				return errors.New("optional persistence unavailable")
			})
			peak, complete, _ := recordBuilderMemory(ctx, pods, started, succeeded)
			if change == "none" {
				if !complete || peak != 1400<<20 || recorded != 1 {
					t.Fatal(peak, complete, recorded)
				}
			} else if complete || peak != 0 || recorded != 0 {
				t.Fatal("incomplete observation learned", peak, complete, recorded)
			}
		})
	}
}

func TestBuilderMemoryPredictionObeysCapacityCeilingAndOOM(t *testing.T) {
	var keys []string
	ctx := WithBuilderMemoryHistory(t.Context(), func(_ context.Context, key string) (int64, error) {
		keys = append(keys, key)
		return 1900<<20 + 1, nil
	}, nil)
	configuration := builderMemoryConfiguration("dockerfile", "toolchain", "recipe")
	policy := prepareBuilderMemory(ctx, BuilderPodPolicy{}, builderWorkloadProfileHeavy, configuration)
	policy2 := prepareBuilderMemory(ctx, BuilderPodPolicy{}, builderWorkloadProfileHeavy, configuration)
	_ = prepareBuilderMemory(ctx, BuilderPodPolicy{}, builderWorkloadProfileHeavy, builderMemoryConfiguration("dockerfile", "different toolchain", "recipe"))
	if keys[0] != keys[1] || keys[0] == keys[2] || policy2.memoryAllocationSource != "history" {
		t.Fatal("incompatible history identity", keys)
	}
	demand, _ := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if demand.MemoryBytes != 1901<<20 {
		t.Fatal("allocation not rounded", demand)
	}
	candidate := builderCandidate{Available: builderResourceDemand{MemoryBytes: 10 << 30}}
	allocation := builderAllocatedDemand(policy, builderWorkloadProfileHeavy, candidate)
	if allocation.MemoryBytes != demand.MemoryBytes {
		t.Fatal("history allocated node maximum", allocation)
	}
	candidate.Available.MemoryBytes = demand.MemoryBytes - 1
	if builderDemandFits(candidate.Available, allocation) {
		t.Fatal("underfunded history admitted")
	}
	policy.HeavyMemoryLimitCeiling = "1Gi"
	allocation = builderAllocatedDemand(policy, builderWorkloadProfileHeavy, candidate)
	if allocation.MemoryBytes != 1<<30 {
		t.Fatal("ceiling exceeded", allocation)
	}
	policy.HeavyMemoryLimitCeiling = "16Gi"
	policy, err := builderPolicyAfterOOM(policy, builderWorkloadProfileHeavy, 2<<30)
	if err != nil || policy.memoryTargetBytes < 3<<30 || policy.memoryAllocationSource != "oom_retry" {
		t.Fatal("OOM not escalated", policy, err)
	}
	policy, err = builderPodPolicyForJobAttempt(policy, builderWorkloadProfileHeavy, builderJobAttempt{Number: 4, OOMRetryCount: 3, MinimumMemoryBytes: 3 << 30})
	if err != nil || policy.memoryTargetBytes != 0 {
		t.Fatal("final bounded retry retained stale prediction", err)
	}
	ctx = WithBuilderMemoryHistory(t.Context(), func(context.Context, string) (int64, error) { return 0, errors.New("unavailable") }, nil)
	policy = prepareBuilderMemory(ctx, BuilderPodPolicy{}, builderWorkloadProfileHeavy, configuration)
	if policy.memoryTargetBytes != 0 || policy.memoryAllocationSource != "cold_start" {
		t.Fatal("failed read must use cold start")
	}
}

func TestBuilderMemoryWrapperPreservesArgvAndExitStatus(t *testing.T) {
	container := map[string]any{"image": "fixture", "command": []string{"sh", "-c", `printf '%s' "$1"; exit 42`, "fixture"}, "args": []string{"`echo bad` $(echo bad) 'quoted'"}}
	pod := map[string]any{"containers": []map[string]any{container}}
	applyBuilderPodPolicy(pod, BuilderPodPolicy{}, builderWorkloadProfileHeavy)
	command := container["command"].([]string)
	args := container["args"].([]string)
	if !reflect.DeepEqual(args, []string{"sh", "-c", `printf '%s' "$1"; exit 42`, "fixture", "`echo bad` $(echo bad) 'quoted'"}) {
		t.Fatal("argv changed", args)
	}
	cmd := exec.Command(command[0], append(command[1:], args...)...)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 42 || string(out) != args[len(args)-1] {
		t.Fatalf("exit/argv lost: %v %q", err, out)
	}
}

func TestBuilderMemoryWrapperForwardsTerminationAndReapsChild(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", builderMemoryWrapper, "fugue-memory", "1", "/bin/sh", "-c", `trap 'exit 23' TERM; printf 'ready\n'; while :; do sleep 0.1; done`)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(stdout).ReadString('\n'); ready <- line }()
	select {
	case line := <-ready:
		if line != "ready\n" {
			t.Fatalf("child did not start: %q", line)
		}
	case <-ctx.Done():
		t.Fatal("child start timed out")
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	var exit *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exit) || exit.ExitCode() != 23 {
		t.Fatalf("termination lost child's exit status: %v (context: %v)", err, ctx.Err())
	}
}

func TestBuilderMemoryAnnotationsPreserveExistingMetadata(t *testing.T) {
	for _, annotations := range []any{map[string]string{"existing": "retained"}, map[string]any{"existing": "retained"}} {
		job := map[string]any{"spec": map[string]any{"template": map[string]any{"metadata": map[string]any{"annotations": annotations}}}}
		annotateBuilderMemory(job, BuilderPodPolicy{memoryAllocationSource: "history", memoryHistoryKey: strings.Repeat("a", 64)})
		got := job["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]string)
		if got["existing"] != "retained" || got[builderMemoryAnnotation] != "history" {
			t.Fatal("existing metadata lost", got)
		}
	}
}

// Real cgroup high-water marks and OOM events cannot be verified with fake
// kubectl JSON. Run this in the disposable CI Docker host, never production.
func TestBuilderMemoryContainerContract(t *testing.T) {
	if os.Getenv("FUGUE_TEST_BUILDER_MEMORY_CONTAINER") != "1" {
		t.Skip("requires disposable Docker host")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	for _, image := range []string{"alpine:3.22", defaultKanikoImage} {
		if out, err := exec.CommandContext(ctx, "docker", "pull", image).CombinedOutput(); err != nil {
			t.Fatalf("pull %s: %v %s", image, err, out)
		}
	}
	t.Run("kaniko actual scratch build", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM scratch\nCOPY payload /payload\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "payload"), []byte("neutral build fixture\n"), 0600); err != nil {
			t.Fatal(err)
		}
		termination := filepath.Join(dir, "termination")
		if err := os.WriteFile(termination, nil, 0600); err != nil {
			t.Fatal(err)
		}
		name := fmt.Sprintf("fugue-memory-test-%d", time.Now().UnixNano())
		t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
		cmd := exec.CommandContext(ctx, "docker", "run", "--name", name, "--network", "none", "--memory", "512m", "--memory-swap", "512m", "--cpus", "1", "--mount", "type=bind,src="+dir+",dst=/workspace", "--mount", "type=bind,src="+termination+",dst=/dev/termination-log", "--entrypoint", "/busybox/sh", defaultKanikoImage, "-c", builderMemoryWrapper, "fugue-memory", fmt.Sprint(512<<20), "/kaniko/executor", "--context=dir:///workspace", "--dockerfile=/workspace/Dockerfile", "--destination=fixture.invalid/build:test", "--no-push", "--cache=false", "--tar-path=/workspace/image.tar")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("wrapped Kaniko build failed: %v %s", err, out)
		}
		if info, err := os.Stat(filepath.Join(dir, "image.tar")); err != nil || info.Size() == 0 {
			t.Fatalf("Kaniko output missing: %v", err)
		}
		data, _ := os.ReadFile(termination)
		var observation struct {
			Peak int64 `json:"peak_bytes"`
		}
		if json.Unmarshal(data, &observation) != nil || observation.Peak <= 0 || observation.Peak > 512<<20 {
			t.Fatalf("Kaniko build peak missing: %s", data)
		}
		t.Logf("Kaniko built scratch image; peak=%d bytes", observation.Peak)
	})
	for _, tc := range []struct {
		name, image, shell, script string
		limit                      int64
		exit                       int
	}{
		{"short peak", "alpine:3.22", "/bin/sh", `awk 'BEGIN{x="0123456789abcdef"; for(i=0;i<21;i++)x=x x; print length(x)}'`, 512 << 20, 0},
		{"kaniko shell", defaultKanikoImage, "/busybox/sh", `/busybox/echo kaniko-shell`, 512 << 20, 0},
		{"command error", "alpine:3.22", "/bin/sh", "exit 42", 512 << 20, 42},
		{"OOM child", "alpine:3.22", "/bin/sh", `awk 'BEGIN{x="0123456789abcdef"; for(i=0;i<28;i++)x=x x; print length(x)}'`, 64 << 20, 137},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			termination := filepath.Join(dir, "termination")
			if err := os.WriteFile(termination, nil, 0600); err != nil {
				t.Fatal(err)
			}
			name := fmt.Sprintf("fugue-memory-test-%d", time.Now().UnixNano())
			t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
			cmd := exec.CommandContext(ctx, "docker", "run", "--name", name, "--network", "none", "--memory", fmt.Sprint(tc.limit), "--memory-swap", fmt.Sprint(tc.limit), "--cpus", "1", "--mount", "type=bind,src="+termination+",dst=/dev/termination-log", "--entrypoint", tc.shell, tc.image, "-c", builderMemoryWrapper, "fugue-memory", fmt.Sprint(tc.limit), tc.shell, "-c", tc.script)
			out, err := cmd.CombinedOutput()
			code := 0
			if err != nil {
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					t.Fatal(err)
				}
				code = exit.ExitCode()
			}
			if code != tc.exit {
				t.Fatalf("exit %d want %d: %s", code, tc.exit, out)
			}
			if tc.exit == 137 {
				out, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.OOMKilled}}", name).CombinedOutput()
				if err != nil || strings.TrimSpace(string(out)) != "true" {
					t.Fatalf("OOM reason lost: %v %s", err, out)
				}
				return
			}
			data, err := os.ReadFile(termination)
			var observation struct {
				Peak int64 `json:"peak_bytes"`
			}
			if err != nil || json.Unmarshal(data, &observation) != nil || observation.Peak <= 0 || observation.Peak > tc.limit {
				t.Fatalf("missing cgroup peak: %s %v", data, err)
			}
			if tc.name == "short peak" && observation.Peak < 32<<20 {
				t.Fatalf("short spike missed: %d", observation.Peak)
			}
			t.Logf("peak=%d bytes exit=%d", observation.Peak, code)
		})
	}
}

func TestBuilderMemoryNestedDockerContract(t *testing.T) {
	if os.Getenv("FUGUE_TEST_BUILDER_MEMORY_CONTAINER") != "1" {
		t.Skip("requires disposable Docker host")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	for _, image := range []string{"alpine:3.22", defaultBuildpacksImage} {
		if out, err := exec.CommandContext(ctx, "docker", "pull", image).CombinedOutput(); err != nil {
			t.Fatalf("pull: %v %s", err, out)
		}
	}
	dir := t.TempDir()
	archive := filepath.Join(dir, "fixture.tar")
	if out, err := exec.CommandContext(ctx, "docker", "save", "-o", archive, "alpine:3.22").CombinedOutput(); err != nil {
		t.Fatalf("save fixture: %v %s", err, out)
	}
	termination := filepath.Join(dir, "termination")
	if err := os.WriteFile(termination, nil, 0600); err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("fugue-nested-memory-test-%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })
	script := `set -eu
test -n "$FUGUE_BUILD_MEMORY_CGROUP"
export DOCKER_HOST=unix:///tmp/docker.sock
dockerd --host="$DOCKER_HOST" --storage-driver=vfs --iptables=false --bridge=none --exec-opt native.cgroupdriver=cgroupfs --cgroup-parent "$FUGUE_BUILD_MEMORY_CGROUP" >/tmp/dockerd.log 2>&1 &
daemon=$!
trap 'kill "$daemon" 2>/dev/null || true; wait "$daemon" || true' EXIT
for attempt in $(seq 1 30); do
  if docker info >/dev/null 2>&1; then break; fi
  sleep 1
done
if ! docker info >/dev/null 2>&1; then cat /tmp/dockerd.log; exit 1; fi
docker load -i /fixture.tar >/dev/null
docker run --rm --network none --cgroupns host alpine:3.22 cat /proc/self/cgroup >/tmp/child-cgroup
child=$(awk -F: '$1=="0" || $2=="memory" {print $3}' /tmp/child-cgroup)
case "$child" in "${FUGUE_BUILD_MEMORY_CGROUP%/}/"*) ;; *) echo "child escaped memory controller: $child parent=$FUGUE_BUILD_MEMORY_CGROUP"; exit 1;; esac
docker run --rm --network none alpine:3.22 awk 'BEGIN{x="0123456789abcdef"; for(i=0;i<22;i++)x=x x; print length(x)}'
`
	cmd := exec.CommandContext(ctx, "docker", "run", "--name", name, "--privileged", "--network", "none", "--memory", "768m", "--memory-swap", "768m", "--cpus", "1", "--mount", "type=bind,src="+archive+",dst=/fixture.tar,readonly", "--mount", "type=bind,src="+termination+",dst=/dev/termination-log", "--entrypoint", "/bin/sh", defaultBuildpacksImage, "-c", builderMemoryWrapper, "fugue-memory", fmt.Sprint(768<<20), "/bin/sh", "-c", script)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("nested Docker: %v %s", err, out)
	}
	data, _ := os.ReadFile(termination)
	var observation struct {
		Peak int64 `json:"peak_bytes"`
	}
	if json.Unmarshal(data, &observation) != nil || observation.Peak < 64<<20 || observation.Peak > 768<<20 {
		t.Fatalf("nested peak missing: %s", data)
	}
	t.Logf("nested Docker cgroups contained; peak=%d bytes", observation.Peak)
}
