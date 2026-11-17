package sourceimport

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"
)

const builderMemoryAnnotation = "fugue.pro/build-memory-source"
const builderMemoryKeyAnnotation = "fugue.pro/build-memory-key"
const builderMemoryHistoryTimeout = 2 * time.Second

type builderMemoryHistoryKey struct{}
type builderMemoryHistory struct {
	load   func(context.Context, string) (int64, error)
	record func(context.Context, string, string, int64) error
}

// WithBuilderMemoryHistory binds optional, rebuildable predictions to an app.
// Failed observations never turn an otherwise successful build into a failure.
func WithBuilderMemoryHistory(ctx context.Context, load func(context.Context, string) (int64, error), record func(context.Context, string, string, int64) error) context.Context {
	return context.WithValue(ctx, builderMemoryHistoryKey{}, builderMemoryHistory{load, record})
}

func prepareBuilderMemory(ctx context.Context, policy BuilderPodPolicy, profile builderWorkloadProfile, configuration any) BuilderPodPolicy {
	policy = normalizeBuilderPodPolicy(policy)
	policy.memoryTargetBytes = 0
	policy.memoryAllocationSource = "cold_start"
	if profile != builderWorkloadProfileHeavy {
		return policy
	}
	// The executed toolchain and generated configuration are included. Commit,
	// archive digest and output image are deliberately absent: normal releases
	// should benefit from previous compatible builds.
	data, err := json.Marshal(configuration)
	if err != nil {
		return policy
	}
	policy.memoryHistoryKey = fmt.Sprintf("%x", sha256.Sum256(data))
	history, _ := ctx.Value(builderMemoryHistoryKey{}).(builderMemoryHistory)
	if history.load != nil {
		readCtx, cancel := context.WithTimeout(ctx, builderMemoryHistoryTimeout)
		defer cancel()
		target, err := history.load(readCtx, policy.memoryHistoryKey)
		if err != nil {
			log.Printf("builder memory history unavailable; using cold-start allocation")
		} else if target > 0 && target <= MaxBuilderHeavyMemoryLimitBytes() {
			policy.memoryTargetBytes = parseBuilderBytes(formatBuilderMemoryBytes(target))
			policy.memoryAllocationSource = "history"
		}
	}
	return policy
}

func annotateBuilderMemory(job map[string]any, policy BuilderPodPolicy) {
	template := job["spec"].(map[string]any)["template"].(map[string]any)
	metadata, _ := template["metadata"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
		template["metadata"] = metadata
	}
	annotations, _ := metadata["annotations"].(map[string]string)
	if annotations == nil {
		annotations = map[string]string{}
		if existing, ok := metadata["annotations"].(map[string]any); ok {
			for key, value := range existing {
				if value, ok := value.(string); ok {
					annotations[key] = value
				}
			}
		}
	}
	annotations[builderMemoryAnnotation] = policy.memoryAllocationSource
	annotations[builderMemoryKeyAnnotation] = policy.memoryHistoryKey
	metadata["annotations"] = annotations
}

// Instrument the original argv, rather than interpolating credentials or build
// commands into shell source. The high-water mark is read before the cgroup is
// destroyed, so even subsecond init containers are covered.
func instrumentBuilderMemory(pod map[string]any) {
	for _, field := range []string{"initContainers", "containers"} {
		containers, _ := pod[field].([]map[string]any)
		for _, container := range containers {
			command, _ := container["command"].([]string)
			args, _ := container["args"].([]string)
			shell := "/bin/sh"
			if container["image"] == defaultKanikoImage {
				shell = "/busybox/sh"
			}
			if len(command) == 0 {
				if container["image"] != defaultKanikoImage {
					continue // Unknown image entrypoints cannot safely be wrapped.
				}
				command = []string{"/kaniko/executor"}
				shell = "/busybox/sh"
			}
			resources, _ := container["resources"].(map[string]any)
			limits, _ := resources["limits"].(map[string]string)
			limit := parseBuilderBytes(limits["memory"])
			if limit <= 0 {
				continue
			}
			container["command"] = []string{shell, "-c", builderMemoryWrapper, "fugue-memory", fmt.Sprint(limit)}
			container["args"] = append(append([]string{}, command...), args...)
			container["terminationMessagePolicy"] = "File"
		}
	}
}

// memory.peak includes descendant cgroups and file cache. Require the matching
// hard limit to avoid accidentally learning a host/root-cgroup measurement.
const builderMemoryWrapper = `fugue_memory_limit=$1
shift
fugue_memory_peak_file=
fugue_memory_group=
if [ -r /proc/self/cgroup ]; then
while IFS=: read -r fugue_memory_id fugue_memory_controllers fugue_memory_path; do
  case ",$fugue_memory_controllers," in
    ,,) fugue_memory_group=$fugue_memory_path ;;
    *,memory,*) fugue_memory_group=$fugue_memory_path ;;
  esac
done < /proc/self/cgroup
fi
for fugue_memory_dir in "/sys/fs/cgroup$fugue_memory_group" /sys/fs/cgroup "/sys/fs/cgroup/memory$fugue_memory_group" /sys/fs/cgroup/memory; do
  if [ -r "$fugue_memory_dir/memory.peak" ] && [ -r "$fugue_memory_dir/memory.max" ]; then
    read -r fugue_memory_cap < "$fugue_memory_dir/memory.max" || fugue_memory_cap=
    if [ "$fugue_memory_cap" = "$fugue_memory_limit" ]; then fugue_memory_peak_file=$fugue_memory_dir/memory.peak; break; fi
  fi
  if [ -r "$fugue_memory_dir/memory.max_usage_in_bytes" ] && [ -r "$fugue_memory_dir/memory.limit_in_bytes" ]; then
    read -r fugue_memory_cap < "$fugue_memory_dir/memory.limit_in_bytes" || fugue_memory_cap=
    if [ "$fugue_memory_cap" = "$fugue_memory_limit" ]; then fugue_memory_peak_file=$fugue_memory_dir/memory.max_usage_in_bytes; break; fi
  fi
done
# The nested Docker daemon must explicitly put its build containers beneath
# this same memory controller. Never export an unverified host/root group.
if [ -n "$fugue_memory_peak_file" ]; then
  export FUGUE_BUILD_MEMORY_CGROUP="$fugue_memory_group"
else
  unset FUGUE_BUILD_MEMORY_CGROUP
fi
"$@" &
fugue_memory_child=$!
fugue_memory_signaled=0
trap 'fugue_memory_signaled=1; kill -TERM "$fugue_memory_child" 2>/dev/null || true' TERM
trap 'fugue_memory_signaled=1; kill -INT "$fugue_memory_child" 2>/dev/null || true' INT
while :; do
  fugue_memory_signaled=0
  wait "$fugue_memory_child"
  fugue_memory_exit=$?
  # A trapped signal interrupts wait before the child has exited. Reap it
  # before exiting; a signal must never turn a live build into a success.
  if [ "$fugue_memory_signaled" = 0 ]; then break; fi
done
if [ -n "$fugue_memory_peak_file" ]; then
  read -r fugue_memory_peak < "$fugue_memory_peak_file" || fugue_memory_peak=
  case "$fugue_memory_peak" in
    ''|*[!0-9]*) ;;
    *) printf '{"fugue_memory_v":1,"peak_bytes":%s}\n' "$fugue_memory_peak" > /dev/termination-log 2>/dev/null || true ;;
  esac
fi
exit "$fugue_memory_exit"
`

func builderPodMemoryPeak(pod builderPod) (int64, bool) {
	if pod.Metadata.UID == "" || pod.Status.Phase != "Succeeded" || builderPodHasConfirmedOOM(pod) {
		return 0, false
	}
	var peak int64
	for index, group := range []struct {
		containers []builderEvidenceContainer
		statuses   []builderContainerStatus
	}{{pod.Spec.InitContainers, pod.Status.InitContainerStatuses}, {pod.Spec.Containers, pod.Status.ContainerStatuses}} {
		var sum int64
		for _, container := range group.containers {
			if container.Name == "" || container.RestartPolicy != "" {
				return 0, false
			}
			var value int64
			for _, status := range group.statuses {
				if status.Name != container.Name || status.RestartCount != 0 || status.State.Terminated == nil || status.State.Terminated.ExitCode != 0 {
					continue
				}
				var measurement struct {
					Version int   `json:"fugue_memory_v"`
					Peak    int64 `json:"peak_bytes"`
				}
				message := status.State.Terminated.Message
				limit := parseBuilderBytes(container.Resources.Limits["memory"])
				if len(message) <= 256 && json.Unmarshal([]byte(message), &measurement) == nil && measurement.Version == 1 && measurement.Peak > 0 && measurement.Peak <= limit {
					value = measurement.Peak
				}
			}
			if value <= 0 {
				return 0, false
			}
			// Init stages are sequential; app containers may overlap. Summing
			// app high-water marks is a conservative bound, never an average.
			if sum > math.MaxInt64-value {
				return 0, false
			}
			sum += value
			peak = maxInt64(peak, value)
		}
		if index == 1 {
			peak = maxInt64(peak, sum)
		}
	}
	return peak, peak > 0 && len(pod.Spec.Containers) > 0
}

// Observe only the single Pod created by this attempt. A stale or duplicate Pod
// is an evidence gap, not a lower memory requirement.
func recordBuilderMemory(ctx context.Context, pods []builderPod, started time.Time, succeeded bool) (int64, bool, string) {
	if len(pods) != 1 || pods[0].Metadata.CreationTimestamp.IsZero() || pods[0].Metadata.CreationTimestamp.Before(started.Truncate(time.Second)) {
		return 0, false, ""
	}
	pod := pods[0]
	source := pod.Metadata.Annotations[builderMemoryAnnotation]
	if source != "cold_start" && source != "history" && source != "oom_retry" {
		source = ""
	}
	peak, complete := builderPodMemoryPeak(pod)
	if !succeeded || !complete {
		return 0, false, source
	}
	history, _ := ctx.Value(builderMemoryHistoryKey{}).(builderMemoryHistory)
	key := pod.Metadata.Annotations[builderMemoryKeyAnnotation]
	if history.record != nil && len(key) == 64 {
		writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), builderMemoryHistoryTimeout)
		defer cancel()
		if err := history.record(writeCtx, key, pod.Metadata.UID, peak); err != nil {
			log.Printf("builder memory history observation not persisted")
		}
	}
	return peak, true, source
}

func builderMemoryConfiguration(kind string, values ...any) any {
	return struct {
		Version int
		Kind    string
		Values  []any
	}{1, strings.TrimSpace(kind), values}
}

func builderMemoryRecipeDigest(root, path string) (string, error) {
	fullPath, err := secureRepoJoin(root, path)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(fullPath)
	if err != nil {
		return "", fmt.Errorf("read build recipe: %w", err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}
