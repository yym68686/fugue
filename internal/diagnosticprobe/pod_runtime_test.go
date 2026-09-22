package diagnosticprobe

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/livediagnostics"
)

func TestPodRuntimeStateRequiresCompleteMatchingRuntimeEvidence(t *testing.T) {
	for _, scenario := range []string{"exited", "stale_pid_absent", "pid_present", "active_sandbox", "open_network", "running_container", "empty_sandboxes", "empty_containers", "duplicate_sandbox", "wrong_uid", "wrong_namespace", "wrong_sandbox", "missing_pid", "missing_network", "read_failure", "changed_state", "changed_container", "canceled", "host_proc_missing"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			target := podRuntimeTarget{Namespace: "tenant", Name: "revision-pod", UID: "01234567-89ab-cdef-0123-456789abcdef"}
			sid, cid := strings.Repeat("a", 64), strings.Repeat("b", 64)
			root := t.TempDir()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "pid_present" {
				if err := os.Mkdir(filepath.Join(root, "123"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "canceled" {
				cancel()
			}
			if scenario == "host_proc_missing" {
				root = filepath.Join(root, "absent")
			}
			calls := 0
			read := func(ctx context.Context, out any, args ...string) error {
				calls++
				if err := ctx.Err(); err != nil {
					return err
				}
				if scenario == "read_failure" {
					return errors.New("source unavailable")
				}
				if len(args) == 0 {
					t.Fatal("no read method")
				}
				metadata := map[string]any{"namespace": target.Namespace, "name": target.Name, "uid": target.UID}
				labels := map[string]any{"io.kubernetes.pod.namespace": target.Namespace, "io.kubernetes.pod.name": target.Name, "io.kubernetes.pod.uid": target.UID}
				if scenario == "wrong_uid" {
					metadata["uid"] = "another"
				}
				if scenario == "wrong_namespace" {
					labels["io.kubernetes.pod.namespace"] = "foreign"
				}
				state := "SANDBOX_NOTREADY"
				if scenario == "active_sandbox" {
					state = "SANDBOX_READY"
				}
				containerState := "CONTAINER_EXITED"
				if scenario == "running_container" || scenario == "changed_state" && calls > 4 {
					containerState = "CONTAINER_RUNNING"
				}
				currentCID := cid
				if scenario == "changed_container" && calls > 4 {
					currentCID = strings.Repeat("c", 64)
				}
				var value any
				switch args[0] {
				case "pods":
					if strings.Join(args, " ") != "pods --label io.kubernetes.pod.uid="+target.UID+" -o json" {
						t.Fatal("unbounded sandbox read", args)
					}
					items := []any{map[string]any{"id": sid, "metadata": metadata, "state": state}}
					if scenario == "empty_sandboxes" {
						items = []any{}
					}
					if scenario == "duplicate_sandbox" {
						items = append(items, items[0])
					}
					value = map[string]any{"items": items}
				case "inspectp":
					if len(args) != 2 || args[1] != sid {
						t.Fatal("wrong sandbox inspection", args)
					}
					info := map[string]any{"pid": 0, "processStatus": "deleted", "netNamespaceClosed": scenario != "open_network", "config": map[string]any{"env": "do-not-export"}}
					if scenario == "missing_network" {
						delete(info, "netNamespaceClosed")
					}
					value = map[string]any{"status": map[string]any{"id": sid, "metadata": metadata, "state": state}, "info": info}
				case "ps":
					if strings.Join(args, " ") != "ps -a --label io.kubernetes.pod.uid="+target.UID+" -o json" {
						t.Fatal("unbounded container read", args)
					}
					podID := sid
					if scenario == "wrong_sandbox" {
						podID = strings.Repeat("f", 64)
					}
					items := []any{map[string]any{"id": currentCID, "podSandboxId": podID, "state": containerState, "metadata": map[string]any{"name": "application"}, "labels": labels}}
					if scenario == "empty_containers" {
						items = []any{}
					}
					value = map[string]any{"containers": items}
				case "inspect":
					if len(args) != 2 || args[1] != currentCID {
						t.Fatal("wrong container inspection", args)
					}
					info := map[string]any{"pid": 0, "runtimeSpec": map[string]any{"env": []string{"PRIVATE=do-not-export"}}}
					if scenario == "stale_pid_absent" || scenario == "pid_present" {
						info["pid"] = 123
					}
					if scenario == "missing_pid" {
						delete(info, "pid")
					}
					value = map[string]any{"status": map[string]any{"id": currentCID, "state": containerState, "metadata": map[string]any{"name": "application"}, "labels": labels}, "info": info}
				default:
					t.Fatal("unexpected CRI method", args)
				}
				raw, err := json.Marshal(value)
				if err != nil {
					return err
				}
				return json.Unmarshal(raw, out)
			}
			facts, err := captureStablePodRuntimeFacts(ctx, target, read, root)
			valid := scenario == "exited" || scenario == "stale_pid_absent" || scenario == "pid_present" || scenario == "active_sandbox" || scenario == "open_network" || scenario == "running_container"
			if !valid {
				if err == nil {
					t.Fatal("incomplete or changing runtime accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := scenario == "exited" || scenario == "stale_pid_absent"
			if facts.Quiescent != want || calls != 8 {
				t.Fatalf("incorrect proof: %+v calls=%d", facts, calls)
			}
			raw, _ := json.Marshal(facts)
			if strings.Contains(string(raw), "do-not-export") {
				t.Fatal("raw CRI config leaked")
			}
		})
	}
}

func TestPodRuntimeCollectorRejectsArbitraryRuntimeAndTarget(t *testing.T) {
	for _, c := range []Collector{
		{Namespace: "tenant", ObjectName: "pod", PodUID: "missing"},
		{Namespace: "tenant", ObjectName: "pod", PodUID: "01234567-89ab-cdef-0123-456789abcdef", RuntimeBinary: "/bin/sh", Path: "/run/containerd/containerd.sock"},
		{Namespace: "tenant", ObjectName: "pod", PodUID: "01234567-89ab-cdef-0123-456789abcdef", RuntimeBinary: "/var/lib/rancher/k3s/data/current/bin/crictl", Path: "/run/other.sock"},
	} {
		if _, err := podRuntimeState(context.Background(), livediagnostics.ProbeRequest{Target: livediagnostics.Target{Type: livediagnostics.TargetNode, Node: "node"}}, c); err == nil {
			t.Fatal("invalid runtime invocation accepted")
		}
	}
}
