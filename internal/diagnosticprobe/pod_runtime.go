package diagnosticprobe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
)

var criIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var podUIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var podNamePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?$`)

type podRuntimeTarget struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	UID       string `json:"uid"`
}
type criSandboxFact struct {
	ID                 string           `json:"id"`
	Metadata           podRuntimeTarget `json:"metadata"`
	State              string           `json:"state"`
	PID                int64            `json:"pid"`
	ProcessStatus      string           `json:"process_status"`
	NetNamespaceClosed bool             `json:"net_namespace_closed"`
}
type criContainerFact struct {
	ID             string `json:"id"`
	SandboxID      string `json:"sandbox_id"`
	Name           string `json:"name"`
	State          string `json:"state"`
	PID            int64  `json:"pid"`
	ProcessPresent bool   `json:"process_present"`
}
type podRuntimeFacts struct {
	Target     podRuntimeTarget   `json:"target"`
	Sandboxes  []criSandboxFact   `json:"sandboxes"`
	Containers []criContainerFact `json:"containers"`
	Quiescent  bool               `json:"quiescent"`
}

type criRead func(context.Context, any, ...string) error

// A registered diagnostic observation only. These facts do not authorize a
// workload deletion or replace the controller's traffic/ownership/CAS gates.
func podRuntimeState(ctx context.Context, req livediagnostics.ProbeRequest, c Collector) (any, error) {
	if req.Target.Type != livediagnostics.TargetNode || req.Target.Node == "" {
		return nil, errors.New("Pod runtime observation requires an explicit node target")
	}
	target := podRuntimeTarget{Namespace: parameter(c.Namespace, req), Name: parameter(c.ObjectName, req), UID: parameter(c.PodUID, req)}
	if !podNamePattern.MatchString(target.Namespace) || len(target.Namespace) > 63 || strings.Contains(target.Namespace, ".") || !podNamePattern.MatchString(target.Name) || !podUIDPattern.MatchString(target.UID) {
		return nil, errors.New("Pod runtime observation requires exact namespace, name and UID")
	}
	// The signed recipe chooses a runtime adapter, never an arbitrary program,
	// endpoint URL or command. All invocations are fixed read-only CRI methods.
	var args []string
	switch c.RuntimeBinary {
	case "/var/lib/rancher/k3s/data/current/bin/crictl", "/usr/local/bin/crictl", "/usr/bin/crictl":
	default:
		return nil, errors.New("unsupported CRI reader binary")
	}
	switch c.Path {
	case "/run/k3s/containerd/containerd.sock", "/run/containerd/containerd.sock", "/var/run/containerd/containerd.sock":
	default:
		return nil, errors.New("unsupported CRI reader socket")
	}
	root := filepath.Join(hostProc, "1/root")
	binary, err := criReaderBinary(root, c.RuntimeBinary)
	if err != nil {
		return nil, err
	}
	socket := filepath.Join(root, c.Path)
	info, err := os.Stat(socket)
	if err != nil || info.Mode()&os.ModeSocket == 0 {
		return nil, errors.New("CRI socket is unavailable")
	}
	boot, err := readBounded(filepath.Join(hostProc, "sys/kernel/random/boot_id"), 80)
	if err != nil || !podUIDPattern.MatchString(strings.TrimSpace(boot)) {
		return nil, errors.New("node boot identity is unavailable")
	}
	peer, err := criReaderPeer(ctx, socket)
	if err != nil {
		return nil, err
	}
	args = append(args, "--config=/dev/null", "--runtime-endpoint=unix://"+socket, "--timeout=3s")
	read := func(ctx context.Context, value any, command ...string) error {
		ctx, cancel := context.WithTimeout(ctx, 4*time.Second)
		defer cancel()
		argv := append(append([]string(nil), args...), command...)
		raw, _, truncated, err := diagnosticCommandEvidence(ctx, 1<<20, binary, argv...)
		if err != nil || truncated {
			return fmt.Errorf("CRI %s read failed or exceeded output budget", command[0])
		}
		if err := json.Unmarshal(raw, value); err != nil {
			return errors.New("invalid CRI response")
		}
		return nil
	}
	started := time.Now().UTC()
	second, err := captureStablePodRuntimeFacts(ctx, target, read, hostProc)
	if err != nil {
		return nil, err
	}
	after, err := os.Stat(socket)
	currentBoot, bootErr := readBounded(filepath.Join(hostProc, "sys/kernel/random/boot_id"), 80)
	currentPeer, peerErr := criReaderPeer(ctx, socket)
	if err != nil || !os.SameFile(info, after) || bootErr != nil || currentBoot != boot || peerErr != nil || peer != currentPeer {
		return nil, errors.New("node boot or runtime process identity changed during observation")
	}
	return map[string]any{"node": req.Target.Node, "boot_id": strings.TrimSpace(boot), "runtime_peer": peer, "observed_from": started, "observed_until": time.Now().UTC(), "facts": second, "scope": "current runtime state for this exact Pod UID; no deletion authorization"}, nil
}

func criReaderBinary(root, name string) (string, error) {
	if name != "/var/lib/rancher/k3s/data/current/bin/crictl" {
		return filepath.Join(root, name), nil
	}
	// current is commonly an absolute host symlink. Resolve that single
	// installation pointer inside the observed host root, never our container.
	const data = "/var/lib/rancher/k3s/data"
	link, err := os.Readlink(filepath.Join(root, data, "current"))
	if err != nil {
		return "", errors.New("installed CRI directory pointer unavailable")
	}
	if !filepath.IsAbs(link) {
		link = filepath.Join(data, link)
	}
	link = filepath.Clean(link)
	if filepath.Dir(link) != data || !criIDPattern.MatchString(filepath.Base(link)) {
		return "", errors.New("installed CRI directory is outside the versioned runtime data")
	}
	binary := filepath.Join(root, link, "bin/crictl")
	if target, err := os.Readlink(binary); err == nil && target != "k3s" {
		return "", errors.New("unexpected installed CRI binary link")
	}
	info, err := os.Stat(binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&0111 == 0 {
		return "", errors.New("installed CRI executable unavailable")
	}
	return binary, nil
}

func captureStablePodRuntimeFacts(ctx context.Context, target podRuntimeTarget, read criRead, procRoot string) (podRuntimeFacts, error) {
	first, err := capturePodRuntimeFacts(ctx, target, read, procRoot)
	if err != nil {
		return podRuntimeFacts{}, err
	}
	second, err := capturePodRuntimeFacts(ctx, target, read, procRoot)
	if err != nil {
		return podRuntimeFacts{}, err
	}
	if !reflect.DeepEqual(first, second) {
		return podRuntimeFacts{}, errors.New("Pod runtime identity or state changed during observation")
	}
	return second, nil
}

func criReaderPeer(ctx context.Context, socket string) (commandProcess, error) {
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "unix", socket)
	if err != nil {
		return commandProcess{}, err
	}
	defer conn.Close()
	pid, err := runtimeSocketPeerPID(conn)
	if err != nil || pid <= 1 {
		return commandProcess{}, errors.New("CRI peer identity unavailable")
	}
	stat, err := readBounded(filepath.Join(hostProc, strconv.Itoa(pid), "stat"), 16<<10)
	if err != nil {
		return commandProcess{}, err
	}
	p, err := parseProcessStat(pid, stat)
	return commandProcess{PID: pid, StartTicks: p.StartTicks, Command: p.Command}, err
}

func capturePodRuntimeFacts(ctx context.Context, target podRuntimeTarget, read criRead, procRoot string) (podRuntimeFacts, error) {
	if _, err := os.Stat(procRoot); err != nil {
		return podRuntimeFacts{}, errors.New("host process inventory is unavailable")
	}
	result := podRuntimeFacts{Target: target, Sandboxes: []criSandboxFact{}, Containers: []criContainerFact{}, Quiescent: true}
	var pods struct {
		Items []struct {
			ID       string           `json:"id"`
			Metadata podRuntimeTarget `json:"metadata"`
			State    string           `json:"state"`
		} `json:"items"`
	}
	if err := read(ctx, &pods, "pods", "--label", "io.kubernetes.pod.uid="+target.UID, "-o", "json"); err != nil {
		return result, err
	}
	if len(pods.Items) == 0 || len(pods.Items) > 16 {
		return result, errors.New("no bounded complete sandbox set; absence is not quiescence proof")
	}
	sandboxes := map[string]bool{}
	for _, p := range pods.Items {
		if !criIDPattern.MatchString(p.ID) || p.Metadata != target || sandboxes[p.ID] {
			return result, errors.New("sandbox ownership or identity differs")
		}
		sandboxes[p.ID] = true
		var inspect struct {
			Status struct {
				ID       string           `json:"id"`
				Metadata podRuntimeTarget `json:"metadata"`
				State    string           `json:"state"`
			} `json:"status"`
			Info struct {
				PID                *int64 `json:"pid"`
				ProcessStatus      string `json:"processStatus"`
				NetNamespaceClosed *bool  `json:"netNamespaceClosed"`
			} `json:"info"`
		}
		if err := read(ctx, &inspect, "inspectp", p.ID); err != nil {
			return result, err
		}
		if inspect.Status.ID != p.ID || inspect.Status.Metadata != target || inspect.Status.State != p.State || inspect.Info.PID == nil || *inspect.Info.PID < 0 || inspect.Info.NetNamespaceClosed == nil || inspect.Info.ProcessStatus == "" {
			return result, errors.New("incomplete or changed sandbox observation")
		}
		fact := criSandboxFact{ID: p.ID, Metadata: target, State: p.State, PID: *inspect.Info.PID, ProcessStatus: inspect.Info.ProcessStatus, NetNamespaceClosed: *inspect.Info.NetNamespaceClosed}
		result.Sandboxes = append(result.Sandboxes, fact)
		result.Quiescent = result.Quiescent && fact.State == "SANDBOX_NOTREADY" && fact.PID == 0 && fact.ProcessStatus == "deleted" && fact.NetNamespaceClosed
	}
	var containers struct {
		Containers []struct {
			ID           string `json:"id"`
			PodSandboxID string `json:"podSandboxId"`
			State        string `json:"state"`
			Metadata     struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Labels map[string]string `json:"labels"`
		} `json:"containers"`
	}
	if err := read(ctx, &containers, "ps", "-a", "--label", "io.kubernetes.pod.uid="+target.UID, "-o", "json"); err != nil {
		return result, err
	}
	if len(containers.Containers) == 0 || len(containers.Containers) > 64 {
		return result, errors.New("no bounded complete container set")
	}
	seen := map[string]bool{}
	for _, c := range containers.Containers {
		if !criIDPattern.MatchString(c.ID) || !sandboxes[c.PodSandboxID] || seen[c.ID] || c.Labels["io.kubernetes.pod.uid"] != target.UID || c.Labels["io.kubernetes.pod.name"] != target.Name || c.Labels["io.kubernetes.pod.namespace"] != target.Namespace || c.Metadata.Name == "" {
			return result, errors.New("container ownership or identity differs")
		}
		seen[c.ID] = true
		var inspect struct {
			Status struct {
				ID       string `json:"id"`
				State    string `json:"state"`
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
				Labels map[string]string `json:"labels"`
			} `json:"status"`
			Info struct {
				PID *int64 `json:"pid"`
			} `json:"info"`
		}
		if err := read(ctx, &inspect, "inspect", c.ID); err != nil {
			return result, err
		}
		labels := inspect.Status.Labels
		if inspect.Status.ID != c.ID || inspect.Status.State != c.State || inspect.Status.Metadata.Name != c.Metadata.Name || inspect.Info.PID == nil || *inspect.Info.PID < 0 || labels["io.kubernetes.pod.uid"] != target.UID || labels["io.kubernetes.pod.name"] != target.Name || labels["io.kubernetes.pod.namespace"] != target.Namespace {
			return result, errors.New("incomplete or changed container observation")
		}
		present := false
		if *inspect.Info.PID > 0 {
			_, err := os.Stat(filepath.Join(procRoot, strconv.FormatInt(*inspect.Info.PID, 10)))
			if err != nil && !os.IsNotExist(err) {
				return result, errors.New("container process presence is unavailable")
			}
			present = err == nil
		}
		result.Containers = append(result.Containers, criContainerFact{ID: c.ID, SandboxID: c.PodSandboxID, Name: c.Metadata.Name, State: c.State, PID: *inspect.Info.PID, ProcessPresent: present})
		result.Quiescent = result.Quiescent && c.State == "CONTAINER_EXITED" && !present
	}
	sort.Slice(result.Sandboxes, func(i, j int) bool { return result.Sandboxes[i].ID < result.Sandboxes[j].ID })
	sort.Slice(result.Containers, func(i, j int) bool { return result.Containers[i].ID < result.Containers[j].ID })
	if err := ctx.Err(); err != nil {
		return result, fmt.Errorf("runtime observation canceled: %w", err)
	}
	return result, nil
}
