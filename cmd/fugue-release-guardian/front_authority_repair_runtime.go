package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"path"
	"reflect"
	"sort"
	"strconv"
	"time"

	"fugue/internal/model/edgecohort"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type frontRepairRuntime struct {
	activator                 *frontAuthorityActivator
	read                      func(context.Context, string, any) error
	nodeTools                 *frontRepairToolSet         // required by runApproved; nil is only the isolated executor path
	requireControlPlaneFences func(context.Context) error // mandatory on the protected live entrypoint
}

type frontRepairRuntimeMember struct {
	frontRepairWitness
	front, worker corev1.Pod
}

func newFrontRepairRuntime(activator *frontAuthorityActivator) frontRepairRuntime {
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext, DisableKeepAlives: true}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return frontRepairRuntime{activator: activator, read: func(ctx context.Context, endpoint string, dst any) error {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("Edge health returned HTTP %d", response.StatusCode)
		}
		const limit = 64 << 10
		body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
		if err != nil || len(body) > limit {
			return errors.Join(errors.New("Edge health response is unreadable or oversized"), err)
		}
		if _, extensible := dst.(*authorityWorkerHealth); extensible {
			return json.Unmarshal(body, dst)
		}
		return decodeStrictJSON(body, dst)
	}}
}

// roster resolves real members through the same configuration and enrollment
// policy as ordinary activation. A declaration never gets to choose subset
// membership, a Pod address or a host activation file.
func (r frontRepairRuntime) roster(ctx context.Context) (map[string]frontRepairNode, *edgecohort.Snapshot, error) {
	a := r.activator
	selector := labels.Set{"fugue.io/edge-group-id": a.config.GroupID}.AsSelector().String()
	result, err := a.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: selector, Limit: 1001})
	if err != nil || result.Continue != "" || len(result.Items) > 1000 {
		return nil, nil, errors.Join(errors.New("Edge repair Node roster is incomplete"), err)
	}
	cohort, err := edgecohort.FromNodes(result.Items, a.config.GroupID, a.config.ExpectedNodes)
	if err != nil {
		return nil, nil, err
	}
	physical := make(map[string]frontRepairNode, cohort.Expected)
	uids := make(map[string]bool, cohort.Expected)
	for _, node := range result.Items {
		if node.Labels["fugue.io/edge-group-id"] != a.config.GroupID || node.Labels["fugue.io/role.edge"] != "true" || cohort.Preparing[node.Name] || cohort.Suspended[node.Name] {
			continue
		}
		uid := string(node.UID)
		if !frontRepairIdentity(uid) || uids[uid] || !repairNodeReady(node) {
			return nil, nil, fmt.Errorf("Edge repair roster cannot prove Node %s", node.Name)
		}
		uids[uid] = true
		physical[node.Name] = frontRepairNode{Name: node.Name, UID: uid, Admitted: cohort.Admitted[node.Name]}
	}
	if len(physical) != cohort.Expected || len(physical) < 2 {
		return nil, nil, errors.New("Edge repair must include the complete live cohort")
	}
	return physical, &cohort, nil
}

// observe reads the host activation through the pinned target Worker, then
// independently reads the live Front and the target Worker on that same node.
// An unhealthy standby remains an observation, not permission to switch.
func (r frontRepairRuntime) observe(ctx context.Context, before releaseguardian.CurrentAuthority, authorized []frontRepairNode) ([]frontRepairRuntimeMember, error) {
	if r.activator == nil || r.activator.now == nil || r.read == nil || before.Validate() != nil || before.GroupID != r.activator.config.GroupID {
		return nil, errors.New("Edge repair runtime observation is not bound to the group authority")
	}
	physical, cohort, err := r.roster(ctx)
	if err != nil {
		return nil, err
	}
	want := make(map[string]frontRepairNode, len(authorized))
	for _, node := range authorized {
		want[node.Name] = node
	}
	if len(want) != len(authorized) || !maps.Equal(physical, want) {
		return nil, errors.New("Edge repair physical roster differs from the authorized cohort")
	}
	frozen := *r.activator
	frozen.cohort, frozen.config.ExpectedNodes = cohort, cohort.Expected
	fronts, err := frozen.servingFrontPods(ctx)
	if err != nil {
		return nil, err
	}
	workers, stamp, err := frozen.observeWorkers(ctx, releaseguardian.FrontAuthorityTarget{
		GroupID: before.GroupID, TargetSlot: before.CurrentWorkerSlot, CandidateBundleGeneration: before.CurrentBundleGeneration,
		WorkerSourceSHA: before.CurrentWorkerSourceSHA, WorkerImageDigest: before.CurrentWorkerImageDigest,
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(fronts, func(i, j int) bool { return fronts[i].Spec.NodeName < fronts[j].Spec.NodeName })
	observed := make([]frontRepairRuntimeMember, 0, len(fronts))
	for _, front := range fronts {
		name := front.Spec.NodeName
		node, nodeOK := physical[name]
		worker, workerOK := workers[name]
		frontMount := repairHostActivationMount(front, "edge-front", false)
		if !nodeOK || !workerOK || frontMount == "" || frontMount != repairHostActivationMount(worker, "edge", true) {
			return nil, fmt.Errorf("Edge repair cannot prove a shared host activation mount on %s", name)
		}
		frontID, err := repairPodProcess(front, frozen.config.Namespace, "edge-front")
		if err != nil {
			return nil, err
		}
		workerID, err := repairPodProcess(worker, frozen.config.Namespace, "edge")
		if err != nil {
			return nil, err
		}
		caddyID, err := repairPodProcess(worker, frozen.config.Namespace, "caddy")
		if err != nil {
			return nil, err
		}
		observedAt := frozen.now().UTC()
		state, err := frozen.readActivation(ctx, worker.Name)
		if err != nil || state.GroupID != before.GroupID {
			return nil, errors.Join(fmt.Errorf("Edge repair cannot prove the durable activation on %s", name), err)
		}
		var live observedFront
		if err := r.read(ctx, repairHealthEndpoint(front.Status.PodIP, frontHealthPort, "/readyz"), &live); err != nil || !frontRepairLiveMatches(live, state) {
			return nil, errors.Join(fmt.Errorf("Edge repair live Front and durable activation disagree on %s", name), err)
		}
		var health authorityWorkerHealth
		// Worker health has an extensible schema, unlike the exact Front shape.
		if err := r.read(ctx, repairHealthEndpoint(worker.Status.PodIP, workerHealthPort, "/healthz"), &health); err != nil || health.EdgeGroupID != before.GroupID {
			return nil, errors.Join(fmt.Errorf("Edge repair target Worker health is unavailable on %s", name), err)
		}
		member := frontRepairRuntimeMember{front: front, worker: worker, frontRepairWitness: frontRepairWitness{
			frontRepairMember: frontRepairMember{Node: node, FrontPodUID: string(front.UID), FrontProcessID: frontID,
				TargetWorkerPodUID: string(worker.UID), TargetWorkerProcessID: workerID, TargetCaddyProcessID: caddyID,
				Activation: state, TargetWorkerSlot: before.CurrentWorkerSlot, TargetWorkerSourceSHA: stamp.WorkerSourceSHA, TargetWorkerImage: stamp.WorkerImageDigest},
			LiveFront: live, TargetWorkerHealth: health, ObservedAt: observedAt,
		}}
		if err := r.requireSameMember(ctx, member); err != nil {
			return nil, err
		}
		observed = append(observed, member)
	}
	after, _, err := r.roster(ctx)
	if err != nil || !maps.Equal(after, physical) {
		return nil, errors.Join(errors.New("Edge repair roster changed while observing the runtime"), err)
	}
	return observed, nil
}

// Use immediately before exec as well as after observation. Pod UID alone
// does not detect a restart or a reassigned address within the same Pod.
func (r frontRepairRuntime) requireSameMember(ctx context.Context, member frontRepairRuntimeMember) error {
	a := r.activator
	node, err := a.client.CoreV1().Nodes().Get(ctx, member.Node.Name, metav1.GetOptions{})
	if err != nil || string(node.UID) != member.Node.UID || !repairNodeReady(*node) ||
		node.Labels["fugue.io/edge-group-id"] != a.config.GroupID || node.Labels["fugue.io/role.edge"] != "true" ||
		node.Labels["fugue.io/schedulable"] == "false" || node.Labels[edgecohort.PhaseLabel] == edgecohort.Preparing ||
		(node.Labels[edgecohort.PhaseLabel] == edgecohort.Admitted) != member.Node.Admitted {
		return errors.Join(fmt.Errorf("Edge repair Node %s changed", member.Node.Name), err)
	}
	if held, err := edgecohort.ReleaseHeld(*node, a.config.GroupID); err != nil || held {
		return errors.Join(fmt.Errorf("Edge repair Node %s is not eligible for release", member.Node.Name), err)
	}
	for _, binding := range []struct {
		pod       corev1.Pod
		processes map[string]string
	}{{member.front, map[string]string{"edge-front": member.FrontProcessID}},
		{member.worker, map[string]string{"edge": member.TargetWorkerProcessID, "caddy": member.TargetCaddyProcessID}}} {
		pod, err := a.client.CoreV1().Pods(a.config.Namespace).Get(ctx, binding.pod.Name, metav1.GetOptions{})
		if err != nil || pod.UID != binding.pod.UID || pod.Spec.NodeName != member.Node.Name || pod.Status.PodIP != binding.pod.Status.PodIP ||
			!maps.Equal(pod.Labels, binding.pod.Labels) || !maps.Equal(pod.Annotations, binding.pod.Annotations) ||
			!reflect.DeepEqual(pod.Spec, binding.pod.Spec) || !reflect.DeepEqual(pod.OwnerReferences, binding.pod.OwnerReferences) {
			return errors.Join(fmt.Errorf("Edge repair Pod %s changed", binding.pod.Name), err)
		}
		for name, expectedID := range binding.processes {
			id, err := repairPodProcess(*pod, a.config.Namespace, name)
			if err != nil || id != expectedID {
				return errors.Join(fmt.Errorf("Edge repair container %s/%s changed", binding.pod.Name, name), err)
			}
		}
	}
	return nil
}

func repairNodeReady(node corev1.Node) bool {
	if node.DeletionTimestamp != nil || node.Spec.Unschedulable {
		return false
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func repairPodProcess(pod corev1.Pod, namespace, name string) (string, error) {
	address, err := netip.ParseAddr(pod.Status.PodIP)
	if err == nil && address.IsValid() && !address.IsUnspecified() && !address.IsMulticast() && !address.IsLoopback() && address.Zone() == "" &&
		pod.Namespace == namespace && pod.DeletionTimestamp == nil && frontRepairIdentity(string(pod.UID)) && pod.Status.Phase == corev1.PodRunning && podReady(pod.Status.Conditions) {
		for _, container := range pod.Status.ContainerStatuses {
			if container.Name == name && container.Ready && container.State.Running != nil && frontRepairIdentity(container.ContainerID) {
				return container.ContainerID, nil
			}
		}
	}
	return "", fmt.Errorf("Edge repair cannot prove the ready Pod process %s/%s", pod.Name, name)
}

func repairHealthEndpoint(ip string, port int, suffix string) string {
	return "http://" + net.JoinHostPort(ip, strconv.Itoa(port)) + suffix
}

func repairHostActivationMount(pod corev1.Pod, container string, writable bool) string {
	for _, c := range pod.Spec.Containers {
		if c.Name != container {
			continue
		}
		for _, mount := range c.VolumeMounts {
			if mount.MountPath != path.Dir(frontActivationStatePath) || mount.SubPath != "" || mount.SubPathExpr != "" || writable && mount.ReadOnly {
				continue
			}
			for _, volume := range pod.Spec.Volumes {
				if volume.Name == mount.Name && volume.HostPath != nil && path.IsAbs(volume.HostPath.Path) && volume.HostPath.Path == path.Clean(volume.HostPath.Path) && volume.HostPath.Path != "/" {
					return volume.HostPath.Path
				}
			}
		}
	}
	return ""
}
