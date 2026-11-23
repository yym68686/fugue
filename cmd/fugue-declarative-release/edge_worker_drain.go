package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

// Leave time for connections accepted under the preceding activation to
// reach the Worker before checking its own namespace's socket inventory.
const edgeWorkerMinimumInactiveAge = 20 * time.Second

// Count half-open and closing sockets as well as established streams. TIME_WAIT,
// CLOSE and LISTEN no longer have an application stream that deletion can cut.
// Do not emit connection addresses. Failure to read either family fails closed.
const countEdgeWorkerSockets = `awk -v first="$1" -v second="$2" '
FNR == 1 {
	if ($1 != "sl" || $2 != "local_address" || $4 != "st") exit 2
	headers++
	next
}
{
	if (split($2, address, ":") != 2 || $4 !~ /^[0-9A-Fa-f][0-9A-Fa-f]$/) exit 2
	port = toupper(address[2])
	state = toupper($4)
	if ((port == first || port == second) && state != "0A" && state != "06" && state != "07") busy++
}
END {
	if (headers != 2) exit 2
	print busy + 0
}' /proc/net/tcp /proc/net/tcp6`

func (cluster *kubectlCluster) deleteDrainedEdgeWorkerPod(ctx context.Context, client dynamic.Interface, release declarativerelease.PlanRelease, transition declarativerelease.EdgeGroupABTransition, workload string, pod edgeGroupPod) error {
	var slot string
	switch workload {
	case transition.WorkerAName:
		slot = "a"
	case transition.WorkerBName:
		slot = "b"
	default:
		return errors.New("Worker drain is not bound to a declared slot")
	}
	namespace := release.Workload.Namespace
	live, err := client.Resource(schema.GroupVersionResource{Version: "v1", Resource: "pods"}).Namespace(namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("observe Worker Pod/%s before drain: %w", pod.Name, err)
	}
	var observed corev1.Pod
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(live.Object, &observed); err != nil ||
		string(observed.UID) != pod.UID || observed.ResourceVersion != pod.ResourceVersion ||
		observed.Spec.NodeName != pod.NodeName || observed.DeletionTimestamp != nil {
		return fmt.Errorf("Worker Pod/%s changed before drain observation", pod.Name)
	}
	owner := metav1.GetControllerOf(&observed)
	if owner == nil || owner.Kind != "DaemonSet" || owner.Name != workload {
		return fmt.Errorf("Worker Pod/%s lacks its declared slot owner", pod.Name)
	}
	ports, err := edgeWorkerDrainPorts(observed.Spec)
	if err != nil {
		return fmt.Errorf("observe Worker Pod/%s drain ports: %w", pod.Name, err)
	}
	readActivation := func() (edgeActivationState, bool, error) {
		return cluster.readEdgeActivationState(ctx, release, transition, pod)
	}
	readSockets := func() (int, error) {
		raw, err := cluster.kubectlRead(ctx, nil, "exec", "--namespace", namespace, pod.Name, "--container", transition.WorkerContainer,
			"--", "sh", "-ceu", countEdgeWorkerSockets, "sh", fmt.Sprintf("%04X", ports[0]), fmt.Sprintf("%04X", ports[1]))
		if err != nil {
			return 0, err
		}
		return strconv.Atoi(strings.TrimSpace(string(raw)))
	}
	if err := verifyEdgeWorkerDrain(slot, time.Now().UTC(), readActivation, readSockets); err != nil {
		return fmt.Errorf("refuse to replace Worker Pod/%s: %w", pod.Name, err)
	}
	return deleteEdgePodExact(ctx, client, namespace, pod)
}

func edgeWorkerDrainPorts(spec corev1.PodSpec) ([2]int32, error) {
	var ports [2]int32
	for _, container := range spec.Containers {
		for _, port := range container.Ports {
			var index int
			switch port.Name {
			case "http-slot":
				index = 0
			case "https-slot":
				index = 1
			default:
				continue
			}
			if ports[index] != 0 || port.Protocol != corev1.ProtocolTCP ||
				port.ContainerPort < 1 || port.ContainerPort > 65535 || port.HostPort != port.ContainerPort {
				return [2]int32{}, errors.New("ambiguous or unbound Worker listener")
			}
			ports[index] = port.ContainerPort
		}
	}
	if ports[0] == 0 || ports[1] == 0 || ports[0] == ports[1] {
		return [2]int32{}, errors.New("complete distinct Worker listeners are required")
	}
	return ports, nil
}

func verifyEdgeWorkerDrain(slot string, now time.Time, readActivation func() (edgeActivationState, bool, error), readSockets func() (int, error)) error {
	if slot != "a" && slot != "b" {
		return errors.New("Worker drain requires an exact slot")
	}
	before, exists, err := readActivation()
	if err != nil || !exists || before.Generation == 0 || before.ActiveSlot != otherEdgeSlot(slot) ||
		before.UpdatedAt.IsZero() || now.Sub(before.UpdatedAt) < edgeWorkerMinimumInactiveAge {
		return errors.New("Worker slot does not have a stable inactive activation")
	}
	busy, err := readSockets()
	if err != nil || busy < 0 {
		return errors.New("complete Worker socket observation unavailable")
	}
	after, exists, err := readActivation()
	if err != nil || !exists || !edgeActivationStatesAgree(before, after) || !before.UpdatedAt.Equal(after.UpdatedAt) {
		return errors.New("Worker slot activation changed during drain observation")
	}
	if busy != 0 {
		return fmt.Errorf("%w: Worker still has %d potentially live TCP sockets", declarativerelease.ErrInactiveWorkerDraining, busy)
	}
	return nil
}
