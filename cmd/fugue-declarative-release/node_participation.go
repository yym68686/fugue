package main

import (
	"context"
	"encoding/json"
	"errors"

	"fugue/internal/declarativerelease"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func participationKey(r declarativerelease.PlanRelease) string {
	return r.Workload.Namespace + "/" + r.Workload.Name
}

func (c *kubectlCluster) BindNodeParticipation(r declarativerelease.PlanRelease, p declarativerelease.NodeParticipation) error {
	if r.Workload.NodeParticipation != "reachable" || p.Validate() != nil {
		return errors.New("node participation does not match the declared workload policy")
	}
	if c.nodeParticipation == nil {
		c.nodeParticipation = map[string]declarativerelease.NodeParticipation{}
	}
	c.nodeParticipation[participationKey(r)] = p
	return nil
}

// Capture before writing. Deferred nodes retain their desired Pod and identity;
// Kubernetes reconciles them to the committed template after reconnection.
func captureNodeParticipation(ds appsv1.DaemonSet, pods corev1.PodList, nodes corev1.NodeList) (declarativerelease.NodeParticipation, error) {
	p := declarativerelease.NodeParticipation{WorkloadUID: string(ds.UID), Required: map[string]string{}, Deferred: map[string]string{}}
	if ds.UID == "" || ds.DeletionTimestamp != nil || pods.Continue != "" || nodes.Continue != "" || len(nodes.Items) > 1000 || len(pods.Items) > 2000 {
		return p, errors.New("node participation inventory is incomplete")
	}
	byName := map[string]corev1.Node{}
	for _, n := range nodes.Items {
		if n.Name == "" || n.UID == "" || n.DeletionTimestamp != nil || byName[n.Name].Name != "" {
			return p, errors.New("node participation identity is ambiguous")
		}
		byName[n.Name] = n
	}
	seen := map[string]bool{}
	for _, pod := range pods.Items {
		if !metav1.IsControlledBy(&pod, &ds) || pod.Spec.NodeName == "" {
			return p, errors.New("node participation Pod lacks exact workload ownership")
		}
		name := pod.Spec.NodeName
		if seen[name] {
			continue
		}
		seen[name] = true
		n, found := byName[name]
		if !found {
			return p, errors.New("Pod has no physical Node identity")
		}
		ready := false
		for _, condition := range n.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				ready = true
			}
		}
		if ready {
			p.Required[name] = string(n.UID)
		} else {
			p.Deferred[name] = string(n.UID)
		}
	}
	if len(seen) != int(ds.Status.DesiredNumberScheduled) {
		return p, errors.New("node participation does not cover scheduled DaemonSet inventory")
	}
	return p, p.Validate()
}

func filterNodeParticipation(p declarativerelease.NodeParticipation, ds appsv1.DaemonSet, pods corev1.PodList, nodes corev1.NodeList) (corev1.PodList, error) {
	fail := errors.New("frozen node participation identity or membership changed")
	if p.Validate() != nil || string(ds.UID) != p.WorkloadUID || ds.DeletionTimestamp != nil || nodes.Continue != "" || pods.Continue != "" || int(ds.Status.DesiredNumberScheduled) != len(p.Required)+len(p.Deferred) {
		return pods, fail
	}
	known := map[string]string{}
	ready := map[string]bool{}
	for _, n := range nodes.Items {
		if n.DeletionTimestamp == nil {
			known[n.Name] = string(n.UID)
			for _, condition := range n.Status.Conditions {
				if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
					ready[n.Name] = true
				}
			}
		}
	}
	for name, uid := range p.Required {
		if known[name] != uid || !ready[name] {
			return pods, fail
		}
	}
	for name, uid := range p.Deferred {
		if known[name] != uid {
			return pods, fail
		}
	}
	out := pods.DeepCopy()
	out.Items = nil
	seen := map[string]bool{}
	for _, pod := range pods.Items {
		if !metav1.IsControlledBy(&pod, &ds) || p.Required[pod.Spec.NodeName] == "" && p.Deferred[pod.Spec.NodeName] == "" {
			return pods, fail
		}
		if p.Required[pod.Spec.NodeName] == "" || pod.DeletionTimestamp != nil {
			continue
		}
		if seen[pod.Spec.NodeName] || pod.UID == "" {
			return pods, fail
		}
		seen[pod.Spec.NodeName] = true
		out.Items = append(out.Items, pod)
	}
	if len(seen) != len(p.Required) {
		return pods, errors.New("participating Pod inventory is incomplete")
	}
	return *out, nil
}

func (c *kubectlCluster) participatingPods(ctx context.Context, r declarativerelease.PlanRelease, workload, pods []byte) ([]byte, *int32, error) {
	if r.Workload.NodeParticipation == "" {
		return c.edgeObservationMembers(r, workload, pods)
	}
	if r.Workload.NodeParticipation != "reachable" || r.Workload.Kind != "DaemonSet" || r.Transition != nil || r.Workload.PreservedUnavailable != 0 {
		return nil, nil, errors.New("unsupported node participation policy")
	}
	var ds appsv1.DaemonSet
	var list corev1.PodList
	var nodes corev1.NodeList
	if json.Unmarshal(workload, &ds) != nil || json.Unmarshal(pods, &list) != nil || ds.Name != r.Workload.Name || ds.Namespace != r.Workload.Namespace {
		return nil, nil, errors.New("invalid node participation workload")
	}
	raw, err := c.kubectlRun(ctx, nil, "get", "nodes", "--output", "json")
	if err != nil {
		return nil, nil, err
	}
	if json.Unmarshal(raw, &nodes) != nil {
		return nil, nil, errors.New("invalid Node inventory")
	}
	p, found := c.nodeParticipation[participationKey(r)]
	if !found {
		p, err = captureNodeParticipation(ds, list, nodes)
		if err != nil {
			return nil, nil, err
		}
		if err := c.BindNodeParticipation(r, p); err != nil {
			return nil, nil, err
		}
	}
	filtered, err := filterNodeParticipation(p, ds, list, nodes)
	if err != nil {
		return nil, nil, err
	}
	encoded, err := json.Marshal(filtered)
	count := int32(len(p.Required))
	return encoded, &count, err
}
