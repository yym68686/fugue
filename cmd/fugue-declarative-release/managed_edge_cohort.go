package main

import (
	"context"
	"encoding/json"
	"errors"

	"fugue/internal/declarativerelease"
	"fugue/internal/model/edgecohort"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *kubectlCluster) managedEdgeTransition(ctx context.Context, transition declarativerelease.EdgeGroupABTransition) (declarativerelease.EdgeGroupABTransition, error) {
	if c.edgeCohorts == nil {
		c.edgeCohorts = map[string]edgecohort.Snapshot{}
	}
	snapshot, found := c.edgeCohorts[transition.GroupID]
	if !found {
		raw, err := c.kubectlRun(ctx, nil, "get", "nodes", "--selector", "fugue.io/edge-group-id="+transition.GroupID, "--output", "json")
		if err != nil {
			return transition, err
		}
		var nodes corev1.NodeList
		if json.Unmarshal(raw, &nodes) != nil || nodes.Continue != "" {
			return transition, errors.New("Edge membership declaration is unavailable")
		}
		snapshot, err = edgecohort.FromNodes(nodes.Items, transition.GroupID, transition.ExpectedNodes)
		if err != nil {
			return transition, err
		}
		c.edgeCohorts[transition.GroupID] = snapshot
	}
	transition.ExpectedNodes = snapshot.Expected
	return transition, nil
}

func (c *kubectlCluster) managedEdgePods(raw []byte, group string) ([]byte, error) {
	snapshot, found := c.edgeCohorts[group]
	if !found {
		return raw, nil
	}
	var pods corev1.PodList
	if err := json.Unmarshal(raw, &pods); err != nil {
		return nil, err
	}
	if pods.Continue != "" {
		return nil, errors.New("Edge Pod cohort is incomplete")
	}
	pods.Items = snapshot.FilterPods(pods.Items)
	return json.Marshal(pods)
}

func (c *kubectlCluster) edgeObservationMembers(release declarativerelease.PlanRelease, workload, pods []byte) ([]byte, *int32, error) {
	if release.Workload.Kind != "DaemonSet" || release.Transition == nil || release.Transition.Type != "edge-group-ab" || release.Transition.EdgeGroupAB == nil {
		return pods, nil, nil
	}
	t := release.Transition.EdgeGroupAB
	s, found := c.edgeCohorts[t.GroupID]
	if !found || len(s.Suspended) == 0 && len(s.Preparing) == 0 {
		return pods, nil, nil
	}
	fail := errors.New("Edge observation lacks complete frozen member ownership")
	if release.Workload.Name != t.FrontName && release.Workload.Name != t.WorkerAName && release.Workload.Name != t.WorkerBName || release.Workload.PreservedUnavailable != 0 {
		return nil, nil, fail
	}
	var ds appsv1.DaemonSet
	var list corev1.PodList
	if json.Unmarshal(workload, &ds) != nil || json.Unmarshal(pods, &list) != nil || list.Continue != "" || ds.UID == "" || ds.Name != release.Workload.Name || ds.Namespace != release.Workload.Namespace || ds.Spec.Template.Labels["fugue.io/edge-group-id"] != t.GroupID {
		return nil, nil, fail
	}
	list.Items = s.FilterPods(list.Items)
	if len(list.Items) != s.Expected || s.Expected < 1 {
		return nil, nil, fail
	}
	seen := map[string]bool{}
	for _, p := range list.Items {
		if p.Spec.NodeName == "" || seen[p.Spec.NodeName] || p.UID == "" || p.DeletionTimestamp != nil || !metav1.IsControlledBy(&p, &ds) {
			return nil, nil, fail
		}
		seen[p.Spec.NodeName] = true
	}
	raw, err := json.Marshal(list)
	count := int32(s.Expected)
	return raw, &count, err
}
