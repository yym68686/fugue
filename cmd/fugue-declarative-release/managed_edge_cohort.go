package main

import (
	"context"
	"encoding/json"
	"errors"

	"fugue/internal/declarativerelease"
	"fugue/internal/model/edgecohort"
	corev1 "k8s.io/api/core/v1"
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
