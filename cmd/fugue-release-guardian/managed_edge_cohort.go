package main

import (
	"context"
	"errors"

	"fugue/internal/model/edgecohort"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// Freeze administrator membership for a complete observation/activation. A
// preparing node cannot alter the established cohort's health or LKG.
func (a *frontAuthorityActivator) withManagedCohort(ctx context.Context) (*frontAuthorityActivator, error) {
	if a.cohort != nil {
		return a, nil
	}
	nodes, err := a.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: labels.Set{"fugue.io/edge-group-id": a.config.GroupID}.AsSelector().String(), Limit: 1001})
	if err != nil {
		return nil, err
	}
	if nodes.Continue != "" || len(nodes.Items) > 1000 {
		return nil, errors.New("Edge membership is incomplete")
	}
	cohort, err := edgecohort.FromNodes(nodes.Items, a.config.GroupID, a.config.ExpectedNodes)
	if err != nil {
		return nil, err
	}
	clone := *a
	clone.cohort = &cohort
	clone.config.ExpectedNodes = cohort.Expected
	return &clone, nil
}

func (a *frontAuthorityActivator) cohortPods(pods []corev1.Pod) []corev1.Pod {
	if a.cohort == nil {
		return pods
	}
	return a.cohort.FilterPods(pods)
}
