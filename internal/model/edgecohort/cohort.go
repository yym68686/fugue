// Package edgecohort projects explicit node enrollment intent into a release
// cohort. Readiness is still proven by the release executor, never by labels.
package edgecohort

import (
	"errors"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const PhaseLabel = "fugue.io/edge-enrollment-phase"
const Preparing = "preparing"
const Admitted = "admitted"
const CapabilityAnnotation = "fugue.pro/managed-edge-cohort"
const CapabilityV1 = "v1"

type Snapshot struct {
	Group     string
	Expected  int
	Preparing map[string]bool
	Admitted  map[string]bool
}

func FromNodes(nodes []corev1.Node, group string, baseline int) (Snapshot, error) {
	s := Snapshot{Group: group, Expected: baseline, Preparing: map[string]bool{}, Admitted: map[string]bool{}}
	if group == "" || baseline < 1 || baseline > 100 {
		return s, errors.New("invalid Edge cohort baseline")
	}
	for _, n := range nodes {
		if n.Labels["fugue.io/edge-group-id"] != group || n.Labels["fugue.io/role.edge"] != "true" {
			continue
		}
		phase := n.Labels[PhaseLabel]
		if phase == "" {
			continue
		}
		if n.Name == "" || n.UID == "" || n.DeletionTimestamp != nil || n.Labels["fugue.io/edge-workload"] != "dynamic" {
			return s, errors.New("invalid managed Edge cohort identity")
		}
		switch phase {
		case Preparing:
			s.Preparing[n.Name] = true
		case Admitted:
			s.Admitted[n.Name] = true
		default:
			return s, errors.New("unknown managed Edge enrollment phase")
		}
	}
	s.Expected += len(s.Admitted)
	if s.Expected > 100 {
		return s, errors.New("managed Edge cohort exceeds capacity")
	}
	return s, nil
}

func (s Snapshot) FilterPods(pods []corev1.Pod) []corev1.Pod {
	out := make([]corev1.Pod, 0, len(pods))
	for _, p := range pods {
		if !s.Preparing[strings.TrimSpace(p.Spec.NodeName)] {
			out = append(out, p)
		}
	}
	return out
}
