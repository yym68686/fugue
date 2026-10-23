package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"fugue/internal/model/edgecohort"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCandidateCanaryHonorsExplicitHoldWithoutClaimingNodeRecovery(t *testing.T) {
	for _, scenario := range []string{"held", "hold-absent", "node-replaced", "all-held", "held-terminating"} {
		t.Run(scenario, func(t *testing.T) {
			candidate := releaseguardian.CandidateAuthority{GroupID: "edge-pool-a", WorkerSlot: releaseguardian.AuthoritySlotB, BundleGeneration: "candidate-generation-1"}
			image := "ghcr.io/example/worker@sha256:" + strings.Repeat("c", 64)
			good := candidateWorkerPod("worker-good", "node-good", "pod-good", "b", strings.Repeat("d", 40), image)
			old := candidateWorkerPod("worker-held", "node-held", "pod-held", "b", strings.Repeat("e", 40), image)
			old.Status.ContainerStatuses[0].Ready = false
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-held", UID: "node-uid", ResourceVersion: "20", Labels: map[string]string{"fugue.io/role.edge": "true", "fugue.io/edge-group-id": candidate.GroupID}}}
			raw, _ := json.Marshal(edgecohort.ReleaseHold{Schema: edgecohort.ReleaseHoldSchema, NodeUID: string(node.UID), EdgeGroupID: candidate.GroupID, Reason: "quota_exhausted"})
			node.Annotations = map[string]string{edgecohort.ReleaseHoldAnnotation: string(raw)}
			switch scenario {
			case "hold-absent":
				node.Annotations = nil
			case "node-replaced":
				node.UID = "replacement"
			case "all-held":
				good.Spec.NodeName = node.Name
			case "held-terminating":
				stamp := metav1.Now()
				old.DeletionTimestamp = &stamp
			}
			client := fake.NewSimpleClientset(good, old, node)
			cohort, err := observeCandidateWorkerCohort(context.Background(), client, "fugue-system", candidate)
			if scenario == "held" || scenario == "held-terminating" {
				if err != nil || len(cohort.Instances) != 1 || cohort.Instances[0].PodUID != "pod-good" {
					t.Fatal("held process still blocks the healthy canary", cohort, err)
				}
			} else if err == nil {
				t.Fatal("candidate ignored a required or unidentified member")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatal("observation mutated the retained node")
				}
			}
		})
	}
}
