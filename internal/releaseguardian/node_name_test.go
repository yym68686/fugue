package releaseguardian

import (
	"strings"
	"testing"
	"time"
)

func TestAuthorityWitnessesUseKubernetesNodeNames(t *testing.T) {
	for _, test := range []struct {
		name  string
		valid bool
	}{
		{name: "edge-node-a", valid: true},
		{name: "7edge", valid: true},
		{name: "12345678-1234-4234-8234-123456789abc", valid: true},
		{name: "edge.region.example", valid: true},
		{name: "edge--a", valid: true},
		{name: strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61), valid: true},
		{name: ""},
		{name: "Edge-node"},
		{name: "edge_node"},
		{name: "-edge"},
		{name: "edge-"},
		{name: "edge."},
		{name: "edge..example"},
		{name: "edge/name"},
		{name: " edge"},
		{name: "edge\n"},
		{name: strings.Repeat("a", 254)},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate, before, now := candidateCanaryFixture(t)
			cohort := candidateWorkerCohortFixture(t, candidate)
			cohort.Instances[0].NodeName = test.name
			witness := AuthorityBaselineNodeWitness{
				NodeName: test.name, FrontPodUID: "front-pod-uid", FrontResourceVersion: "10",
				WorkerPodUID: "worker-pod-uid", WorkerResourceVersion: "11", ActivationGeneration: 1,
				BundleGeneration: "serving.p1.r0", ServingGeneration: "serving.p1.r0",
				WorkerSourceSHA: testSHA, WorkerImageDigest: testDigest,
			}
			before.BaselineReceiptDigest = testDigest
			after := before
			after.CurrentRecordDigest, after.CurrentWorkerSlot, after.AuthorityEpoch = otherDigest, AuthoritySlotB, before.AuthorityEpoch+1
			after.CurrentFrontGeneration, after.CurrentBundleGeneration = witness.ActivationGeneration, witness.BundleGeneration
			after.CurrentWorkerSourceSHA, after.CurrentWorkerImageDigest = witness.WorkerSourceSHA, witness.WorkerImageDigest
			candidate.State, candidate.CanaryResultDigest = CandidateAuthorityVerified, testDigest
			baseline := AuthorityBaselineReceipt{
				GroupID: before.GroupID, BeforeRecordDigest: before.CurrentRecordDigest, BeforeWorkerSlot: before.CurrentWorkerSlot,
				BeforeAuthorityEpoch: before.AuthorityEpoch, RecordDigest: after.CurrentRecordDigest,
				WorkerSlot: after.CurrentWorkerSlot, AuthorityEpoch: after.AuthorityEpoch,
				Nodes: []AuthorityBaselineNodeWitness{witness}, ObservedAt: now.Format(time.RFC3339Nano),
			}
			normalization := AuthorityNormalizationReceipt{
				GroupID: before.GroupID, BaselineReceiptDigest: testDigest, Before: before, After: after,
				Nodes: []AuthorityBaselineNodeWitness{witness}, ObservedAt: baseline.ObservedAt,
			}
			journal := AuthorityTransitionJournal{
				GroupID: before.GroupID, Phase: AuthorityTransitionPrepared, CurrentUID: "authority-uid", CurrentRV: "12",
				Before: before, Candidate: candidate, CanaryResultDigest: candidate.CanaryResultDigest,
				PreviousNodes: []AuthorityBaselineNodeWitness{witness}, CreatedAt: baseline.ObservedAt,
			}
			for kind, verify := range map[string]func() error{
				"cohort": func() error {
					value, err := cohort.Seal()
					if err != nil {
						return err
					}
					return value.Validate()
				},
				"baseline": func() error {
					value, err := baseline.Seal()
					if err != nil {
						return err
					}
					return value.Validate()
				},
				"normalization": func() error {
					value, err := normalization.Seal()
					if err != nil {
						return err
					}
					return value.Validate()
				},
				"journal": func() error {
					value, err := journal.Seal()
					if err != nil {
						return err
					}
					return value.Validate()
				},
			} {
				t.Run(kind, func(t *testing.T) {
					if err := verify(); (err == nil) != test.valid {
						t.Fatalf("node %q: valid=%v, error=%v", test.name, test.valid, err)
					}
				})
			}
		})
	}
}

func TestCandidateCohortStillBindsUniqueNodeAndPodIdentity(t *testing.T) {
	candidate, _, _ := candidateCanaryFixture(t)
	for _, member := range []CandidateWorkerInstance{
		{NodeName: "edge-node-a", PodUID: "another-pod-uid"},
		{NodeName: "7edge", PodUID: "pod-uid-candidate-a"},
		{NodeName: "7edge", PodUID: ""},
		{NodeName: "7edge", PodUID: "unsafe\npod-uid"},
	} {
		cohort := candidateWorkerCohortFixture(t, candidate)
		cohort.Instances = append(cohort.Instances, member)
		if _, err := cohort.Seal(); err == nil {
			t.Fatalf("accepted duplicate or invalid identity: %+v", member)
		}
	}
}
