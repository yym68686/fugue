package store

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
)

func TestProducedExpectationsRequireCompleteCurrentPhaseAndPreserveMembership(t *testing.T) {
	f := newServingFixture(t, "")
	parent := f.candidate(t)
	_, source, _, _, err := f.s.ReleaseProducedPlatformArtifact(parent.ID, f.authority.ID, "", producerTestPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	prepareProducedPublication(t, f.s, parent, source, false, false)
	var raw []byte
	if err := f.s.withLockedState(false, func(st *model.State) error { raw, err = json.Marshal(st); return err }); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"complete", "missing-kind", "foreign-generation", "failed-source", "frozen-lane"} {
		t.Run(scenario, func(t *testing.T) {
			var state model.State
			if err := json.Unmarshal(raw, &state); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "missing-kind":
				state.ExpectedConsumerSets = state.ExpectedConsumerSets[1:]
			case "foreign-generation":
				state.ExpectedConsumerSets[0].ExpectedGeneration = "foreign"
			case "failed-source":
				state.PlatformArtifactReleases[platformArtifactReleaseIndex(state.PlatformArtifactReleases, source.ID)].VerificationState = model.PlatformArtifactVerificationStateFailed
			case "frozen-lane":
				for i := range state.PlatformReleaseLanes {
					if state.PlatformReleaseLanes[i].ActiveReleaseID == source.ID {
						state.PlatformReleaseLanes[i].Frozen = true
					}
				}
			}
			now := time.Now().UTC()
			next := source
			next.ID = "next-gray"
			next.ReleaseChannel = "gray"
			sets, err := producedReleaseExpectations(&state, parent, next, &platformProducerReleaseGuard{Phase: "gray"}, now)
			if scenario != "complete" {
				if err == nil {
					t.Fatal("invalid prior phase admitted")
				}
				return
			}
			if err != nil || len(sets) != 3 {
				t.Fatal(len(sets), err)
			}
			for _, set := range sets {
				if set.ArtifactReleaseID != next.ID || !set.CreatedAt.Equal(now) {
					t.Fatal("new publication binding missing")
				}
				old := model.PlatformExpectedConsumerSet{}
				for _, prior := range state.ExpectedConsumerSets {
					if prior.ArtifactKind == set.ArtifactKind && prior.ArtifactReleaseID == source.ID {
						old = prior
					}
				}
				if old.ID == set.ID || old.TopologyRevision != set.TopologyRevision || old.RequiredCardinality != set.RequiredCardinality || len(old.Consumers) != len(set.Consumers) {
					t.Fatal("membership changed")
				}
				for i, fact := range set.Consumers {
					if fact.ConsumerID != old.Consumers[i].ConsumerID || fact.ConvergenceDeadline.Sub(set.CreatedAt) != old.Consumers[i].ConvergenceDeadline.Sub(old.CreatedAt) {
						t.Fatal("deadline or owner changed")
					}
				}
				if err := platformcontrol.ValidateDeclaredTrafficConsumerSet(parent, set); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
