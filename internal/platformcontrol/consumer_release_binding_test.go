package platformcontrol

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func boundPassingConsumer(set model.PlatformExpectedConsumerSet, expected model.PlatformExpectedConsumer, now time.Time) model.PlatformConsumerInstance {
	c := passingConsumer(expected, now)
	c.IdentityVerified, c.CredentialID, c.TokenID = true, "credential", "token"
	c.ExpectedConsumerSetID, c.ReleaseSetID = set.ID, set.ReleaseSetID
	c.FencingToken, c.GenerationSequence, c.Sequence = 4, 7, 1
	c.IssuedAt = &now
	c.Nonce = strings.Repeat("a", 32)
	c.EvidenceHash = "sha256:" + strings.Repeat("b", 64)
	return c
}

func TestReleaseSetConvergenceRequiresAuthoritativeExactReceiptBinding(t *testing.T) {
	now := time.Now().UTC()
	set := mustBuildExpectedConsumerSet(t, ExpectedConsumerSetBuildRequest{ReleaseSetID: "release-set", ArtifactReleaseID: "release", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ScopeKey: "global", Generation: "generation", Topology: ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "node-a"}}}})
	binding := &ConsumerReleaseBinding{ReleaseSetID: set.ReleaseSetID, ArtifactReleaseID: set.ArtifactReleaseID, ArtifactKind: set.ArtifactKind, ScopeKey: set.ScopeKey, Generation: set.ExpectedGeneration, FencingToken: 4, GenerationSequence: 7}
	c := boundPassingConsumer(set, set.Consumers[0], now)
	if status := EvaluateConsumerConvergence(set, []model.PlatformConsumerInstance{c}, now, binding); !status.Pass {
		t.Fatal(status)
	}
	retired := c
	retired.ConsumerID, retired.NodeID, retired.ExpectedConsumerSetID = "edge-worker:retired", "retired", "older-set"
	if status := EvaluateConsumerConvergence(set, []model.PlatformConsumerInstance{c, retired}, now, binding); !status.Pass {
		t.Fatal("historical nonmember receipt blocked current cohort", status)
	}
	retired.ExpectedConsumerSetID = set.ID
	if status := EvaluateConsumerConvergence(set, []model.PlatformConsumerInstance{c, retired}, now, binding); status.Pass {
		t.Fatal("unexpected current-cohort receipt accepted", status)
	}
	if status := EvaluateConsumerConvergence(set, []model.PlatformConsumerInstance{c}, now); status.Pass || status.State != model.InvariantEvidenceStateUnknown {
		t.Fatal("missing authority passed", status)
	}
	for name, mutate := range map[string]func(*model.PlatformConsumerInstance){
		"unverified":              func(c *model.PlatformConsumerInstance) { c.IdentityVerified = false },
		"missing credential":      func(c *model.PlatformConsumerInstance) { c.CredentialID = "" },
		"other expectation":       func(c *model.PlatformConsumerInstance) { c.ExpectedConsumerSetID = "other" },
		"other release set":       func(c *model.PlatformConsumerInstance) { c.ReleaseSetID = "other" },
		"old fence":               func(c *model.PlatformConsumerInstance) { c.FencingToken-- },
		"future fence":            func(c *model.PlatformConsumerInstance) { c.FencingToken++ },
		"other artifact sequence": func(c *model.PlatformConsumerInstance) { c.GenerationSequence++ },
		"missing evidence":        func(c *model.PlatformConsumerInstance) { c.EvidenceHash = "" },
		"missing sequence":        func(c *model.PlatformConsumerInstance) { c.Sequence = 0 },
		"missing issued time":     func(c *model.PlatformConsumerInstance) { c.IssuedAt = nil },
	} {
		t.Run(name, func(t *testing.T) {
			changed := c
			mutate(&changed)
			if status := EvaluateConsumerConvergence(set, []model.PlatformConsumerInstance{changed}, now, binding); status.Pass || status.RequiredPassing != 0 {
				t.Fatal("unbound receipt passed", status)
			}
		})
	}
	other := *binding
	other.ArtifactReleaseID = "other"
	if status := EvaluateConsumerConvergence(set, []model.PlatformConsumerInstance{c}, now, &other); status.Pass {
		t.Fatal("other release context accepted")
	}
}

func TestCanarySubsetNeverPassesCompleteTrafficConvergence(t *testing.T) {
	now := time.Now().UTC()
	set := mustBuildExpectedConsumerSet(t, ExpectedConsumerSetBuildRequest{ReleaseSetID: "set", ArtifactReleaseID: "release", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ScopeKey: "global", Generation: "generation", Topology: ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "node-a", EdgeGroupID: "edge-group-a"}, {ID: "node-b", EdgeGroupID: "edge-group-b"}}}})
	binding := &ConsumerReleaseBinding{ReleaseSetID: set.ReleaseSetID, ArtifactReleaseID: set.ArtifactReleaseID, ArtifactKind: set.ArtifactKind, ScopeKey: set.ScopeKey, Generation: set.ExpectedGeneration, FencingToken: 4, GenerationSequence: 7, ReleaseChannel: "gray", CanaryEdgeGroups: []string{"edge-group-a"}}
	consumers := []model.PlatformConsumerInstance{}
	for _, c := range set.Consumers {
		consumers = append(consumers, boundPassingConsumer(set, c, now))
	}
	status := EvaluateConsumerConvergence(set, consumers, now, binding)
	if status.Pass || status.RequiredExpected != 2 || status.RequiredPassing != 1 {
		t.Fatal("partial canary passed complete topology", status)
	}
	binding.CanaryEdgeGroups = append(binding.CanaryEdgeGroups, "edge-group-b")
	if status := EvaluateConsumerConvergence(set, consumers, now, binding); !status.Pass {
		t.Fatal("complete signed cohort rejected", status)
	}
}
