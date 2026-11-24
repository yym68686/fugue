package store

import (
	"errors"
	"maps"
	"testing"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

func TestHostedDeclarationHistoryRequiresExactSignedVerifiedFull(t *testing.T) {
	req, _ := celldns.TransitionRequestWithSource(t)
	source := req.PreviousTrafficPublication
	ref := source.Reference
	verified := time.Now().UTC()
	plan := pc.HostedDeclarationRestorePlan{
		History: ref,
		Target:  "target.example.test", Family: "A",
		Held: pc.HostedAddressHold{EdgeID: "worker", EdgeGroupID: "edge-group-one", Address: "1.1.1.1"},
	}
	exact := model.PlatformArtifactRelease{ID: ref.ReleaseID, ArtifactID: ref.ReleaseSetID, ArtifactKind: model.PlatformArtifactKindReleaseSet,
		ScopeKey: pc.GlobalScopeKey, Generation: source.Parent.Generation, VerifiedLKGGeneration: source.Parent.Generation,
		FencingToken: ref.FencingToken, ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		Status: model.PlatformArtifactReleaseStatusSuperseded, VerificationState: model.PlatformArtifactVerificationStateVerified, VerifiedAt: &verified}
	for _, scenario := range []string{"valid", "wrong channel", "unverified", "different fence", "bypassed invariant", "foreign signature", "revoked TLS", "not selected"} {
		t.Run(scenario, func(t *testing.T) {
			ledger := exact
			all := map[string]model.PlatformArtifact{ref.ReleaseSetID: source.Parent, ref.DNSArtifactID: source.DNS,
				ref.RouteArtifactID: source.Route, ref.TLSArtifactID: source.TLS}
			switch scenario {
			case "wrong channel":
				ledger.ReleaseChannel = model.PlatformArtifactReleaseChannelGray
			case "unverified":
				ledger.VerifiedAt = nil
			case "different fence":
				ledger.FencingToken++
			case "bypassed invariant":
				ledger.BypassedInvariants = []string{"some_guard"}
			case "foreign signature":
				a := all[ref.DNSArtifactID]
				a.Metadata = maps.Clone(a.Metadata)
				a.Metadata["wrong-owner"] = "changed"
				all[ref.DNSArtifactID] = a
			case "revoked TLS":
				a := all[ref.TLSArtifactID]
				a.Status = model.PlatformArtifactStatusRejected
				all[ref.TLSArtifactID] = a
			case "not selected":
				ledger.ArtifactID = "other-full"
			}
			r := hostedDeclarationHistoryReader{
				release: func(string) (model.PlatformArtifactRelease, error) { return ledger, nil },
				artifact: func(id string) (model.PlatformArtifact, error) {
					a, ok := all[id]
					if !ok {
						return a, ErrNotFound
					}
					return a, nil
				},
				keys: celldns.Keys(),
			}
			_, err := r.read(plan)
			if scenario == "valid" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrConflict) {
				t.Fatalf("history substitution admitted: %v", err)
			}
		})
	}
}
