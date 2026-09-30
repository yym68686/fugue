package api

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
	"fugue/internal/testfixture/celldns"
)

func TestDNSAuthorityTransitionAPIRequiresCurrentTrustedCompletePreviousRelease(t *testing.T) {
	for _, scenario := range []string{"complete", "forged previous DNS", "previous unverified", "previous full changed", "previous gray selected", "previous fence changed", "previous frozen", "retained DNS absent"} {
		t.Run(scenario, func(t *testing.T) {
			_, s, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
			req := celldns.TransitionRequest(t)
			state := model.State{}
			seedArtifacts := func(values ...*model.PlatformArtifact) {
				for _, a := range values {
					var err error
					*a, err = platformsafety.SignPlatformArtifact(*a, s.bundleKeyring())
					if err != nil {
						t.Fatal(err)
					}
					state.PlatformArtifacts = append(state.PlatformArtifacts, *a)
				}
			}
			seedRelease := func(r model.PlatformArtifactRelease) {
				state.PlatformArtifactReleases = append(state.PlatformArtifactReleases, r)
				state.PlatformReleaseLanes = append(state.PlatformReleaseLanes, model.PlatformReleaseLane{LaneKey: r.LaneKey, ArtifactKind: r.ArtifactKind, ScopeKey: r.ScopeKey, ReleaseChannel: r.ReleaseChannel, ActiveReleaseID: r.ID, FencingToken: r.FencingToken, Version: 1, UpdatedAt: r.ReleasedAt})
			}
			for i := range req.CellRoutePublications {
				p := &req.CellRoutePublications[i]
				seedArtifacts(&p.Parent, &p.Route, &p.TLS)
				seedRelease(celldns.Publication(*p))
			}
			previous := req.PreviousTrafficPublication
			seedArtifacts(&previous.Parent, &previous.Route, &previous.TLS, &previous.DNS)
			seedRelease(celldns.PreviousPublication(*previous))
			last := len(state.PlatformArtifactReleases) - 1
			switch scenario {
			case "forged previous DNS":
				previous.DNS.Provenance.Signature = "forged"
			case "previous unverified":
				state.PlatformArtifactReleases[last].VerificationState = model.PlatformArtifactVerificationStateServingUnverified
			case "previous full changed":
				state.PlatformArtifactReleases[last].ID = "new-full"
				state.PlatformReleaseLanes[last].ActiveReleaseID = "new-full"
			case "previous fence changed":
				state.PlatformReleaseLanes[last].FencingToken++
			case "previous frozen":
				state.PlatformReleaseLanes[last].Frozen = true
			case "retained DNS absent":
				state.PlatformArtifacts = state.PlatformArtifacts[:len(state.PlatformArtifacts)-1]
			case "previous gray selected":
				r := state.PlatformArtifactReleases[last]
				r.ID = "new-gray"
				r.ReleaseChannel = "gray"
				r.CanaryRuleRef = "cohort=complete"
				r.LaneKey = platformsafety.ReleaseLaneKey(r.ArtifactKind, r.ScopeKey, r.ReleaseChannel)
				r.ReleasedAt = r.ReleasedAt.Add(time.Second)
				seedRelease(r)
			}
			originalCount := len(state.PlatformArtifacts)
			path := t.TempDir() + "/state.json"
			raw, _ := json.Marshal(state)
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			s.store = store.New(path)
			s.store.ConfigurePlatformArtifactSigning(s.bundleKeyring())
			request := platformConfigCompileRequest{PreviousTrafficPublication: req.PreviousTrafficPublication, CellRoutePublications: req.CellRoutePublications, Intent: req.Intent, Policy: req.Policy, RuntimeSnapshot: req.RuntimeSnapshot}
			response := performJSONRequest(t, s, http.MethodPost, "/v1/admin/platform-config/compile", admin, request)
			if scenario != "complete" {
				if response.Code != http.StatusConflict {
					t.Fatalf("invalid previous authority response %d", response.Code)
				}
				artifacts, err := s.store.ListPlatformArtifacts(model.PlatformArtifactFilter{Limit: 100})
				if err != nil || len(artifacts) != originalCount {
					t.Fatal("rejected transition wrote artifacts", err)
				}
				return
			}
			if response.Code != http.StatusCreated {
				t.Fatalf("compile failed %d: %s", response.Code, response.Body.String())
			}
			var result platformConfigCompileResponse
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if _, err := decodePlatformDNSArtifact(result.DNSArtifact); err != nil {
				t.Fatal(err)
			}
			replay := performJSONRequest(t, s, http.MethodPost, "/v1/admin/platform-config/compile-from-artifacts", admin, platformConfigCompileArtifactsRequest{PreviousTrafficPublication: req.PreviousTrafficPublication, CellRoutePublications: req.CellRoutePublications, IntentArtifactID: result.IntentArtifact.ID, PolicyArtifactID: result.PolicyArtifact.ID, RuntimeSnapshot: req.RuntimeSnapshot})
			if replay.Code != http.StatusCreated {
				t.Fatalf("replay failed %d: %s", replay.Code, replay.Body.String())
			}
			var again platformConfigCompileResponse
			json.Unmarshal(replay.Body.Bytes(), &again)
			if again.DNSArtifact.ID != result.DNSArtifact.ID || again.ReleaseArtifact.ID != result.ReleaseArtifact.ID {
				t.Fatal("immutable transition replay changed")
			}
		})
	}
}
