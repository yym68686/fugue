package api

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/store"
)

func TestTrafficControlReadRequiresActualSelectedInputAndIgnoresColdShadow(t *testing.T) {
	path := t.TempDir() + "/state.json"
	st := store.New(path)
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(st, auth.New(st, ""), nil, ServerConfig{BundleSigningKey: "synthetic-route-input-key", BundleSigningKeyID: "route-input-key"})
	s.auth.EdgeRouteIntentIdentityKeyring = edgeRouteIntentTestKeyring()
	principal := model.Principal{ActorType: "test", ActorID: "test"}
	var revision int64
	publish := func(generation, channel string) platformConfigCompileResponse {
		t.Helper()
		compiled, err := platformconfig.Compile(platformconfig.CompileRequest{
			Intent:    platformconfig.PlatformIntent{Scope: "global", Generation: generation, Routes: []platformconfig.RouteIntent{{Hostname: generation + ".example.test", UpstreamURL: "http://origin:8080", Enabled: true}}},
			Policy:    platformconfig.PolicySnapshot{Scope: "global", Generation: generation + "-policy", TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "first", EdgeGroupIDs: []string{"edge-group-a"}}}},
			CreatedAt: time.Now().UTC(),
		})
		if err != nil {
			t.Fatal(err)
		}
		result, err := s.materializePlatformCompilation(t.Context(), compiled, principal, nil)
		if err != nil {
			t.Fatal(err)
		}
		request := model.PlatformArtifactReleaseRequest{ReleaseChannel: channel, IdempotencyKey: generation}
		if channel == "gray" {
			request.CanaryRuleRef = "cohort=first"
		}
		_, release, _, _, err := st.ReleasePlatformArtifact(result.ReleaseArtifact.ID, request, principal)
		if err != nil {
			t.Fatal(err)
		}
		for _, child := range []model.PlatformArtifact{result.RouteArtifact, result.DNSArtifact, result.TLSArtifact} {
			revision++
			set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{
				ReleaseSetID: result.ReleaseArtifact.ID, ArtifactReleaseID: release.ID, ArtifactKind: child.ArtifactKind,
				ScopeKey: "global", Generation: child.Generation, Revision: revision,
				Topology: platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a"}}, DNSNodes: []model.DNSNode{{ID: "dns-a", PhysicalNodeID: "dns-a", EdgeGroupID: "edge-group-a", Zone: "example.test"}}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = st.CreatePlatformExpectedConsumerSet(set); err != nil {
				t.Fatal(err)
			}
		}
		return result
	}
	serving := publish("serving", "gray")
	shadow := publish("unselected-shadow", "shadow")
	token, err := platformcontrol.IssuePlatformComponentIdentity(edgeRouteIntentTestKeyring(), *edgeRouteIntentTestClaims(model.PlatformConsumerComponentEdgeControl, "global", []string{model.PlatformArtifactKindEdgeRouteIntent}), time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	read := func(want int) {
		t.Helper()
		r := performJSONRequest(t, s, http.MethodGet, "/v1/edge/route-intents?edge_group_id=edge-group-a", token, nil)
		if r.Code != want {
			t.Fatal(r.Code, want, r.Body.String())
		}
		if want == http.StatusOK {
			var out model.EdgeRouteIntentSnapshot
			mustDecodeJSON(t, r, &out)
			if out.TrafficRelease == nil || out.TrafficRelease.ReleaseSetID != serving.ReleaseArtifact.ID || strings.Contains(r.Body.String(), "unselected-shadow.example.test") {
				t.Fatal("serving read selected or disclosed the cold shadow")
			}
		} else if r.Header().Get("ETag") != "" || r.Header().Get("X-Fugue-Route-Intent-Generation") != "" || strings.Contains(r.Body.String(), "serving.example.test") {
			t.Fatal("unavailable original input disclosed a successful route response")
		}
	}
	read(http.StatusOK)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"missing selected", "corrupt selected", "selected size", "missing unselected shadow"} {
		t.Run(scenario, func(t *testing.T) {
			var current model.State
			if err := json.Unmarshal(raw, &current); err != nil {
				t.Fatal(err)
			}
			digest := serving.ReleaseArtifact.Metadata["input_snapshot_digest"]
			want := http.StatusServiceUnavailable
			if scenario == "missing unselected shadow" {
				digest, want = shadow.ReleaseArtifact.Metadata["input_snapshot_digest"], http.StatusOK
			}
			changed := false
			for i := range current.PlatformArtifactContents {
				c := &current.PlatformArtifactContents[i]
				if c.ContentHash != digest {
					continue
				}
				changed = true
				switch scenario {
				case "corrupt selected":
					c.Content = map[string]any{"intent_generation": "corrupt"}
				case "selected size":
					c.SizeBytes++
				default:
					current.PlatformArtifactContents = append(current.PlatformArtifactContents[:i], current.PlatformArtifactContents[i+1:]...)
				}
				break
			}
			if !changed {
				t.Fatal("input fixture did not alter the selected original")
			}
			body, _ := json.Marshal(current)
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			read(want)
			after, err := os.ReadFile(path)
			if err != nil || string(body) != string(after) {
				t.Fatal("read restored an input or mutated business state", err)
			}
		})
	}
}
