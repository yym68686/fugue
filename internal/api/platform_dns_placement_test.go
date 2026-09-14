package api

import (
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestDNSPlacementCompilationPreservesReleaseAndRejectsUnreadyEvidence(t *testing.T) {
	state, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	now := time.Now().UTC()
	intent := platformconfig.PlatformIntent{Generation: "placement-intent", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", AppID: "app-a", TenantID: "tenant-a", Enabled: true, UpstreamURL: "http://origin:8080"}}, DNS: []platformconfig.DNSIntent{{Hostname: "app.example.test", Type: "FUGUE_APP", AppID: "app-a", TenantID: "tenant-a", Values: []string{"app-a"}, TTL: 60, Application: &platformconfig.DNSApplicationIntent{IPv4Policy: "auto", IPv6Policy: "auto", TTLPolicy: "record", FallbackPolicy: "fail_closed"}}}}
	policy := platformconfig.PolicySnapshot{Generation: "placement-policy", MaxStaleSeconds: 60}
	digest, err := platformconfig.DNSPlacementInputDigest(intent.DNS[0], []platformconfig.CompiledRoute{{RouteIntent: intent.Routes[0]}}, policy)
	if err != nil {
		t.Fatal(err)
	}
	request := platformConfigCompileRequest{Intent: intent, Policy: policy, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, DNSPlacements: []platformconfig.DNSPlacementObservation{{InputDigest: digest, CheckedAt: now, Status: "resolved", TargetTTL: 60, Candidates: []platformconfig.DNSPlacementCandidate{{EdgeID: "edge-a", EdgeGroupID: "group-a", ServingGeneration: "loaded-a", ObservedAt: now.Add(-time.Second), ValidUntil: now.Add(30 * time.Second), Healthy: true, RouteReady: true, TLSReady: true, A: []string{"93.184.216.34"}}}}}}}
	var first platformConfigCompileResponse
	for i := range 2 {
		response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, request)
		if response.Code != http.StatusCreated {
			t.Fatal(response.Code, response.Body.String())
		}
		var compiled platformConfigCompileResponse
		mustDecodeJSON(t, response, &compiled)
		if i == 0 {
			first = compiled
		} else if compiled.DNSArtifact.ID != first.DNSArtifact.ID || compiled.DNSArtifact.ContentHash != first.DNSArtifact.ContentHash {
			t.Fatal("identical snapshot did not replay")
		}
	}
	for _, artifact := range []model.PlatformArtifact{first.DNSArtifact, first.ReleaseArtifact} {
		for _, channel := range []string{"gray", "full"} {
			response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+artifact.ID+"/release", admin, model.PlatformArtifactReleaseRequest{ReleaseChannel: channel})
			if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "DNS value expiration requires consumer support") {
				t.Fatal("leased address promotion escaped rollout gate", response.Code, response.Body.String())
			}
		}
		response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+artifact.ID+"/rollback", admin, model.PlatformArtifactRollbackRequest{ReleaseChannel: "full", ToGeneration: artifact.Generation, Reason: "address compatibility regression"})
		if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "DNS value expiration requires consumer support") {
			t.Fatal("leased address rollback escaped rollout gate", response.Code, response.Body.String())
		}
	}
	before, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil {
		t.Fatal(err)
	}
	request.RuntimeSnapshot.DNSPlacements[0].Candidates[0].TLSReady = false
	response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, request)
	if response.Code != http.StatusBadRequest {
		t.Fatal("TLS failure generated artifacts", response.Code, response.Body.String())
	}
	after, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("rejected compilation modified artifact storage", err)
	}
	for _, kind := range []string{model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindReleaseSet, model.PlatformArtifactKindPolicySnapshot} {
		lkg, err := state.GetPlatformLKG(kind, "global")
		if err != nil || lkg != nil {
			t.Fatal("compilation or rejected promotion manufactured an LKG", err)
		}
	}
}
