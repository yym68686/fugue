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

func TestACMEProjectionPreservesExpiredDesiredInput(t *testing.T) {
	now := time.Now().UTC()
	challenges := []model.DNSACMEChallenge{{ID: "challenge", Zone: "example", Name: "_acme-challenge.example", Value: "token", TTL: 60, ExpiresAt: now.Add(time.Minute)}}
	first := platformIntentProjectionResponse{CapturedAt: now}
	if err := projectACMEChallengeIntents(&first, challenges); err != nil {
		t.Fatal(err)
	}
	second := platformIntentProjectionResponse{CapturedAt: now.Add(time.Hour)}
	if err := projectACMEChallengeIntents(&second, challenges); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Intent, second.Intent) || len(second.Intent.ACMEChallenges) != 1 || first.RuntimeSnapshot.IntentGeneration != first.Intent.Generation {
		t.Fatal("time changed desired challenge configuration")
	}
}

func TestACMEArtifactsCannotEnterTrafficBeforeConsumerSupport(t *testing.T) {
	_, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	now := time.Now().UTC()
	response := performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, platformConfigCompileRequest{Intent: platformconfig.PlatformIntent{Generation: "acme-test", ACMEChallenges: []platformconfig.ACMEChallengeIntent{{ID: "challenge", Zone: "example", Hostname: "_acme-challenge.example", Value: "token", TTL: 60, ExpiresAt: now.Add(time.Hour)}}}, Policy: platformconfig.PolicySnapshot{Generation: "policy"}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now}})
	if response.Code != http.StatusCreated {
		t.Fatal(response.Body.String())
	}
	var compiled platformConfigCompileResponse
	mustDecodeJSON(t, response, &compiled)
	for _, id := range []string{compiled.DNSArtifact.ID, compiled.ReleaseArtifact.ID} {
		for _, channel := range []string{"gray", "full"} {
			r := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+id+"/release", admin, model.PlatformArtifactReleaseRequest{ReleaseChannel: channel})
			if r.Code != http.StatusConflict || !strings.Contains(r.Body.String(), "DNS value expiration requires consumer support") {
				t.Fatalf("leased DNS release accepted: %d %s", r.Code, r.Body.String())
			}
		}
	}
	response = performJSONRequest(t, server, http.MethodPost, "/v1/admin/platform-config/compile", admin, platformConfigCompileRequest{Intent: platformconfig.PlatformIntent{Generation: "later-empty"}, Policy: platformconfig.PolicySnapshot{Generation: "later-policy"}})
	if response.Code != http.StatusCreated {
		t.Fatal(response.Body.String())
	}
	var later platformConfigCompileResponse
	mustDecodeJSON(t, response, &later)
	for _, pair := range []struct{ id, generation string }{{later.DNSArtifact.ID, compiled.DNSArtifact.Generation}, {later.ReleaseArtifact.ID, compiled.ReleaseArtifact.Generation}} {
		r := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+pair.id+"/rollback", admin, model.PlatformArtifactRollbackRequest{ReleaseChannel: "full", ToGeneration: pair.generation, Reason: "compatibility test"})
		if r.Code != http.StatusConflict || !strings.Contains(r.Body.String(), "DNS value expiration requires consumer support") {
			t.Fatalf("leased rollback accepted: %d %s", r.Code, r.Body.String())
		}
	}
}
