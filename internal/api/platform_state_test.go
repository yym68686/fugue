package api

import (
	"net/http"
	"testing"

	"fugue/internal/model"
)

func TestPlatformArtifactAPIReleaseConsumerAndFailureContracts(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")

	create := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", platformAdminKey, model.PlatformArtifactCreateRequest{
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   "rank_api_gen_1",
		Content:      map[string]any{"weights": map[string]any{"ttfb": 1, "error_rate": 2}},
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("expected create status %d, got %d body=%s", http.StatusCreated, create.Code, create.Body.String())
	}
	var created model.PlatformArtifactResponse
	mustDecodeJSON(t, create, &created)
	if created.Artifact.ContentHash == "" || created.Artifact.ScopeKey != "global" {
		t.Fatalf("unexpected created artifact: %+v", created.Artifact)
	}

	validate := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/validate", platformAdminKey, map[string]any{"dry_run": false})
	if validate.Code != http.StatusOK {
		t.Fatalf("expected validate status %d, got %d body=%s", http.StatusOK, validate.Code, validate.Body.String())
	}
	var validation model.PlatformArtifactValidationResponse
	mustDecodeJSON(t, validate, &validation)
	if !validation.Pass || validation.Artifact.Status != model.PlatformArtifactStatusValidated {
		t.Fatalf("expected persisted validation pass, got %+v", validation)
	}

	release := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/release", platformAdminKey, model.PlatformArtifactReleaseRequest{ReleaseChannel: model.PlatformArtifactReleaseChannelFull})
	if release.Code != http.StatusOK {
		t.Fatalf("expected release status %d, got %d body=%s", http.StatusOK, release.Code, release.Body.String())
	}
	var released model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, release, &released)
	if released.Release.Generation != created.Artifact.Generation || released.LKG == nil {
		t.Fatalf("expected full release and LKG, got %+v", released)
	}

	pull := performJSONRequest(t, server, http.MethodGet, "/v1/platform-state/artifacts/edge_ranking_policy?scope_key=global", platformAdminKey, nil)
	if pull.Code != http.StatusOK {
		t.Fatalf("expected pull status %d, got %d body=%s", http.StatusOK, pull.Code, pull.Body.String())
	}
	var pulled model.PlatformStateArtifactResponse
	mustDecodeJSON(t, pull, &pulled)
	if pulled.Artifact == nil || pulled.Artifact.Generation != created.Artifact.Generation || pulled.LKG == nil {
		t.Fatalf("expected active artifact and LKG in pull response, got %+v", pulled)
	}

	heartbeat := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/heartbeat", platformAdminKey, model.PlatformConsumerHeartbeatRequest{
		ConsumerID:        "edge-worker-api-test",
		Component:         "edge-worker",
		ArtifactKind:      model.PlatformArtifactKindEdgeRankingPolicy,
		ScopeKey:          "global",
		DesiredGeneration: created.Artifact.Generation,
		ActualGeneration:  created.Artifact.Generation,
		ApplyStatus:       "applied",
		ProbeStatus:       "passed",
	})
	if heartbeat.Code != http.StatusOK {
		t.Fatalf("expected heartbeat status %d, got %d body=%s", http.StatusOK, heartbeat.Code, heartbeat.Body.String())
	}
	consumers := performJSONRequest(t, server, http.MethodGet, "/v1/admin/artifacts/"+created.Artifact.ID+"/consumers", platformAdminKey, nil)
	if consumers.Code != http.StatusOK {
		t.Fatalf("expected consumers status %d, got %d body=%s", http.StatusOK, consumers.Code, consumers.Body.String())
	}
	var consumerResponse model.PlatformArtifactConsumersResponse
	mustDecodeJSON(t, consumers, &consumerResponse)
	if len(consumerResponse.Consumers) != 1 || consumerResponse.Consumers[0].ConsumerID != "edge-worker-api-test" {
		t.Fatalf("expected heartbeat consumer, got %+v", consumerResponse.Consumers)
	}

	contracts := performJSONRequest(t, server, http.MethodGet, "/v1/admin/failure-contracts", platformAdminKey, nil)
	if contracts.Code != http.StatusOK {
		t.Fatalf("expected contracts status %d, got %d body=%s", http.StatusOK, contracts.Code, contracts.Body.String())
	}
	var contractList model.SubsystemFailureContractListResponse
	mustDecodeJSON(t, contracts, &contractList)
	if len(contractList.Contracts) < 16 {
		t.Fatalf("expected critical subsystem contracts, got %d", len(contractList.Contracts))
	}
}
