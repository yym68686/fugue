package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

func TestDNSProducerDraftHTTPRecognizesOnlyTypedFence(t *testing.T) {
	for _, scenario := range []string{"valid", "string", "fraction", "zero", "overflow", "unknown ref", "unknown field", "label secret", "foreign scope", "foreign kind", "extra token"} {
		t.Run(scenario, func(t *testing.T) {
			_, _, p, _ := dnsProducerServer(t)
			_, s, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
			raw, _ := json.Marshal(p)
			content := map[string]any{}
			json.Unmarshal(raw, &content)
			source := content["dns_source"].(map[string]any)
			pin := source["producer_policy"].(map[string]any)
			scope, kind := "platform-config-producer:cell-dns", model.PlatformArtifactKindPolicySnapshot
			switch scenario {
			case "string":
				pin["fencing_token"] = "1"
			case "fraction":
				pin["fencing_token"] = 1.5
			case "zero":
				pin["fencing_token"] = 0
			case "overflow":
				pin["fencing_token"] = float64(9007199254740992)
			case "unknown ref":
				pin["credential"] = "value"
			case "unknown field":
				content["extra"] = true
			case "label secret":
				source["previous_topology"].(map[string]any)["edges"].([]any)[0].(map[string]any)["labels"] = map[string]any{"access_token": "private"}
			case "foreign scope":
				scope = "global"
			case "foreign kind":
				kind = model.PlatformArtifactKindPlatformIntent
			case "extra token":
				content["token"] = "private"
			}
			created := performJSONRequest(t, s, http.MethodPost, "/v1/admin/artifacts", admin, model.PlatformArtifactCreateRequest{ArtifactKind: kind, Scope: model.PlatformArtifactScope{Key: scope}, Generation: p.Generation, Content: content})
			if created.Code != http.StatusCreated {
				t.Fatalf("create %d %s", created.Code, created.Body.String())
			}
			var draft model.PlatformArtifactResponse
			mustDecodeJSON(t, created, &draft)
			before, _ := json.Marshal(draft.Artifact.Content)
			result := performJSONRequest(t, s, http.MethodPost, "/v1/admin/artifacts/"+draft.Artifact.ID+"/validate", admin, model.PlatformArtifactValidateRequest{DryRun: false})
			var value model.PlatformArtifactValidationResponse
			if result.Code == http.StatusOK {
				mustDecodeJSON(t, result, &value)
			}
			if value.Pass != (scenario == "valid") {
				t.Fatalf("unexpected validation: %d %s", result.Code, result.Body.String())
			}
			stored, err := s.store.GetPlatformArtifact(draft.Artifact.ID)
			if err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(stored.Content)
			if string(before) != string(after) {
				t.Fatal("secret scan mutated signed input")
			}
			if scenario == "valid" {
				decoded, err := platformproducer.Decode(stored)
				if err != nil || decoded.DNSSource.ProducerPolicy.FencingToken != p.DNSSource.ProducerPolicy.FencingToken {
					t.Fatal("publication fence was erased", err)
				}
			}
		})
	}
}
