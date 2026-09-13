package api

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
)

func TestArtifactCompilerRejectsUntrustedInputsBeforeWriting(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*model.PlatformArtifact)
		resign bool
	}{
		{name: "modified signed content", change: func(a *model.PlatformArtifact) {
			a.Content["routes"] = []any{map[string]any{"hostname": "other.example", "upstream_url": "http://other", "enabled": true}}
		}},
		{name: "invalid signature", change: func(a *model.PlatformArtifact) { a.Provenance.Signature = "untrusted" }},
		{name: "wrong kind", change: func(a *model.PlatformArtifact) { a.ArtifactKind = model.PlatformArtifactKindPolicySnapshot }, resign: true},
		{name: "unknown typed field", change: func(a *model.PlatformArtifact) { a.Content["unsupported_policy"] = true }, resign: true},
		{name: "scope mismatch", change: func(a *model.PlatformArtifact) { a.Scope.Key = "other" }, resign: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, server, _, admin, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
			compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "intent-integrity"}, Policy: platformconfig.PolicySnapshot{Generation: "policy-integrity"}})
			if err != nil {
				t.Fatal(err)
			}
			if test.resign {
				test.change(&compiled.IntentArtifact)
			}
			inputs := []*model.PlatformArtifact{&compiled.IntentArtifact, &compiled.PolicyArtifact}
			for _, input := range inputs {
				created, err := server.store.CreatePlatformArtifact(*input)
				if err != nil {
					t.Fatal(err)
				}
				validated, err := server.store.ValidatePlatformArtifact(created.ID, []model.PlatformArtifactValidationResult{{Name: "fixture", Pass: true}})
				if err != nil {
					t.Fatal(err)
				}
				*input = validated
			}
			if !test.resign {
				test.change(&compiled.IntentArtifact)
			}
			state := model.State{PlatformArtifacts: []model.PlatformArtifact{compiled.IntentArtifact, compiled.PolicyArtifact}}
			raw, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "state.json")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			server.store = store.New(path)
			server.store.ConfigurePlatformArtifactSigning(server.bundleKeyring())
			response := performJSONRequest(t, server, http.MethodPost, "http://localhost/v1/admin/platform-config/compile-from-artifacts", admin, map[string]any{"intent_artifact_id": compiled.IntentArtifact.ID, "policy_artifact_id": compiled.PolicyArtifact.ID})
			if response.Code != http.StatusConflict {
				t.Fatalf("untrusted input accepted: %d %s", response.Code, response.Body.String())
			}
			artifacts, err := server.store.ListPlatformArtifacts(model.PlatformArtifactFilter{Limit: 100})
			if err != nil || len(artifacts) != 2 {
				t.Fatalf("input rejection wrote output: %d %v", len(artifacts), err)
			}
		})
	}
}
