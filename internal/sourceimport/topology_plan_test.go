package sourceimport

import "testing"

func TestAnalyzeNormalizedTopologyIgnoresComposeServiceSelectorEnvBindings(t *testing.T) {
	t.Parallel()

	plan, err := AnalyzeNormalizedTopology(NormalizedTopology{
		Services: []ComposeService{
			{
				Name:        "gateway",
				Kind:        ComposeServiceKindApp,
				ServiceType: ServiceTypeApp,
				Environment: map[string]string{
					"ARGUS_FUGUE_RUNTIME_COMPOSE_SERVICE": "runtime",
				},
			},
			{
				Name:        "runtime",
				Kind:        ComposeServiceKindApp,
				ServiceType: ServiceTypeApp,
				Environment: map[string]string{
					"ARGUS_CODEX_MCP_URL": "http://gateway:8080/mcp",
				},
			},
		},
	}, "gateway")
	if err != nil {
		t.Fatalf("analyze normalized topology: %v", err)
	}

	if got := plan.BindingsBySource["gateway"]; len(got) != 0 {
		t.Fatalf("expected compose service selector env to be ignored, got %+v", got)
	}
	got := plan.BindingsBySource["runtime"]
	if len(got) != 1 || got[0].Service != "gateway" || got[0].Source != BindingSourceEnv {
		t.Fatalf("expected runtime env binding to gateway, got %+v", got)
	}
}
