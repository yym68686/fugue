package api

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestAdminCompilerInputRequiresCompleteOriginalClosureWithoutActivation(t *testing.T) {
	s, _, _, path := retainedTrafficAPIFixture(t)
	// Bootstrap administrative authority avoids the ordinary API-key
	// LastUsedAt audit write when asserting byte-for-byte read-only behavior.
	key := "synthetic-input-read-admin"
	s.auth.BootstrapAdminKey = key
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{
		Intent:          platformconfig.PlatformIntent{Scope: "global", Generation: "unpublished-input"},
		Policy:          platformconfig.PolicySnapshot{Scope: "global", Generation: "unpublished-policy"},
		RuntimeSnapshot: platformconfig.RuntimeSnapshot{Facts: map[string]any{"source": "frozen-private-input"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.materializePlatformCompilation(t.Context(), compiled, platformProducerPrincipal(), nil)
	if err != nil {
		t.Fatal(err)
	}
	read := func(view string, status int) {
		t.Helper()
		response := performJSONRequest(t, s, http.MethodGet, "/v1/admin/artifacts/"+result.ReleaseArtifact.ID+"/compiler-input?view="+view, key, nil)
		if response.Code != status {
			t.Fatal(response.Code, status, response.Body.String())
		}
		if status == http.StatusOK && !strings.Contains(response.Body.String(), result.Lineage.InputSnapshotDigest) {
			t.Fatal("admin historical read lost original digest")
		}
		if status != http.StatusOK && (response.Header().Get("ETag") != "" || strings.Contains(response.Body.String(), "runtime_snapshot") || strings.Contains(response.Body.String(), "frozen-private-input")) {
			t.Fatal("unproved original disclosed snapshot/projection")
		}
	}
	read("full", http.StatusOK) // No release/activation exists for this input.
	read("dns_configuration", http.StatusOK)
	baseline, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"missing input", "corrupt input", "input size", "missing sibling", "unrelated LKG missing"} {
		t.Run(scenario, func(t *testing.T) {
			var state model.State
			if err := json.Unmarshal(baseline, &state); err != nil {
				t.Fatal(err)
			}
			status := http.StatusServiceUnavailable
			for i := range state.PlatformArtifactContents {
				if state.PlatformArtifactContents[i].ContentHash != result.Lineage.InputSnapshotDigest {
					continue
				}
				switch scenario {
				case "missing input":
					state.PlatformArtifactContents = append(state.PlatformArtifactContents[:i], state.PlatformArtifactContents[i+1:]...)
				case "corrupt input":
					state.PlatformArtifactContents[i].Content = map[string]any{"intent_generation": "wrong"}
				case "input size":
					state.PlatformArtifactContents[i].SizeBytes++
				}
				break
			}
			if scenario == "missing sibling" {
				for i, a := range state.PlatformArtifacts {
					if a.ID == result.TLSArtifact.ID {
						state.PlatformArtifacts = append(state.PlatformArtifacts[:i], state.PlatformArtifacts[i+1:]...)
						break
					}
				}
			}
			if scenario == "unrelated LKG missing" {
				state.PlatformLKGSnapshots, status = nil, http.StatusOK
			}
			body, _ := json.Marshal(state)
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			read("full", status)
			read("dns_configuration", status)
			after, err := os.ReadFile(path)
			if err != nil || string(after) != string(body) {
				t.Fatal("admin read restored input or activated historical configuration", err)
			}
		})
	}
}
