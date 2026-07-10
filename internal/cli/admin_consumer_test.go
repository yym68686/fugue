package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAdminConsumerExpectedFiltersAndPrintsTopology(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/admin/expected-consumer-sets" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		query := r.URL.Query()
		if query.Get("release_set_id") != "release-set-cli-test" ||
			query.Get("artifact_kind") != model.PlatformArtifactKindCaddyRouteConfig ||
			query.Get("scope_key") != "global" ||
			query.Get("limit") != "10" {
			t.Fatalf("unexpected expected-consumer query: %s", r.URL.RawQuery)
		}
		now := time.Date(2026, 7, 10, 10, 0, 0, 0, time.UTC)
		writeJSONResponse(t, w, model.PlatformExpectedConsumerSetListResponse{
			ExpectedConsumerSets: []model.PlatformExpectedConsumerSet{{
				ID:                  "expectedconsumerset-cli-test",
				ReleaseSetID:        "release-set-cli-test",
				ArtifactKind:        model.PlatformArtifactKindCaddyRouteConfig,
				ScopeKey:            "global",
				ExpectedGeneration:  "generation-cli-test",
				Revision:            1,
				RequiredCardinality: 1,
				Consumers: []model.PlatformExpectedConsumer{{
					ConsumerID:              "caddy-edge-front:edge-cli-test",
					Component:               model.PlatformConsumerComponentCaddyEdgeFront,
					NodeID:                  "edge-cli-test",
					FailureDomain:           "edge-group:edge-group-cli-test",
					Cohort:                  "edge-group-cli-test",
					Required:                true,
					ExpectedProtocolVersion: model.PlatformConsumerProtocolVersionV1,
					ExpectedSchemaVersion:   model.PlatformConsumerSchemaVersionV1,
				}},
				CreatedAt: now,
				UpdatedAt: now,
			}},
			GeneratedAt: now,
		})
	}))
	defer server.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := runWithStreams([]string{
		"--base-url", server.URL,
		"--token", "token",
		"admin", "consumer", "expected",
		"--release-set", "release-set-cli-test",
		"--kind", model.PlatformArtifactKindCaddyRouteConfig,
		"--scope", "global",
		"--limit", "10",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run expected consumer command: %v stderr=%s", err, stderr.String())
	}
	for _, expected := range []string{
		"expectedconsumerset-cli-test",
		"release-set-cli-test",
		model.PlatformConsumerComponentCaddyEdgeFront,
		"edge-cli-test",
		"edge-group:edge-group-cli-test",
	} {
		if !strings.Contains(stdout.String(), expected) {
			t.Fatalf("expected output to contain %q, got:\n%s", expected, stdout.String())
		}
	}
}
