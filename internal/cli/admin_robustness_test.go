package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestWriteRobustnessStatusShowsLKGServingConsumers(t *testing.T) {
	status := model.RobustnessStatus{
		GeneratedAt: time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC),
		Pass:        true,
		Checks: []model.RobustnessCheck{{
			Name:     "platform_consumer_generation_inventory",
			Pass:     true,
			Severity: model.RobustnessSeverityInfo,
		}},
		PlatformConsumers: []model.PlatformConsumerInstance{
			{
				ConsumerID:        "consumer-live",
				Component:         "edge-worker",
				NodeID:            "edge-a",
				ArtifactKind:      model.PlatformArtifactKindEdgeRouteBundle,
				ScopeKey:          "edge-group-a",
				DesiredGeneration: "routegen_2",
				ActualGeneration:  "routegen_2",
				LKGGeneration:     "routegen_2",
				UpdatedAt:         time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC),
			},
			{
				ConsumerID:        "consumer-lkg",
				Component:         "dns-server",
				NodeID:            "dns-a",
				ArtifactKind:      model.PlatformArtifactKindDNSAnswerBundle,
				ScopeKey:          "zone:fugue.pro",
				DesiredGeneration: "dnsgen_2",
				ActualGeneration:  "dnsgen_1",
				LKGGeneration:     "dnsgen_1",
				ServingLKG:        true,
				UpdatedAt:         time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC),
			},
		},
	}
	var out bytes.Buffer
	if err := writeRobustnessStatus(&out, status); err != nil {
		t.Fatalf("write robustness status: %v", err)
	}
	body := out.String()
	if !strings.Contains(body, "SERVING_LKG") || !strings.Contains(body, "consumer-lkg") {
		t.Fatalf("expected LKG consumer table, got:\n%s", body)
	}
	if strings.Contains(body, "consumer-live") {
		t.Fatalf("healthy non-LKG consumer should not be shown in compact robustness output:\n%s", body)
	}
}
