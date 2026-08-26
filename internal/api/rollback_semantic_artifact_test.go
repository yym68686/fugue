package api

import (
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/rollbackpreflight"
)

func TestRollbackSemanticArtifactGenerationMatchesEdgeDNSAPI(t *testing.T) {
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	record := model.EdgeDNSRecord{
		Name:                "api.example.test",
		Type:                "A",
		Values:              []string{"192.0.2.10"},
		TTL:                 60,
		RecordKind:          model.EdgeDNSRecordKindCustomDomainTarget,
		AppID:               "app-1",
		TenantID:            "tenant-1",
		EdgeGroupID:         "edge-group-a",
		FallbackEdgeGroupID: "edge-group-b",
		Status:              "ready",
		StatusReason:        "route ready",
		AnswerPolicy: model.DNSAnswerPolicy{
			PolicyKind:                "quality",
			AllowedEdgeGroups:         []string{"edge-group-a", "edge-group-b"},
			PreferredEdgeGroups:       []string{"edge-group-a"},
			FallbackEdgeGroups:        []string{"edge-group-b"},
			TTLSeconds:                60,
			ECSEnabled:                true,
			HealthRequired:            true,
			RouteReadyRequired:        true,
			ExplorationPercent:        5,
			SwitchCooldownSec:         120,
			RankingVersion:            "rank-v1",
			RankingScope:              "global",
			Region:                    "us-west",
			Country:                   "US",
			Priority:                  10,
			Weight:                    100,
			Reason:                    "healthy",
			SelectedEdgeGroupID:       "edge-group-a",
			ShadowSelectedEdgeGroupID: "edge-group-b",
			ShadowReason:              "exploration",
		},
		Candidates: []model.EdgeDNSAnswerCandidate{{
			IP:                "192.0.2.10",
			EdgeID:            "edge-a",
			EdgeGroupID:       "edge-group-a",
			Region:            "us-west",
			Country:           "US",
			WorkloadMode:      "edge",
			CanaryState:       model.EdgeCanaryStateActive,
			CanaryWeight:      100,
			PublicProbeStatus: model.EdgePublicProbeStatusPassing,
			ServingGeneration: "routegen_current",
			LKGGeneration:     "routegen_previous",
			CacheStatus:       "fresh",
			DNSEligible:       true,
			Priority:          10,
			Weight:            100,
			Reason:            "healthy",
			TrafficClass:      "api",
			Score:             0.91,
			ScoreBreakdown:    map[string]float64{"latency": 0.9},
			Healthy:           true,
			RouteReady:        true,
			TLSReady:          true,
			MaxStaleExceeded:  false,
		}},
		ScopedCandidates: []model.EdgeDNSScopedAnswerCandidates{{
			ScopeKey:            "country=US",
			Country:             "US",
			Region:              "us-west",
			ASN:                 "64500",
			PolicyKind:          "quality",
			Reason:              "regional quality",
			SelectedEdgeGroupID: "edge-group-a",
			CooldownUntil:       now.Add(time.Minute),
			Candidates: []model.EdgeDNSAnswerCandidate{{
				IP:          "192.0.2.11",
				EdgeID:      "edge-b",
				EdgeGroupID: "edge-group-b",
				Weight:      10,
				Healthy:     true,
				RouteReady:  true,
				TLSReady:    true,
			}},
		}},
	}
	record.RecordGeneration = edgeDNSRecordGeneration(record)
	bundle := model.EdgeDNSBundle{
		SchemaVersion: model.BundleSchemaVersionV1,
		DNSNodeID:     "dns-a",
		EdgeGroupID:   "edge-group-a",
		Zone:          ".EXAMPLE.TEST.",
		Records:       []model.EdgeDNSRecord{record},
	}
	bundle.Version = edgeDNSBundleVersion(bundle)
	bundle.Generation = bundle.Version
	artifact, err := rollbackpreflight.BuildDNSAnswerBundleArtifact(bundle)
	if err != nil {
		t.Fatalf("semantic DNS artifact rejected API generation %s: %v", bundle.Generation, err)
	}
	if artifact.Generation != bundle.Generation {
		t.Fatalf("semantic DNS artifact generation = %s, want %s", artifact.Generation, bundle.Generation)
	}
}
