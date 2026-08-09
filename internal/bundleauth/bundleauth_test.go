package bundleauth

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEdgeRouteBundleSignsOnlyCurrentKeyringPayloads(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	keyring := NewKeyring("current-route-key", "route-current", "previous-route-key", "route-previous", nil)
	bundle := model.EdgeRouteBundle{
		SchemaVersion: model.BundleSchemaVersionV1,
		Version:       "routegen_current",
		Generation:    "routegen_current",
		GeneratedAt:   now,
		Issuer:        "fugue-edge-control",
		EdgeGroupID:   "edge-group-test",
		Routes: []model.EdgeRouteBinding{{
			Hostname:        "api.example.test",
			RouteKind:       model.EdgeRouteKindPlatform,
			AppID:           "app_1",
			TenantID:        "tenant_1",
			RuntimeID:       "runtime_1",
			EdgeGroupID:     "edge-group-test",
			RoutePolicy:     model.EdgeRoutePolicyEnabled,
			UpstreamKind:    model.EdgeRouteUpstreamKindKubernetesService,
			UpstreamURL:     "http://api.default.svc.cluster.local:80",
			ServicePort:     80,
			TLSPolicy:       model.EdgeRouteTLSPolicyPlatform,
			Status:          model.EdgeRouteStatusActive,
			DecisionID:      "decision_current",
			RouteGeneration: "route_binding_current",
			CreatedAt:       now,
			UpdatedAt:       now,
		}},
	}

	signed := SignEdgeRouteBundleWithKeyring(bundle, keyring, time.Hour)
	if len(signed.Signatures) != 2 {
		t.Fatalf("route signature count = %d, want one current payload per active key: %+v", len(signed.Signatures), signed.Signatures)
	}
	if signed.Signatures[0].KeyID != "route-current" || signed.Signatures[0].Signature != signed.Signature ||
		signed.Signatures[1].KeyID != "route-previous" {
		t.Fatalf("route keyring signatures are not canonical: %+v", signed.Signatures)
	}
	if err := VerifyEdgeRouteBundleWithKeyring(signed, keyring, now); err != nil {
		t.Fatalf("verify current route bundle: %v", err)
	}

	droppedCurrentField := signed
	droppedCurrentField.Routes = append([]model.EdgeRouteBinding(nil), signed.Routes...)
	droppedCurrentField.Routes[0].DecisionID = ""
	if err := VerifyEdgeRouteBundleWithKeyring(droppedCurrentField, keyring, now); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("route verifier accepted a projection that dropped a current field: %v", err)
	}
}

func TestEdgeDNSBundleSignsOnlyCurrentKeyringPayloads(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	keyring := NewKeyring("current-dns-key", "dns-current", "previous-dns-key", "dns-previous", nil)
	bundle := model.EdgeDNSBundle{
		SchemaVersion: model.BundleSchemaVersionV1,
		Version:       "dnsgen_current",
		Generation:    "dnsgen_current",
		GeneratedAt:   now,
		Issuer:        "fugue-edge-control",
		DNSNodeID:     "dns-test-1",
		EdgeGroupID:   "edge-group-test",
		Zone:          "example.test",
		Records: []model.EdgeDNSRecord{{
			Name:             "api.example.test.",
			Type:             "A",
			Values:           []string{"203.0.113.10"},
			TTL:              30,
			RecordKind:       "platform",
			EdgeGroupID:      "edge-group-test",
			Status:           "active",
			RecordGeneration: "record_current",
			Candidates: []model.EdgeDNSAnswerCandidate{{
				IP:                "203.0.113.10",
				EdgeID:            "edge-test-1",
				EdgeGroupID:       "edge-group-test",
				WorkloadMode:      "dynamic",
				CanaryState:       "active",
				PublicProbeStatus: "pass",
				DNSEligible:       true,
				Healthy:           true,
				RouteReady:        true,
				TLSReady:          true,
			}},
		}},
	}

	signed := SignEdgeDNSBundleWithKeyring(bundle, keyring, time.Hour)
	if len(signed.Signatures) != 2 {
		t.Fatalf("DNS signature count = %d, want one current payload per active key: %+v", len(signed.Signatures), signed.Signatures)
	}
	if signed.Signatures[0].KeyID != "dns-current" || signed.Signatures[0].Signature != signed.Signature ||
		signed.Signatures[1].KeyID != "dns-previous" {
		t.Fatalf("DNS keyring signatures are not canonical: %+v", signed.Signatures)
	}
	if err := VerifyEdgeDNSBundleWithKeyring(signed, keyring, now); err != nil {
		t.Fatalf("verify current DNS bundle: %v", err)
	}

	droppedCurrentField := signed
	droppedCurrentField.Records = append([]model.EdgeDNSRecord(nil), signed.Records...)
	droppedCurrentField.Records[0].Candidates = append([]model.EdgeDNSAnswerCandidate(nil), signed.Records[0].Candidates...)
	droppedCurrentField.Records[0].Candidates[0].PublicProbeStatus = ""
	if err := VerifyEdgeDNSBundleWithKeyring(droppedCurrentField, keyring, now); !errors.Is(err, ErrInvalidSignature) {
		t.Fatalf("DNS verifier accepted a projection that dropped a current field: %v", err)
	}
}
