package api

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEdgeRouteBundleInvariantRejectsEmptyRoutableBundle(t *testing.T) {
	t.Parallel()

	err := validateEdgeRouteBundleForPublish(model.EdgeRouteBundle{
		Version:     "routegen_empty",
		Generation:  "routegen_empty",
		GeneratedAt: time.Now().UTC(),
		Routes:      nil,
	}, edgeRouteBundleInvariantInput{
		Apps: []model.App{
			{ID: "app_demo", Route: &model.AppRoute{Hostname: "demo.fugue.pro"}},
		},
		HealthyEdgeGroups: map[string]bool{"edge-group-country-us": true},
		Options:           edgeRouteBundleOptions{EdgeGroupID: "edge-group-country-us"},
	})
	if err == nil || !strings.Contains(err.Error(), "refusing to publish empty route bundle") {
		t.Fatalf("expected empty route bundle invariant failure, got %v", err)
	}
}

func TestEdgeRouteBundleInvariantRejectsEmptyBundleForStaleEdgeGroup(t *testing.T) {
	t.Parallel()

	err := validateEdgeRouteBundleForPublish(model.EdgeRouteBundle{
		Version:     "routegen_empty",
		Generation:  "routegen_empty",
		GeneratedAt: time.Now().UTC(),
		Routes:      nil,
	}, edgeRouteBundleInvariantInput{
		Apps: []model.App{
			{ID: "app_demo", Route: &model.AppRoute{Hostname: "demo.fugue.pro"}},
		},
		HealthyEdgeGroups:          map[string]bool{"edge-group-country-de": false},
		ExpectedNonEmptyEdgeGroups: map[string]bool{"edge-group-country-de": true},
		Options:                    edgeRouteBundleOptions{EdgeGroupID: "edge-group-country-de"},
	})
	if err == nil || !strings.Contains(err.Error(), "refusing to publish empty route bundle") {
		t.Fatalf("expected stale edge group empty route bundle invariant failure, got %v", err)
	}
}

func TestEdgeDNSBundleInvariantRejectsMissingProtectedRecord(t *testing.T) {
	t.Parallel()

	protected := []model.EdgeDNSRecord{
		{
			Name:       "fugue.pro",
			Type:       model.EdgeDNSRecordTypeNS,
			Values:     []string{"ns1.fugue.pro"},
			RecordKind: model.EdgeDNSRecordKindProtected,
			Status:     model.EdgeRouteStatusActive,
		},
	}
	bundle := model.EdgeDNSBundle{
		Version:     "dnsgen_missing_protected",
		Generation:  "dnsgen_missing_protected",
		GeneratedAt: time.Now().UTC(),
		Zone:        "fugue.pro",
		Records: []model.EdgeDNSRecord{
			{
				Name:       "d-test.fugue.pro",
				Type:       model.EdgeDNSRecordTypeA,
				Values:     []string{"203.0.113.10"},
				RecordKind: model.EdgeDNSRecordKindProbe,
				Status:     model.EdgeRouteStatusActive,
			},
		},
	}
	err := validateEdgeDNSBundleForPublish(bundle, edgeDNSBundleOptions{Zone: "fugue.pro"}, protected)
	if err == nil || !strings.Contains(err.Error(), "protected fugue.pro NS value is missing") {
		t.Fatalf("expected protected record invariant failure, got %v", err)
	}
}
