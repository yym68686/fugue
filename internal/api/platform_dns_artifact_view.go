package api

import (
	"fmt"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Decode the exact typed consumer view after its caller verifies artifact
// integrity and publication. No business state or legacy compiler is consulted.
type platformDNSArtifactPayload = platformconfig.DNSArtifactPayload

func decodePlatformDNSArtifact(artifact model.PlatformArtifact) (platformDNSArtifactPayload, error) {
	return platformconfig.DecodeDNSArtifact(artifact)
}

func platformDNSArtifactView(artifact model.PlatformArtifact, nodeID, zone string) ([]model.EdgeDNSRecord, string, error) {
	payload, err := decodePlatformDNSArtifact(artifact)
	if err != nil {
		return nil, "", err
	}
	group := ""
	for _, view := range payload.ConsumerViews {
		if view.NodeID == nodeID && view.Zone == zone {
			if group != "" {
				return nil, "", fmt.Errorf("ambiguous DNS consumer view")
			}
			group = view.EdgeGroupID
		}
	}
	if group == "" {
		return nil, "", fmt.Errorf("missing DNS consumer view")
	}
	records, err := platformconfig.DNSConsumerViewRecords(payload.Records, payload.ConsumerViews, nodeID, group, zone)
	if err != nil {
		return nil, "", err
	}
	out := make([]model.EdgeDNSRecord, 0, len(records))
	for _, r := range records {
		out = append(out, model.EdgeDNSRecord{Name: r.Hostname, Type: r.Type, Values: r.Values, TTL: r.TTL, ValueExpirations: r.ValueExpirations, RecordKind: r.RecordKind, Status: r.Status, StatusReason: r.StatusReason, AppID: r.AppID, TenantID: r.TenantID, EdgeGroupID: r.EdgeGroupID, FallbackEdgeGroupID: r.FallbackEdgeGroupID})
	}
	return out, group, nil
}
