package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Decode the exact typed consumer view after its caller verifies artifact
// integrity and publication. No business state or legacy compiler is consulted.
func platformDNSArtifactView(artifact model.PlatformArtifact, nodeID, zone string) ([]model.EdgeDNSRecord, string, error) {
	var payload struct {
		Schema        string                           `json:"schema_version"`
		Generation    string                           `json:"generation"`
		Records       []platformconfig.DNSIntent       `json:"records"`
		ConsumerViews []platformconfig.DNSConsumerView `json:"consumer_views,omitempty"`
		ReadinessPlan *platformconfig.DNSReadinessPlan `json:"readiness_plan,omitempty"`
		QueryViews    []platformconfig.DNSQueryView    `json:"query_views,omitempty"`
		Policy        platformconfig.PolicySnapshot    `json:"policy"`
		Lineage       platformconfig.Lineage           `json:"lineage"`
	}
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return nil, "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&payload) != nil || payload.Schema != platformconfig.SchemaVersion || payload.Generation != artifact.Metadata["intent_generation"] || platformconfig.ValidatePolicySnapshot(payload.Policy) != nil {
		return nil, "", fmt.Errorf("invalid DNS candidate schema")
	}
	digest, err := platformconfig.Digest(payload.Policy)
	if err != nil || payload.Policy.Scope != artifact.ScopeKey || digest != payload.Lineage.PolicyDigest || payload.Lineage.IntentGeneration != payload.Generation || payload.Lineage.PolicyGeneration != payload.Policy.Generation {
		return nil, "", fmt.Errorf("invalid DNS policy binding")
	}
	for key, value := range platformconfig.LineageMetadata(payload.Lineage) {
		if value == "" || artifact.Metadata[key] != value {
			return nil, "", fmt.Errorf("invalid DNS lineage")
		}
	}
	if err := platformconfig.ValidateDNSIntents(payload.Records); err != nil {
		return nil, "", err
	}
	if err := platformconfig.ValidateDNSReadinessPlan(payload.ReadinessPlan, payload.Policy.DNSReadiness); err != nil {
		return nil, "", err
	}
	if err := platformconfig.ValidateDNSQueryViews(payload.QueryViews, payload.Records, payload.ConsumerViews, payload.ReadinessPlan, payload.Policy); err != nil {
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
