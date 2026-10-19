package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"

	"fugue/internal/model"
)

// DNSArtifactPayload is the exact typed consumer view of a DNS artifact.
// Decoding validates its content; callers still verify integrity and current
// publication independently, including after network observations.
type DNSArtifactPayload struct {
	PreviousTrafficPublication *PreviousTrafficPublicationInput `json:"previous_traffic_publication,omitempty"`
	CellRoutePublications      []CellRoutePublicationInput      `json:"cell_route_publications,omitempty"`
	CellDNSSource              *CellDNSPlanSource               `json:"cell_dns_source,omitempty"`
	Schema                     string                           `json:"schema_version"`
	Generation                 string                           `json:"generation"`
	Records                    []DNSIntent                      `json:"records"`
	ConsumerViews              []DNSConsumerView                `json:"consumer_views,omitempty"`
	ReadinessPlan              *DNSReadinessPlan                `json:"readiness_plan,omitempty"`
	QueryViews                 []DNSQueryView                   `json:"query_views,omitempty"`
	Policy                     PolicySnapshot                   `json:"policy"`
	Lineage                    Lineage                          `json:"lineage"`
}

// DecodeDNSArtifact performs the consumer schema, lineage, Cell, readiness and
// query-view checks using one encoding of this call's content. It retains no
// state between calls and does not establish serving authorization.
func DecodeDNSArtifact(artifact model.PlatformArtifact) (DNSArtifactPayload, error) {
	var payload DNSArtifactPayload
	raw, err := json.Marshal(artifact.Content)
	if err != nil {
		return payload, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&payload) != nil || payload.Schema != SchemaVersion || payload.Generation != artifact.Metadata["intent_generation"] || ValidatePolicySnapshot(payload.Policy) != nil {
		return payload, fmt.Errorf("invalid DNS candidate schema")
	}
	digest, err := Digest(payload.Policy)
	if err != nil || payload.Policy.Scope != artifact.ScopeKey || digest != payload.Lineage.PolicyDigest || payload.Lineage.IntentGeneration != payload.Generation || payload.Lineage.PolicyGeneration != payload.Policy.Generation {
		return payload, fmt.Errorf("invalid DNS policy binding")
	}
	for key, value := range LineageMetadata(payload.Lineage) {
		if value == "" || artifact.Metadata[key] != value {
			return payload, fmt.Errorf("invalid DNS lineage")
		}
	}
	if err := validateDNSCellPlanEncoded(artifact, raw); err != nil {
		return payload, err
	}
	if err := ValidateDNSIntents(payload.Records); err != nil {
		return payload, err
	}
	if err := ValidateDNSReadinessPlan(payload.ReadinessPlan, payload.Policy.DNSReadiness); err != nil {
		return payload, err
	}
	if err := ValidateDNSQueryViews(payload.QueryViews, payload.Records, payload.ConsumerViews, payload.ReadinessPlan, payload.Policy); err != nil {
		return payload, err
	}
	return payload, nil
}
