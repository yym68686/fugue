package platformconfig

import (
	"encoding/json"

	"fugue/internal/model"
)

// DecodeProjectedDNSRouteDependencyEvidenceObserved accepts the canonical
// dependency projection decoded into a generic map from PostgreSQL JSONB.
// Encode each field with the ordinary JSON normalization, then use the same
// typed decoder and validation as the ordinary artifact path. No publication
// or signature authority is inferred from the projection.
func DecodeProjectedDNSRouteDependencyEvidenceObserved(a model.PlatformArtifact, cost *DNSDependencyDecodeCost) (evidence DNSRouteDependencyEvidence, err error) {
	for key := range a.Content {
		switch key {
		case "policy", "cell_route_publications", "cell_dns_source", "previous_traffic_publication", "lineage":
		default:
			// The ordinary decoder owns noncanonical field alias semantics.
			return DecodeDNSRouteDependencyEvidenceObserved(a, cost)
		}
	}
	marshal := func(key string) ([]byte, error) {
		if value, present := a.Content[key]; present {
			return json.Marshal(value)
		}
		return nil, nil
	}
	started := cost.start()
	sourceRaw, sourceErr := marshal("cell_dns_source")
	publications, publicationsErr := marshal("cell_route_publications")
	_, lineageErr := marshal("lineage")
	policy, policyErr := marshal("policy")
	previous, previousErr := marshal("previous_traffic_publication")
	cost.record(dnsDependencyEnvelopeEncode, started)
	if sourceErr != nil || publicationsErr != nil || lineageErr != nil || policyErr != nil || previousErr != nil {
		return DecodeDNSRouteDependencyEvidenceObserved(a, cost)
	}
	started = cost.start()
	var source *CellDNSPlanSource
	var decodeErr error
	if len(sourceRaw) > 0 {
		decodeErr = json.Unmarshal(sourceRaw, &source)
	}
	cost.record(dnsDependencyEnvelopeDecode, started)
	if decodeErr != nil {
		// Preserve the envelope's exact type-error context and precedence.
		return DecodeDNSRouteDependencyEvidenceObserved(a, cost)
	}
	if cost != nil {
		cost.Attempts++
		defer func() {
			if err != nil {
				cost.Rejected++
			}
		}()
	}
	return decodeDNSRouteDependencyFieldsCached(publications, policy, previous, source, cost, &dnsCellPublicationInspectionValidations)
}
