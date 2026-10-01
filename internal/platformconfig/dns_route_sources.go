package platformconfig

import (
	"encoding/json"
	"fmt"
	"strings"

	"fugue/internal/model"
)

const ProducerPolicyReleaseMetadata = "producer_policy_release_id"

// DNSRouteSourceAuthorization approves immutable configuration. A policy's
// activation and the traffic publications it produces remain separately bound
// facts; approval alone cannot select an artifact or advance a fence.
type DNSRouteSourceAuthorization struct {
	ScopeKey         string `json:"scope_key"`
	PolicyArtifactID string `json:"policy_artifact_id"`
	PolicyDigest     string `json:"policy_digest"`
}

func validateDNSRouteSourceDeclarations(in PlatformIntent) error {
	if len(in.DNSRouteSources) == 0 {
		return nil
	}
	if in.PublicationRole != PublicationRoleCellDNS || len(in.DNSRouteSources) > 32 {
		return fmt.Errorf("bounded DNS-only routing source authorization required")
	}
	scopes, seen, covered := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, ref := range in.CellRoutePublications {
		scopes[AuthorityCellScope(ref.AuthorityCellID)] = true
	}
	if in.RouteAuthorityTransition != nil {
		scopes[GlobalScopeKey] = true
	}
	for _, source := range in.DNSRouteSources {
		key := source.ScopeKey + "\x00" + source.PolicyArtifactID
		if !scopes[source.ScopeKey] || seen[key] || !validDNSConsumerIdentity(source.PolicyArtifactID) || strings.ContainsAny(source.PolicyArtifactID, " /\r\n\t") || !trafficConsumerDigest.MatchString(source.PolicyDigest) {
			return fmt.Errorf("DNS source scope, policy identity or digest invalid or duplicated")
		}
		seen[key], covered[source.ScopeKey] = true, true
	}
	if len(scopes) != len(covered) {
		return fmt.Errorf("DNS source authorization must cover every referenced routing authority")
	}
	return nil
}

func validateDNSRouteSourceBindings(in PlatformIntent, cells []CellRoutePublicationInput, previous *PreviousTrafficPublicationInput) error {
	check := func(parent model.PlatformArtifact, policy *model.PlatformPublicationPrecondition) error {
		if len(in.DNSRouteSources) == 0 {
			if policy != nil {
				return fmt.Errorf("producer binding requires explicit DNS routing source authorization")
			}
			return nil
		}
		if policy == nil || policy.FencingToken <= 0 || !validDNSConsumerIdentity(policy.ReleaseID) || strings.ContainsAny(policy.ReleaseID, " /\r\n\t") || parent.Metadata[ProducerPolicyReleaseMetadata] != policy.ReleaseID {
			return fmt.Errorf("DNS baseline source lacks exact producer publication binding")
		}
		for _, source := range in.DNSRouteSources {
			if source.ScopeKey == parent.ScopeKey && source.PolicyArtifactID == policy.ArtifactID && source.PolicyDigest == policy.ContentHash {
				return nil
			}
		}
		return fmt.Errorf("DNS baseline source producer is not approved")
	}
	for _, cell := range cells {
		if err := check(cell.Parent, cell.ProducerPolicy); err != nil {
			return err
		}
	}
	if previous != nil {
		return check(previous.Parent, previous.ProducerPolicy)
	}
	return nil
}

// Read the small authorization list without decoding retained route/TLS bodies.
// Callers still validate the complete signed artifact before trusting this list.
func DNSRouteSourceAuthorizations(a model.PlatformArtifact) ([]DNSRouteSourceAuthorization, error) {
	var value any
	switch source := a.Content["cell_dns_source"].(type) {
	case nil:
		return nil, nil
	case CellDNSPlanSource:
		return append([]DNSRouteSourceAuthorization(nil), source.Intent.DNSRouteSources...), nil
	case *CellDNSPlanSource:
		if source != nil {
			return append([]DNSRouteSourceAuthorization(nil), source.Intent.DNSRouteSources...), nil
		}
		return nil, fmt.Errorf("DNS source is null")
	case map[string]any:
		intent, ok := source["intent"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("DNS source intent missing")
		}
		value = intent["dns_route_sources"]
	default:
		return nil, fmt.Errorf("DNS source encoding invalid")
	}
	if value == nil {
		return nil, nil
	}
	raw, err := json.Marshal(value)
	if err != nil || len(raw) > 64<<10 {
		return nil, fmt.Errorf("DNS routing source authorization exceeds bound")
	}
	var sources []DNSRouteSourceAuthorization
	if json.Unmarshal(raw, &sources) != nil || len(sources) > 32 {
		return nil, fmt.Errorf("DNS routing source authorization invalid")
	}
	return sources, nil
}
