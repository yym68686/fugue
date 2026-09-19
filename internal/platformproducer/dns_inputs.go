package platformproducer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// DNSPolicyInput is a typed projection of PolicySnapshot. Disallowing unused
// fields prevents silently accepting configuration that this producer ignores.
type DNSPolicyInput struct {
	SchemaVersion string                                `json:"schema_version"`
	Generation    string                                `json:"generation"`
	Scope         string                                `json:"scope"`
	Authorities   []platformconfig.DNSAuthorityPolicy   `json:"dns_authorities"`
	Clients       []platformconfig.DNSClientPolicy      `json:"dns_client_policies"`
	DNSReadiness  *platformconfig.ReadinessProbePolicy  `json:"dns_readiness"`
	TLSReadiness  *platformconfig.ReadinessProbePolicy  `json:"tls_readiness"`
	Cohorts       []platformconfig.TrafficRolloutCohort `json:"traffic_rollout_cohorts"`
}

func DecodeDNSInputs(a model.PlatformArtifact, consumers []platformconfig.DNSConsumerIntent, templates []HostedZoneTemplate) (DNSPolicyInput, error) {
	var p DNSPolicyInput
	fail := func() (DNSPolicyInput, error) {
		return p, fmt.Errorf("DNS input policy identity or complete ownership invalid")
	}
	raw, err := json.Marshal(a.Content)
	if err != nil {
		return p, err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil {
		return fail()
	}
	if a.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || a.ScopeKey != "global" || p.Scope != "global" || p.Generation != a.Generation || p.SchemaVersion != platformconfig.SchemaVersion || len(consumers) == 0 || platformconfig.ValidateDNSConsumers(consumers) != nil || len(p.Authorities) == 0 || len(p.Clients) != len(consumers) || p.DNSReadiness == nil || p.TLSReadiness == nil || len(p.Cohorts) == 0 {
		return fail()
	}
	if platformconfig.ValidateDNSAuthorityOwnership(p.Authorities, consumers) != nil || platformconfig.ValidateDNSClientPolicyOwnership(p.Clients, consumers) != nil || platformconfig.ValidateReadinessProbePolicy(p.DNSReadiness) != nil || platformconfig.ValidateReadinessProbePolicy(p.TLSReadiness) != nil || platformconfig.ValidateTrafficRolloutCohorts(p.Cohorts) != nil {
		return fail()
	}
	if len(templates) > 0 {
		if len(templates) != len(consumers) {
			return fail()
		}
		seen := map[string]bool{}
		for _, t := range templates {
			found := false
			for _, a := range p.Authorities {
				found = found || (a.NodeID == t.NodeID && a.Zone == t.TemplateZone)
			}
			if !found || seen[t.NodeID] {
				return fail()
			}
			seen[t.NodeID] = true
		}
	}
	return p, nil
}
