package platformconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"fugue/internal/model"
)

type DNSAvailabilityRoute struct {
	Hostname string                      `json:"hostname"`
	Policy   model.DNSAvailabilityPolicy `json:"policy"`
}

func (r *DNSAvailabilityRoute) UnmarshalJSON(raw []byte) error {
	type plain DNSAvailabilityRoute
	var value plain
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 2 || fields["hostname"] == nil || fields["policy"] == nil || bytes.Equal(bytes.TrimSpace(fields["policy"]), []byte("null")) {
		return fmt.Errorf("complete explicit DNS availability route required")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	*r = DNSAvailabilityRoute(value)
	return nil
}

func DNSAvailabilityForHost(p *DNSQueryPolicy, hostname string) *model.DNSAvailabilityPolicy {
	if p != nil {
		for _, r := range p.AvailabilityRoutes {
			if r.Hostname == hostname {
				return model.CloneDNSAvailabilityPolicy(&r.Policy)
			}
		}
	}
	return nil
}

func validateDNSAvailabilityIntent(record model.EdgeDNSRecord, policy PolicySnapshot) error {
	want := DNSAvailabilityForHost(policy.DNSQueryPolicy, record.Name)
	if !reflect.DeepEqual(want, record.AnswerPolicy.Availability) {
		return fmt.Errorf("DNS availability differs from explicit signed host policy")
	}
	if want != nil {
		if policy.DNSReadiness == nil || want.RecoveryMinSeconds > 15*policy.DNSReadiness.ProbeIntervalSeconds {
			return fmt.Errorf("availability recovery exceeds bounded probe history")
		}
		if err := model.ValidateDNSAvailabilityPolicy(want); err != nil {
			return err
		}
		if (record.Type != "A" && record.Type != "AAAA") || len(record.Candidates) == 0 || len(record.ScopedCandidates) != 0 ||
			!record.AnswerPolicy.HealthRequired || !record.AnswerPolicy.RouteReadyRequired ||
			record.AnswerPolicy.PolicyKind != model.DNSAnswerPolicyKindPhysicalOrder && record.AnswerPolicy.PolicyKind != model.DNSAnswerPolicyKindPhysicalQuality {
			return fmt.Errorf("availability requires an authorized physical dynamic record")
		}
	}
	return nil
}
