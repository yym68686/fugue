package dnsserver

import (
	"reflect"
	"slices"
	"time"

	"fugue/internal/model"
)

// A positive route proof can be fresh before the signed availability policy
// considers its endpoint recovered. Keep that proof for validation, but do not
// let its long lifetime delay the next distinct recovery observation.
func dnsDeferRecoveredProbes(p dnsServingPayload, node string, deferrable map[string]dnsReadinessFact, now time.Time) {
	if len(deferrable) == 0 || p.Plan == nil || p.Policy.DNSReadiness == nil {
		return
	}
	type recordKey struct{ host, family string }
	records := make(map[recordKey][]*model.DNSAvailabilityPolicy)
	for _, view := range p.Queries {
		if view.NodeID != node {
			continue
		}
		for _, record := range view.Records {
			if (record.Type == "A" || record.Type == "AAAA") && record.AnswerPolicy.Availability != nil {
				key := recordKey{record.Name, record.Type}
				records[key] = append(records[key], record.AnswerPolicy.Availability)
			}
		}
	}
	gap := time.Duration(p.Policy.DNSReadiness.FactFreshnessSeconds) * time.Second
	for _, record := range p.Plan.Records {
		for _, target := range record.Targets {
			for _, policy := range records[recordKey{record.Hostname, target.Family}] {
				if !slices.Contains(policy.OrderedEdgeIDs, target.EdgeID) {
					continue
				}
				for _, id := range target.ProbeIDs {
					if fact, ok := deferrable[id]; ok && !dnsAvailabilityRecovered(policy, fact, gap, now) {
						delete(deferrable, id)
					}
				}
			}
		}
	}
}

// A bounded scan can end before a candidate has enough proof to be published.
// Keep only its exact verified positive observations for the next scan. This
// cache is not query-visible and cannot replace a signed positive checkpoint.
type dnsServingProbeWindow struct {
	assignment                model.PlatformConsumerAssignment
	parentID, digest, routeID string
	facts                     []dnsReadinessFact
}

func newDNSServingProbeWindow(a model.PlatformConsumerAssignment, parent model.PlatformArtifact, routeID string, p dnsServingPayload, facts []dnsReadinessFact, now time.Time) *dnsServingProbeWindow {
	valid := validDNSReadinessFacts(p.Plan, p.Policy.DNSReadiness, facts, now)
	if len(valid) == 0 {
		return nil
	}
	w := &dnsServingProbeWindow{assignment: a, parentID: parent.ID, digest: parent.ContentHash, routeID: routeID, facts: make([]dnsReadinessFact, 0, len(valid))}
	for _, fact := range valid {
		w.facts = append(w.facts, fact)
	}
	return w
}

func (w *dnsServingProbeWindow) matches(a model.PlatformConsumerAssignment, parent model.PlatformArtifact, routeID string) bool {
	return w != nil && w.parentID == parent.ID && w.digest == parent.ContentHash && w.routeID == routeID &&
		reflect.DeepEqual(w.assignment, a)
}
