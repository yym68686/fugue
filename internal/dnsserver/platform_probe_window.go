package dnsserver

import (
	"reflect"
	"time"

	"fugue/internal/model"
)

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
