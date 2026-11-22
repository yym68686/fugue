package dnsserver

import (
	"slices"
	"time"

	"fugue/internal/model"
)

// A private two-pass collection for an already assigned full release. This
// is not a serving checkpoint, receipt, or cache of previously served facts.
type dnsCheckpointScan struct {
	window  *dnsServingProbeWindow
	base    time.Time
	started time.Time
}

func physicalDNSScanFact(f dnsReadinessFact, start, end time.Time) bool {
	return f.Ready && f.Reason == "" && f.ObservationID != "" &&
		!f.probeObservedAt.Before(start) && !f.probeObservedAt.After(end) &&
		!f.Proof.CheckedAt.Before(start) && !f.Proof.CheckedAt.After(f.probeObservedAt)
}

func newDNSCheckpointScan(a model.PlatformConsumerAssignment, parent model.PlatformArtifact, candidate dnsPlatformCandidate, routeID string, p dnsServingPayload, base, start, end time.Time, raw []dnsReadinessFact) *dnsCheckpointScan {
	if start.IsZero() || !start.After(base) || end.Before(start) {
		return nil
	}
	// Validate the entire input first so filtering never hides duplicate IDs.
	valid := validDNSReadinessFacts(p.Plan, p.Policy.DNSReadiness, raw, end)
	facts := make([]dnsReadinessFact, 0, len(valid))
	for _, f := range valid {
		if physicalDNSScanFact(f, start, end) && dnsFactMatchesRelease(f, parent, candidate, routeID, p) {
			facts = append(facts, f)
		}
	}
	if len(facts) == 0 {
		return nil
	}
	return &dnsCheckpointScan{window: newDNSServingProbeWindow(a, parent, routeID, p, facts, end), base: base, started: start}
}

func (c *dnsCheckpointScan) prior(a model.PlatformConsumerAssignment, parent model.PlatformArtifact, routeID string, base time.Time, p dnsServingPayload, now time.Time) map[string]dnsReadinessFact {
	if c == nil || !c.base.Equal(base) || !c.window.matches(a, parent, routeID) ||
		now.Before(c.started) || now.Sub(c.started) >= time.Duration(p.Policy.DNSReadiness.FactFreshnessSeconds)*time.Second {
		return nil
	}
	return validDNSReadinessFacts(p.Plan, p.Policy.DNSReadiness, c.window.facts, now)
}

func completeDNSCheckpointScan(p dnsServingPayload, parent model.PlatformArtifact, candidate dnsPlatformCandidate, routeID string, first map[string]dnsReadinessFact, second []dnsReadinessFact, start, end time.Time) ([]dnsReadinessFact, bool) {
	if len(first) == 0 || len(second) != len(p.Plan.Probes) {
		return nil, false
	}
	joint := slices.Clone(second)
	usedFirst, usedSecond := false, false
	for i, f := range joint {
		// Only a lack of observation can use a first-pass witness. In
		// particular, a transport failure is NOT missing evidence.
		if f.Reason == "not_observed" && !f.Ready {
			prior, ok := first[f.ProbeID]
			if !ok {
				return nil, false
			}
			joint[i] = prior
			usedFirst = true
		} else if !physicalDNSScanFact(f, start, end) {
			return nil, false
		} else {
			usedSecond = true
		}
	}
	if !usedFirst || !usedSecond {
		return nil, false
	}
	valid := validDNSReadinessFacts(p.Plan, p.Policy.DNSReadiness, joint, end)
	if len(valid) != len(p.Plan.Probes) {
		return nil, false
	}
	for _, f := range valid {
		if !dnsFactMatchesRelease(f, parent, candidate, routeID, p) {
			return nil, false
		}
	}
	return joint, true
}
