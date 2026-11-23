package platformconfig

import (
	"errors"
	"reflect"
	"slices"
)

// HostedAddressSource is supplied by the trusted publisher, not decoded from
// a public compile request. A syntactically valid reference is not authority.
type HostedAddressSource struct {
	Publication                PreviousTrafficPublicationReference
	PriorIntent                PlatformIntent
	Prior                      DNSArtifactPayload
	Inherited                  []HostedAddressReservation
	PriorHolds, EffectiveHolds []HostedAddressHold
	// nil is an unrestricted diagnostic compile. Production always supplies
	// the explicit same-owner transitions and previously retained records.
	managedHosts map[string]bool
}

type hostedRecordKey struct{ host, family string }

// A selector can use its Values when a candidate is unavailable. Verify
// that field independently of candidate membership, including on Cells.
func checkHostedDynamicSelectors(keys map[hostedRecordKey]bool, views []DNSQueryView, plan *DNSReadinessPlan, policy PolicySnapshot) error {
	bad := errors.New("reserved hosted address lacks a strictly eligible dynamic selector")
	if plan == nil || policy.DNSQueryPolicy == nil || policy.DNSPlacementMode != DNSPlacementConsumerReadiness || len(views) == 0 {
		return bad
	}
	active := map[hostedRecordKey]map[string]bool{}
	for _, rr := range plan.Records {
		for _, t := range rr.Targets {
			k := hostedRecordKey{rr.Hostname, t.Family}
			if _, ok := keys[k]; !ok {
				continue
			}
			if active[k] == nil {
				active[k] = map[string]bool{}
			}
			active[k][t.Address] = true
		}
	}
	seen := map[hostedRecordKey]bool{}
	for _, v := range views {
		for _, r := range v.Records {
			k := hostedRecordKey{r.Name, r.Type}
			if _, ok := keys[k]; !ok {
				continue
			}
			if len(r.Candidates) == 0 || len(r.Values) == 0 || !r.AnswerPolicy.HealthRequired || !r.AnswerPolicy.RouteReadyRequired {
				return bad
			}
			for _, ip := range r.Values {
				if !active[k][ip] {
					return bad
				}
			}
			seen[k] = true
		}
	}
	if len(seen) != len(keys) {
		return bad
	}
	return nil
}

// ReserveHostedAddressProjection changes only the declaration. Leave the
// candidate's read/probe requirements, per-consumer views, default and scoped
// query candidates, policy and observations untouched. Reuse the consumer's
// verifier and require a dynamic view for every reserved record.
func ReserveHostedAddressProjection(next DNSArtifactPayload, source HostedAddressSource) ([]DNSIntent, []HostedAddressReservation, error) {
	reservations, err := captureHostedAddressChanges(source.Publication, source.Prior, source.Inherited, source.PriorHolds, next, source.EffectiveHolds, source.managedHosts)
	if err != nil || len(reservations) == 0 {
		return next.Records, nil, err
	}
	if next.Policy.DNSQueryPolicy == nil || next.Policy.DNSPlacementMode != DNSPlacementConsumerReadiness || len(next.QueryViews) == 0 {
		return nil, nil, errors.New("cannot reserve hosted addresses for static or absent DNS selectors")
	}
	index := map[hostedRecordKey]int{}
	for i, r := range next.Records {
		index[hostedRecordKey{r.Hostname, r.Type}] = i
	}
	out := slices.Clone(next.Records)
	covered := map[hostedRecordKey]bool{}
	for _, p := range reservations {
		k := hostedRecordKey{p.Record.Hostname, p.Record.Type}
		out[index[k]] = p.Record
		covered[k] = false
	}
	if err := checkHostedDynamicSelectors(covered, next.QueryViews, next.ReadinessPlan, next.Policy); err != nil {
		return nil, nil, err
	}
	if err := ValidateDNSQueryViews(next.QueryViews, out, next.ConsumerViews, next.ReadinessPlan, next.Policy); err != nil {
		return nil, nil, err
	}
	return out, reservations, nil
}

// CaptureHostedAddressChanges is a dry compilation step; it never changes
// either signed document. The caller authenticates the exact predecessor,
// its stored runtime input and both publications' independent offline policy.
// A reservation only explains subtraction of an existing, now-held address.
// Other record changes are not explained by this result. The caller must
// independently classify them; this function does not authorize their release.
func CaptureHostedAddressChanges(source PreviousTrafficPublicationReference, prior DNSArtifactPayload, inherited []HostedAddressReservation, priorHolds []HostedAddressHold, next DNSArtifactPayload, nextHolds []HostedAddressHold) ([]HostedAddressReservation, error) {
	return captureHostedAddressChanges(source, prior, inherited, priorHolds, next, nextHolds, nil)
}

func captureHostedAddressChanges(source PreviousTrafficPublicationReference, prior DNSArtifactPayload, inherited []HostedAddressReservation, priorHolds []HostedAddressHold, next DNSArtifactPayload, nextHolds []HostedAddressHold, hosts map[string]bool) ([]HostedAddressReservation, error) {
	bad := errors.New("incomplete or ambiguous hosted address projection")
	if source.Validate() != nil || len(inherited) > 4096 || len(prior.Records) > 20000 || len(next.Records) > 20000 ||
		prior.ReadinessPlan == nil || next.ReadinessPlan == nil {
		return nil, bad
	}
	type key struct{ host, family string }
	index := func(p DNSArtifactPayload) (map[key]DNSIntent, map[string]DNSReadinessRecord, error) {
		byKey, byHost := map[key]DNSIntent{}, map[string]DNSReadinessRecord{}
		for _, r := range p.Records {
			if r.Type != "A" && r.Type != "AAAA" {
				continue
			}
			k := key{r.Hostname, r.Type}
			if _, ok := byKey[k]; ok {
				return nil, nil, bad
			}
			byKey[k] = r
		}
		for _, r := range p.ReadinessPlan.Records {
			if _, ok := byHost[r.Hostname]; ok {
				return nil, nil, bad
			}
			byHost[r.Hostname] = r
		}
		return byKey, byHost, nil
	}
	before, oldPhysical, err := index(prior)
	if err != nil {
		return nil, err
	}
	_, live, err := index(next)
	if err != nil {
		return nil, err
	}
	custody := map[key]*HostedAddressReservation{}
	for i := range inherited {
		p := &inherited[i]
		k := key{p.Record.Hostname, p.Record.Type}
		if custody[k] != nil || !reflect.DeepEqual(before[k], p.Record) {
			return nil, bad
		}
		custody[k] = p
	}
	var retained []HostedAddressReservation
	targetCount := 0
	for _, r := range next.Records {
		if hosts != nil && !hosts[r.Hostname] {
			continue
		}
		k := key{r.Hostname, r.Type}
		old, ok := before[k]
		if !ok || r.RecordKind != "hosted" || old.RecordKind != r.RecordKind || old.TenantID != r.TenantID || old.AppID != r.AppID ||
			slices.Equal(old.Values, r.Values) {
			continue
		}
		if len(r.Values) >= len(old.Values) {
			continue
		}
		a, b := oldPhysical[k.host], live[k.host]
		// No matching hold: the change cannot be explained as a
		// reservation. Never manufacture a roster for a new tenant/IP.
		if len(nextHolds) == 0 {
			if custody[k] != nil {
				return nil, bad
			}
			continue
		}
		p, err := CaptureHostedAddressReservation(source, old, a, priorHolds, custody[k], r, b, nextHolds)
		if err != nil {
			return nil, err
		}
		targetCount += len(p.Physical.Targets)
		if targetCount > 32768 || len(retained) >= 4096 {
			return nil, bad
		}
		retained = append(retained, p)
	}
	return retained, nil
}
