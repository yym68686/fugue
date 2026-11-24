package platformconfig

import (
	"errors"
	"reflect"
	"slices"

	"fugue/internal/model"
)

// RestoreExternalHeldDeclaration builds a declaration-only projection for
// one shared target and one address family. The caller must independently
// authenticate both exact full publications and their DNS members, verify
// that base is the current positive LKG, and CAS that fence at publication.
// The historical artifact contributes an RR and physical identity only: its
// query views, probes, serving evidence and policy are never replayed.
func RestoreExternalHeldDeclaration(base, historical DNSArtifactPayload, target, family string, held HostedAddressHold, prior, effective []HostedAddressHold) (DNSArtifactPayload, error) {
	out, _, err := restoreExternalHeldDeclaration(base, historical, target, family, held, prior, effective)
	return out, err
}

func restoreExternalHeldDeclaration(base, historical DNSArtifactPayload, target, family string, held HostedAddressHold, prior, effective []HostedAddressHold) (DNSArtifactPayload, HostedAddressReservation, error) {
	var reservation HostedAddressReservation
	bad := errors.New("cannot restore the shared DNS declaration without an unchanged held physical identity")
	if family != "A" && family != "AAAA" ||
		!slices.Contains(prior, held) || !slices.Contains(effective, held) ||
		base.ReadinessPlan == nil || historical.ReadinessPlan == nil ||
		ValidateDNSReadinessPlan(base.ReadinessPlan, base.Policy.DNSReadiness) != nil ||
		ValidateDNSReadinessPlan(historical.ReadinessPlan, historical.Policy.DNSReadiness) != nil {
		return DNSArtifactPayload{}, reservation, bad
	}
	group, ok := HostedExternalDNSSharedTarget(target, base.Records, base.ReadinessPlan)
	oldGroup, oldOK := HostedExternalDNSSharedTarget(target, historical.Records, historical.ReadinessPlan)
	if !ok || !oldOK || !reflect.DeepEqual(group, oldGroup) {
		return DNSArtifactPayload{}, reservation, bad
	}
	name, app, tenant := group.Target, group.AppID, group.TenantID
	recordIndex := func(records []DNSIntent) int {
		idx := -1
		for i, r := range records {
			if r.Hostname != name || r.Type != family {
				continue
			}
			if idx >= 0 || r.RecordKind != model.EdgeDNSRecordKindCustomDomainTarget || r.AppID != app || r.TenantID != tenant {
				return -1
			}
			idx = i
		}
		return idx
	}
	i, j := recordIndex(base.Records), recordIndex(historical.Records)
	if i < 0 || j < 0 || len(historical.Records[j].Values) != len(base.Records[i].Values)+1 || !slices.Contains(historical.Records[j].Values, held.Address) || slices.Contains(base.Records[i].Values, held.Address) {
		return DNSArtifactPayload{}, reservation, bad
	}
	readiness := func(p *DNSReadinessPlan) (DNSReadinessRecord, bool) {
		var found DNSReadinessRecord
		for _, r := range p.Records {
			if r.Hostname == name {
				if found.Hostname != "" {
					return DNSReadinessRecord{}, false
				}
				found = r
			}
		}
		return found, found.Hostname != ""
	}
	phys, ok := readiness(historical.ReadinessPlan)
	active, activeOK := readiness(base.ReadinessPlan)
	if !ok || !activeOK {
		return DNSArtifactPayload{}, reservation, bad
	}
	wire, err := retainBoundHostedRecord(historical.Records[j], base.Records[i], phys, active, []HostedAddressHold{held}, true)
	if err != nil {
		return DNSArtifactPayload{}, reservation, bad
	}
	out := base
	out.Records = slices.Clone(base.Records)
	out.Records[i] = wire
	if err = checkHostedDynamicSelectors(map[hostedRecordKey]bool{{name, family}: true}, out.QueryViews, out.ReadinessPlan, out.Policy); err != nil {
		return DNSArtifactPayload{}, reservation, err
	}
	if err = ValidateDNSQueryViews(out.QueryViews, out.Records, out.ConsumerViews, out.ReadinessPlan, out.Policy); err != nil {
		return DNSArtifactPayload{}, reservation, err
	}
	reservation.Record = wire
	reservation.Physical = hostedPhysicalRoster(phys, family)
	reservation.Shared = &group
	return out, reservation, nil
}

// BindRestoredExternalDeclaration gives a declaration-only recovery a normal
// predecessor custody record after validating the signed history projection.
// Source must name the authenticated *current* full, never the historical
// full. It changes on each recapture; the physical roster is immutable.
func BindRestoredExternalDeclaration(source HostedAddressSource, historical DNSArtifactPayload, target, family string, held HostedAddressHold) (HostedAddressReservation, error) {
	if err := source.Publication.Validate(); err != nil {
		return HostedAddressReservation{}, err
	}
	_, r, err := restoreExternalHeldDeclaration(source.Prior, historical, target, family, held, source.PriorHolds, source.EffectiveHolds)
	if err != nil {
		return HostedAddressReservation{}, err
	}
	r.Source = source.Publication
	return r, nil
}

func sharedDeclarationMatches(p DNSArtifactPayload, a *ExternalPathTarget, r DNSIntent) bool {
	if a == nil || a.Target != r.Hostname || a.AppID != r.AppID || a.TenantID != r.TenantID || r.RecordKind != model.EdgeDNSRecordKindCustomDomainTarget {
		return false
	}
	group, ok := HostedExternalDNSSharedTarget(a.Target, p.Records, p.ReadinessPlan)
	return ok && reflect.DeepEqual(*a, group)
}

func copySharedDeclaration(a *ExternalPathTarget) *ExternalPathTarget {
	if a == nil {
		return nil
	}
	out := *a
	out.Members = slices.Clone(a.Members)
	return &out
}

// viewScopedAddressRecords is for comparisons and digests, not publication.
func viewScopedAddressRecords(rows []DNSIntent, name string) []DNSIntent {
	var out []DNSIntent
	for _, r := range rows {
		if r.Hostname == name && (r.Type == "A" || r.Type == "AAAA") {
			out = append(out, r)
		}
	}
	return out
}

func sameScopedAddressRecords(base, next []DNSIntent, name string) bool {
	return reflect.DeepEqual(viewScopedAddressRecords(base, name), viewScopedAddressRecords(next, name))
}
