package platformconfig

import (
	"errors"
	"fmt"
	"reflect"
	"slices"

	"fugue/internal/model"
)

const HostedFleetHandoffMetadata = "hosted_fleet_handoff_inherited_digest_v1"

// CompileHostedFleetHandoff may only carry existing custody into the first
// ordinary baseline of a new fleet-default policy. The only reservation
// field that can change is the source pin. Do not emit offers, scoped
// successors, or historical declaration restorations from this input.
func CompileHostedFleetHandoff(req CompileRequest, source HostedAddressSource) (CompileResult, error) {
	bad := errors.New("fleet default handoff must carry exact existing hosted address reservations")
	if len(source.Inherited) == 0 {
		return CompileResult{}, bad
	}
	managed := ManagedChangedHosts(source.PriorIntent, req.Intent)
	for host := range NewHostedPathReservationHosts(source.Prior, req.Intent, source.EffectiveHolds) {
		if !slices.Contains(managed, host) {
			return CompileResult{}, bad
		}
	}
	stable, err := stageSelectedManagedPaths(source.PriorIntent, req, managed)
	if err != nil {
		return CompileResult{}, err
	}
	stable, err = stageBaselineExternalPaths(source.PriorIntent, stable, ExternalChangedHosts(source.PriorIntent, req.Intent))
	if err != nil {
		return CompileResult{}, err
	}
	out, err := compileWithInheritedCustody(stable, source)
	if err != nil {
		return CompileResult{}, err
	}
	if !sameHostedFleetCustody(source.Inherited, out.InputSnapshot.HostedAddressReservations, source.Publication) {
		return CompileResult{}, bad
	}
	digest, err := Digest(source.Inherited)
	if err != nil {
		return CompileResult{}, err
	}
	out.DNSArtifact.Metadata[HostedFleetHandoffMetadata] = digest
	out.ReleaseArtifact.Metadata[HostedFleetHandoffMetadata] = digest
	return out, nil
}

func sameHostedFleetCustody(before, after []HostedAddressReservation, pin PreviousTrafficPublicationReference) bool {
	if len(before) == 0 || len(before) != len(after) {
		return false
	}
	custody := make(map[hostedRecordKey]HostedAddressReservation, len(before))
	for _, reservation := range before {
		key := hostedRecordKey{reservation.Record.Hostname, reservation.Record.Type}
		if _, exists := custody[key]; exists || reservation.Source == pin {
			return false
		}
		reservation.Source = pin
		custody[key] = reservation
	}
	for _, reservation := range after {
		key := hostedRecordKey{reservation.Record.Hostname, reservation.Record.Type}
		previous, exists := custody[key]
		if !exists || !reflect.DeepEqual(previous, reservation) {
			return false
		}
		delete(custody, key)
	}
	return len(custody) == 0
}

// CompileHostedBaselineRefresh separates ordinary topology changes from the
// creation of a new hosted reservation. Use the current capture and the
// already proven intermediate route bindings, never the previous live facts.
// When unrelated wire records changed, withdraw held endpoints through the
// ordinary DNS lane first; keep any custody the selected full already has.
func CompileHostedBaselineRefresh(staged CompileResult, source HostedAddressSource, capture *HostedTransitionCapture) (CompileResult, error) {
	bad := errors.New("cannot isolate an ordinary DNS baseline refresh")
	if capture == nil || capture.baseline == nil || len(capture.Hosts) == 0 || len(staged.InputSnapshot.HostedAddressReservations) == 0 {
		return CompileResult{}, bad
	}
	previous := buildArtifact(model.PlatformArtifactKindDNSAnswerBundle, GlobalScopeKey, "", struct {
		Records []DNSIntent `json:"records"`
	}{source.Prior.Records}, nil, staged.DNSArtifact.CreatedAt)
	if len(capture.ExternalHosts) == 0 && ValidateHostedPublicationIsolation(previous, staged.DNSArtifact) == nil {
		return staged, nil
	}
	req, err := stageBaselineExternalPaths(source.PriorIntent, *capture.baseline, capture.ExternalHosts)
	if err != nil {
		return CompileResult{}, err
	}
	if len(source.Inherited) == 0 {
		return Compile(req)
	}
	return compileWithInheritedCustody(req, source)
}

func compileWithInheritedCustody(req CompileRequest, source HostedAddressSource) (CompileResult, error) {
	bad := errors.New("cannot isolate an ordinary DNS baseline refresh")
	// No newly proposed host gets a reservation on this ordinary release.
	source.managedHosts = make(map[string]bool)
	custody := make(map[hostedRecordKey]DNSIntent, len(source.Inherited))
	for _, r := range source.Inherited {
		if r.Record.RecordKind == "hosted" {
			source.managedHosts[r.Record.Hostname] = true
		}
		custody[hostedRecordKey{r.Record.Hostname, r.Record.Type}] = r.Record
	}
	source.stableAliases = stableHostedAliasDeclarations(source.PriorIntent, req.Intent)
	got, err := CompileWithHostedAddressSource(req, source)
	if err != nil {
		return CompileResult{}, err
	}
	if len(custody) != len(got.InputSnapshot.HostedAddressReservations) {
		return CompileResult{}, fmt.Errorf("%w: inherited custody changed", bad)
	}
	for _, r := range got.InputSnapshot.HostedAddressReservations {
		if !reflect.DeepEqual(custody[hostedRecordKey{r.Record.Hostname, r.Record.Type}], r.Record) {
			return CompileResult{}, fmt.Errorf("%w: inherited custody changed", bad)
		}
	}
	return got, nil
}

// Keep pending external path edits out of the ordinary baseline too. Only
// retain paths with the same live binding, and restore only the DNS target's
// path references (not its addresses, evidence or policy).
func stageBaselineExternalPaths(prior PlatformIntent, req CompileRequest, hosts []string) (CompileRequest, error) {
	if len(hosts) == 0 {
		return req, nil
	}
	bad := errors.New("cannot retain external paths during DNS baseline refresh")
	changes := externalPathChanges(prior, req.Intent)
	wanted := make(map[string]externalPathChange, len(hosts))
	targets := make(map[string]DNSIntent)
	for _, h := range hosts {
		c, ok := changes[h]
		if !ok {
			return CompileRequest{}, fmt.Errorf("%w: missing path change for %s", bad, h)
		}
		wanted[h], targets[c.target.Target] = c, DNSIntent{}
	}
	for _, d := range prior.DNS {
		if _, ok := targets[d.Hostname]; ok && d.Type == "FUGUE_ROUTE" && d.RecordKind == model.EdgeDNSRecordKindCustomDomainTarget {
			targets[d.Hostname] = d
		}
	}
	intent := req.Intent
	intent.Routes = make([]RouteIntent, 0, len(req.Intent.Routes))
	seen := make(map[string]int, len(wanted))
	for _, r := range req.Intent.Routes {
		if c, ok := wanted[r.Hostname]; ok {
			owner, exists := c.before[r.PathPrefix]
			if !exists || owner != r.AppID || c.target.TenantID != r.TenantID {
				continue
			}
			seen[r.Hostname]++
		}
		intent.Routes = append(intent.Routes, r)
	}
	for h, c := range wanted {
		if seen[h] != len(c.before) {
			return CompileRequest{}, fmt.Errorf("%w: live previous bindings unavailable for %s", bad, h)
		}
	}
	intent.DNS = slices.Clone(intent.DNS)
	for i, d := range intent.DNS {
		old, ok := targets[d.Hostname]
		if !ok || d.Type != "FUGUE_ROUTE" || d.RecordKind != model.EdgeDNSRecordKindCustomDomainTarget {
			continue
		}
		if old.Route == nil || d.Route == nil {
			return CompileRequest{}, fmt.Errorf("%w: missing target for %s", bad, d.Hostname)
		}
		before, live := *old.Route, *d.Route
		before.Bindings, live.Bindings = nil, nil
		old.Route, d.Route = &before, &live
		if !reflect.DeepEqual(old, d) {
			return CompileRequest{}, fmt.Errorf("%w: changed target declaration for %s", bad, d.Hostname)
		}
		// Retain only the pending domains. Other users of a shared target
		// may have independently changed their bindings in this capture.
		bindings := slices.DeleteFunc(slices.Clone(intent.DNS[i].Route.Bindings), func(b DNSRouteBinding) bool {
			_, pending := wanted[b.Hostname]
			return pending
		})
		for _, b := range targets[d.Hostname].Route.Bindings {
			if _, pending := wanted[b.Hostname]; pending {
				bindings = append(bindings, b)
			}
		}
		// An empty list is the shorthand for every listed host's root.
		// If one peer still needs explicit paths, spell out retained roots.
		if len(bindings) > 0 {
			for _, h := range live.Hostnames {
				if _, ok := wanted[h]; !ok || slices.ContainsFunc(bindings, func(b DNSRouteBinding) bool { return b.Hostname == h }) {
					continue
				}
				for path, app := range wanted[h].before {
					bindings = append(bindings, DNSRouteBinding{Hostname: h, PathPrefix: path, AppID: app})
				}
			}
		}
		live.Bindings = bindings
		intent.DNS[i].Route = &live
		delete(targets, d.Hostname)
	}
	if len(targets) != 0 {
		return CompileRequest{}, bad
	}
	intent = NormalizePlatformIntent(intent)
	generation, err := PlatformIntentGeneration(intent)
	if err != nil {
		return CompileRequest{}, err
	}
	intent.Generation = generation
	req.Intent, req.RuntimeSnapshot.IntentGeneration = intent, generation
	return req, nil
}
