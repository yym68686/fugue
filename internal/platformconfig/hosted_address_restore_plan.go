package platformconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/netip"
	"reflect"
	"slices"

	"fugue/internal/model"
)

// HostedDeclarationRestoreMetadata marks a dedicated publication, never a
// scoped host route operation. Both the release and DNS child sign this pin.
const HostedDeclarationRestoreMetadata = "hosted_declaration_restore_v1"

// HostedDeclarationRestorePlan is an explicit operator policy, not inferred
// from an address, current route intent, or the latest historical artifact.
// History contributes identities and a declaration, not serving authority.
type HostedDeclarationRestorePlan struct {
	History PreviousTrafficPublicationReference `json:"history"`
	Target  string                              `json:"target"`
	Family  string                              `json:"family"`
	Held    HostedAddressHold                   `json:"held"`
}

// ParseHostedDeclarationRestorePlan reads one optional deployment setting.
// The normal API and configuration producer have no history recovery default.
func ParseHostedDeclarationRestorePlan(raw string) (*HostedDeclarationRestorePlan, error) {
	if raw == "" {
		return nil, nil
	}
	var p HostedDeclarationRestorePlan
	bad := errors.New("invalid FUGUE_HOSTED_DNS_DECLARATION_RESTORE")
	dec := json.NewDecoder(bytes.NewBufferString(raw))
	dec.DisallowUnknownFields()
	if len(raw) > 8192 || dec.Decode(&p) != nil || dec.Decode(new(any)) != io.EOF || p.Validate() != nil {
		return nil, bad
	}
	return &p, nil
}

func (p HostedDeclarationRestorePlan) Validate() error {
	ip, err := netip.ParseAddr(p.Held.Address)
	if p.History.Validate() != nil || !validDNSConsumerZone(p.Target) ||
		p.Family != "A" && p.Family != "AAAA" || err != nil || ip.String() != p.Held.Address ||
		!PublicDNSFlattenIP(ip) || (p.Family == "A") != ip.Is4() || !validDNSConsumerIdentity(p.Held.EdgeID) ||
		!platformRouteArtifactGroupID.MatchString(p.Held.EdgeGroupID) {
		return errors.New("invalid shared DNS declaration restore plan")
	}
	return nil
}

func HostedDeclarationRestoreAuthority(a model.PlatformArtifact) (*HostedDeclarationRestorePlan, error) {
	raw, ok := a.Metadata[HostedDeclarationRestoreMetadata]
	if !ok {
		return nil, nil
	}
	var plan HostedDeclarationRestorePlan
	ref, _, err := HostedAddressAuthority(a)
	bad := errors.New("invalid signed DNS declaration restore authority")
	if len(raw) > 8192 || err != nil || ref == nil ||
		json.Unmarshal([]byte(raw), &plan) != nil || plan.Validate() != nil ||
		plan.History.FencingToken >= ref.FencingToken || plan.History.ReleaseSetID == ref.ReleaseSetID {
		return nil, bad
	}
	canonical, err := json.Marshal(plan)
	if err != nil || string(canonical) != raw {
		return nil, bad
	}
	return &plan, nil
}

type hostedDeclarationRestoreSource struct {
	history DNSArtifactPayload
	plan    HostedDeclarationRestorePlan
}

// CompileHostedDeclarationRestore is not reachable from the generic compile
// API. Callers authenticate the historical parent and all its children, and
// read current positive authority through the regular producer fence.
func CompileHostedDeclarationRestore(req CompileRequest, source HostedAddressSource, plan HostedDeclarationRestorePlan, history DNSArtifactPayload) (CompileResult, error) {
	if plan.Validate() != nil || source.Publication.Validate() != nil || plan.History.FencingToken >= source.Publication.FencingToken ||
		plan.History.ReleaseSetID == source.Publication.ReleaseSetID || source.PriorRoute == nil || source.PriorTLS == nil {
		return CompileResult{}, errors.New("restore needs separate current and historical full publications")
	}
	if source.PriorRuntime.CapturedAt == nil || source.PriorRuntime.CapturedAt.IsZero() ||
		source.PriorRuntime.IntentGeneration != source.PriorIntent.Generation || source.PriorRuntime.PolicyGeneration != source.Prior.Policy.Generation ||
		!sameDeclarationHolds(source.PriorHolds, source.EffectiveHolds) {
		return CompileResult{}, errors.New("shared DNS restore needs the exact selected compiler input and unchanged effective holds")
	}
	if len(source.PriorRuntime.RetainedTrafficPartitions) != 0 {
		return CompileResult{}, errors.New("shared DNS restore requires an ordinary baseline without retained traffic partitions")
	}
	priorInput, digest := source.PriorRuntime, source.Prior.Lineage.InputSnapshotDigest
	if source.Prior.Lineage.RuntimeInputDigest != "" {
		priorInput, digest = ConfigurationRuntimeInput(priorInput), source.Prior.Lineage.RuntimeInputDigest
	}
	if got, err := RuntimeSnapshotDigest(priorInput); err != nil || digest == "" || got != digest {
		return CompileResult{}, errors.New("shared DNS restore compiler input differs from selected lineage")
	}
	source.restoring = &hostedDeclarationRestoreSource{history, plan}
	// The restore is deliberately compiled from the selected full's exact
	// signed intent, policy and compact runtime input. The caller's fresh
	// projection may contain an unrelated ranking/policy refresh; accepting it
	// here would turn a declaration-only recovery into a whole DNS rewrite.
	formal := CompileRequest{OmitOriginalInput: req.OmitOriginalInput, CreatedAt: req.CreatedAt,
		Intent: source.PriorIntent, Policy: source.Prior.Policy, RuntimeSnapshot: priorInput}
	formal.RuntimeSnapshot.HostedAddressReservations = nil
	// These are producer-originated publication bindings, not runtime
	// observations. Do not carry arbitrary Facts from either invocation.
	formal.RuntimeSnapshot.Facts = map[string]any{}
	for _, k := range []string{"configuration_producer", "offline_edge_recovery"} {
		if v, ok := req.RuntimeSnapshot.Facts[k]; ok {
			formal.RuntimeSnapshot.Facts[k] = v
		}
	}
	got, err := CompileWithHostedAddressSource(formal, source)
	if err != nil {
		return CompileResult{}, err
	}
	nextDNS, err := globalProposalArtifact(got.DNSArtifact, model.PlatformArtifactKindDNSAnswerBundle)
	if err != nil {
		return CompileResult{}, err
	}
	next, err := DecodeDNSArtifact(nextDNS)
	if err != nil {
		return CompileResult{}, err
	}
	if err = ValidateHostedDeclarationRestoreDelta(source.Prior, history, next, plan, source.PriorHolds, source.EffectiveHolds,
		*source.PriorRoute, *source.PriorTLS, got.RouteArtifact, got.TLSArtifact); err != nil {
		return CompileResult{}, err
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return CompileResult{}, err
	}
	got.DNSArtifact.Metadata[HostedDeclarationRestoreMetadata] = string(raw)
	got.ReleaseArtifact.Metadata[HostedDeclarationRestoreMetadata] = string(raw)
	return got, nil
}

func sameDeclarationHolds(a, b []HostedAddressHold) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[HostedAddressHold]bool, len(a))
	for _, h := range a {
		set[h] = true
	}
	for _, h := range b {
		if !set[h] {
			return false
		}
		delete(set, h)
	}
	return len(set) == 0
}

// ValidateHostedDeclarationRestoreDelta is also called in the locked serving
// transaction against the exact current full, not merely the compile inputs.
// No route, certificate, probe requirement, selector policy or unrelated
// declaration may hitch a ride on this publication. Ignore only lineage
// bookkeeping that a new signed ReleaseSet must regenerate.
func ValidateHostedDeclarationRestoreDelta(previous, history, next DNSArtifactPayload, plan HostedDeclarationRestorePlan, priorHolds, nextHolds []HostedAddressHold, priorRoute, priorTLS, nextRoute, nextTLS model.PlatformArtifact) error {
	want, err := RestoreExternalHeldDeclaration(previous, history, plan.Target, plan.Family, plan.Held, priorHolds, nextHolds)
	if err != nil {
		return err
	}
	want.Lineage, next.Lineage = Lineage{}, Lineage{}
	want.Generation, next.Generation = "", ""
	want.Policy.Generation, next.Policy.Generation = "", ""
	if !reflect.DeepEqual(want.Records, next.Records) {
		return errors.New("shared DNS restore changed another declaration or did not restore the pinned RR")
	}
	for _, field := range []struct {
		name      string
		want, got any
	}{
		{"DNS query selectors", want.QueryViews, next.QueryViews},
		{"DNS readiness requirements", want.ReadinessPlan, next.ReadinessPlan},
		{"DNS consumer views", want.ConsumerViews, next.ConsumerViews},
		{"DNS policy", want.Policy, next.Policy},
	} {
		if !reflect.DeepEqual(field.want, field.got) {
			return errors.New("shared DNS restore also changes " + field.name)
		}
	}
	if !reflect.DeepEqual(want, next) {
		return errors.New("shared DNS restore also changes the DNS artifact envelope")
	}
	if !sameDeclarationRestoreDispatch(priorRoute, nextRoute, model.PlatformArtifactKindEdgeRouteBundle) {
		return errors.New("shared DNS restore also changes the serving route configuration")
	}
	if !sameDeclarationRestoreDispatch(priorTLS, nextTLS, model.PlatformArtifactKindCaddyRouteConfig) {
		return errors.New("shared DNS restore also changes the serving TLS configuration")
	}
	return nil
}

func sameDeclarationRestoreDispatch(prior, next model.PlatformArtifact, kind string) bool {
	if prior.ArtifactKind != kind || next.ArtifactKind != kind {
		return false
	}
	left, right := maps.Clone(prior.Content), maps.Clone(next.Content)
	for _, key := range []string{"generation", "lineage"} {
		delete(left, key)
		delete(right, key)
	}
	before, beforeOK := left["policy"].(map[string]any)
	after, afterOK := right["policy"].(map[string]any)
	if !beforeOK || !afterOK {
		return false
	}
	left["policy"], right["policy"] = maps.Clone(before), maps.Clone(after)
	delete(left["policy"].(map[string]any), "generation")
	delete(right["policy"].(map[string]any), "generation")
	return reflect.DeepEqual(left, right)
}

func appendHostedDeclarationRecovery(next DNSArtifactPayload, source HostedAddressSource, reservations []HostedAddressReservation) ([]HostedAddressReservation, error) {
	restore := source.restoring
	if restore == nil {
		return reservations, nil
	}
	p := restore.plan
	r, err := BindRestoredExternalDeclaration(source, restore.history, p.Target, p.Family, p.Held)
	if err != nil {
		return nil, err
	}
	// Re-prove the same roster against the fresh compile. Fail closed if
	// this would replace an existing reservation instead of adding one.
	_, projected, err := restoreExternalHeldDeclaration(next, restore.history, p.Target, p.Family, p.Held, source.PriorHolds, source.EffectiveHolds)
	if err != nil || !reflect.DeepEqual(projected.Record, r.Record) || !reflect.DeepEqual(projected.Physical, r.Physical) ||
		!reflect.DeepEqual(projected.Shared, r.Shared) || slices.ContainsFunc(reservations, func(prior HostedAddressReservation) bool {
		return prior.Record.Hostname == r.Record.Hostname && prior.Record.Type == r.Record.Type
	}) {
		return nil, errors.New("shared DNS restore does not match the fresh effective projection")
	}
	return append(reservations, r), nil
}
