package routeartifact

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
)

// ComposeHostedExecutorChange constructs unsigned executor inputs on top of
// the CURRENT selected groups and zones. A caller must pin the image digest
// independently and authenticate the proposal, live selections and phase.
// Neither an old signed bundle nor a successful composition is an ACK.
// Only the returned physical groups are replaced, never other group slots.
// The wire RRs and TTL never need writing for a path transition.
func ComposeHostedExecutorChange(groups map[string]model.EdgeRouteBundle, dns pc.DNSArtifactPayload, p pc.HostedScopeProposal, phase, draftDigest, imageDigest string, body []byte) (HostedPhase, error) {
	var empty HostedPhase
	bad := errors.New("host executable unit differs from the current selection or its compiled image")
	if len(body) == 0 || len(body) > maxHostedProgramBytes {
		return empty, bad
	}
	sum := sha256.Sum256(body)
	if "sha256:"+hex.EncodeToString(sum[:]) != imageDigest {
		return empty, bad
	}
	var image hostedProgramImage
	if e := json.Unmarshal(body, &image); e != nil || image.Schema != "fugue.hosted-program/v1" || image.Host != p.Host || image.Base != p.Base || image.DraftDigest != draftDigest || !reflect.DeepEqual(image.Records, p.Records) {
		return empty, bad
	}
	expect := image.Prepared
	switch phase {
	case "forward":
		expect = image.Forward
	case "prepare", "withdraw":
	default:
		return empty, bad
	}
	out := HostedPhase{Groups: map[string]HostedGroupContent{}}
	scope := map[string]bool{}
	for _, member := range p.Physical {
		scope[member.EdgeGroupID] = true
	}
	if len(scope) == 0 || len(expect.Groups) != len(scope) {
		return empty, bad
	}
	for group := range scope {
		live, ok := groups[group]
		unit, found := expect.Groups[group]
		if !ok || !found || live.EdgeGroupID != group {
			return empty, bad
		}
		// The predecessor CAS is host-only. B can have the same app ID and
		// a separately published stable release without invalidating A.
		next, e := ComposeHostedGroupEdit(live, p, phase)
		if e != nil {
			return empty, e
		}
		live.Routes, live.CachePolicies, live.TLSAllowlist = next.Routes, next.CachePolicies, next.TLSAllowlist
		rows, e := hostedGroupSource(live, p)
		if e != nil || !reflect.DeepEqual(rows.Routes, unit.Routes) || !reflect.DeepEqual(rows.TLSAllowlist, unit.TLSAllowlist) || !reflect.DeepEqual(rows.CachePolicies, unit.CachePolicies) {
			return empty, bad
		}
		out.Groups[group] = next
	}
	// Start at the effective DNS requirements, never a recapture of every
	// domain's physical endpoints. Unrelated selectors remain unchanged. This function never
	// evaluates live vetoes or issues a proof for either A or B.
	merged, e := pc.ComposeHostedDNSPhase(dns, p, phase)
	if e != nil {
		return empty, e
	}
	dns.ReadinessPlan, dns.QueryViews, dns.Policy.DNSAnswerRules = &merged.Plan, merged.Views, merged.Rules
	local, e := pc.HostedDNSProgramSource(dns, p)
	if e != nil {
		return empty, e
	}
	scoped := pc.HostedDNSContent{Plan: *local.ReadinessPlan, Views: hostedUnitViews(p, local.QueryViews), Rules: local.Policy.DNSAnswerRules}
	if !reflect.DeepEqual(scoped, expect.DNS) {
		return empty, bad
	}
	out.DNS = merged
	return out, nil
}
