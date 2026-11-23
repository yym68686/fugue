package platformconfig

import (
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/routebinding"
	"fugue/internal/routeproof"
)

// A replayable host-only change, not a serving assignment, observation or LKG.
// Include the physical ingress of the selected global RR (even a held
// address) separately from the effective subjects of DNS readiness. Neither
// a routing Cell's membership nor its serving evidence may fill either set.
const HostedProposalSchema = "fugue.hosted-path-proposal/v1"

type HostedProposalAuthority struct {
	Producer     model.PlatformPublicationPrecondition `json:"producer"`
	LKGID        string                                `json:"lkg_id"`
	EvidenceHash string                                `json:"verification_evidence_hash"`
	ExpiresAt    time.Time                             `json:"expires_at"`
}

// A signed JSON timestamp is an instant, not Go's location pointer or
// monotonic clock metadata. Compare original expiry without renewing it.
func (a HostedProposalAuthority) Same(b HostedProposalAuthority) bool {
	return a.Producer == b.Producer && a.LKGID == b.LKGID && a.EvidenceHash == b.EvidenceHash && a.ExpiresAt.Equal(b.ExpiresAt)
}

type HostedScopeProposal struct {
	Schema    string                              `json:"schema"`
	Host      string                              `json:"host"`
	Base      PreviousTrafficPublicationReference `json:"base"`
	Authority HostedProposalAuthority             `json:"authority"`
	Before    HostedDispatchRows                  `json:"before"`
	Prepared  HostedDispatchRows                  `json:"prepared"`
	After     HostedDispatchRows                  `json:"after"`
	Records   []DNSIntent                         `json:"records"`
	Physical  []HostedAddressHold                 `json:"physical"`
	Eligible  []HostedAddressHold                 `json:"eligible"`
	Hold      []HostedHoldRequirements            `json:"hold"`
	Changes   []HostedReadinessTargetEdit         `json:"changes"`
	// Only host-owned requirements, not availability observations, leases
	// or snapshots of a different tenant's routes.
	BaseProbes []DNSReadinessProbe `json:"base_probes"`
	HoldProbes []DNSReadinessProbe `json:"hold_probes"`
	PathProbes []DNSReadinessProbe `json:"path_probes"`
	// Optional, only when a held measured primary must yield to an already
	// declared, independently authorized deterministic fallback.
	Fallbacks []HostedQueryFallback `json:"query_fallbacks,omitempty"`
}

func globalProposalArtifact(a model.PlatformArtifact, kind string) (model.PlatformArtifact, error) {
	if a.ArtifactKind != kind || a.Scope.Key != GlobalScopeKey || a.ScopeKey != "" && a.ScopeKey != GlobalScopeKey {
		return model.PlatformArtifact{}, errors.New("host proposal requires global compiler inputs")
	}
	// The compiler sets Scope.Key; the store sets redundant ScopeKey on
	// persistence. Normalizing this local copy does not give a draft a fence.
	a.ScopeKey = GlobalScopeKey
	return a, nil
}

// Called only with an authenticated global predecessor and two products of
// one compiler invocation (old path shape and actual desired path shape).
// No addresses, node lists, TLS ownership, or upstreams from a request.
func BuildHostedScopeProposals(src HostedAddressSource, stage CompileResult, capture *HostedTransitionCapture) ([]HostedScopeProposal, error) {
	bad := errors.New("host-scoped change cannot be derived from the selected global source and its compiled edit")
	if capture == nil || len(capture.Hosts) == 0 || len(capture.Hosts) > 64 || !slices.IsSorted(capture.Hosts) || src.Publication.Validate() != nil ||
		src.PriorRoute == nil || src.PriorTLS == nil || src.PriorRoute.ID != src.Publication.RouteArtifactID || src.PriorRoute.ContentHash != src.Publication.RouteArtifactDigest ||
		src.PriorTLS.ID != src.Publication.TLSArtifactID || src.PriorTLS.ContentHash != src.Publication.TLSArtifactDigest || src.PriorIntent.Scope != GlobalScopeKey || src.Prior.Policy.Scope != GlobalScopeKey {
		return nil, bad
	}
	want := capture.Desired
	type side struct {
		dns    DNSArtifactPayload
		routes model.EdgeRouteIntentSnapshot
		certs  []model.EdgeTLSAllowlistEntry
	}
	project := func(route, tls model.PlatformArtifact, payload DNSArtifactPayload) (side, error) {
		a, e := globalProposalArtifact(route, model.PlatformArtifactKindEdgeRouteBundle)
		if e != nil {
			return side{}, e
		}
		t, e := globalProposalArtifact(tls, model.PlatformArtifactKindCaddyRouteConfig)
		if e != nil {
			return side{}, e
		}
		r, e := ProjectRouteArtifact(a)
		if e != nil {
			return side{}, e
		}
		var certs []model.EdgeTLSAllowlistEntry
		b, e := json.Marshal(t.Content["tls_allowlist"])
		if e != nil || json.Unmarshal(b, &certs) != nil || len(certs) == 0 {
			return side{}, bad
		}
		if payload.Policy.Scope != GlobalScopeKey || payload.ReadinessPlan == nil {
			return side{}, bad
		}
		return side{payload, r, certs}, nil
	}
	compiled := func(v CompileResult) (side, error) {
		a, e := globalProposalArtifact(v.DNSArtifact, model.PlatformArtifactKindDNSAnswerBundle)
		if e != nil {
			return side{}, e
		}
		payload, e := DecodeDNSArtifact(a)
		if e != nil {
			return side{}, e
		}
		if v.ReleaseSet.Scope != GlobalScopeKey || v.ReleaseSet.PublicationRole != "" || v.Lineage != payload.Lineage {
			return side{}, bad
		}
		return project(v.RouteArtifact, v.TLSArtifact, payload)
	}
	x, e := project(*src.PriorRoute, *src.PriorTLS, src.Prior)
	if e != nil {
		return nil, e
	}
	y, e := compiled(stage)
	if e != nil {
		return nil, e
	}
	z, e := compiled(want)
	if e != nil {
		return nil, e
	}
	if !reflect.DeepEqual(y.dns.Policy.DNSReadiness, z.dns.Policy.DNSReadiness) {
		return nil, bad
	}
	var intent PlatformIntent
	raw, e := json.Marshal(want.IntentArtifact.Content)
	if e != nil || json.Unmarshal(raw, &intent) != nil || intent.Scope != GlobalScopeKey {
		return nil, bad
	}
	wire := func(v DNSArtifactPayload, host, kind string) (DNSIntent, bool) {
		for _, r := range v.Records {
			if r.Hostname == host && r.Type == kind {
				return r, true
			}
		}
		return DNSIntent{}, false
	}
	makeSet := func(v []HostedAddressHold) ([]HostedAddressHold, error) {
		slices.SortFunc(v, func(a, b HostedAddressHold) int {
			if a.EdgeID != b.EdgeID {
				return strings.Compare(a.EdgeID, b.EdgeID)
			}
			if a.EdgeGroupID != b.EdgeGroupID {
				return strings.Compare(a.EdgeGroupID, b.EdgeGroupID)
			}
			return strings.Compare(a.Address, b.Address)
		})
		for i, n := range v {
			if i > 0 && v[i-1] == n {
				return nil, bad
			}
		}
		return v, nil
	}
	var out []HostedScopeProposal
	for i, h := range capture.Hosts {
		if i > 0 && capture.Hosts[i-1] == h {
			return nil, bad
		}
		p := HostedScopeProposal{Schema: HostedProposalSchema, Base: src.Publication, Host: h, Authority: src.Authority}
		owner, tenant := "", ""
		after := map[string]string{}
		for _, r := range intent.DNS {
			if r.Hostname != h || r.Type != "FUGUE_ROUTE" || r.RecordKind != "hosted" {
				continue
			}
			if owner != "" || r.AppID == "" || r.TenantID == "" || r.Route == nil || !r.Route.ManagedHost {
				return nil, bad
			}
			owner, tenant = r.AppID, r.TenantID
			for _, b := range r.Route.Bindings {
				if b.Hostname != h || after[b.PathPrefix] != "" || b.AppID == "" {
					return nil, bad
				}
				after[b.PathPrefix] = b.AppID
			}
		}
		before := map[string]string{}
		for _, r := range src.PriorIntent.Routes {
			if r.Hostname == h {
				path := model.NormalizeAppRoutePathPrefix(r.PathPrefix)
				if before[path] != "" || r.TenantID != tenant || r.AppID == "" {
					return nil, bad
				}
				before[path] = r.AppID
			}
		}
		if owner == "" || before["/"] != owner || maps.Equal(before, after) {
			return nil, bad
		}
		p.Before, e = SelectHostedDispatchRows(h, tenant, owner, before, x.routes, x.certs)
		if e != nil {
			return nil, e
		}
		mid, e := SelectHostedDispatchRows(h, tenant, owner, before, y.routes, y.certs)
		if e != nil {
			return nil, e
		}
		p.After, e = SelectHostedDispatchRows(h, tenant, owner, after, z.routes, z.certs)
		if e != nil {
			return nil, e
		}
		if p.Before.Owner != p.After.Owner || p.Before.Owner != mid.Owner {
			return nil, bad
		}
		p.Prepared = mid
		p.Hold, e = SelectHostedHoldRequirements(h, x.dns, y.dns, src.EffectiveHolds)
		if e != nil {
			return nil, e
		}
		delta, e := PlanHostedReadinessDelta(h, y.dns.ReadinessPlan, z.dns.ReadinessPlan, y.dns.Policy.DNSReadiness, slices.Sorted(maps.Keys(before)), slices.Sorted(maps.Keys(after)))
		if e != nil {
			return nil, e
		}
		p.Changes = delta.Edits
		parents := hostedAliasParents(x.dns.Records, x.dns.ReadinessPlan)
		for _, r := range x.dns.Records {
			if r.Type != "A" && r.Type != "AAAA" || !(r.Hostname == h && r.RecordKind == "hosted" || parents[hostedRecordKey{r.Hostname, r.Type}].host == h) {
				continue
			}
			if r.TenantID != tenant || r.AppID != owner {
				return nil, bad
			}
			for _, v := range []side{y, z} {
				o, ok := wire(v.dns, r.Hostname, r.Type)
				if !ok || !sameHostedAddressSet(r.Values, o.Values) {
					return nil, bad
				}
				o.Values = r.Values
				if !reflect.DeepEqual(o, r) {
					return nil, bad
				}
			}
			p.Records = append(p.Records, r)
			if r.Hostname != h {
				continue
			}
			targets := func(v side) ([]HostedAddressHold, []string) {
				var ts []HostedAddressHold
				var ips []string
				for _, rr := range v.dns.ReadinessPlan.Records {
					if rr.Hostname == h {
						for _, t := range rr.Targets {
							if t.Family == r.Type {
								ts = append(ts, HostedAddressHold{t.EdgeID, t.EdgeGroupID, t.Address})
								ips = append(ips, t.Address)
							}
						}
					}
				}
				return ts, ips
			}
			phys, ips := targets(x)
			if !sameHostedAddressSet(ips, r.Values) {
				return nil, bad
			}
			eligible, _ := targets(y)
			desired, _ := targets(z)
			l, e := makeSet(eligible)
			if e != nil {
				return nil, e
			}
			m, e := makeSet(desired)
			if e != nil || !slices.Equal(l, m) {
				return nil, bad
			}
			p.Physical = append(p.Physical, phys...)
			p.Eligible = append(p.Eligible, l...)
		}
		if len(p.Physical) == 0 || len(p.Eligible) == 0 || len(p.Eligible) >= len(p.Physical) {
			return nil, bad
		}
		p.Physical, e = makeSet(p.Physical)
		if e != nil {
			return nil, e
		}
		p.Eligible, e = makeSet(p.Eligible)
		if e != nil {
			return nil, e
		}
		for _, v := range []struct {
			s    side
			rows HostedDispatchRows
			dst  *[]DNSReadinessProbe
		}{{x, p.Before, &p.BaseProbes}, {y, mid, &p.HoldProbes}, {z, p.After, &p.PathProbes}} {
			paths := map[string]model.EdgeRouteIntent{}
			for _, r := range v.rows.Routes {
				paths[r.PathPrefix] = r
			}
			for _, probe := range v.s.dns.ReadinessPlan.Probes {
				if probe.Hostname == h {
					r, ok := paths[probe.Path]
					if !ok {
						return nil, bad
					}
					d, e := routeproof.Digest(routebinding.FromIntent(r, probe.EdgeGroupID))
					if e != nil || d != probe.RouteDigest {
						return nil, errors.Join(bad, errors.New("probe differs from its own route projection"))
					}
					*v.dst = append(*v.dst, probe)
				}
			}
			if len(*v.dst) == 0 {
				return nil, bad
			}
		}
		// The desired compiler might attach a target's requirements to a
		// different record. The reachability diff is not ownership proof.
		owned := map[string]bool{}
		for _, r := range p.Records {
			owned[r.Hostname] = true
		}
		probeSet := func(ps []DNSReadinessProbe) map[string]bool {
			out := map[string]bool{}
			for _, pr := range ps {
				out[pr.ID] = true
			}
			return out
		}
		baseIDs, holdIDs, pathIDs := probeSet(p.BaseProbes), probeSet(p.HoldProbes), probeSet(p.PathProbes)
		within := func(ids []string, allowed map[string]bool) bool {
			return len(ids) > 0 && !slices.ContainsFunc(ids, func(id string) bool { return !allowed[id] })
		}
		for _, prep := range p.Hold {
			if !owned[prep.Record] {
				return nil, bad
			}
			for _, t := range prep.Before.Targets {
				if !within(t.ProbeIDs, baseIDs) {
					return nil, bad
				}
			}
			for _, t := range prep.After.Targets {
				if !within(t.ProbeIDs, holdIDs) {
					return nil, bad
				}
			}
		}
		for _, edit := range p.Changes {
			if !owned[edit.Record] || !within(edit.Before, holdIDs) || !within(edit.After, pathIDs) {
				return nil, bad
			}
		}
		p.Fallbacks, e = captureHostedFallbacks(p, x.dns, y.dns, z.dns)
		if e != nil {
			return nil, e
		}
		raw, e := json.Marshal(p)
		if e != nil || len(raw) > 1<<20 {
			return nil, bad
		}
		out = append(out, p)
	}
	return out, nil
}
