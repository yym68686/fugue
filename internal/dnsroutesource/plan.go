package dnsroutesource

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

// Context is retained inside the authenticated DNS checkpoint. It contains no
// positive readiness. Selections choose one complete publication per target.
type Context struct {
	Snapshot   model.PlatformDNSRouteSourceSnapshot `json:"snapshot"`
	Selections map[string]string                    `json:"selections"`
}
type variant struct {
	source string
	scope  string
	target platformconfig.DNSReadinessTarget
}
type Plans struct {
	Snapshot   model.PlatformDNSRouteSourceSnapshot
	Probes     map[string]platformconfig.DNSReadinessProbe
	Bindings   map[string]*model.TrafficReleaseBinding
	records    []platformconfig.DNSReadinessRecord
	targets    map[string][]variant
	targetKeys map[string][]string
}

func targetKey(host string, t platformconfig.DNSReadinessTarget) string {
	return host + "\x00" + t.EdgeID + "\x00" + t.Address
}

func Build(dns model.PlatformArtifact, snapshot model.PlatformDNSRouteSourceSnapshot, keys bundleauth.Keyring, now time.Time) (*Plans, error) {
	if err := VerifySnapshot(snapshot, dns, keys, now); err != nil {
		return nil, err
	}
	var p struct {
		Source *platformconfig.CellDNSPlanSource `json:"cell_dns_source"`
		Policy platformconfig.PolicySnapshot     `json:"policy"`
		Plan   *platformconfig.DNSReadinessPlan  `json:"readiness_plan"`
	}
	raw, _ := json.Marshal(dns.Content)
	if json.Unmarshal(raw, &p) != nil || p.Source == nil || p.Plan == nil {
		return nil, fmt.Errorf("DNS runtime source missing")
	}
	out := &Plans{Snapshot: snapshot, Probes: map[string]platformconfig.DNSReadinessProbe{}, Bindings: map[string]*model.TrafficReleaseBinding{}, targets: map[string][]variant{}, targetKeys: map[string][]string{}}
	baselines := map[string]bool{}
	records := map[string]platformconfig.DNSReadinessRecord{}
	for _, r := range p.Plan.Records {
		for _, t := range r.Targets {
			baselines[targetKey(r.Hostname, t)] = true
		}
		r.Targets = nil
		records[r.Hostname] = r
	}
	// Explicit current source policy restrictions also constrain its retained LKG.
	for _, scope := range snapshot.Scopes {
		selected := []model.PlatformDNSRouteSourcePublication{}
		for _, pub := range scope.Publications {
			if slices.Contains(pub.Selections, "full") || slices.Contains(pub.Selections, "gray") {
				selected = append(selected, pub)
			}
		}
		for _, pub := range scope.Publications {
			for _, cell := range p.Source.Intent.EdgeTopology.Cells {
				if scope.ScopeKey != platformconfig.GlobalScopeKey && scope.ScopeKey != platformconfig.AuthorityCellScope(cell.ID) {
					continue
				}
				plan, b, err := platformconfig.DNSRuntimeSourcePlan(*p.Source, p.Policy, pub, cell.ID)
				if err != nil {
					return nil, err
				}
				if err := platformconfig.RestrictDNSRuntimeSourcePlan(plan, *p.Source, pub, selected); err != nil {
					return nil, err
				}
				for _, probe := range plan.Probes {
					out.Probes[probe.ID] = probe
					out.Bindings[probe.CellPublicationDigest] = b
				}
				for _, r := range plan.Records {
					base, ok := records[r.Hostname]
					if !ok {
						continue
					}
					base.MinimumHealthyEdges = max(base.MinimumHealthyEdges, r.MinimumHealthyEdges)
					base.MinDistinctCells = max(base.MinDistinctCells, r.MinDistinctCells)
					if base.MinDistinctDomains == nil {
						base.MinDistinctDomains = map[string]int{}
					}
					for d, n := range r.MinDistinctDomains {
						base.MinDistinctDomains[d] = max(base.MinDistinctDomains[d], n)
					}
					records[r.Hostname] = base
					for _, t := range r.Targets {
						k := targetKey(r.Hostname, t)
						if !baselines[k] {
							continue
						}
						if _, exists := out.targets[k]; !exists {
							out.targetKeys[r.Hostname] = append(out.targetKeys[r.Hostname], k)
						}
						out.targets[k] = append(out.targets[k], variant{scope: scope.ScopeKey, source: scope.ScopeKey + "/" + pub.Release.ID, target: t})
					}
				}
			}
		}
	}
	for key, variants := range out.targets {
		sort.Slice(variants, func(i, j int) bool { return variants[i].source < variants[j].source })
		out.targets[key] = variants
	}

	for _, r := range records {
		out.records = append(out.records, r)
	}
	sort.Slice(out.records, func(i, j int) bool { return out.records[i].Hostname < out.records[j].Hostname })
	if len(out.targets) > 8192 || len(out.Probes) > 6*p.Policy.DNSReadiness.MaxProbes {
		return nil, fmt.Errorf("DNS source expansion exceeds bounds")
	}
	// The actual network work remains within the signed DNS probe budget.
	physical := map[string]bool{}
	for _, probe := range out.Probes {
		physical[ProbeKey(probe)] = true
	}
	if len(physical) > p.Policy.DNSReadiness.MaxProbes {
		return nil, fmt.Errorf("DNS source probe budget exceeded")
	}
	return out, nil
}
func ProbeKey(p platformconfig.DNSReadinessProbe) string {
	return p.Address + "\x00" + p.Hostname + "\x00" + p.Path + "\x00" + p.State
}
func (p *Plans) Matches(requirement platformconfig.DNSReadinessProbe, proof routeprobe.Proof) bool {
	if !platformconfig.DNSReadinessProofMatches(requirement, proof.EdgeID, proof.GroupID, proof.Digest) {
		return false
	}
	b := p.Bindings[requirement.CellPublicationDigest]
	return b != nil && reflect.DeepEqual(b, proof.TrafficRelease)
}

// Choose uses real facts only to choose a coherent publication. The returned
// requirements must still be evaluated for freshness and quorum by the caller.
func (p *Plans) Choose(ready func(string) bool) *Context {
	c := &Context{Snapshot: p.Snapshot, Selections: map[string]string{}}
	for key, variants := range p.targets {
		if len(variants) == 0 {
			continue
		}
		choice := variants[0]
		for _, v := range variants {
			if len(v.target.ProbeIDs) > 0 && func() bool {
				for _, id := range v.target.ProbeIDs {
					if !ready(id) {
						return false
					}
				}
				return true
			}() {
				choice = v
				break
			}
		}
		c.Selections[key] = choice.source
	}
	return c
}
func (p *Plans) Replay(c *Context) (*platformconfig.DNSReadinessPlan, error) {
	if c == nil || c.Snapshot.SelectionDigest != p.Snapshot.SelectionDigest || len(c.Selections) > 8192 {
		return nil, fmt.Errorf("DNS source selection invalid")
	}
	result := &platformconfig.DNSReadinessPlan{Records: []platformconfig.DNSReadinessRecord{}, Probes: []platformconfig.DNSReadinessProbe{}}
	seen, used := map[string]bool{}, map[string]bool{}
	for _, base := range p.records {
		r := base
		r.Targets = []platformconfig.DNSReadinessTarget{}
		for _, key := range p.targetKeys[r.Hostname] {
			variants := p.targets[key]
			selection, exists := c.Selections[key]
			if len(variants) == 0 {
				if exists {
					return nil, fmt.Errorf("unavailable DNS source selected")
				}
				continue
			}
			found := false
			for _, v := range variants {
				if v.source == selection {
					r.Targets = append(r.Targets, v.target)
					for _, id := range v.target.ProbeIDs {
						used[id] = true
					}
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("DNS target source not authorized")
			}
			seen[key] = true
		}
		sort.Slice(r.Targets, func(i, j int) bool { return targetKey(r.Hostname, r.Targets[i]) < targetKey(r.Hostname, r.Targets[j]) })
		result.Records = append(result.Records, r)
	}
	if len(seen) != len(c.Selections) {
		return nil, fmt.Errorf("extra DNS source selection")
	}
	for id := range used {
		result.Probes = append(result.Probes, p.Probes[id])
	}
	sort.Slice(result.Probes, func(i, j int) bool { return result.Probes[i].ID < result.Probes[j].ID })
	return result, nil
}

// A DNS configuration change can authorize new policies, but cannot roll back
// previously observed ledger versions within the same routing scope/lane.
func Monotonic(previous, next model.PlatformDNSRouteSourceSnapshot) bool {
	for _, old := range previous.Scopes {
		if !slices.ContainsFunc(next.Scopes, func(scope model.PlatformDNSRouteSourceScope) bool { return scope.ScopeKey == old.ScopeKey }) {
			continue
		}
		for _, lane := range old.Lanes {
			found := false
			for _, scope := range next.Scopes {
				if scope.ScopeKey != old.ScopeKey {
					continue
				}
				for _, n := range scope.Lanes {
					if n.ReleaseChannel == lane.ReleaseChannel {
						found = true
						if n.Version < lane.Version || n.FencingToken < lane.FencingToken || n.Version == lane.Version && !reflect.DeepEqual(n, lane) {
							return false
						}
					}
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

// ProofDeadline bounds retained-only LKG observations even when the control
// plane is unavailable. Active gray/full authority uses the checkpoint bound.
func (c *Context) ProofDeadline(binding *model.TrafficReleaseBinding) time.Time {
	if c == nil || binding == nil {
		return time.Time{}
	}
	for _, scope := range c.Snapshot.Scopes {
		for _, p := range scope.Publications {
			if p.Release.ID == binding.ReleaseID && p.Parent.ScopeKey == binding.ScopeKey && len(p.Selections) == 1 && p.Selections[0] == "lkg" && p.LKG != nil {
				return p.LKG.ExpiresAt
			}
		}
	}
	return time.Time{}
}
