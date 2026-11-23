package routeartifact

import (
	"errors"
	"slices"
	"sort"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
)

// A recipient is a physical member, not just a country/group. DNS
// permissions are per consumer and zone, never all views of the same name.
type HostedRecipient struct {
	Component string `json:"component"`
	Kind      string `json:"kind"`
	Node      string `json:"node"`
	Group     string `json:"group"`
	Zone      string `json:"zone,omitempty"`
}

type HostedRecipientInput struct {
	Routes  []model.EdgeRouteBinding      `json:"routes,omitempty"`
	Cache   []model.CachePolicy           `json:"cache_policies,omitempty"`
	TLS     []model.EdgeTLSAllowlistEntry `json:"tls,omitempty"`
	Records []pc.DNSIntent                `json:"records,omitempty"`
	DNS     *pc.HostedDNSContent          `json:"dns,omitempty"`
}

// Project a compiler-owned phase into exact individual recipient contents.
// The caller must bind each recipient independently to a currently enrolled
// identity. This is NOT a heartbeat or activation. Held ingresses still need
// routes and TLS; they do not become eligible DNS answers by being listed.
func HostedRecipientInputs(p pc.HostedScopeProposal, phase HostedPhase) (map[HostedRecipient]HostedRecipientInput, error) {
	bad := errors.New("host executable content cannot be partitioned among its declared recipients")
	out := map[HostedRecipient]HostedRecipientInput{}
	groups := map[string]bool{}
	nodes := map[string]string{}
	if p.Schema != pc.HostedProposalSchema || p.Host == "" || p.Before.Owner.Hostname != p.Host || len(phase.Groups) == 0 || len(p.Physical) > 4096 {
		return nil, bad
	}
	for _, n := range p.Physical {
		rows, ok := phase.Groups[n.EdgeGroupID]
		if !ok || n.EdgeID == "" || n.EdgeGroupID == "" || len(rows.Routes) == 0 || len(rows.TLSAllowlist) != 1 || rows.TLSAllowlist[0] != p.Before.Owner {
			return nil, bad
		}
		if group, ok := nodes[n.EdgeID]; ok {
			if group != n.EdgeGroupID {
				return nil, bad
			}
			continue
		}
		nodes[n.EdgeID] = n.EdgeGroupID
		groups[n.EdgeGroupID] = true
		for _, r := range rows.Routes {
			if r.Hostname != p.Host || r.TenantID != p.Before.Owner.TenantID || r.EdgeGroupID != n.EdgeGroupID {
				return nil, bad
			}
		}
		out[HostedRecipient{model.PlatformConsumerComponentEdgeWorker, model.PlatformArtifactKindEdgeRouteBundle, n.EdgeID, n.EdgeGroupID, ""}] = HostedRecipientInput{Routes: rows.Routes, Cache: rows.CachePolicies}
		out[HostedRecipient{model.PlatformConsumerComponentEdgeWorker, model.PlatformArtifactKindCaddyRouteConfig, n.EdgeID, n.EdgeGroupID, ""}] = HostedRecipientInput{TLS: rows.TLSAllowlist}
	}
	if len(groups) != len(phase.Groups) || len(phase.DNS.Views) == 0 {
		return nil, bad
	}
	type key struct{ host, typ string }
	records := map[key]pc.DNSIntent{}
	for _, r := range p.Records {
		k := key{r.Hostname, r.Type}
		if _, dup := records[k]; dup || r.AppID != p.Before.Owner.AppID || r.TenantID != p.Before.Owner.TenantID {
			return nil, bad
		}
		records[k] = r
	}
	byHost := map[string]pc.DNSReadinessRecord{}
	for _, r := range phase.DNS.Plan.Records {
		if _, dup := byHost[r.Hostname]; dup {
			return nil, bad
		}
		byHost[r.Hostname] = r
	}
	probes := map[string]pc.DNSReadinessProbe{}
	for _, r := range phase.DNS.Plan.Probes {
		if r.Hostname != p.Host || r.ID == "" {
			return nil, bad
		}
		if _, dup := probes[r.ID]; dup {
			return nil, bad
		}
		probes[r.ID] = r
	}
	type ruleKey struct {
		node string
		k    key
	}
	rules := map[ruleKey]pc.DNSAnswerRule{}
	for _, r := range phase.DNS.Rules {
		k := ruleKey{r.NodeID, key{r.Hostname, r.Type}}
		if _, dup := rules[k]; dup {
			return nil, bad
		}
		rules[k] = r
	}
	for _, v := range phase.DNS.Views {
		who := HostedRecipient{model.PlatformConsumerComponentDNSServer, model.PlatformArtifactKindDNSAnswerBundle, v.NodeID, v.EdgeGroupID, v.Zone}
		if _, dup := out[who]; dup || v.Zone == "" || v.NodeID == "" || v.EdgeGroupID == "" || len(v.Records) == 0 {
			return nil, bad
		}
		item := HostedRecipientInput{DNS: &pc.HostedDNSContent{Views: []pc.DNSQueryView{v}}}
		families := map[string]map[string]bool{}
		for _, q := range v.Records {
			k := key{q.Name, q.Type}
			r, ok := records[k]
			rule, has := rules[ruleKey{v.NodeID, k}]
			if !ok || !has || q.AppID != r.AppID || q.TenantID != r.TenantID {
				return nil, bad
			}
			if families[k.host] == nil {
				families[k.host] = map[string]bool{}
			}
			if families[k.host][k.typ] {
				return nil, bad
			}
			families[k.host][k.typ] = true
			item.Records = append(item.Records, r)
			item.DNS.Rules = append(item.DNS.Rules, rule)
		}
		needed := map[string]bool{}
		for host, fams := range families {
			r, ok := byHost[host]
			if !ok {
				return nil, bad
			}
			r.Targets = slices.DeleteFunc(slices.Clone(r.Targets), func(target pc.DNSReadinessTarget) bool { return !fams[target.Family] })
			if len(r.Targets) == 0 {
				return nil, bad
			}
			for _, target := range r.Targets {
				for _, id := range target.ProbeIDs {
					if _, ok := probes[id]; !ok {
						return nil, bad
					}
					needed[id] = true
				}
			}
			item.DNS.Plan.Records = append(item.DNS.Plan.Records, r)
		}
		sort.Slice(item.DNS.Plan.Records, func(i, j int) bool { return item.DNS.Plan.Records[i].Hostname < item.DNS.Plan.Records[j].Hostname })
		for _, probe := range phase.DNS.Plan.Probes {
			if needed[probe.ID] {
				item.DNS.Plan.Probes = append(item.DNS.Plan.Probes, probe)
			}
		}
		out[who] = item
	}
	return out, nil
}
