package platformconfig

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/netip"
	"slices"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

type DNSAnswerRule struct {
	NodeID                string   `json:"node_id"`
	Hostname              string   `json:"hostname"`
	Type                  string   `json:"type"`
	SelectionMode         string   `json:"selection_mode"`
	ScopedSelectionMode   string   `json:"scoped_selection_mode,omitempty"`
	PreferredEdgeGroups   []string `json:"preferred_edge_groups,omitempty"`
	FallbackEdgeGroups    []string `json:"fallback_edge_groups,omitempty"`
	TTLSeconds            int      `json:"ttl_seconds"`
	ECSEnabled            bool     `json:"ecs_enabled"`
	ExplorationPercent    int      `json:"exploration_percent"`
	SwitchCooldownSeconds int      `json:"switch_cooldown_seconds"`
}

type DNSSelectionCandidate struct {
	IP             string             `json:"ip"`
	EdgeID         string             `json:"edge_id"`
	EdgeGroupID    string             `json:"edge_group_id"`
	Country        string             `json:"country,omitempty"`
	Region         string             `json:"region,omitempty"`
	Priority       int                `json:"priority,omitempty"`
	Weight         int                `json:"weight,omitempty"`
	Score          float64            `json:"score,omitempty"`
	TrafficClass   string             `json:"traffic_class,omitempty"`
	Reason         string             `json:"reason,omitempty"`
	ScoreBreakdown map[string]float64 `json:"score_breakdown,omitempty"`
}

type DNSSelectionScope struct {
	ScopeKey            string                  `json:"scope_key"`
	Country             string                  `json:"country,omitempty"`
	Region              string                  `json:"region,omitempty"`
	ASN                 string                  `json:"asn,omitempty"`
	SelectedEdgeGroupID string                  `json:"selected_edge_group_id,omitempty"`
	CooldownUntil       time.Time               `json:"cooldown_until,omitempty"`
	Reason              string                  `json:"reason,omitempty"`
	Candidates          []DNSSelectionCandidate `json:"candidates"`
}

type DNSSelectionObservation struct {
	NodeID                    string                  `json:"node_id"`
	Hostname                  string                  `json:"hostname"`
	Type                      string                  `json:"type"`
	SourceGeneration          string                  `json:"source_generation"`
	SourceDigest              string                  `json:"source_digest"`
	ObservedAt                time.Time               `json:"observed_at"`
	SelectedEdgeGroupID       string                  `json:"selected_edge_group_id,omitempty"`
	ShadowSelectedEdgeGroupID string                  `json:"shadow_selected_edge_group_id,omitempty"`
	RankingVersion            string                  `json:"ranking_version,omitempty"`
	RankingScope              string                  `json:"ranking_scope,omitempty"`
	Reason                    string                  `json:"reason,omitempty"`
	ShadowReason              string                  `json:"shadow_reason,omitempty"`
	Weight                    int                     `json:"weight,omitempty"`
	Candidates                []DNSSelectionCandidate `json:"candidates"`
	ScopedCandidates          []DNSSelectionScope     `json:"scoped_candidates,omitempty"`
}

// Query views are authorized selection inputs, separate from finite historical
// address leases. Dynamic candidates contain no positive readiness until joined
// with independently collected exact route/TLS facts by the consumer.
type DNSQueryView struct {
	NodeID      string                `json:"node_id"`
	EdgeGroupID string                `json:"edge_group_id"`
	Zone        string                `json:"zone"`
	Records     []model.EdgeDNSRecord `json:"records"`
}

func dnsQueryKey(node, hostname, kind string) string { return node + "\x00" + hostname + "\x00" + kind }
func dnsSelectionModeValid(mode string) bool {
	return slices.Contains([]string{"geo", "latency_aware", "global", "weighted", "pinned", "disabled"}, mode)
}
func ValidateDNSAnswerRules(rules []DNSAnswerRule) error {
	if len(rules) > 20000 {
		return fmt.Errorf("too many DNS answer rules")
	}
	seen := map[string]bool{}
	for _, r := range rules {
		key := dnsQueryKey(r.NodeID, r.Hostname, r.Type)
		if !validDNSConsumerIdentity(r.NodeID) || !validDNSConsumerZone(r.Hostname) || (r.Type != "A" && r.Type != "AAAA") || seen[key] || !dnsSelectionModeValid(r.SelectionMode) || (r.ScopedSelectionMode != "" && !dnsSelectionModeValid(r.ScopedSelectionMode)) || r.TTLSeconds < 1 || r.TTLSeconds > 3600 || r.ExplorationPercent < 0 || r.ExplorationPercent > 50 || r.SwitchCooldownSeconds < 0 || r.SwitchCooldownSeconds > 86400 {
			return fmt.Errorf("invalid DNS answer rule")
		}
		seen[key] = true
		for _, groups := range [][]string{r.PreferredEdgeGroups, r.FallbackEdgeGroups} {
			if len(groups) > 4096 {
				return fmt.Errorf("too many DNS preferred groups")
			}
			seenGroups := map[string]bool{}
			for _, g := range groups {
				if !platformRouteArtifactGroupID.MatchString(g) || seenGroups[g] {
					return fmt.Errorf("invalid DNS preferred group")
				}
				seenGroups[g] = true
			}
		}
	}
	return nil
}
func normalizeDNSAnswerRules(in []DNSAnswerRule) []DNSAnswerRule {
	out := append([]DNSAnswerRule(nil), in...)
	for i := range out {
		out[i].PreferredEdgeGroups = append([]string(nil), in[i].PreferredEdgeGroups...)
		out[i].FallbackEdgeGroups = append([]string(nil), in[i].FallbackEdgeGroups...)
	}
	sort.Slice(out, func(i, j int) bool {
		return dnsQueryKey(out[i].NodeID, out[i].Hostname, out[i].Type) < dnsQueryKey(out[j].NodeID, out[j].Hostname, out[j].Type)
	})
	return out
}
func normalizeDNSSelections(in []DNSSelectionObservation) []DNSSelectionObservation {
	out := append([]DNSSelectionObservation(nil), in...)
	clone := func(candidates []DNSSelectionCandidate) []DNSSelectionCandidate {
		out := append([]DNSSelectionCandidate(nil), candidates...)
		for i := range out {
			if candidates[i].ScoreBreakdown != nil {
				out[i].ScoreBreakdown = map[string]float64{}
				for k, v := range candidates[i].ScoreBreakdown {
					out[i].ScoreBreakdown[k] = v
				}
			}
		}
		return out
	}
	for i := range out {
		out[i].Candidates = clone(in[i].Candidates)
		out[i].ScopedCandidates = append([]DNSSelectionScope(nil), in[i].ScopedCandidates...)
		for j := range out[i].ScopedCandidates {
			out[i].ScopedCandidates[j].Candidates = clone(in[i].ScopedCandidates[j].Candidates)
		}
	}
	// Nested candidate/scope order is significant to the selector.
	sort.Slice(out, func(i, j int) bool {
		return dnsQueryKey(out[i].NodeID, out[i].Hostname, out[i].Type) < dnsQueryKey(out[j].NodeID, out[j].Hostname, out[j].Type)
	})
	return out
}

func validateDNSSelectionCandidates(in []DNSSelectionCandidate, kind string) error {
	if len(in) > 4096 {
		return fmt.Errorf("too many DNS selection candidates")
	}
	seen := map[string]bool{}
	for _, c := range in {
		ip, err := netip.ParseAddr(c.IP)
		if err != nil || ip.String() != c.IP || !PublicDNSFlattenIP(ip) || ip.Is4() != (kind == "A") || !validDNSConsumerIdentity(c.EdgeID) || !platformRouteArtifactGroupID.MatchString(c.EdgeGroupID) || seen[c.IP] || c.Priority < 0 || c.Priority > 10000 || c.Weight < 0 || c.Weight > 10000 || c.Score < 0 || c.Score > 1e12 || math.IsNaN(c.Score) || math.IsInf(c.Score, 0) || len(c.ScoreBreakdown) > 64 {
			return fmt.Errorf("invalid DNS selection candidate")
		}
		seen[c.IP] = true
		if len(c.Country) > 16 || len(c.Region) > 128 || len(c.TrafficClass) > 128 || len(c.Reason) > 2048 {
			return fmt.Errorf("DNS selection metadata exceeds limits")
		}
		for key, value := range c.ScoreBreakdown {
			if key == "" || len(key) > 128 || math.IsNaN(value) || math.IsInf(value, 0) {
				return fmt.Errorf("invalid DNS candidate score")
			}
		}
	}
	return nil
}
func selectionCandidate(c DNSSelectionCandidate) model.EdgeDNSAnswerCandidate {
	return model.EdgeDNSAnswerCandidate{IP: c.IP, EdgeID: c.EdgeID, EdgeGroupID: c.EdgeGroupID, Country: c.Country, Region: c.Region, Priority: c.Priority, Weight: c.Weight, Score: c.Score, TrafficClass: c.TrafficClass, Reason: c.Reason, ScoreBreakdown: c.ScoreBreakdown}
}

func CompileDNSQueryViews(global []DNSIntent, views []DNSConsumerView, plan *DNSReadinessPlan, snapshot RuntimeSnapshot, policy PolicySnapshot) ([]DNSQueryView, error) {
	if len(policy.DNSAnswerRules) == 0 && len(snapshot.DNSSelections) == 0 {
		return nil, nil
	}
	if err := ValidateDNSAnswerRules(policy.DNSAnswerRules); err != nil {
		return nil, err
	}
	if plan == nil || snapshot.CapturedAt == nil || len(snapshot.DNSSelections) > 20000 {
		return nil, fmt.Errorf("DNS query compilation requires readiness and fixed observations")
	}
	if err := ValidateDNSReadinessPlan(plan, policy.DNSReadiness); err != nil {
		return nil, err
	}
	rules := map[string]DNSAnswerRule{}
	for _, r := range policy.DNSAnswerRules {
		rules[dnsQueryKey(r.NodeID, r.Hostname, r.Type)] = r
	}
	facts := map[string]DNSSelectionObservation{}
	for _, f := range snapshot.DNSSelections {
		key := dnsQueryKey(f.NodeID, f.Hostname, f.Type)
		digest, err := hex.DecodeString(strings.TrimPrefix(f.SourceDigest, "sha256:"))
		if _, exists := rules[key]; !exists {
			return nil, fmt.Errorf("DNS selection lacks matching query rule")
		}
		if _, exists := facts[key]; exists || err != nil || len(digest) != 32 || len(f.SourceDigest) != 71 || !strings.HasPrefix(f.SourceDigest, "sha256:") || f.SourceGeneration == "" || f.ObservedAt.IsZero() || f.ObservedAt.After(*snapshot.CapturedAt) || f.Weight < 0 || f.Weight > 10000 || len(f.ScopedCandidates) > 256 {
			return nil, fmt.Errorf("invalid DNS selection observation")
		}
		if err := validateDNSSelectionCandidates(f.Candidates, f.Type); err != nil {
			return nil, err
		}
		scopes := map[string]bool{}
		for _, scope := range f.ScopedCandidates {
			if scope.ScopeKey == "" || len(scope.ScopeKey) > 512 || scopes[scope.ScopeKey] {
				return nil, fmt.Errorf("invalid DNS selection scope")
			}
			scopes[scope.ScopeKey] = true
			if err := validateDNSSelectionCandidates(scope.Candidates, f.Type); err != nil {
				return nil, err
			}
		}
		facts[key] = f
	}
	readiness := map[string]DNSReadinessRecord{}
	for _, r := range plan.Records {
		readiness[r.Hostname] = r
	}
	out := []DNSQueryView{}
	used := map[string]bool{}
	for _, view := range views {
		records, err := DNSConsumerViewRecords(global, views, view.NodeID, view.EdgeGroupID, view.Zone)
		if err != nil {
			return nil, err
		}
		query := DNSQueryView{NodeID: view.NodeID, EdgeGroupID: view.EdgeGroupID, Zone: view.Zone, Records: []model.EdgeDNSRecord{}}
		for _, r := range records {
			record := model.EdgeDNSRecord{Name: r.Hostname, Type: r.Type, Values: append([]string(nil), r.Values...), TTL: r.TTL, RecordKind: r.RecordKind, Status: r.Status, StatusReason: r.StatusReason, AppID: r.AppID, TenantID: r.TenantID, EdgeGroupID: r.EdgeGroupID, FallbackEdgeGroupID: r.FallbackEdgeGroupID, ValueExpirations: r.ValueExpirations}
			ready, dynamic := readiness[r.Hostname]
			key := dnsQueryKey(view.NodeID, r.Hostname, r.Type)
			if dynamic && (r.Type == "A" || r.Type == "AAAA") {
				rule, ok := rules[key]
				fact, observed := facts[key]
				if !ok || !observed {
					return nil, fmt.Errorf("DNS dynamic query lacks explicit rule or observation")
				}
				used[key] = true
				allowed := map[string]DNSReadinessTarget{}
				groups := []string{}
				for _, target := range ready.Targets {
					if target.Family == r.Type {
						allowed[target.Address] = target
						groups = append(groups, target.EdgeGroupID)
					}
				}
				convert := func(in []DNSSelectionCandidate) ([]model.EdgeDNSAnswerCandidate, error) {
					selected := []model.EdgeDNSAnswerCandidate{}
					for _, c := range in {
						target, ok := allowed[c.IP]
						if !ok {
							continue
						}
						if target.EdgeID != c.EdgeID || target.EdgeGroupID != c.EdgeGroupID {
							return nil, fmt.Errorf("DNS selection candidate differs from authorized endpoint")
						}
						selected = append(selected, selectionCandidate(c))
					}
					return selected, nil
				}
				candidates, err := convert(fact.Candidates)
				if err != nil {
					return nil, err
				}
				if len(candidates) == 0 {
					return nil, fmt.Errorf("DNS query has no authorized captured selection candidates")
				}
				record.Values = nil
				record.ValueExpirations = nil
				record.TTL = rule.TTLSeconds
				record.Candidates = candidates
				for _, c := range candidates {
					record.Values = append(record.Values, c.IP)
				}
				sort.Strings(record.Values)
				record.AnswerPolicy = model.DNSAnswerPolicy{PolicyKind: rule.SelectionMode, AllowedEdgeGroups: uniqueSorted(groups), PreferredEdgeGroups: rule.PreferredEdgeGroups, FallbackEdgeGroups: rule.FallbackEdgeGroups, TTLSeconds: rule.TTLSeconds, ECSEnabled: rule.ECSEnabled, HealthRequired: true, RouteReadyRequired: true, ExplorationPercent: rule.ExplorationPercent, SwitchCooldownSec: rule.SwitchCooldownSeconds, RankingVersion: fact.RankingVersion, RankingScope: fact.RankingScope, Reason: fact.Reason, ShadowReason: fact.ShadowReason, Weight: fact.Weight}
				candidateGroups := []string{}
				for _, c := range candidates {
					candidateGroups = append(candidateGroups, c.EdgeGroupID)
				}
				if slices.Contains(candidateGroups, fact.SelectedEdgeGroupID) {
					record.AnswerPolicy.SelectedEdgeGroupID = fact.SelectedEdgeGroupID
				}
				if slices.Contains(candidateGroups, fact.ShadowSelectedEdgeGroupID) {
					record.AnswerPolicy.ShadowSelectedEdgeGroupID = fact.ShadowSelectedEdgeGroupID
				}
				for _, scope := range fact.ScopedCandidates {
					candidates, err := convert(scope.Candidates)
					if err != nil {
						return nil, err
					}
					if len(candidates) == 0 {
						continue
					}
					scoped := model.EdgeDNSScopedAnswerCandidates{ScopeKey: scope.ScopeKey, Country: scope.Country, Region: scope.Region, ASN: scope.ASN, PolicyKind: firstNonEmpty(rule.ScopedSelectionMode, rule.SelectionMode), Reason: scope.Reason, CooldownUntil: scope.CooldownUntil, Candidates: candidates}
					for _, c := range candidates {
						if c.EdgeGroupID == scope.SelectedEdgeGroupID {
							scoped.SelectedEdgeGroupID = scope.SelectedEdgeGroupID
						}
					}
					record.ScopedCandidates = append(record.ScopedCandidates, scoped)
				}
			} else if _, exists := rules[key]; exists {
				return nil, fmt.Errorf("DNS query rule has no readiness authorization")
			}
			query.Records = append(query.Records, record)
		}
		out = append(out, query)
	}
	if len(used) != len(rules) || len(used) != len(facts) {
		return nil, fmt.Errorf("DNS query rules or observations are outside declared consumer views")
	}
	return out, nil
}

func ValidateDNSQueryViews(query []DNSQueryView, global []DNSIntent, views []DNSConsumerView, plan *DNSReadinessPlan, policy PolicySnapshot) error {
	if len(query) == 0 {
		if len(policy.DNSAnswerRules) > 0 {
			return fmt.Errorf("DNS query views missing")
		}
		return nil
	}
	if plan == nil || len(query) != len(views) {
		return fmt.Errorf("DNS query view topology differs")
	}
	if err := ValidateDNSAnswerRules(policy.DNSAnswerRules); err != nil {
		return err
	}
	rules := map[string]DNSAnswerRule{}
	for _, r := range policy.DNSAnswerRules {
		rules[dnsQueryKey(r.NodeID, r.Hostname, r.Type)] = r
	}
	readiness := map[string]DNSReadinessRecord{}
	for _, r := range plan.Records {
		readiness[r.Hostname] = r
	}
	seen, used := map[string]bool{}, map[string]bool{}
	for _, view := range query {
		key := view.NodeID + "\x00" + view.Zone
		if seen[key] {
			return fmt.Errorf("duplicate DNS query view")
		}
		seen[key] = true
		source, err := DNSConsumerViewRecords(global, views, view.NodeID, view.EdgeGroupID, view.Zone)
		if err != nil {
			return err
		}
		expected := map[string]DNSIntent{}
		for _, r := range source {
			expected[r.Hostname+"\x00"+r.Type] = r
		}
		if len(expected) != len(view.Records) {
			return fmt.Errorf("DNS query view omits declared records")
		}
		recordsSeen := map[string]bool{}
		for _, r := range view.Records {
			key := r.Name + "\x00" + r.Type
			original, exists := expected[key]
			if !exists || recordsSeen[key] {
				return fmt.Errorf("DNS query view contains extra or duplicate records")
			}
			recordsSeen[key] = true
			if err := ValidateDNSIntents([]DNSIntent{{Hostname: r.Name, Type: r.Type, Values: r.Values, TTL: r.TTL, ValueExpirations: r.ValueExpirations}}); err != nil {
				return err
			}
			rule, hasRule := rules[dnsQueryKey(view.NodeID, r.Name, r.Type)]
			ready, dynamic := readiness[r.Name]
			dynamic = dynamic && (r.Type == "A" || r.Type == "AAAA")
			if !dynamic {
				want := model.EdgeDNSRecord{Name: original.Hostname, Type: original.Type, Values: original.Values, TTL: original.TTL, RecordKind: original.RecordKind, Status: original.Status, StatusReason: original.StatusReason, AppID: original.AppID, TenantID: original.TenantID, EdgeGroupID: original.EdgeGroupID, FallbackEdgeGroupID: original.FallbackEdgeGroupID, ValueExpirations: original.ValueExpirations}
				a, _ := json.Marshal(want)
				b, _ := json.Marshal(r)
				if string(a) != string(b) || hasRule {
					return fmt.Errorf("static DNS query view differs from signed intent")
				}
				continue
			}
			if !hasRule || len(r.ValueExpirations) > 0 || r.TTL != rule.TTLSeconds || r.AnswerPolicy.PolicyKind != rule.SelectionMode || !slices.Equal(r.AnswerPolicy.PreferredEdgeGroups, rule.PreferredEdgeGroups) || !slices.Equal(r.AnswerPolicy.FallbackEdgeGroups, rule.FallbackEdgeGroups) || r.AnswerPolicy.TTLSeconds != rule.TTLSeconds || r.AnswerPolicy.ECSEnabled != rule.ECSEnabled || r.AnswerPolicy.ExplorationPercent != rule.ExplorationPercent || r.AnswerPolicy.SwitchCooldownSec != rule.SwitchCooldownSeconds || !r.AnswerPolicy.HealthRequired || !r.AnswerPolicy.RouteReadyRequired {
				return fmt.Errorf("DNS query policy does not match explicit rule")
			}
			if r.AppID != original.AppID || r.TenantID != original.TenantID || r.RecordKind != original.RecordKind || r.Status != original.Status || r.StatusReason != original.StatusReason || r.EdgeGroupID != original.EdgeGroupID || r.FallbackEdgeGroupID != original.FallbackEdgeGroupID {
				return fmt.Errorf("DNS query record ownership differs")
			}
			used[dnsQueryKey(view.NodeID, r.Name, r.Type)] = true
			allowed := map[string]DNSReadinessTarget{}
			for _, t := range ready.Targets {
				if t.Family == r.Type {
					allowed[t.Address] = t
				}
			}
			validate := func(candidates []model.EdgeDNSAnswerCandidate) error {
				if len(candidates) == 0 || len(candidates) > 4096 {
					return fmt.Errorf("DNS query candidates missing or unbounded")
				}
				seen := map[string]bool{}
				for _, c := range candidates {
					t, ok := allowed[c.IP]
					if !ok || seen[c.IP] || t.EdgeID != c.EdgeID || t.EdgeGroupID != c.EdgeGroupID || !slices.Contains(r.Values, c.IP) || c.Healthy || c.RouteReady || c.TLSReady || c.DNSEligible || c.ServingGeneration != "" || c.LKGGeneration != "" || c.CacheStatus != "" || c.MaxStaleExceeded {
						return fmt.Errorf("DNS query candidate has unauthorized endpoint or fabricated readiness")
					}
					if c.WorkloadMode != "" || c.CanaryState != "" || c.CanaryWeight != 0 || c.PublicProbeStatus != "" {
						return fmt.Errorf("DNS query candidate contains uncaptured runtime fields")
					}
					if err := validateDNSSelectionCandidates([]DNSSelectionCandidate{{IP: c.IP, EdgeID: c.EdgeID, EdgeGroupID: c.EdgeGroupID, Country: c.Country, Region: c.Region, Priority: c.Priority, Weight: c.Weight, Score: c.Score, TrafficClass: c.TrafficClass, Reason: c.Reason, ScoreBreakdown: c.ScoreBreakdown}}, r.Type); err != nil {
						return err
					}
					seen[c.IP] = true
				}
				return nil
			}
			if err := validate(r.Candidates); err != nil {
				return err
			}
			if len(r.Candidates) != len(r.Values) {
				return fmt.Errorf("DNS query address lacks candidate authorization")
			}
			if len(r.ScopedCandidates) > 256 {
				return fmt.Errorf("too many DNS query scopes")
			}
			scopes := map[string]bool{}
			for _, scope := range r.ScopedCandidates {
				if scope.ScopeKey == "" || len(scope.ScopeKey) > 512 || scopes[scope.ScopeKey] || scope.PolicyKind != firstNonEmpty(rule.ScopedSelectionMode, rule.SelectionMode) {
					return fmt.Errorf("DNS query scoped policy differs from explicit rule")
				}
				scopes[scope.ScopeKey] = true
				if err := validate(scope.Candidates); err != nil {
					return err
				}
			}
		}
	}
	if len(used) != len(rules) {
		return fmt.Errorf("orphan DNS answer rule")
	}
	return nil
}
