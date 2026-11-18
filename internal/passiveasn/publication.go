package passiveasn

import (
	"encoding/json"
	"errors"
	"net/netip"
	"slices"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
)

const (
	PublicationKind = "dns_passive_asn_observation"
	PublicationTTL  = 12 * time.Minute
	MaxBindings     = 4096
	MaxResolvers    = 1024
)

type Binding struct {
	Prefix netip.Prefix `json:"prefix"`
	ASN    uint32       `json:"asn"`
}

// Publication is a supplemental observation, never a source of DNS candidate
// authority. It cannot be attached to a different signed serving release.
type Publication struct {
	Mode             string                             `json:"mode"`
	DNSArtifactID    string                             `json:"dns_artifact_id"`
	DNSDigest        string                             `json:"dns_digest"`
	ReleaseSetID     string                             `json:"release_set_id"`
	ConsumerSetID    string                             `json:"consumer_set_id"`
	FencingToken     int64                              `json:"fencing_token"`
	NodeID           string                             `json:"node_id"`
	ObservedAt       time.Time                          `json:"observed_at"`
	ExpiresAt        time.Time                          `json:"expires_at"`
	Bindings         []Binding                          `json:"bindings"`
	Resolvers        []netip.Prefix                     `json:"resolvers"`
	ResolverSourceAt time.Time                          `json:"resolver_source_at"`
	Pairs            []edgequality.PassiveASNComparison `json:"pairs"`
}

type View struct {
	Publication
	Digest                       string
	bindings                     map[netip.Prefix]uint32
	resolverPrefixes             map[netip.Prefix]bool
	resolverBits4, resolverBits6 []int
	pairs                        map[comparisonKey]edgequality.PassiveASNComparison
	aggregate                    map[comparisonKey]aggregatePair
}

type comparisonKey struct {
	asn         uint32
	family      int
	left, right edgequality.PassiveEndpoint
}

type aggregatePair struct {
	total, leftWins, rightWins int
	leftAt, rightAt            time.Time
}

func earlierEvidence(current, sample time.Time) time.Time {
	if current.IsZero() || sample.Before(current) {
		return sample
	}
	return current
}

func Sign(p Publication, keys bundleauth.Keyring) (model.PlatformArtifact, error) {
	if p.Bindings == nil {
		p.Bindings = []Binding{}
	}
	if p.Pairs == nil {
		p.Pairs = []edgequality.PassiveASNComparison{}
	}
	if _, err := buildView(p); err != nil {
		return model.PlatformArtifact{}, err
	}
	var content map[string]any
	raw, err := json.Marshal(p)
	if err != nil || json.Unmarshal(raw, &content) != nil {
		return model.PlatformArtifact{}, errors.New("passive ASN publication serialization failed")
	}
	digest, err := platformconfig.Digest(content)
	if err != nil {
		return model.PlatformArtifact{}, err
	}
	return platformsafety.SignPlatformArtifact(model.PlatformArtifact{
		ID: "observation:" + digest, ArtifactKind: PublicationKind, SchemaVersion: model.PlatformArtifactSchemaVersionV1,
		Scope:    model.PlatformArtifactScope{ScopeType: "node", Key: p.NodeID, NodeID: p.NodeID},
		ScopeKey: p.NodeID, Generation: digest, GenerationSequence: p.ObservedAt.Unix(),
		Status: model.PlatformArtifactStatusValidated, Content: content, ContentHash: digest,
		CreatedAt: p.ObservedAt, UpdatedAt: p.ObservedAt,
	}, keys)
}

func Verify(a model.PlatformArtifact, assignment model.PlatformConsumerAssignment, node string, keys bundleauth.Keyring, now time.Time) (*View, error) {
	if a.ArtifactKind != PublicationKind || a.ID != "observation:"+a.ContentHash || a.Generation != a.ContentHash ||
		a.ScopeKey != node || a.Status != model.PlatformArtifactStatusValidated || !platformsafety.EvaluateArtifactIntegrity(a, keys).Pass {
		return nil, errors.New("passive ASN publication signature or scope invalid")
	}
	raw, err := json.Marshal(a.Content)
	var p Publication
	if err != nil || json.Unmarshal(raw, &p) != nil || p.NodeID != node ||
		p.DNSArtifactID != assignment.ArtifactID || p.DNSDigest != assignment.ContentHash ||
		p.ReleaseSetID != assignment.ReleaseSetID || p.ConsumerSetID != assignment.ExpectedConsumerSetID ||
		p.FencingToken != assignment.FencingToken || !p.ExpiresAt.After(now) ||
		p.ObservedAt.After(now.Add(time.Minute)) {
		return nil, errors.New("passive ASN publication does not bind current DNS release or time")
	}
	v, err := buildView(p)
	if err != nil {
		return nil, err
	}
	v.Digest = a.ContentHash
	return v, nil
}

func buildView(p Publication) (*View, error) {
	if p.Mode != "shadow" && p.Mode != "active" || p.DNSArtifactID == "" || p.DNSDigest == "" || p.ReleaseSetID == "" || p.NodeID == "" ||
		p.ConsumerSetID == "" || p.FencingToken <= 0 ||
		p.ObservedAt.IsZero() || !p.ExpiresAt.After(p.ObservedAt) || p.ExpiresAt.Sub(p.ObservedAt) > PublicationTTL ||
		len(p.Bindings) > MaxBindings || len(p.Resolvers) == 0 || len(p.Resolvers) > MaxResolvers || len(p.Pairs) > edgequality.MaxPassiveASNComparisons {
		return nil, errors.New("passive ASN publication exceeds validity bounds")
	}
	v := &View{Publication: p, bindings: make(map[netip.Prefix]uint32, len(p.Bindings)),
		resolverPrefixes: make(map[netip.Prefix]bool, len(p.Resolvers)), pairs: make(map[comparisonKey]edgequality.PassiveASNComparison, len(p.Pairs)),
		aggregate: make(map[comparisonKey]aggregatePair)}
	for _, b := range p.Bindings {
		if !validPrefix(b.Prefix) || b.ASN == 0 || b.Prefix.Bits() != cohortBits(b.Prefix.Addr()) || v.bindings[b.Prefix] != 0 {
			return nil, errors.New("invalid or duplicate passive ASN binding")
		}
		v.bindings[b.Prefix] = b.ASN
	}
	for _, r := range p.Resolvers {
		if !validPrefix(r) || r.Bits() < cohortBits(r.Addr()) || v.resolverPrefixes[r] {
			return nil, errors.New("invalid passive ASN resolver source")
		}
		v.resolverPrefixes[r] = true
		bits := &v.resolverBits6
		if r.Addr().Is4() {
			bits = &v.resolverBits4
		}
		if !slices.Contains(*bits, r.Bits()) {
			*bits = append(*bits, r.Bits())
		}
	}
	seen := make(map[string]bool, len(p.Pairs))
	for _, pair := range p.Pairs {
		if pair.ASN == 0 || (pair.Family != 4 && pair.Family != 6) ||
			!validEndpoint(pair.Left, pair.Family) || !validEndpoint(pair.Right, pair.Family) ||
			pair.Left.EdgeID >= pair.Right.EdgeID || (pair.Preferred != "" && pair.Preferred != pair.Left.EdgeID && pair.Preferred != pair.Right.EdgeID) ||
			pair.SharedWindows < 3 || pair.SharedWindows > int(edgequality.PassiveASNWindow/edgequality.PassiveASNInterval) ||
			pair.LeftWinWindows < 0 || pair.RightWinWindows < 0 || pair.LeftWinWindows+pair.RightWinWindows > pair.SharedWindows ||
			(pair.Preferred == pair.Left.EdgeID) != (pair.LeftWinWindows == pair.SharedWindows) ||
			(pair.Preferred == pair.Right.EdgeID) != (pair.RightWinWindows == pair.SharedWindows) ||
			pair.ObservedAt.IsZero() || pair.ObservedAt.After(p.ObservedAt) ||
			len(pair.Prefixes) == 0 || len(pair.Prefixes) > 32 || seen[pair.Key()] {
			return nil, errors.New("invalid or duplicate passive ASN comparison")
		}
		for _, prefix := range pair.Prefixes {
			if v.bindings[prefix] != pair.ASN || (pair.Family == 4) != prefix.Addr().Is4() {
				return nil, errors.New("passive ASN comparison mapping is not bound")
			}
		}
		seen[pair.Key()] = true
		v.pairs[comparisonKey{pair.ASN, pair.Family, pair.Left, pair.Right}] = pair
		// Pairs are already unique per ASN and exact endpoint pair. Sample
		// volume and number of prefixes never increase an ASN's weight.
		key := comparisonKey{0, pair.Family, pair.Left, pair.Right}
		count := v.aggregate[key]
		count.total++
		switch pair.Preferred {
		case pair.Left.EdgeID:
			count.leftWins++
			count.leftAt = earlierEvidence(count.leftAt, pair.ObservedAt)
		case pair.Right.EdgeID:
			count.rightWins++
			count.rightAt = earlierEvidence(count.rightAt, pair.ObservedAt)
		}
		v.aggregate[key] = count
	}
	return v, nil
}

func validPrefix(p netip.Prefix) bool {
	return p.IsValid() && p == p.Masked() && p.Bits() > 0 && !p.Addr().Is4In6() &&
		p.Addr().IsGlobalUnicast() && !p.Addr().IsPrivate()
}

func cohortBits(a netip.Addr) int {
	if a.Is4() {
		return 24
	}
	return 48
}

func validEndpoint(e edgequality.PassiveEndpoint, family int) bool {
	addr, err := netip.ParseAddr(e.Address)
	return err == nil && !addr.Is4In6() && addr.IsGlobalUnicast() && !addr.IsPrivate() &&
		addr.Zone() == "" && (family == 4) == addr.Is4() && e.EdgeID != "" && e.GroupID != ""
}

func (v *View) trustedResolver(addr netip.Addr) bool {
	bits := v.resolverBits6
	if addr.Is4() {
		bits = v.resolverBits4
	}
	for _, n := range bits {
		if v.resolverPrefixes[netip.PrefixFrom(addr, n).Masked()] {
			return true
		}
	}
	return false
}

// Classify only recognizes a complete prefix contained by an already verified
// coarse binding. It does not treat the resolver's ASN as customer evidence.
func (v *View) Classify(p netip.Prefix, resolver netip.Addr, now time.Time) (uint32, string) {
	if v == nil || !v.ExpiresAt.After(now) {
		return 0, "publication_unavailable"
	}
	if !validPrefix(p) {
		return 0, "ecs_absent_or_invalid"
	}
	if !v.trustedResolver(resolver) {
		return 0, "ecs_resolver_untrusted"
	}
	bits := cohortBits(p.Addr())
	if p.Bits() < bits {
		return 0, "ecs_prefix_too_broad"
	}
	if now.Sub(v.ResolverSourceAt) > 8*24*time.Hour || v.ResolverSourceAt.After(now.Add(time.Minute)) {
		return 0, "ecs_resolver_source_stale"
	}
	asn := v.bindings[netip.PrefixFrom(p.Addr(), bits).Masked()]
	if asn == 0 {
		return 0, "ecs_origin_unknown"
	}
	return asn, "ecs_origin_valid"
}

type Choice struct {
	Edge                                           edgequality.PassiveEndpoint
	Reason                                         string
	EvidenceAt                                     time.Time
	SharedWindows, LeftWinWindows, RightWinWindows int
	AggregateASNs, AggregateSupportingASNs         int
}

func choiceWithEvidence(edge edgequality.PassiveEndpoint, reason string, comparison edgequality.PassiveASNComparison) Choice {
	return Choice{Edge: edge, Reason: reason, EvidenceAt: comparison.ObservedAt,
		SharedWindows: comparison.SharedWindows, LeftWinWindows: comparison.LeftWinWindows, RightWinWindows: comparison.RightWinWindows}
}

func (v *View) aggregateChoice(a, b edgequality.PassiveEndpoint, family int) Choice {
	left, right := a, b
	if a.EdgeID > b.EdgeID {
		left, right = b, a
	}
	pair := v.aggregate[comparisonKey{0, family, left, right}]
	for _, side := range []struct {
		edge edgequality.PassiveEndpoint
		wins int
		at   time.Time
	}{{left, pair.leftWins, pair.leftAt}, {right, pair.rightWins, pair.rightAt}} {
		if side.wins >= 3 && 3*side.wins >= 2*pair.total {
			return Choice{Edge: side.edge, Reason: "passive_aggregate_preferred", EvidenceAt: side.at,
				AggregateASNs: pair.total, AggregateSupportingASNs: side.wins}
		}
	}
	return Choice{}
}

// Rank only endpoints that already have permission to serve. Unknown edges
// keep their baseline order and remain eligible for bounded exploration.
func (v *View) chooseAggregate(family int, candidates []edgequality.PassiveEndpoint) Choice {
	base := candidates[0]
	answer := Choice{Edge: base, Reason: "retained_no_comparison"}
	winners := []Choice{}
	for _, edge := range candidates[1:] {
		choice := v.aggregateChoice(base, edge, family)
		if choice.Edge == edge {
			winners = append(winners, choice)
		} else if choice.Edge == base && choice.EvidenceAt.After(answer.EvidenceAt) {
			answer = choice
			answer.Reason = "retained_passive_aggregate_advantage"
		}
	}
	for _, winner := range winners {
		proven := true
		for _, other := range winners {
			if other.Edge != winner.Edge && v.aggregateChoice(winner.Edge, other.Edge, family).Edge != winner.Edge {
				proven = false
				break
			}
		}
		if !proven {
			continue
		}
		for _, other := range candidates[1:] {
			if other != winner.Edge && v.aggregateChoice(winner.Edge, other, family).Edge == other {
				proven = false
				break
			}
		}
		if proven {
			return winner
		}
	}
	if len(winners) > 0 {
		return Choice{Edge: base, Reason: "retained_conflicting_aggregate_comparisons"}
	}
	return answer
}

// Choose only receives already eligible members and preserves their baseline
// if no challenger beats it or conflicting wins make the result ambiguous.
// A complete non-winning comparison is a stop barrier, not an unknown pair.
func (v *View) Choose(asn uint32, family int, candidates []edgequality.PassiveEndpoint, exploration uint64) Choice {
	if len(candidates) == 0 {
		return Choice{Reason: "no_eligible_edge"}
	}
	base := candidates[0]
	answer := Choice{Edge: base, Reason: "retained_no_comparison"}
	if len(candidates) < 2 || v == nil {
		return answer
	}
	find := func(a, b edgequality.PassiveEndpoint) (edgequality.PassiveASNComparison, bool) {
		if a.EdgeID > b.EdgeID {
			a, b = b, a
		}
		pair, ok := v.pairs[comparisonKey{asn, family, a, b}]
		return pair, ok
	}
	winners := []Choice{}
	var unknown []edgequality.PassiveEndpoint
	for _, edge := range candidates[1:] {
		if pair, ok := find(base, edge); ok {
			if pair.Preferred == edge.EdgeID {
				winners = append(winners, choiceWithEvidence(edge, "passive_asn_preferred", pair))
			} else {
				if pair.ObservedAt.After(answer.EvidenceAt) {
					answer = choiceWithEvidence(base, "retained_no_challenger_advantage", pair)
				}
			}
		} else {
			unknown = append(unknown, edge)
		}
	}
	// Don't let a fleet average override a local tie, loss, or an
	// unresolved conflict between eligible members of this ASN.
	if len(unknown) == len(candidates)-1 {
		hasLocal := false
		for i, a := range unknown {
			for _, b := range unknown[i+1:] {
				if _, ok := find(a, b); ok {
					hasLocal = true
					break
				}
			}
			if hasLocal {
				break
			}
		}
		if !hasLocal {
			answer = v.chooseAggregate(family, candidates)
			if answer.Reason == "retained_conflicting_aggregate_comparisons" {
				return answer
			}
			unknown = unknown[:0]
			for _, edge := range candidates {
				if edge != answer.Edge && v.aggregateChoice(answer.Edge, edge, family).Edge.EdgeID == "" {
					unknown = append(unknown, edge)
				}
			}
		}
	}
	if len(winners) != 0 {
		for _, a := range winners {
			beatsAll := true
			for _, b := range winners {
				if a.Edge == b.Edge {
					continue
				}
				if pair, ok := find(a.Edge, b.Edge); !ok || pair.Preferred != a.Edge.EdgeID {
					beatsAll = false
					break
				}
			}
			if beatsAll {
				answer = a
				break
			}
		}
		if answer.Edge == base {
			answer.Reason = "retained_conflicting_comparisons"
			return answer
		}
		// Only unknown pairs against the proven winner remain eligible
		// for exploration, not a challenger with negative evidence.
		unknown = unknown[:0]
		for _, edge := range candidates[1:] {
			if edge != answer.Edge {
				if pair, ok := find(answer.Edge, edge); !ok {
					unknown = append(unknown, edge)
				} else if pair.Preferred == edge.EdgeID {
					return choiceWithEvidence(base, "retained_conflicting_comparisons", pair)
				}
			}
		}
	}
	if exploration > 0 && len(unknown) > 0 {
		return Choice{Edge: unknown[(exploration-1)%uint64(len(unknown))], Reason: "bounded_passive_asn_exploration"}
	}
	return answer
}
