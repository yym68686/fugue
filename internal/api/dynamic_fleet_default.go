package api

import (
	"context"
	"encoding/json"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"fugue/internal/dnsfacts"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/passiveasn"
	"fugue/internal/platformconfig"
)

const fleetDefaultMaxHistory = 24 * time.Hour
const fleetDefaultEvidencePrefix = "fleet_evidence_at="

type fleetDefaultQuery struct {
	host, family string
}

type fleetDefaultRankInput struct {
	recent, previous, historical passiveFleetSnapshot
}

type fleetDefaultInput struct {
	ranks map[string]fleetDefaultRankInput
	ready map[string]map[fleetDefaultQuery]map[edgequality.PassiveEndpoint]time.Time
	holds map[edgequality.PassiveEndpoint]bool
}

type fleetDefaultChoice struct {
	edge   string
	reason string
	at     time.Time
}

func fleetDefaultEvidenceAt(fact string) time.Time {
	raw, marked := strings.CutPrefix(fact, fleetDefaultEvidencePrefix)
	if !marked {
		return time.Time{}
	}
	at, _ := time.Parse(time.RFC3339Nano, raw)
	return at
}

func fleetDefaultHostEnabled(dynamic *platformconfig.DynamicQualityPolicy, host string) bool {
	return dynamic != nil && (len(dynamic.FleetDefaultHosts) == 0 || slices.Contains(dynamic.FleetDefaultHosts, host))
}

func (s *Server) loadFleetDefaultRanks(ctx context.Context, now time.Time) fleetDefaultInput {
	var input fleetDefaultInput
	raw, err := s.store.ReadObservationCheckpoint(ctx, passiveASNCheckpointKey)
	var checkpoint passiveASNCheckpoint
	if err != nil || len(raw) > 4<<20 || json.Unmarshal(raw, &checkpoint) != nil ||
		checkpoint.ObservedAt.After(now) || !now.Before(checkpoint.ObservedAt.Add(passiveasn.PublicationTTL)) {
		return input
	}
	policies, err := s.passiveASNSignedMappings(ctx)
	if err != nil || len(policies) == 0 {
		return input
	}
	resolve, ruleCount, err := buildPassiveASNResolver(policies)
	if err != nil {
		return input
	}
	if checkpoint.Mapping == "ripe-ris-v1" {
		index := s.passiveASN.ris.Load()
		if ruleCount != 0 || index == nil || index.SourceAt.After(now) || now.Sub(index.SourceAt) > passiveasn.MaxSourceAge {
			return input
		}
		resolve = index.Lookup
	} else if mapping, err := platformconfig.Digest(policies); err != nil || checkpoint.Mapping != mapping {
		return input
	}
	input.ranks = map[string]fleetDefaultRankInput{}
	input.holds = map[edgequality.PassiveEndpoint]bool{}
	for _, edge := range checkpoint.ExplorationHolds {
		input.holds[edge] = true
	}
	for kind, family := range map[string]int{"A": 4, "AAAA": 6} {
		windows := checkpoint.FleetLive[family]
		history := checkpoint.FleetBootstrap[family]
		if checkpoint.FleetLive == nil {
			history = checkpoint.FleetSnapshots[family]
		}
		input.ranks[kind] = fleetDefaultRankInput{
			recent:     fleetDefaultUsableSnapshot(windows[0], family, now, edgequality.PassiveASNWindow, resolve),
			previous:   fleetDefaultUsableSnapshot(windows[1], family, now, 2*edgequality.PassiveASNWindow, resolve),
			historical: fleetDefaultUsableSnapshot(history, family, now, fleetDefaultMaxHistory, resolve),
		}
	}
	return input
}

func fleetDefaultUsableSnapshot(snapshot passiveFleetSnapshot, family int, now time.Time, maxAge time.Duration, resolve func(netip.Prefix) uint32) passiveFleetSnapshot {
	if now.Sub(snapshot.At) > maxAge || !snapshot.matches(now, snapshot.Ranks, resolve) {
		return passiveFleetSnapshot{}
	}
	seen := map[string]bool{}
	for _, rank := range snapshot.Ranks {
		if rank.Family != family || now.Sub(rank.EvidenceAt) > maxAge || seen[rank.Edge.EdgeID] {
			return passiveFleetSnapshot{}
		}
		seen[rank.Edge.EdgeID] = true
	}
	return snapshot
}

func (s *Server) loadFleetDefaultInput(parent context.Context, policy platformconfig.DNSQueryPolicy, nodes []platformconfig.DNSConsumerObservation, now time.Time) fleetDefaultInput {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	input := s.loadFleetDefaultRanks(ctx, now)
	if len(input.ranks) == 0 || len(nodes) == 0 || len(nodes) > 8 {
		return fleetDefaultInput{}
	}
	var available bool
	for _, rank := range input.ranks {
		available = available || !rank.recent.At.IsZero() || !rank.historical.At.IsZero()
	}
	if !available {
		return fleetDefaultInput{}
	}
	results := make([]map[fleetDefaultQuery]map[edgequality.PassiveEndpoint]time.Time, len(nodes))
	var workers sync.WaitGroup
	for index, node := range nodes {
		workers.Go(func() {
			facts, err := s.readPlatformDNSRuntimeFacts(ctx, node.NodeID)
			if err == nil && facts.Snapshot.Assignment.ReleaseChannel == model.PlatformArtifactReleaseChannelFull {
				results[index] = fleetDefaultReadyTargets(facts, policy)
			}
		})
	}
	workers.Wait()
	input.ready = make(map[string]map[fleetDefaultQuery]map[edgequality.PassiveEndpoint]time.Time, len(nodes))
	for index, node := range nodes {
		input.ready[node.NodeID] = results[index]
	}
	return input
}

func fleetDefaultReadyTargets(facts platformDNSRuntimeFactsResponse, policy platformconfig.DNSQueryPolicy) map[fleetDefaultQuery]map[edgequality.PassiveEndpoint]time.Time {
	if facts.readinessPlan == nil || !facts.Ready {
		return nil
	}
	probes := make(map[string]dnsfacts.Probe, len(facts.ReadyProbeIDs))
	for _, probe := range facts.Snapshot.Facts {
		if _, present := slices.BinarySearch(facts.ReadyProbeIDs, probe.ProbeID); present {
			probes[probe.ProbeID] = probe
		}
	}
	ready := map[fleetDefaultQuery]map[edgequality.PassiveEndpoint]time.Time{}
	for _, record := range facts.readinessPlan.Records {
		availability := platformconfig.DNSAvailabilityForHost(&policy, record.Hostname)
		for _, target := range record.Targets {
			deadline := facts.Snapshot.CheckpointValidUntil
			passed := platformconfig.DNSReadinessTargetReady(target, func(id string) (string, *model.TrafficReleaseBinding, bool) {
				probe, ok := probes[id]
				if ok && availability != nil {
					ok = model.DNSAvailabilityRecoveryReady(availability, probe.ProbeHistory, probe.Proof.CheckedAt, facts.EvaluatedAt, facts.probeFreshness)
				}
				if ok && probe.Proof.ValidUntil.Before(deadline) {
					deadline = probe.Proof.ValidUntil
				}
				return probe.Proof.GroupID, probe.Proof.TrafficRelease, ok
			})
			if !passed {
				continue
			}
			key := fleetDefaultQuery{record.Hostname, target.Family}
			if ready[key] == nil {
				ready[key] = map[edgequality.PassiveEndpoint]time.Time{}
			}
			ready[key][edgequality.PassiveEndpoint{EdgeID: target.EdgeID, GroupID: target.EdgeGroupID, Address: target.Address}] = deadline
		}
	}
	return ready
}

func (input fleetDefaultInput) choice(query fleetDefaultQuery, current string, order []string, candidates []platformconfig.DNSSelectionObservation, now time.Time) fleetDefaultChoice {
	if len(candidates) == 0 {
		return fleetDefaultChoice{}
	}
	rank := input.ranks[query.family]
	eligible := map[edgequality.PassiveEndpoint]int{}
	members := make(map[string]bool, len(order))
	for _, edge := range order {
		members[edge] = true
	}
	for _, fact := range candidates {
		seen := make(map[edgequality.PassiveEndpoint]bool, len(fact.Candidates))
		for _, candidate := range fact.Candidates {
			edge := edgequality.PassiveEndpoint{EdgeID: candidate.EdgeID, GroupID: candidate.EdgeGroupID, Address: candidate.IP}
			if members[edge.EdgeID] && !seen[edge] && !input.holds[edge] && input.ready[fact.NodeID][query][edge].After(now) {
				eligible[edge]++
				seen[edge] = true
			}
		}
	}
	pick := func(snapshot passiveFleetSnapshot) fleetDefaultChoice {
		best := fleetDefaultChoice{}
		tier := 0
		ranks := make(map[string]passiveasn.FleetRank, len(snapshot.Ranks))
		for _, row := range snapshot.Ranks {
			ranks[row.Edge.EdgeID] = row
		}
		for _, edgeID := range order {
			row := ranks[edgeID]
			if row.Tier > 0 && eligible[row.Edge] == len(candidates) && (tier == 0 || row.Tier < tier || row.Tier == tier && edgeID == current) {
				best, tier = fleetDefaultChoice{edge: edgeID, at: row.EvidenceAt}, row.Tier
			}
		}
		return best
	}
	if winner := pick(rank.recent); winner.edge != "" {
		prior := pick(rank.previous)
		if winner.edge == current || prior.edge == winner.edge && !rank.previous.At.After(rank.recent.At.Add(-edgequality.PassiveASNWindow)) {
			winner.reason = model.DNSFleetDefaultLive
			return winner
		}
		return fleetDefaultChoice{}
	}
	if winner := pick(rank.historical); winner.edge != "" {
		winner.reason = model.DNSFleetDefaultHistorical
		return winner
	}
	return fleetDefaultChoice{}
}

func fleetDefaultStateEligible(state string) bool {
	switch state {
	case "learning_current_ready_primary", "learning_evidence_unavailable", "learning_publication_validation_failed",
		"learning_address_family_evidence_required", "availability_only_quality_unavailable", "retained_previous_verified_order", "measured":
		return true
	default:
		return false
	}
}

func fleetDefaultHasTargets(dynamic *platformconfig.DynamicQualityPolicy, states map[string]string) bool {
	for host, state := range states {
		if fleetDefaultStateEligible(state) && fleetDefaultHostEnabled(dynamic, host) {
			return true
		}
	}
	return false
}

func fleetDefaultFixedQueries(policy *platformconfig.DNSQueryPolicy) map[string]bool {
	fixed := map[string]bool{}
	if policy != nil && policy.OrderedProjection != nil {
		for _, override := range policy.OrderedProjection.Overrides {
			if !override.BootstrapOnly {
				fixed[dynamicQualityFamilyKey(override.NodeID, override.Hostname, override.Type)] = true
			}
		}
	}
	return fixed
}

func applyFleetDefaults(projection *platformIntentProjectionResponse, policy platformconfig.DNSQueryPolicy, states map[string]string, input fleetDefaultInput, now time.Time) error {
	dynamic := policy.DynamicQuality
	if dynamic == nil || policy.OrderedProjection == nil || len(input.ready) == 0 {
		return nil
	}
	rules := slices.Clone(projection.Policy.DNSAnswerRules)
	facts := slices.Clone(projection.RuntimeSnapshot.DNSSelections)
	type rows struct {
		ruleIndexes, factIndexes []int
	}
	queries := map[fleetDefaultQuery]*rows{}
	for index, fact := range facts {
		state := states[fact.Hostname]
		if !fleetDefaultStateEligible(state) || state == "measured" && fact.Type == "A" {
			continue
		}
		if !fleetDefaultHostEnabled(dynamic, fact.Hostname) ||
			fact.Type != "A" && fact.Type != "AAAA" ||
			fact.PhysicalSelection != nil && (fact.PhysicalSelection.QualityState != "learning" || fact.PhysicalSelection.BusinessAllocation != nil) {
			continue
		}
		key := fleetDefaultQuery{fact.Hostname, fact.Type}
		if queries[key] == nil {
			queries[key] = &rows{}
		}
		queries[key].factIndexes = append(queries[key].factIndexes, index)
	}
	fixed := fleetDefaultFixedQueries(&policy)
	for index, rule := range rules {
		if row := queries[fleetDefaultQuery{rule.Hostname, rule.Type}]; row != nil {
			row.ruleIndexes = append(row.ruleIndexes, index)
		}
	}
	changed := false
	shadow := map[string]map[string]fleetDefaultChoice{}
	applied := map[string]map[string]fleetDefaultChoice{}
	for key, row := range queries {
		availability := platformconfig.DNSAvailabilityForHost(&policy, key.host)
		if len(row.ruleIndexes) != len(row.factIndexes) || availability != nil && availability.Version != model.DNSAvailabilityVersion {
			continue
		}
		observations := make([]platformconfig.DNSSelectionObservation, 0, len(row.factIndexes))
		factByNode := map[string]platformconfig.DNSSelectionObservation{}
		for _, index := range row.factIndexes {
			fact := facts[index]
			observations = append(observations, fact)
			factByNode[fact.NodeID] = fact
		}
		orders := map[int][]string{}
		var common []string
		var incumbent string
		var incumbentEvidence time.Time
		for _, index := range row.ruleIndexes {
			rule := rules[index]
			fact, present := factByNode[rule.NodeID]
			if !present || fixed[dynamicQualityFamilyKey(rule.NodeID, key.host, key.family)] || rule.ECSEnabled || rule.ExplorationPercent != 0 ||
				rule.ScopedSelectionMode != "" || len(rule.PreferredEdgeGroups) != 0 || len(rule.FallbackEdgeGroups) != 0 || len(fact.ScopedCandidates) != 0 ||
				rule.PhysicalOrder != nil && rule.PhysicalOrder.QualityObservedAt != nil && fact.RankingVersion != model.DNSFleetDefaultRankingVersion {
				break
			}
			var order []string
			if rule.PhysicalOrder != nil && rule.SelectionMode == model.DNSAnswerPolicyKindPhysicalOrder {
				order = rule.PhysicalOrder.OrderedEdgeIDs
			} else if fact.PhysicalSelection != nil && rule.SelectionMode == model.DNSAnswerPolicyKindPhysicalQuality {
				order = fact.PhysicalSelection.OrderedEdgeIDs
			}
			if len(order) == 0 {
				break
			}
			if fact.RankingVersion == model.DNSFleetDefaultRankingVersion {
				evidence := fleetDefaultEvidenceAt(fact.ShadowReason)
				if evidence.After(incumbentEvidence) {
					incumbentEvidence = evidence
				}
			}
			orders[index] = order
			if len(orders) == 1 {
				common, incumbent = slices.Clone(order), order[0]
			} else {
				common = slices.DeleteFunc(common, func(edge string) bool { return !slices.Contains(order, edge) })
				if incumbent != order[0] {
					incumbent = ""
				}
			}
		}
		if len(orders) != len(row.ruleIndexes) || len(common) < 2 {
			continue
		}
		slices.SortFunc(common, func(left, right string) int {
			order := policy.OrderedProjection.DefaultOrder.OrderedEdgeIDs
			if availability != nil {
				order = availability.OrderedEdgeIDs
			}
			leftIndex, rightIndex := slices.Index(order, left), slices.Index(order, right)
			if leftIndex < 0 {
				leftIndex = len(order)
			}
			if rightIndex < 0 {
				rightIndex = len(order)
			}
			if leftIndex != rightIndex {
				return leftIndex - rightIndex
			}
			return strings.Compare(left, right)
		})
		winner := input.choice(key, incumbent, common, observations, now)
		if winner.edge == "" || winner.edge != incumbent && !winner.at.After(incumbentEvidence) {
			continue
		}
		if dynamic.FleetDefault != "active" {
			if shadow[key.host] == nil {
				shadow[key.host] = map[string]fleetDefaultChoice{}
			}
			shadow[key.host][key.family] = winner
			continue
		}
		for index, prior := range orders {
			rule := &rules[index]
			order := append([]string{winner.edge}, slices.DeleteFunc(slices.Clone(prior), func(edge string) bool { return edge == winner.edge })...)
			next := &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: order}
			if winner.edge == prior[0] && rule.PhysicalOrder != nil {
				next.PrimarySince = rule.PhysicalOrder.PrimarySince
			}
			rule.SelectionMode, rule.PhysicalOrder = model.DNSAnswerPolicyKindPhysicalOrder, next
		}
		for _, index := range row.factIndexes {
			fact := &facts[index]
			fact.RetainedPhysicalEvidence = retainedPhysicalHistory(*fact, nil)
			fact.PhysicalSelection, fact.PhysicalEvidence = nil, nil
			fact.RankingVersion, fact.RankingScope, fact.Reason = model.DNSFleetDefaultRankingVersion, "global", winner.reason
			fact.ShadowReason = fleetDefaultEvidencePrefix + winner.at.Format(time.RFC3339Nano)
			if err := digestFleetDefaultFact(fact); err != nil {
				return err
			}
		}
		if applied[key.host] == nil {
			applied[key.host] = map[string]fleetDefaultChoice{}
		}
		applied[key.host][key.family] = winner
		changed = true
	}
	for key, choices := range map[string]map[string]map[string]fleetDefaultChoice{"fleet_default_shadow": shadow, "fleet_default_active": applied} {
		if len(choices) == 0 {
			continue
		}
		if projection.RuntimeSnapshot.Facts == nil {
			projection.RuntimeSnapshot.Facts = map[string]any{}
		}
		values := map[string]map[string]map[string]string{}
		for hostname, families := range choices {
			values[hostname] = map[string]map[string]string{}
			for family, choice := range families {
				values[hostname][family] = map[string]string{"edge_id": choice.edge, "reason": choice.reason, "evidence_at": choice.at.Format(time.RFC3339Nano)}
			}
		}
		projection.RuntimeSnapshot.Facts[key] = values
	}
	if !changed {
		return nil
	}
	next := platformconfig.NormalizePolicySnapshot(projection.Policy)
	next.DNSAnswerRules = rules
	generation, err := platformconfig.PolicySnapshotGeneration(next)
	if err != nil {
		return err
	}
	next.Generation = generation
	projection.Policy = next
	projection.RuntimeSnapshot.PolicyGeneration = generation
	projection.RuntimeSnapshot.DNSSelections = facts
	return nil
}

func digestFleetDefaultFact(fact *platformconfig.DNSSelectionObservation) error {
	fact.SourceDigest, fact.SourceGeneration = "", ""
	digest, err := platformconfig.Digest(*fact)
	if err == nil {
		fact.SourceDigest, fact.SourceGeneration = digest, "dns-observation_"+strings.TrimPrefix(digest, "sha256:")
	}
	return err
}
