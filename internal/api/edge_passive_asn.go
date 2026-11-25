package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/frontnetwork"
	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/passiveasn"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

const passiveASNCheckpointKey = "edge-passive-asn-comparison/v1"

type passiveASNCheckpoint struct {
	CohortExploration bool                                        `json:"cohort_exploration,omitempty"`
	Mapping           string                                      `json:"mapping"`
	Comparisons       map[string]edgequality.PassiveASNComparison `json:"comparisons"`
	Cohorts           []passiveasn.Binding                        `json:"cohorts,omitempty"`
	ObservedAt        time.Time                                   `json:"observed_at,omitempty"`
	DNSProofs         map[string]passiveASNDNSProof               `json:"dns_proofs,omitempty"`
	ExplorationHolds  []edgequality.PassiveEndpoint               `json:"exploration_holds,omitempty"`
	Estimates         map[string]edgequality.PassiveASNComparison `json:"fleet_estimates,omitempty"`
	FleetSnapshots    map[int]passiveFleetSnapshot                `json:"fleet_snapshots,omitempty"`
	FleetLive         map[int][2]passiveFleetSnapshot             `json:"fleet_live_windows,omitempty"`
	FleetBootstrap    map[int]passiveFleetSnapshot                `json:"fleet_bootstrap,omitempty"`
}

type passiveASNDNSProof struct {
	Process, PodUID, ScopeKey string
	Publication               dnsserver.DNSDecisionPublication
	WireVerified              bool
	Verified                  bool // A replayable answer carried a classified client ASN.
}

func (p passiveASNDNSProof) inherit(prior passiveASNDNSProof) passiveASNDNSProof {
	if p.Process != prior.Process || p.PodUID != prior.PodUID || p.ScopeKey != prior.ScopeKey || p.Publication != prior.Publication {
		return p
	}
	p.WireVerified = p.WireVerified || prior.WireVerified || prior.Verified
	p.Verified = p.Verified || prior.Verified
	return p
}

type passiveASNState struct {
	mu        sync.Mutex
	window    passiveASNWindow
	failures  int64
	ris       atomic.Pointer[passiveasn.Index]
	resolvers atomic.Pointer[passiveasn.ResolverSources]
}

type passiveASNWindow struct {
	observedAt, bgpSourceAt                                          time.Time
	coverage                                                         edgequality.PassiveASNCoverage
	rules, edges, cappedEdges, retained, preferring, saturated       int
	dnsNodes, dnsReads, dnsAnswers, dnsECS, dnsASNHints, dnsVerified int
	dnsWireVerified                                                  int
	dnsProofs                                                        map[string]passiveASNDNSProof
	fleet                                                            []passiveFleetEdge
}

type passiveFleetEdge struct {
	id, status string
	samples    int
	stats      *frontnetwork.PassiveStats
	ranks      []passiveFleetStatus
}

type passiveFleetStatus struct {
	rank, live passiveasn.FleetRank
	historical time.Time
}

// Default-on observation over the authenticated Front sample path. This
// worker cannot change placement, publish DNS, or issue business requests.
func (s *Server) StartBackgroundPassiveASN(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	go s.refreshPassiveASNRIS(ctx)
	ticker := time.NewTicker(edgequality.PassiveASNInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		run, cancel := context.WithTimeout(ctx, 30*time.Second)
		_, err := s.store.WithAdvisoryLock(run, passiveASNCheckpointKey, func() error {
			return s.observePassiveASN(run, time.Now().UTC())
		})
		cancel()
		if err != nil && ctx.Err() == nil {
			s.passiveASN.mu.Lock()
			s.passiveASN.failures++
			s.passiveASN.mu.Unlock()
			if s.log != nil {
				s.log.Printf("passive ASN shadow retained previous results: %v", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) refreshPassiveASNRIS(ctx context.Context) {
	for ctx.Err() == nil {
		run, cancel := context.WithTimeout(ctx, 75*time.Second)
		index, err := passiveasn.FetchRIS(run, time.Now().UTC())
		cancel()
		interval := 6 * time.Hour
		if previous := s.passiveASN.ris.Load(); err == nil && previous != nil && index.SourceAt.Before(previous.SourceAt) {
			err = errors.New("RIS snapshot moved backwards")
		}
		if err == nil {
			s.passiveASN.ris.Store(index)
		} else if ctx.Err() == nil {
			interval = 15 * time.Minute
			if s.log != nil {
				s.log.Printf("passive ASN bulk mapping refresh failed: %v", err)
			}
		}
		if sources, err := passiveasn.FetchResolverSources(ctx, time.Now().UTC()); err == nil {
			if previous := s.passiveASN.resolvers.Load(); previous == nil || !sources.PublishedAt.Before(previous.PublishedAt) {
				s.passiveASN.resolvers.Store(sources)
			}
		} else if ctx.Err() == nil {
			interval = 15 * time.Minute
			if s.log != nil {
				s.log.Printf("passive ASN resolver trust refresh failed: %v", err)
			}
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Server) observePassiveASN(ctx context.Context, now time.Time) error {
	window := passiveASNWindow{observedAt: now}
	policies, err := s.passiveASNSignedMappings(ctx)
	if err != nil {
		return err
	}
	resolver, rules, err := buildPassiveASNResolver(policies)
	if err != nil {
		return err
	}
	window.rules, window.dnsNodes = rules, len(policies)
	mapping, err := platformconfig.Digest(policies)
	if err != nil {
		return err
	}
	useRIS := len(policies) > 0
	for _, policy := range policies {
		useRIS = useRIS && len(policy.Rules) == 0
	}
	const risMapping = "ripe-ris-v1"
	if index := s.passiveASN.ris.Load(); useRIS && index != nil && now.Sub(index.SourceAt) <= passiveasn.MaxSourceAge {
		resolver, mapping, window.bgpSourceAt = index.Lookup, risMapping, index.SourceAt
	}
	nodes, _, err := s.store.ListActiveEdgeNodes("")
	if err != nil {
		return err
	}
	if len(nodes) > 32 {
		return errors.New("passive ASN edge inventory exceeds observation bound")
	}
	identities := make(map[string]edgequality.PassiveEdgeIdentity)
	var edgeIDs []string
	var eligible []model.EdgeNode
	for _, node := range nodes {
		if !node.Healthy || !edgeNodeHeartbeatFresh(node, now) || node.Draining || node.RouteBundleVersion == "" ||
			node.PublicIPv4 == "" && node.PublicIPv6 == "" {
			continue
		}
		if _, exists := identities[node.ID]; exists {
			return errors.New("ambiguous physical edge identity for passive ASN")
		}
		identities[node.ID] = edgequality.PassiveEdgeIdentity{
			GroupID: node.EdgeGroupID, IPv4: node.PublicIPv4, IPv6: node.PublicIPv6, Bundle: node.RouteBundleVersion,
		}
		edgeIDs = append(edgeIDs, node.ID)
		eligible = append(eligible, node)
	}
	fronts := s.collectPassiveFronts(ctx, eligible)
	fleet := make(map[string]passiveFleetEdge, len(nodes))
	for _, node := range nodes {
		status := "inventory_not_eligible"
		if _, ok := identities[node.ID]; ok {
			status = "front_unavailable"
		}
		fleet[node.ID] = passiveFleetEdge{id: node.ID, status: status}
	}
	var holds []edgequality.PassiveEndpoint
	writeCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	for _, read := range fronts {
		id := identities[read.node]
		id.Front = read.identity
		identities[read.node] = id
		// Front reservoirs can still replace samples in the current open
		// bucket. Persist only closed buckets so first-write wins remains valid.
		closed := slices.DeleteFunc(read.samples, func(sample model.EdgeNetworkSample) bool {
			return !sample.ObservedAt.Before(now.Truncate(edgequality.PassiveASNInterval))
		})
		if len(closed) != 0 {
			for chunk := range slices.Chunk(closed, 128) {
				if err := s.store.RecordEdgeNetworkSamples(writeCtx, chunk, time.Time{}); err != nil {
					read.status = "sample_store_unavailable"
					break
				}
			}
		}
		fleet[read.node] = passiveFleetEdge{id: read.node, status: read.status, samples: len(closed), stats: read.stats}
		if s.log != nil {
			s.log.Printf("passive Front: edge_id=%s status=%s samples=%d", read.node, read.status, len(read.samples))
		}
	}
	cancel()
	for edge, id := range identities {
		row := fleet[edge]
		// Without a selected Front providing feedback, the endpoint may
		// keep serving but must not receive exploratory traffic.
		if row.status == "ready" && !passiveFrontExplorationHold(row.stats) {
			continue
		}
		for _, address := range []string{id.IPv4, id.IPv6} {
			if address != "" {
				holds = append(holds, edgequality.PassiveEndpoint{EdgeID: edge, GroupID: id.GroupID, Address: address})
			}
		}
	}
	slices.SortFunc(holds, func(a, b edgequality.PassiveEndpoint) int {
		if a.EdgeID != b.EdgeID {
			return strings.Compare(a.EdgeID, b.EdgeID)
		}
		return strings.Compare(a.Address, b.Address)
	})
	const perEdge = 1023
	sampleReadCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	complete := now.Truncate(edgequality.PassiveASNInterval)
	previousWindowEnd := complete.Add(-edgequality.PassiveASNWindow)
	capped := make(map[string]bool)
	readWindow := func(end time.Time) ([]model.EdgeNetworkSample, error) {
		batch, err := s.store.ListPassiveEdgeNetworkSamples(sampleReadCtx, edgeIDs, end.Add(-edgequality.PassiveASNWindow), end, perEdge+1)
		if err != nil {
			return nil, err
		}
		counts := make(map[string]int, len(edgeIDs))
		return slices.DeleteFunc(batch, func(sample model.EdgeNetworkSample) bool {
			counts[sample.EdgeID]++
			over := counts[sample.EdgeID] > perEdge
			if over {
				capped[sample.EdgeID] = true
			}
			return over
		}), nil
	}
	samples, err := readWindow(complete)
	if err != nil {
		return err
	}
	prior, err := readWindow(previousWindowEnd)
	if err != nil {
		return err
	}
	cancel()
	window.cappedEdges = len(capped)
	window.edges = len(identities)
	currentComparisons, coverage := edgequality.ComparePassiveASN(samples, identities, resolver, now)
	// Replay the preceding real window before raw storage prunes it. Never
	// mix widely separated windows or promote its original timestamps.
	historical, priorCoverage := edgequality.ComparePassiveASN(prior, identities, resolver, previousWindowEnd)
	comparisons := append(slices.Clone(historical), currentComparisons...)
	currentEstimates := coverage.FleetEstimates
	estimates := append(slices.Clone(priorCoverage.FleetEstimates), currentEstimates...)
	coverage.FleetEstimates = nil
	window.coverage = coverage
	var ranks []passiveFleetStatus
	// Observe each selected authority even when it received no new sample.
	// Read failure never fails or renews the measurement history.
	s.observePassiveASNDNS(ctx, policies, &window)
	if rules > 0 || mapping == risMapping {
		const maxCheckpointBytes = 4 << 20
		updated, err := s.store.UpdateObservationCheckpoint(ctx, passiveASNCheckpointKey, func(raw []byte) ([]byte, error) {
			if len(raw) > maxCheckpointBytes {
				return nil, errors.New("passive ASN checkpoint exceeds bound")
			}
			last := passiveASNCheckpoint{}
			if len(raw) > 0 && json.Unmarshal(raw, &last) != nil {
				return nil, errors.New("passive ASN checkpoint is invalid")
			}
			if mapping == risMapping {
				comparisons = revalidatePassiveASN(comparisons, resolver)
			}
			next, changed, saturated := retainPassiveASN(last, mapping, comparisons)
			provisional, _, _ := retainPassiveASN(passiveASNCheckpoint{Mapping: last.Mapping, Comparisons: last.Estimates}, mapping, estimates)
			next.Estimates = provisional.Comparisons
			var admittedEstimates []edgequality.PassiveASNComparison
			for _, estimate := range next.Estimates {
				if passiveASNMatchesMapping(estimate, resolver) {
					admittedEstimates = append(admittedEstimates, estimate)
				}
			}
			window.saturated = saturated
			admitted := make([]edgequality.PassiveASNComparison, 0, len(next.Comparisons))
			for _, comparison := range next.Comparisons {
				if mapping == risMapping && !passiveASNMatchesMapping(comparison, resolver) {
					continue
				}
				window.retained++
				admitted = append(admitted, comparison)
				if comparison.Preferred != "" {
					window.preferring++
				}
			}
			next.FleetSnapshots = make(map[int]passiveFleetSnapshot, 2)
			next.FleetLive = make(map[int][2]passiveFleetSnapshot, 2)
			next.FleetBootstrap = make(map[int]passiveFleetSnapshot, 2)
			for _, family := range []int{4, 6} {
				var endpoints []edgequality.PassiveEndpoint
				for edge, id := range identities {
					address := id.IPv4
					if family == 6 {
						address = id.IPv6
					}
					if address != "" {
						endpoints = append(endpoints, edgequality.PassiveEndpoint{EdgeID: edge, GroupID: id.GroupID, Address: address})
					}
				}
				live, effective, snapshot, fallbackAt := selectPassiveFleet(now, family, endpoints, admitted, admittedEstimates,
					previousWindowEnd, historical, priorCoverage.FleetEstimates, last.FleetSnapshots[family], resolver)
				if !snapshot.At.IsZero() {
					next.FleetSnapshots[family] = snapshot
				}
				current := capturePassiveFleet(complete, passiveasn.RankFleet(family, endpoints, currentComparisons), currentComparisons)
				previous := capturePassiveFleet(previousWindowEnd, passiveasn.RankFleet(family, endpoints, historical), historical)
				next.FleetLive[family] = [2]passiveFleetSnapshot{current, previous}
				// A single new live window must not bypass the two-window gate
				// by becoming the observer's diagnostic historical fallback.
				bootstrap := last.FleetBootstrap[family]
				if last.FleetLive == nil {
					bootstrap = last.FleetSnapshots[family]
				}
				if !fleetDefaultUsableSnapshot(bootstrap, family, now, fleetDefaultMaxHistory, resolver).At.IsZero() {
					next.FleetBootstrap[family] = bootstrap
				}
				for i, rank := range effective {
					ranks = append(ranks, passiveFleetStatus{rank: rank, live: live[i], historical: fallbackAt})
				}
			}
			next.ObservedAt = now
			next.CohortExploration = true
			next.ExplorationHolds = holds
			next.Cohorts = retainPassiveASNCohorts(collectPassiveASNCohorts(append(samples, prior...), identities, resolver), next.Cohorts, resolver)
			next.DNSProofs = window.dnsProofs
			for node, proof := range next.DNSProofs {
				proof = proof.inherit(last.DNSProofs[node])
				next.DNSProofs[node] = proof
				if proof.WireVerified {
					window.dnsWireVerified++
				}
				if proof.Verified {
					window.dnsVerified++
				}
			}
			changed = changed || !last.ObservedAt.Equal(now)
			if !changed {
				return nil, nil
			}
			encoded, err := json.Marshal(next)
			if len(encoded) > maxCheckpointBytes {
				return nil, errors.New("new passive ASN checkpoint exceeds bound")
			}
			return encoded, err
		})
		if err != nil {
			return err
		}
		if !updated {
			return errors.New("passive ASN observation checkpoint busy")
		}
	}
	for _, rank := range ranks {
		row := fleet[rank.rank.Edge.EdgeID]
		row.ranks = append(row.ranks, rank)
		fleet[row.id] = row
	}
	for _, row := range fleet {
		window.fleet = append(window.fleet, row)
	}
	slices.SortFunc(window.fleet, func(a, b passiveFleetEdge) int { return strings.Compare(a.id, b.id) })
	if s.log != nil {
		for _, row := range window.fleet {
			for _, result := range row.ranks {
				rank := result.rank
				s.log.Printf("passive fleet: edge_id=%s family=%d tier=%d basis=%s confident_peers=%d estimated_peers=%d comparable_peers=%d evidence_at=%s front=%s closed_samples=%d live_tier=%d live_basis=%s historical_snapshot_at=%s vote_score=%d",
					row.id, rank.Family, rank.Tier, rank.Basis, rank.ConfidentPeers, rank.EstimatedPeers, rank.ComparablePeers, rank.EvidenceAt.Format(time.RFC3339), row.status, row.samples, result.live.Tier, result.live.Basis, result.historical.Format(time.RFC3339), rank.VoteScore)
			}
		}
	}
	s.passiveASN.mu.Lock()
	s.passiveASN.window = window
	s.passiveASN.mu.Unlock()
	if s.log != nil {
		s.log.Printf("passive ASN: mapping_rules=%d bgp_source_at=%s edges=%d capped_edges=%d connections=%d classified=%d rtt=%d classified_rtt=%d reused_bundle_samples=%d retained=%d preferred=%d saturated=%d dns_nodes=%d dns_reads=%d dns_answers=%d ecs=%d ecs_asn_hints=%d dns_verified=%d dns_wire_verified=%d",
			window.rules, window.bgpSourceAt.Format(time.RFC3339), window.edges, window.cappedEdges, coverage.Connections, coverage.Classified, coverage.TCPRTT, coverage.ClassifiedRTT,
			coverage.ReusedBundleSamples, window.retained, window.preferring, window.saturated, window.dnsNodes, window.dnsReads, window.dnsAnswers, window.dnsECS, window.dnsASNHints, window.dnsVerified, window.dnsWireVerified)
	}
	return nil
}

func retainPassiveASNCohorts(current, history []passiveasn.Binding, lookup func(netip.Prefix) uint32) []passiveasn.Binding {
	seen := make(map[netip.Prefix]bool, len(current))
	for _, cohort := range current {
		seen[cohort.Prefix] = true
	}
	for _, cohort := range history {
		if len(current) >= passiveasn.MaxBindings {
			break
		}
		if !seen[cohort.Prefix] && cohort.ASN != 0 && lookup(cohort.Prefix) == cohort.ASN {
			current = append(current, cohort)
			seen[cohort.Prefix] = true
		}
	}
	return current
}

func collectPassiveASNCohorts(samples []model.EdgeNetworkSample, identities map[string]edgequality.PassiveEdgeIdentity, lookup func(netip.Prefix) uint32) []passiveasn.Binding {
	results := make([]passiveasn.Binding, 0)
	seen := map[netip.Prefix]bool{}
	for _, sample := range samples {
		id, ok := identities[sample.EdgeID]
		if !ok {
			continue
		}
		p, admitted := edgequality.PassiveSamplePrefix(sample, id)
		if !admitted || seen[p] {
			continue
		}
		seen[p] = true
		if asn := lookup(p); asn != 0 {
			results = append(results, passiveasn.Binding{Prefix: p, ASN: asn})
		}
		if len(results) >= passiveasn.MaxBindings {
			break
		}
	}
	slices.SortFunc(results, func(a, b passiveasn.Binding) int { return a.Prefix.Addr().Compare(b.Prefix.Addr()) })
	return results
}

func passiveASNMatchesMapping(comparison edgequality.PassiveASNComparison, resolve func(netip.Prefix) uint32) bool {
	if len(comparison.Prefixes) == 0 || len(comparison.Prefixes) > 32 || comparison.ASN == 0 {
		return false
	}
	for _, prefix := range comparison.Prefixes {
		if resolve(prefix) != comparison.ASN {
			return false
		}
	}
	return true
}

func revalidatePassiveASN(comparisons []edgequality.PassiveASNComparison, resolve func(netip.Prefix) uint32) []edgequality.PassiveASNComparison {
	valid := comparisons[:0]
	for _, comparison := range comparisons {
		if passiveASNMatchesMapping(comparison, resolve) {
			valid = append(valid, comparison)
		}
	}
	return valid
}

func (s *Server) passiveASNSignedMappings(ctx context.Context) ([]platformconfig.DNSClientPolicy, error) {
	gate, _, found, err := s.store.GetActivePlatformArtifactWithReaderContext(ctx, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow", nil)
	if err != nil || !found {
		return nil, err
	}
	signed := func(a model.PlatformArtifact) bool {
		return a.Status == model.PlatformArtifactStatusValidated && s.store.VerifyPlatformArtifactIntegrity(a) == nil &&
			platformsafety.EvaluateArtifactIntegrity(a, s.bundleKeyring()).Pass
	}
	policy, err := platformproducer.Decode(gate)
	if err != nil || !signed(gate) {
		return nil, errors.New("passive ASN producer authority invalid")
	}
	if policy.DNSPolicyArtifactID == "" || policy.Mode == "paused" {
		return nil, nil
	}
	source, err := s.store.GetPlatformArtifactContext(ctx, policy.DNSPolicyArtifactID)
	if err != nil {
		return nil, err
	}
	if source.ArtifactKind != model.PlatformArtifactKindPolicySnapshot || source.ScopeKey != policy.TargetScope ||
		source.ContentHash != policy.DNSPolicyDigest || !signed(source) {
		return nil, errors.New("passive ASN mapping source not trusted")
	}
	var input platformproducer.ProjectionPolicyInput
	if decodeCompilerArtifactContent(source, &input) != nil || len(input.Clients) > 8 ||
		platformconfig.ValidateDNSClientPolicies(input.Clients) != nil {
		return nil, errors.New("passive ASN DNS client mappings invalid or out of bounds")
	}
	return input.Clients, nil
}

func buildPassiveASNResolver(policies []platformconfig.DNSClientPolicy) (func(netip.Prefix) uint32, int, error) {
	matchers := make([]platformconfig.DNSClientMatcher, 0, len(policies))
	rules := 0
	for _, policy := range policies {
		m, err := platformconfig.NewDNSClientMatcher(policy.Rules)
		if err != nil {
			return nil, 0, err
		}
		matchers = append(matchers, m)
		for _, rule := range policy.Rules {
			if platformconfig.ClientASN(rule.ASN) != 0 {
				rules++
			}
		}
	}
	return func(prefix netip.Prefix) uint32 {
		asn := uint32(0)
		for i, matcher := range matchers {
			got := matcher.LookupPrefixASN(prefix)
			if got == 0 || i > 0 && got != asn {
				return 0
			}
			asn = got
		}
		return asn
	}, rules, nil
}

func retainPassiveASN(last passiveASNCheckpoint, mapping string, comparisons []edgequality.PassiveASNComparison) (passiveASNCheckpoint, bool, int) {
	changed := false
	if last.Mapping != mapping || last.Comparisons == nil {
		last = passiveASNCheckpoint{Mapping: mapping, Comparisons: make(map[string]edgequality.PassiveASNComparison)}
		changed = len(comparisons) > 0
	}
	saturated := 0
	for _, next := range comparisons {
		key := next.Key()
		before, exists := last.Comparisons[key]
		if exists && !next.ObservedAt.After(before.ObservedAt) {
			continue
		}
		if !exists && len(last.Comparisons) >= edgequality.MaxPassiveASNComparisons {
			saturated++
			continue
		}
		last.Comparisons[key], changed = next, true
	}
	return last, changed, saturated
}

func (s *Server) observePassiveASNDNS(ctx context.Context, policies []platformconfig.DNSClientPolicy, window *passiveASNWindow) {
	dnsCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	window.dnsProofs = make(map[string]passiveASNDNSProof, len(policies))
	for _, policy := range policies {
		if dnsCtx.Err() != nil {
			break
		}
		nodeCtx, cancel := context.WithTimeout(dnsCtx, 4*time.Second)
		read, err := s.readPlatformDNSDecisions(nodeCtx, policy.NodeID, "", "", 16)
		cancel()
		if err != nil {
			continue
		}
		window.dnsReads++
		loaded := read.Snapshot.Publication.Loaded
		proof := passiveASNDNSProof{Process: read.Snapshot.ProcessID, PodUID: read.Backend.PodUID, ScopeKey: read.scopeKey}
		if loaded != nil && loaded.ArtifactID != "" && loaded.Digest != "" && loaded.ReleaseSetID != "" &&
			loaded.ReleaseChannel == model.PlatformArtifactReleaseChannelFull && !read.Snapshot.Publication.ServingLKG {
			proof.Publication = *loaded
		}
		for _, receipt := range read.Snapshot.Receipts {
			if receipt.ProcessID != read.Snapshot.ProcessID || receipt.ObservedAt.Before(window.observedAt.Add(-10*time.Minute)) ||
				receipt.ObservedAt.After(read.EvaluatedAt) {
				continue
			}
			ecs, hint, valid := dnsserver.DecisionECSCoverage(receipt)
			if valid {
				window.dnsAnswers++
			}
			if ecs {
				window.dnsECS++
			}
			if hint {
				window.dnsASNHints++
			}
			if valid && proof.Publication.ArtifactID != "" && receipt.AnswerPublication != nil &&
				*receipt.AnswerPublication == proof.Publication {
				proof.WireVerified = true
				if ecs {
					for _, record := range receipt.Records {
						if record.PassiveASN != nil && record.PassiveASN.ASN != 0 && record.PassiveASN.ObservationDigest != "" {
							proof.Verified = true
							break
						}
					}
				}
			}
		}
		if proof.Publication.ArtifactID != "" && proof.PodUID != "" {
			window.dnsProofs[policy.NodeID] = proof
		}
	}
}

func (s *Server) writePassiveASNMetrics(w io.Writer) {
	s.passiveASN.mu.Lock()
	window, failures := s.passiveASN.window, s.passiveASN.failures
	s.passiveASN.mu.Unlock()
	observability.WriteCounterMetric(w, "fugue_edge_passive_asn_errors_total", "Passive ASN shadow observation failures.", nil, float64(failures))
	if window.observedAt.IsZero() {
		return
	}
	observability.WriteGaugeMetric(w, "fugue_edge_passive_asn_last_observed_timestamp_seconds", "Last completed passive ASN observation window.", nil, float64(window.observedAt.Unix()))
	if !window.bgpSourceAt.IsZero() {
		observability.WriteGaugeMetric(w, "fugue_edge_passive_asn_bgp_source_timestamp_seconds", "Oldest family timestamp of the bulk BGP mapping in use.", nil, float64(window.bgpSourceAt.Unix()))
	}
	const observationMetric = "fugue_edge_passive_asn_observation"
	observability.WriteMetricHeader(w, observationMetric, "Counts from the last bounded passive ASN observation window.", "gauge")
	for _, entry := range []struct {
		kind  string
		count int
	}{
		{"mapping_rules", window.rules}, {"edges", window.edges}, {"capped_edges", window.cappedEdges},
		{"connections", window.coverage.Connections}, {"asn_connections", window.coverage.Classified},
		{"tcp_rtt_connections", window.coverage.TCPRTT}, {"asn_tcp_rtt_connections", window.coverage.ClassifiedRTT},
		{"reused_bundle_samples", window.coverage.ReusedBundleSamples},
		{"retained_comparisons", window.retained}, {"preferred_comparisons", window.preferring}, {"unretained_new_comparisons", window.saturated},
		{"dns_nodes", window.dnsNodes}, {"dns_reads", window.dnsReads}, {"dns_answers", window.dnsAnswers},
		{"dns_ecs", window.dnsECS}, {"dns_ecs_asn_hint_upper_bound", window.dnsASNHints},
		{"dns_passive_verified", window.dnsVerified},
		{"dns_wire_verified", window.dnsWireVerified},
	} {
		observability.WriteMetricSample(w, observationMetric, map[string]string{"kind": entry.kind}, float64(entry.count))
	}
	const frontMetric = "fugue_edge_passive_front_observation"
	const frontStatus = "fugue_edge_passive_front_status"
	const rankMetric = "fugue_edge_passive_fleet_rank"
	const liveRankMetric = "fugue_edge_passive_fleet_live_rank"
	observability.WriteMetricHeader(w, frontMetric, "Last verified per-edge Front funnel snapshot.", "gauge")
	observability.WriteMetricHeader(w, frontStatus, "Status of the most recent Front read.", "gauge")
	observability.WriteMetricHeader(w, rankMetric, "Cross-ASN evidence tier; zero means unranked.", "gauge")
	observability.WriteMetricHeader(w, liveRankMetric, "Current cross-ASN evidence tier without historical fallback; zero means unranked.", "gauge")
	for _, edge := range window.fleet {
		write := func(kind string, n uint64) {
			observability.WriteMetricSample(w, frontMetric, map[string]string{"edge": edge.id, "kind": kind}, float64(n))
		}
		observability.WriteMetricSample(w, frontStatus, map[string]string{"edge": edge.id, "status": edge.status}, 1)
		write("closed_batch_samples", uint64(edge.samples))
		if p := edge.stats; p != nil {
			write("eligible_total", p.Eligible)
			write("recorded_total", p.Recorded)
			write("delivery_total", p.Delivery)
			write("active_samples_total", p.ActiveSamples)
			write("active_limited_total", p.ActiveLimited)
			write("no_payload_total", p.NoPayload)
			write("no_rtt_total", p.NoRTT)
			write("invalid_total", p.Invalid)
			write("lock_busy_total", p.LockBusy)
			write("prefix_limited_total", p.PrefixLimited)
			write("bucket_limited_total", p.BucketLimited)
			write("recent_forward_ok", p.RecentForwardOK)
			write("recent_forward_error", p.RecentForwardError)
		}
		for _, result := range edge.ranks {
			rank := result.rank
			observability.WriteMetricSample(w, rankMetric, map[string]string{
				"edge": edge.id, "family": strconv.Itoa(rank.Family), "basis": rank.Basis,
			}, float64(rank.Tier))
			observability.WriteMetricSample(w, liveRankMetric, map[string]string{
				"edge": edge.id, "family": strconv.Itoa(rank.Family), "basis": result.live.Basis,
			}, float64(result.live.Tier))
		}
	}
}
