package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/passiveasn"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

const passiveASNCheckpointKey = "edge-passive-asn-comparison/v1"

type passiveASNCheckpoint struct {
	Mapping     string                                      `json:"mapping"`
	Comparisons map[string]edgequality.PassiveASNComparison `json:"comparisons"`
	Cohorts     []passiveasn.Binding                        `json:"cohorts,omitempty"`
	ObservedAt  time.Time                                   `json:"observed_at,omitempty"`
	DNSProofs   map[string]passiveASNDNSProof               `json:"dns_proofs,omitempty"`
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
	writeCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	for _, read := range fronts {
		id := identities[read.node]
		id.Front = read.identity
		identities[read.node] = id
		if len(read.samples) != 0 {
			for chunk := range slices.Chunk(read.samples, 128) {
				if err := s.store.RecordEdgeNetworkSamples(writeCtx, chunk, time.Time{}); err != nil {
					read.status = "sample_store_unavailable"
					break
				}
			}
		}
		if s.log != nil {
			s.log.Printf("passive Front: edge_id=%s status=%s samples=%d", read.node, read.status, len(read.samples))
		}
	}
	cancel()
	const perEdge = 512
	sampleReadCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
	previousWindowEnd := now.Truncate(edgequality.PassiveASNInterval).Add(-edgequality.PassiveASNWindow)
	batch, err := s.store.ListPassiveEdgeNetworkSamples(sampleReadCtx, edgeIDs, previousWindowEnd.Add(-edgequality.PassiveASNWindow), perEdge+1)
	cancel()
	if err != nil {
		return err
	}
	samples := batch[:0]
	counts := make(map[string]int)
	for _, sample := range batch {
		counts[sample.EdgeID]++
		if counts[sample.EdgeID] == perEdge+1 {
			window.cappedEdges++
		}
		if counts[sample.EdgeID] <= perEdge {
			samples = append(samples, sample)
		}
	}
	window.edges = len(identities)
	comparisons, coverage := edgequality.ComparePassiveASN(samples, identities, resolver, now)
	// Replay the preceding real window before raw storage prunes it. Never
	// mix widely separated windows or promote its original timestamps.
	historical, _ := edgequality.ComparePassiveASN(samples, identities, resolver, previousWindowEnd)
	comparisons = append(historical, comparisons...)
	window.coverage = coverage
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
			window.saturated = saturated
			for _, comparison := range next.Comparisons {
				if mapping == risMapping && !passiveASNMatchesMapping(comparison, resolver) {
					continue
				}
				window.retained++
				if comparison.Preferred != "" {
					window.preferring++
				}
			}
			next.ObservedAt = now
			next.Cohorts = retainPassiveASNCohorts(collectPassiveASNCohorts(samples, identities, resolver), next.Cohorts, resolver)
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
		observability.WriteGaugeMetric(w, "fugue_edge_passive_asn_observation", "Counts from the last bounded passive ASN observation window.", map[string]string{"kind": entry.kind}, float64(entry.count))
	}
}
