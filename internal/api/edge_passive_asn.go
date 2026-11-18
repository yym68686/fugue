package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"sync"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/observability"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

const passiveASNCheckpointKey = "edge-passive-asn-comparison/v1"

type passiveASNCheckpoint struct {
	Mapping     string                                      `json:"mapping"`
	Comparisons map[string]edgequality.PassiveASNComparison `json:"comparisons"`
}

type passiveASNState struct {
	mu       sync.Mutex
	window   passiveASNWindow
	failures int64
}

type passiveASNWindow struct {
	observedAt                                                 time.Time
	coverage                                                   edgequality.PassiveASNCoverage
	rules, edges, cappedEdges, retained, preferring, saturated int
	dnsNodes, dnsReads, dnsAnswers, dnsECS, dnsASNHints        int
}

// Default-on observation over the authenticated Front sample path. This
// worker cannot change placement, publish DNS, or issue business requests.
func (s *Server) StartBackgroundPassiveASN(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
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
	nodes, _, err := s.store.ListActiveEdgeNodes("")
	if err != nil {
		return err
	}
	if len(nodes) > 32 {
		return errors.New("passive ASN edge inventory exceeds observation bound")
	}
	identities := make(map[string]edgequality.PassiveEdgeIdentity)
	var edgeIDs []string
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
	}
	const perEdge = 512
	sampleReadCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
	batch, err := s.store.ListPassiveEdgeNetworkSamples(sampleReadCtx, edgeIDs, now.Truncate(edgequality.PassiveASNInterval).Add(-edgequality.PassiveASNWindow), perEdge+1)
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
	window.coverage = coverage
	if rules > 0 {
		mapping, err := platformconfig.Digest(policies)
		if err != nil {
			return err
		}
		const maxCheckpointBytes = 1 << 20
		updated, err := s.store.UpdateObservationCheckpoint(ctx, passiveASNCheckpointKey, func(raw []byte) ([]byte, error) {
			if len(raw) > maxCheckpointBytes {
				return nil, errors.New("passive ASN checkpoint exceeds bound")
			}
			last := passiveASNCheckpoint{}
			if len(raw) > 0 && json.Unmarshal(raw, &last) != nil {
				return nil, errors.New("passive ASN checkpoint is invalid")
			}
			next, changed, saturated := retainPassiveASN(last, mapping, comparisons)
			window.retained, window.saturated = len(next.Comparisons), saturated
			for _, comparison := range next.Comparisons {
				if comparison.Preferred != "" {
					window.preferring++
				}
			}
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
	// A small bounded sample of existing authoritative decisions. Timeouts
	// never fail the network observation or mutate the DNS journal.
	s.observePassiveASNDNS(ctx, policies, &window)
	s.passiveASN.mu.Lock()
	s.passiveASN.window = window
	s.passiveASN.mu.Unlock()
	if s.log != nil {
		s.log.Printf("passive ASN shadow: mapping_rules=%d edges=%d capped_edges=%d connections=%d classified=%d rtt=%d classified_rtt=%d retained=%d preferred=%d saturated=%d dns_nodes=%d dns_reads=%d dns_answers=%d ecs=%d ecs_asn_hints=%d",
			window.rules, window.edges, window.cappedEdges, coverage.Connections, coverage.Classified, coverage.TCPRTT, coverage.ClassifiedRTT,
			window.retained, window.preferring, window.saturated, window.dnsNodes, window.dnsReads, window.dnsAnswers, window.dnsECS, window.dnsASNHints)
	}
	return nil
}

func (s *Server) passiveASNSignedMappings(ctx context.Context) ([]platformconfig.DNSClientPolicy, error) {
	gate, _, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
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
	for _, entry := range []struct {
		kind  string
		count int
	}{
		{"mapping_rules", window.rules}, {"edges", window.edges}, {"capped_edges", window.cappedEdges},
		{"connections", window.coverage.Connections}, {"asn_connections", window.coverage.Classified},
		{"tcp_rtt_connections", window.coverage.TCPRTT}, {"asn_tcp_rtt_connections", window.coverage.ClassifiedRTT},
		{"retained_comparisons", window.retained}, {"preferred_comparisons", window.preferring}, {"unretained_new_comparisons", window.saturated},
		{"dns_nodes", window.dnsNodes}, {"dns_reads", window.dnsReads}, {"dns_answers", window.dnsAnswers},
		{"dns_ecs", window.dnsECS}, {"dns_ecs_asn_hint_upper_bound", window.dnsASNHints},
	} {
		observability.WriteGaugeMetric(w, "fugue_edge_passive_asn_observation", "Counts from the last bounded passive ASN observation window.", map[string]string{"kind": entry.kind}, float64(entry.count))
	}
}
