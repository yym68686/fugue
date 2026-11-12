package api

import (
	"context"
	"sync"
	"time"

	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
)

// Observe candidates independently of the incumbent's materialized DNS set.
// These fixed HEAD probes establish identity/route/TLS, never client latency.
func (s *Server) captureIndependentQualityProofs(ctx context.Context, snapshot *edgequality.Snapshot, nodes []model.EdgeNode) {
	if len(nodes) > 8 || len(snapshot.NetworkSamples)+len(nodes) > edgequality.MaxObservations {
		return
	}
	path := snapshot.PathPrefix
	if path == "" {
		path = "/"
	}
	results := make([]*model.EdgeNetworkSample, len(nodes))
	var wg sync.WaitGroup
	limit := make(chan struct{}, 4)
	for index, node := range nodes {
		allowed := false
		for _, candidate := range snapshot.Candidates {
			allowed = allowed || candidate.EdgeID == node.ID && candidate.EdgeGroupID == node.EdgeGroupID && len(candidate.HardGates) == 0
		}
		if !allowed || node.PublicIPv4 == "" {
			continue
		}
		wg.Add(1)
		go func(index int, node model.EdgeNode) {
			defer wg.Done()
			select {
			case limit <- struct{}{}:
				defer func() { <-limit }()
			case <-ctx.Done():
				return
			}
			proof, err := routeprobe.Probe(ctx, snapshot.Hostname, path, node.PublicIPv4, "", 2*time.Second)
			if err != nil || proof.EdgeID != node.ID || proof.GroupID != node.EdgeGroupID || proof.State != "" {
				return
			}
			sample := model.EdgeNetworkSample{ID: model.NewID("quality_route"), EdgeID: node.ID, EdgeGroupID: node.EdgeGroupID,
				Hostname: snapshot.Hostname, PathPrefix: path, TrafficClass: snapshot.TrafficClass, RouteDigest: proof.Digest,
				BundleVersion: proof.Version, Source: "route_tls_witness_v1", ObservedAt: proof.CheckedAt,
				RouteWitness: &model.EdgeNetworkRouteWitness{Address: node.PublicIPv4, ValidUntil: proof.ValidUntil}}
			if model.ValidateEdgeNetworkSample(sample) == nil {
				results[index] = &sample
			}
		}(index, node)
	}
	wg.Wait()
	for _, sample := range results {
		if sample != nil {
			snapshot.NetworkSamples = append(snapshot.NetworkSamples, *sample)
		}
	}
	snapshot.CapturedAt = time.Now().UTC()
}

// All raw witnesses remain in the captured receipt. Choose the latest exact
// positive witness, bounded by both age and its original lease. Replaying v1-v6
// never consults this independent measurement universe.
func independentQualityEvidence(snapshot edgequality.Snapshot, evidence dnsserver.QualityAnswerEvidence) dnsserver.QualityAnswerEvidence {
	evidence.Proofs = append([]dnsserver.QualityRouteProof(nil), evidence.Proofs...)
	evidence.Candidates = append([]model.EdgeDNSAnswerCandidate(nil), evidence.Candidates...)
	for _, candidate := range snapshot.Candidates {
		if len(candidate.HardGates) != 0 {
			continue
		}
		var chosen *model.EdgeNetworkSample
		ambiguous := false
		for index := range snapshot.NetworkSamples {
			sample := &snapshot.NetworkSamples[index]
			if sample.Source != "route_tls_witness_v1" || sample.RouteWitness == nil || model.ValidateEdgeNetworkSample(*sample) != nil ||
				sample.EdgeID != candidate.EdgeID || sample.EdgeGroupID != candidate.EdgeGroupID || sample.Hostname != snapshot.Hostname ||
				snapshot.PathPrefix != "" && sample.PathPrefix != snapshot.PathPrefix || sample.TrafficClass != snapshot.TrafficClass ||
				sample.ObservedAt.After(snapshot.CapturedAt) || snapshot.CapturedAt.Sub(sample.ObservedAt) > time.Duration(snapshot.Policy.EvidenceMaxAgeSeconds)*time.Second || !sample.RouteWitness.ValidUntil.After(snapshot.CapturedAt) {
				continue
			}
			if chosen == nil || sample.ObservedAt.After(chosen.ObservedAt) {
				chosen, ambiguous = sample, false
			} else if sample.ObservedAt.Equal(chosen.ObservedAt) && (sample.RouteDigest != chosen.RouteDigest || sample.BundleVersion != chosen.BundleVersion || sample.RouteWitness.Address != chosen.RouteWitness.Address || sample.PathPrefix != chosen.PathPrefix) {
				ambiguous = true
			}
		}
		if chosen == nil || ambiguous {
			continue
		}
		filtered := evidence.Proofs[:0]
		for _, proof := range evidence.Proofs {
			if proof.EdgeID != candidate.EdgeID || proof.Hostname != snapshot.Hostname || snapshot.PathPrefix != "" && proof.Path != snapshot.PathPrefix {
				filtered = append(filtered, proof)
			}
		}
		evidence.Proofs = append(filtered, dnsserver.QualityRouteProof{EdgeID: chosen.EdgeID, EdgeGroupID: chosen.EdgeGroupID, Hostname: chosen.Hostname, Path: chosen.PathPrefix,
			Proof: routeprobe.Proof{EdgeID: chosen.EdgeID, GroupID: chosen.EdgeGroupID, Digest: chosen.RouteDigest, Version: chosen.BundleVersion, CheckedAt: chosen.ObservedAt, ValidUntil: chosen.RouteWitness.ValidUntil}})
		evidence.Candidates = append(evidence.Candidates, model.EdgeDNSAnswerCandidate{EdgeID: chosen.EdgeID, EdgeGroupID: chosen.EdgeGroupID, IP: chosen.RouteWitness.Address})
	}
	return evidence
}
