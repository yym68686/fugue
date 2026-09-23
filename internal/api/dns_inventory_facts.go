package api

import (
	"context"
	"strings"
	"sync"
	"time"

	"fugue/internal/model"
)

// Inventory provides membership and historical counters. Once a node has a
// bound artifact-consumer identity, the selected backend owns serving health.
// A failing read must not downgrade authority to the old inventory writer.
func (s *Server) dnsInventoryServingFacts(ctx context.Context, nodes []model.DNSNode) ([]model.DNSNode, error) {
	if len(nodes) == 0 {
		return nodes, nil
	}
	consumers, err := s.store.ListPlatformConsumers(model.PlatformArtifactKindDNSAnswerBundle, "global")
	if err != nil {
		return nil, err
	}
	enrolled := map[string]bool{}
	for _, consumer := range consumers {
		if consumer.Component == model.PlatformConsumerComponentDNSServer && strings.HasPrefix(consumer.CredentialID, "kubernetes:") {
			enrolled[consumer.NodeID] = true
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	type observation struct {
		facts platformDNSRuntimeFactsResponse
		err   error
	}
	results := map[string]observation{}
	var mu sync.Mutex
	var workers sync.WaitGroup
	queue := make(chan string)
	for i := 0; i < min(4, len(enrolled)); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for node := range queue {
				facts, err := s.readPlatformDNSRuntimeFacts(ctx, node)
				mu.Lock()
				results[node] = observation{facts, err}
				mu.Unlock()
			}
		}()
	}
	seen := map[string]bool{}
	for _, node := range nodes {
		id := firstNonEmpty(node.PhysicalNodeID, node.ID)
		if !enrolled[id] || seen[id] {
			continue
		}
		seen[id] = true
		select {
		case queue <- id:
		case <-ctx.Done():
		}
	}
	close(queue)
	workers.Wait()
	out := append([]model.DNSNode(nil), nodes...)
	for i := range out {
		id := firstNonEmpty(out[i].PhysicalNodeID, out[i].ID)
		if !enrolled[id] {
			continue
		}
		result, found := results[id]
		out[i] = projectDNSInventoryServingFact(out[i], result.facts, !found || result.err != nil, time.Now().UTC())
	}
	return out, nil
}

func projectDNSInventoryServingFact(node model.DNSNode, facts platformDNSRuntimeFactsResponse, unavailable bool, now time.Time) model.DNSNode {
	node.ServingObservation = &model.DNSNodeServingObservation{Source: "selected_artifact_consumer", State: "unknown", EvaluatedAt: now,
		InventoryObservedAt: latestNodeActivity(node.LastHeartbeatAt, node.LastSeenAt)}
	// Never mix selected health with an old Pod's claimed serving generation,
	// cache verdict or transport error. Counts retain explicit historical scope.
	node.Status, node.Healthy = model.EdgeHealthUnknown, false
	node.DNSBundleVersion, node.ServingGeneration, node.LKGGeneration = "", "", ""
	node.CacheStatus, node.LastError = "unknown", "selected DNS backend observation unavailable"
	node.UDPListen, node.TCPListen = false, false
	if unavailable || !facts.heartbeatValidUntil.After(now) || facts.Snapshot.NodeID != firstNonEmpty(node.PhysicalNodeID, node.ID) || facts.Snapshot.EdgeGroupID != node.EdgeGroupID {
		return node
	}
	observation := node.ServingObservation
	observed := facts.Snapshot.ObservedAt
	observation.ObservedAt = &observed
	observation.BackendPodUID = facts.Backend.PodUID
	observation.ArtifactID = facts.Snapshot.Assignment.ArtifactID
	observation.ReleaseSetID = facts.Snapshot.Assignment.ReleaseSetID
	observation.ArtifactReleaseID = facts.Snapshot.Assignment.ArtifactReleaseID
	node.DNSBundleVersion = facts.Snapshot.Assignment.ExpectedGeneration
	node.ServingGeneration = node.DNSBundleVersion
	node.LKGGeneration = facts.lkgGeneration
	// The verified checkpoint's immutable configuration is known, but this
	// projection does not turn it into a new global positive LKG receipt.
	node.CacheStatus, node.Status, node.LastError = "ready", model.EdgeHealthUnhealthy, "selected DNS backend is not ready"
	observation.State = "not_ready"
	proofsFresh := facts.Ready && facts.Snapshot.CheckpointValidUntil.After(now)
	valid := map[string]bool{}
	for _, fact := range facts.Snapshot.Facts {
		valid[fact.ProbeID] = fact.Ready && fact.Proof.ValidUntil.After(now)
	}
	for _, id := range facts.ReadyProbeIDs {
		proofsFresh = proofsFresh && valid[id]
	}
	if proofsFresh {
		node.Status, node.Healthy, node.LastError = model.EdgeHealthHealthy, true, ""
		node.UDPListen, node.TCPListen = true, true
		observation.State = "ready"
	}
	return node
}
