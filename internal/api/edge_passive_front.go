package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"fugue/internal/edgequality"
	"fugue/internal/frontnetwork"
	"fugue/internal/model"
)

type passiveFrontRead struct {
	node     string
	identity *edgequality.PassiveFrontIdentity
	samples  []model.EdgeNetworkSample
	status   string
}

func (s *Server) collectPassiveFronts(ctx context.Context, nodes []model.EdgeNode) []passiveFrontRead {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	client, err := s.newClusterNodeClient()
	if err != nil {
		return nil
	}
	defer client.closeIdleConnections()
	out := make([]passiveFrontRead, len(nodes))
	var wg sync.WaitGroup
	budget := make(chan struct{}, 4)
	for i, node := range nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case budget <- struct{}{}:
				defer func() { <-budget }()
				out[i] = s.readPassiveFront(ctx, client, node)
			case <-ctx.Done():
				out[i] = passiveFrontRead{node: node.ID, status: "observation_budget"}
			}
		}()
	}
	wg.Wait()
	return out
}

func (s *Server) readPassiveFront(ctx context.Context, client *clusterNodeClient, node model.EdgeNode) passiveFrontRead {
	result := passiveFrontRead{node: node.ID, status: "binding_unavailable"}
	b, err := s.resolvePublicFrontBinding(ctx, client, node)
	if err != nil {
		return result
	}
	var batch frontnetwork.PassiveBatch
	batchErr := readDNSPodObservation(ctx, client, b.proxyPath+frontnetwork.PassivePath, &batch, frontnetwork.PassiveResponseLimit)
	backend, err := b.verifiedBackend(ctx, client, node)
	if err != nil {
		return result
	}
	identity := &edgequality.PassiveFrontIdentity{Backend: *backend}
	for _, c := range b.pod.Status.ContainerStatuses {
		if c.Name == "edge-front" && c.State.Running != nil {
			identity.ProcessStartedAt = c.State.Running.StartedAt.Time
		}
	}
	if identity.ProcessStartedAt.IsZero() {
		return result
	}
	if slices.Contains(b.service.Spec.ExternalIPs, node.PublicIPv4) {
		identity.IPv4 = node.PublicIPv4
	}
	if slices.Contains(b.service.Spec.ExternalIPs, node.PublicIPv6) {
		identity.IPv6 = node.PublicIPv6
	}
	result.identity = identity
	result.status = "batch_unavailable"
	// An old Front can still prove its selected runtime identity. Its
	// absence of the new batch API must not erase verified legacy evidence.
	if batchErr != nil {
		return result
	}
	result.samples, result.status = bindPassiveFrontBatch(batch, node, identity, time.Now().UTC())
	return result
}

func bindPassiveFrontBatch(batch frontnetwork.PassiveBatch, node model.EdgeNode, identity *edgequality.PassiveFrontIdentity, now time.Time) ([]model.EdgeNetworkSample, string) {
	if batch.Schema != frontnetwork.PassiveSchema || batch.EdgeID != node.ID || batch.GroupID != node.EdgeGroupID ||
		batch.Samples == nil || len(batch.Samples) > frontnetwork.PassiveBatchLimit {
		return nil, "batch_identity"
	}
	buckets := make(map[time.Time]int)
	prefixes := make(map[time.Time]map[string]int)
	seen := make(map[string]bool)
	out := make([]model.EdgeNetworkSample, 0, len(batch.Samples))
	oldest := now.Truncate(frontnetwork.PassiveInterval).Add(-frontnetwork.PassiveBuckets * frontnetwork.PassiveInterval)
	for _, c := range batch.Samples {
		if c.Backend != nil || model.ValidateEdgeClientNetworkSample(&c) != nil || c.RTTMS == nil || *c.RTTMS <= 0 ||
			c.ObservedAt.After(now) || c.ObservedAt.Before(oldest) || c.StartedAt.Before(identity.ProcessStartedAt) || seen[c.ConnectionID] {
			return nil, "batch_sample"
		}
		seen[c.ConnectionID] = true
		bucket := c.ObservedAt.UTC().Truncate(frontnetwork.PassiveInterval)
		buckets[bucket]++
		if prefixes[bucket] == nil {
			prefixes[bucket] = make(map[string]int)
		}
		prefixes[bucket][c.Scope]++
		if buckets[bucket] > frontnetwork.PassiveBucketLimit || prefixes[bucket][c.Scope] > frontnetwork.PassivePrefixLimit {
			return nil, "batch_limit"
		}
		prefix, _ := netip.ParsePrefix(strings.TrimPrefix(c.Scope, "tcp_peer:"))
		if (prefix.Addr().Is4() && identity.IPv4 == "") || (!prefix.Addr().Is4() && identity.IPv6 == "") {
			continue // Not a publicly exposed address family for this service.
		}
		c.Backend = &identity.Backend
		key := sha256.Sum256([]byte(identity.Backend.PodUID + "\x00" + c.ConnectionID))
		out = append(out, model.EdgeNetworkSample{ID: "front_passive_" + hex.EncodeToString(key[:]), EdgeID: node.ID,
			EdgeGroupID: node.EdgeGroupID, Source: model.EdgeNetworkPassiveFrontSource, ClientNetwork: &c, ObservedAt: c.ObservedAt})
	}
	return out, "ready"
}
