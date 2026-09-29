package api

import (
	"context"
	"fmt"
	"net/netip"
	"slices"
	"sync"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

// Capture only physical node addresses, without importing old Edge/DNS
// ownership, health, loaded digests or serving receipts. The pinned declaration
// supplies membership; these observations supply no serving authorization.
func (s *Server) captureCellEndpoints(ctx context.Context, static platformproducer.StaticIntentInput) ([]model.EdgeNode, []model.DNSNode, error) {
	topology, err := platformconfig.TrafficConsumerTopologyFromIntent(platformconfig.PlatformIntent{Scope: static.Scope, AuthorityCellID: static.AuthorityCellID, EdgeTopology: static.EdgeTopology, DNSConsumers: static.Consumers})
	if err != nil || topology == nil {
		return nil, nil, fmt.Errorf("cell endpoint capture requires complete declared membership")
	}
	client, err := s.requireClusterNodeClient()
	if err != nil {
		return nil, nil, err
	}
	defer client.closeIdleConnections()
	ids := append(append([]string{}, topology.EdgeNodeIDs...), topology.DNSNodeIDs...)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	type endpoint struct {
		v4, v6 string
		err    error
	}
	values := make([]endpoint, len(ids))
	queue := make(chan int)
	var workers sync.WaitGroup
	for i := 0; i < min(4, len(ids)); i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range queue {
				n, err := client.getNode(ctx, ids[i])
				if err != nil || n.Metadata.Name != ids[i] {
					values[i].err = fmt.Errorf("declared physical endpoint unavailable")
					continue
				}
				addresses := []string{kubeNodePublicIP(n)}
				for _, a := range n.Status.Addresses {
					if a.Type == "ExternalIP" {
						addresses = append(addresses, a.Address)
					}
				}
				for _, a := range addresses {
					if a == "" {
						continue
					}
					ip, e := netip.ParseAddr(a)
					if e != nil || !platformconfig.PublicDNSFlattenIP(ip) || ip.String() != a {
						values[i].err = fmt.Errorf("node endpoint must be a canonical public address")
						break
					}
					dest := &values[i].v6
					if ip.Is4() {
						dest = &values[i].v4
					}
					if *dest != "" && *dest != a {
						values[i].err = fmt.Errorf("node has ambiguous public endpoints")
						break
					}
					*dest = a
				}
				if values[i].v4 == "" && values[i].v6 == "" {
					values[i].err = fmt.Errorf("declared physical node has no public endpoint")
				}
			}
		}()
	}
	for i := range ids {
		select {
		case queue <- i:
		case <-ctx.Done():
		}
	}
	close(queue)
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	byID := map[string]endpoint{}
	for i, id := range ids {
		if values[i].err != nil {
			return nil, nil, values[i].err
		}
		byID[id] = values[i]
	}
	edges := make([]model.EdgeNode, 0, len(topology.EdgeNodeIDs))
	for _, id := range topology.EdgeNodeIDs {
		v := byID[id]
		edges = append(edges, model.EdgeNode{ID: id, EdgeGroupID: static.AuthorityCellID, PublicIPv4: v.v4, PublicIPv6: v.v6})
	}
	dns := []model.DNSNode{}
	for _, c := range static.Consumers {
		v := byID[c.NodeID]
		for _, zone := range c.Zones {
			dns = append(dns, model.DNSNode{ID: c.NodeID, PhysicalNodeID: c.NodeID, EdgeGroupID: c.EdgeGroupID, Zone: zone, PublicIPv4: v.v4, PublicIPv6: v.v6})
		}
	}
	return edges, dns, nil
}
