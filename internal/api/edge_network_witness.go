package api

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

type networkRouteWitnessState struct {
	mu       sync.Mutex
	last     map[string]time.Time
	inFlight int
}

func (state *networkRouteWitnessState) reserve(edgeID string, now time.Time) bool {
	if !state.mu.TryLock() {
		return false
	}
	defer state.mu.Unlock()
	if state.inFlight >= 4 || now.Sub(state.last[edgeID]) < time.Minute {
		return false
	}
	if state.last == nil {
		state.last = map[string]time.Time{}
	}
	for key, previous := range state.last {
		if now.Sub(previous) >= time.Minute {
			delete(state.last, key)
		}
	}
	if len(state.last) >= 256 {
		return false
	}
	state.last[edgeID] = now
	state.inFlight++
	return true
}

func (state *networkRouteWitnessState) release() {
	state.mu.Lock()
	state.inFlight--
	state.mu.Unlock()
}

func (s *Server) observeNetworkRouteWitness(node model.EdgeNode, samples []model.EdgeNetworkSample, now time.Time) {
	address := node.PublicIPv4
	if address == "" {
		address = node.PublicIPv6
	}
	ip, err := netip.ParseAddr(address)
	if err != nil || !platformconfig.PublicDNSFlattenIP(ip) {
		return
	}
	var selected *model.EdgeNetworkSample
	for index := range samples {
		sample := &samples[index]
		if sample.EdgeID != node.ID || sample.EdgeGroupID != node.EdgeGroupID || sample.ObservedAt.After(now) ||
			now.Sub(sample.ObservedAt) > 90*time.Second || model.ValidateEdgeNetworkSample(*sample) != nil ||
			(sample.Source != "service_endpoint_tcp_info_v1" && sample.Source != "public_front_tcp_info_v1") {
			continue
		}
		if selected == nil || sample.ObservedAt.After(selected.ObservedAt) {
			selected = sample
		}
	}
	if selected == nil || !s.networkRouteWitness.reserve(node.ID, now) {
		return
	}
	sample := *selected
	go func() {
		defer s.networkRouteWitness.release()
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		proof, err := routeprobe.Probe(ctx, sample.Hostname, sample.PathPrefix, address, "", 2*time.Second)
		if err == nil {
			var witness model.EdgeNetworkSample
			witness, err = networkRouteWitnessSample(sample, address, proof, time.Now().UTC())
			if err == nil {
				err = s.store.RecordEdgeNetworkRouteWitnesses(ctx, []model.EdgeNetworkSample{witness}, time.Now().UTC().Add(-time.Hour))
			}
		}
		if err != nil && s.log != nil {
			s.log.Printf("edge network route witness unavailable; edge_id=%s error=%v", node.ID, err)
		}
	}()
}

func networkRouteWitnessSample(sample model.EdgeNetworkSample, address string, proof routeprobe.Proof, now time.Time) (model.EdgeNetworkSample, error) {
	if proof.EdgeID != sample.EdgeID || proof.GroupID != sample.EdgeGroupID || proof.Digest != sample.RouteDigest ||
		proof.Version != sample.BundleVersion || proof.State != "" || proof.CheckedAt.IsZero() || proof.CheckedAt.After(now) || now.Sub(proof.CheckedAt) > 5*time.Second {
		return model.EdgeNetworkSample{}, errors.New("route witness does not match observed immutable bundle")
	}
	witness := model.EdgeNetworkSample{ID: model.NewID("route_witness"), EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID,
		Hostname: sample.Hostname, PathPrefix: sample.PathPrefix, TrafficClass: sample.TrafficClass, RouteDigest: proof.Digest,
		BundleVersion: proof.Version, Source: "route_tls_witness_v1", ObservedAt: proof.CheckedAt,
		RouteWitness: &model.EdgeNetworkRouteWitness{Address: address, ValidUntil: proof.ValidUntil}}
	if !model.EdgeNetworkWitnessMatches(sample, witness) {
		return model.EdgeNetworkSample{}, errors.New("route witness is outside the bounded observation interval")
	}
	return witness, nil
}
