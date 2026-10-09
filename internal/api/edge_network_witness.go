package api

import (
	"context"
	"errors"
	"net/netip"
	"sort"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeprobe"
)

type networkRouteWitnessState struct {
	mu       sync.Mutex
	last     map[string]networkRouteWitnessCursor
	inFlight int
}

type networkRouteWitnessCursor struct {
	at         time.Time
	key        string
	probeKey   string
	probeFirst bool
}

func (state *networkRouteWitnessState) release() {
	state.mu.Lock()
	state.inFlight--
	state.mu.Unlock()
}

func (state *networkRouteWitnessState) selectSample(node model.EdgeNode, samples []model.EdgeNetworkSample, now time.Time) (model.EdgeNetworkSample, bool) {
	eligible := map[string]model.EdgeNetworkSample{}
	probes := map[string]model.EdgeNetworkSample{}
	for index := range samples {
		sample := &samples[index]
		if sample.EdgeID != node.ID || sample.EdgeGroupID != node.EdgeGroupID || sample.ObservedAt.After(now) ||
			now.Sub(sample.ObservedAt) > 90*time.Second || model.ValidateEdgeNetworkSample(*sample) != nil ||
			(!model.EdgeNetworkServiceSource(sample.Source) && sample.Source != "public_front_tcp_info_v1") {
			continue
		}
		key := sample.Hostname + "\x00" + sample.PathPrefix + "\x00" + sample.TrafficClass
		target := eligible
		if sample.Source == "service_endpoint_tcp_probe_v1" {
			target = probes
		}
		previous, found := target[key]
		if !found || sample.ObservedAt.After(previous.ObservedAt) || sample.ObservedAt.Equal(previous.ObservedAt) && sample.ID < previous.ID {
			target[key] = *sample
		}
	}
	if len(eligible)+len(probes) == 0 || !state.mu.TryLock() {
		return model.EdgeNetworkSample{}, false
	}
	defer state.mu.Unlock()
	previous, found := state.last[node.ID]
	if state.inFlight >= 4 || found && now.Sub(previous.at) < time.Minute {
		return model.EdgeNetworkSample{}, false
	}
	if state.last == nil {
		state.last = map[string]networkRouteWitnessCursor{}
	}
	for edgeID, cursor := range state.last {
		if now.Sub(cursor.at) >= 10*time.Minute {
			delete(state.last, edgeID)
		}
	}
	if _, exists := state.last[node.ID]; !exists && len(state.last) >= 256 {
		return model.EdgeNetworkSample{}, false
	}
	target, cursor := eligible, previous.key
	useProbe := len(probes) > 0 && (len(eligible) == 0 || !previous.probeFirst)
	if useProbe {
		target, cursor = probes, previous.probeKey
	}
	keys := make([]string, 0, len(target))
	for key := range target {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	selected := keys[0]
	for _, key := range keys {
		if key > cursor {
			selected = key
			break
		}
	}
	previous.at, previous.probeFirst = now, useProbe
	if useProbe {
		previous.probeKey = selected
	} else {
		previous.key = selected
	}
	state.last[node.ID] = previous
	state.inFlight++
	return target[selected], true
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
	sample, ok := s.networkRouteWitness.selectSample(node, samples, now)
	if !ok {
		return
	}
	go func() {
		defer s.networkRouteWitness.release()
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		proof, err := routeprobe.Probe(ctx, sample.Hostname, sample.PathPrefix, address, "", 2*time.Second)
		if err == nil {
			var witness model.EdgeNetworkSample
			witness, err = networkRouteWitnessSample(sample, address, proof, time.Now().UTC())
			if err == nil {
				if client, clientErr := s.newClusterNodeClient(); clientErr == nil {
					capacityContext, capacityCancel := context.WithTimeout(ctx, time.Second)
					var capacityErr error
					witness.RouteWitness.NodeCapacity, capacityErr = readNetworkNodeCapacity(capacityContext, client, sample.EdgeID, address, time.Now)
					capacityCancel()
					client.closeIdleConnections()
					if capacityErr != nil && s.log != nil {
						s.log.Printf("edge network node capacity unavailable; edge_id=%s error=%v", node.ID, capacityErr)
					}
				}
				err = s.store.RecordEdgeNetworkRouteWitnesses(ctx, []model.EdgeNetworkSample{witness}, time.Now().UTC().Add(-time.Hour))
			}
		}
		if err != nil && s.log != nil {
			s.log.Printf("edge network route witness unavailable; edge_id=%s hostname=%s path=%s traffic_class=%s sample_id=%s sample_bundle=%s sample_digest=%s proof_bundle=%s proof_digest=%s error=%v", node.ID, sample.Hostname, sample.PathPrefix, sample.TrafficClass, sample.ID, sample.BundleVersion, sample.RouteDigest, proof.Version, proof.Digest, err)
		}
	}()
}

func networkRouteWitnessSample(sample model.EdgeNetworkSample, address string, proof routeprobe.Proof, now time.Time) (model.EdgeNetworkSample, error) {
	reason := ""
	switch {
	case model.ValidateEdgeNetworkSample(sample) != nil:
		reason = "invalid_network_sample"
	case proof.EdgeID != sample.EdgeID || proof.GroupID != sample.EdgeGroupID:
		reason = "physical_identity_mismatch"
	case proof.Digest != sample.RouteDigest:
		reason = "route_digest_mismatch"
	case proof.State != "":
		reason = "route_not_serving"
	case proof.CheckedAt.IsZero() || proof.CheckedAt.After(now):
		reason = "invalid_proof_time"
	case now.Sub(proof.CheckedAt) > 5*time.Second:
		reason = "stale_proof"
	case sample.ObservedAt.After(now):
		reason = "future_sample"
	case now.Sub(sample.ObservedAt) > 2*time.Minute:
		reason = "stale_sample"
	}
	if reason != "" {
		return model.EdgeNetworkSample{}, errors.New("route witness rejected: " + reason)
	}
	witness := model.EdgeNetworkSample{ID: model.NewID("route_witness"), EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID,
		Hostname: sample.Hostname, PathPrefix: sample.PathPrefix, TrafficClass: sample.TrafficClass, RouteDigest: proof.Digest,
		BundleVersion: proof.Version, Source: "route_tls_witness_v1", ObservedAt: proof.CheckedAt,
		RouteWitness: &model.EdgeNetworkRouteWitness{Address: address, ValidUntil: proof.ValidUntil}}
	if model.ValidateEdgeNetworkSample(witness) != nil {
		return model.EdgeNetworkSample{}, errors.New("current route witness invalid")
	}
	return witness, nil
}
