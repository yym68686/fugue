package edgegroupfront

import (
	"net"
	"net/http"
	"net/netip"
	"time"

	"fugue/internal/frontnetwork"
	"fugue/internal/model"
	"fugue/internal/tcpdiag"
)

type passiveNetworkBucket struct {
	start        time.Time
	samples      []model.EdgeClientNetworkSample
	prefixCounts map[string]int
}

var sharedIPv4Space = netip.MustParsePrefix("100.64.0.0/10")

func passiveConnection(cfg Config, protocol, slot, id string, conn net.Conn, started time.Time) (edgeFrontActiveTCPConnection, bool) {
	if protocol != ProtocolHTTPS || !cfg.ProxyProtocol || cfg.EdgeID == "" || cfg.EdgeGroupID == "" {
		return edgeFrontActiveTCPConnection{}, false
	}
	remote := connAddr(conn.RemoteAddr())
	peer, err := frontnetwork.ParseRemote(remote)
	if err != nil || sharedIPv4Space.Contains(peer.Addr()) {
		return edgeFrontActiveTCPConnection{}, false
	}
	return edgeFrontActiveTCPConnection{ID: id, Slot: slot, DownstreamRemote: remote, StartedAt: started.UTC()}, true
}

func (s *Service) recordPassiveNetworkSample(connection edgeFrontActiveTCPConnection, baseline model.EdgeClientNetworkSample, snapshot tcpdiag.Snapshot, now time.Time) {
	sample := publicClientNetworkSample(connection, snapshot, now)
	if sample.RTTMS == nil {
		return
	}
	// Closed or very long downloads may still provide RTT. Never treat
	// missing/timed-out delivery counters as a zero-rate measurement.
	if sample.Delivery != nil && baseline.Delivery != nil {
		sample.DeliveryBaseline = baseline.Delivery
		if model.ValidateEdgeClientNetworkSample(&sample) != nil {
			sample.DeliveryBaseline = nil
		}
	}
	if model.ValidateEdgeClientNetworkSample(&sample) != nil || !s.passiveMu.TryLock() {
		return
	}
	defer s.passiveMu.Unlock()
	start := now.UTC().Truncate(frontnetwork.PassiveInterval)
	pos := int((start.Unix() / int64(frontnetwork.PassiveInterval.Seconds())) % frontnetwork.PassiveBuckets)
	bucket := &s.passive[pos]
	if bucket.start.Before(start) {
		*bucket = passiveNetworkBucket{start: start, prefixCounts: map[string]int{}}
	}
	if !bucket.start.Equal(start) || len(bucket.samples) >= frontnetwork.PassiveBucketLimit ||
		bucket.prefixCounts[sample.Scope] >= frontnetwork.PassivePrefixLimit {
		return
	}
	bucket.samples = append(bucket.samples, sample)
	bucket.prefixCounts[sample.Scope]++
}

func (s *Service) handlePassiveNetworkSamples(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	if r.Method != http.MethodGet || r.URL.RawQuery != "" {
		http.Error(w, "invalid passive Front batch request", http.StatusBadRequest)
		return
	}
	if s.Config.EdgeID == "" || s.Config.EdgeGroupID == "" || !s.passiveMu.TryLock() {
		http.Error(w, "passive Front observation unavailable", http.StatusServiceUnavailable)
		return
	}
	latest := time.Now().UTC().Truncate(frontnetwork.PassiveInterval)
	oldest := latest.Add(-(frontnetwork.PassiveBuckets - 1) * frontnetwork.PassiveInterval)
	batch := frontnetwork.PassiveBatch{Schema: frontnetwork.PassiveSchema, EdgeID: s.Config.EdgeID, GroupID: s.Config.EdgeGroupID,
		Samples: []model.EdgeClientNetworkSample{}}
	for _, bucket := range s.passive {
		if !bucket.start.Before(oldest) && !bucket.start.After(latest) {
			batch.Samples = append(batch.Samples, bucket.samples...)
		}
	}
	s.passiveMu.Unlock()
	writeJSON(w, http.StatusOK, batch)
}
