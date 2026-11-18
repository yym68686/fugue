package edgegroupfront

import (
	"context"
	"fmt"
	"hash/maphash"
	"io"
	"net"
	"net/http"
	"net/netip"
	"sync/atomic"
	"time"

	"fugue/internal/frontnetwork"
	"fugue/internal/model"
	"fugue/internal/tcpdiag"
)

type passiveNetworkBucket struct {
	start                   time.Time
	samples                 []model.EdgeClientNetworkSample
	prefixCounts            map[string]int
	forwardOK, forwardError uint64
}

var sharedIPv4Space = netip.MustParsePrefix("100.64.0.0/10")
var passiveSampleSeed = maphash.MakeSeed()

const (
	passiveActiveLimit    = 512
	passiveActiveInterval = 15 * time.Second
)

type passiveNetworkState struct {
	active                                                                         map[string]passiveLiveConnection // guarded by passiveMu
	eligible, noPayload, noRTT, invalid, lockBusy                                  atomic.Uint64
	prefixLimited, bucketLimited, activeLimited, activeSamples, delivery, recorded atomic.Uint64
}

type passiveLiveConnection struct {
	connection edgeFrontActiveTCPConnection
	previous   model.EdgeClientNetworkSample
	sampled    bool
}

func passiveConnection(cfg Config, protocol, slot, id string, conn net.Conn, started time.Time) (edgeFrontActiveTCPConnection, bool) {
	if protocol != ProtocolHTTPS || !cfg.ProxyProtocol || cfg.EdgeID == "" || cfg.EdgeGroupID == "" {
		return edgeFrontActiveTCPConnection{}, false
	}
	remote := connAddr(conn.RemoteAddr())
	peer, err := frontnetwork.ParseRemote(remote)
	if err != nil || sharedIPv4Space.Contains(peer.Addr()) {
		return edgeFrontActiveTCPConnection{}, false
	}
	return edgeFrontActiveTCPConnection{ID: id, Slot: slot, DownstreamRemote: remote, StartedAt: started.UTC(), Downstream: conn}, true
}

func (s *Service) trackPassiveConnection(conn edgeFrontActiveTCPConnection, baseline model.EdgeClientNetworkSample) {
	if !s.passiveMu.TryLock() {
		s.passiveState.lockBusy.Add(1)
		return
	}
	defer s.passiveMu.Unlock()
	if len(s.passiveState.active) >= passiveActiveLimit {
		s.passiveState.activeLimited.Add(1)
		return
	}
	if s.passiveState.active == nil {
		s.passiveState.active = make(map[string]passiveLiveConnection)
	}
	s.passiveState.active[conn.ID] = passiveLiveConnection{connection: conn, previous: baseline}
}

func (s *Service) finishPassiveConnection(id string) (model.EdgeClientNetworkSample, bool) {
	s.passiveMu.Lock()
	entry := s.passiveState.active[id]
	delete(s.passiveState.active, id)
	s.passiveMu.Unlock()
	return entry.previous, entry.sampled
}

func (s *Service) runPassiveNetwork(ctx context.Context) {
	ticker := time.NewTicker(passiveActiveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.samplePassiveActive(tcpdiag.SnapshotFromConn, time.Now)
		}
	}
}

func (s *Service) samplePassiveActive(read func(net.Conn) tcpdiag.Snapshot, clock func() time.Time) {
	s.passiveMu.Lock()
	active := make([]passiveLiveConnection, 0, len(s.passiveState.active))
	for _, entry := range s.passiveState.active {
		if !entry.sampled {
			active = append(active, entry)
		}
	}
	s.passiveMu.Unlock()
	for _, entry := range active {
		conn := entry.connection
		snapshot, now := read(conn.Downstream), clock().UTC()
		if !snapshot.Available {
			continue
		}
		sample := publicClientNetworkSample(conn, snapshot, now)
		s.passiveMu.Lock()
		if _, ok := s.passiveState.active[conn.ID]; ok {
			// Do not accept idle socket snapshots or a SYN/TLS attempt with
			// no downstream progress. Use each socket at most once.
			if snapshot.BytesReceived > 0 && sample.BytesSent > entry.previous.BytesSent {
				entry.sampled = s.addPassiveSampleLocked(sample, entry.previous, now)
				if entry.sampled {
					s.passiveState.activeSamples.Add(1)
				}
			}
			entry.previous = sample
			s.passiveState.active[conn.ID] = entry
		}
		s.passiveMu.Unlock()
	}
}

func (s *Service) recordPassiveNetworkSample(connection edgeFrontActiveTCPConnection, baseline model.EdgeClientNetworkSample, snapshot tcpdiag.Snapshot, now time.Time) {
	sample := publicClientNetworkSample(connection, snapshot, now)
	if !s.passiveMu.TryLock() {
		s.passiveState.lockBusy.Add(1)
		return
	}
	defer s.passiveMu.Unlock()
	s.addPassiveSampleLocked(sample, baseline, now)
}

func (s *Service) passiveBucketLocked(now time.Time) *passiveNetworkBucket {
	start := now.UTC().Truncate(frontnetwork.PassiveInterval)
	pos := int((start.Unix() / int64(frontnetwork.PassiveInterval.Seconds())) % frontnetwork.PassiveBuckets)
	bucket := &s.passive[pos]
	if bucket.start.Before(start) {
		*bucket = passiveNetworkBucket{start: start, prefixCounts: map[string]int{}}
	}
	if !bucket.start.Equal(start) {
		return nil
	}
	return bucket
}

func (s *Service) addPassiveSampleLocked(sample, baseline model.EdgeClientNetworkSample, now time.Time) bool {
	if sample.RTTMS == nil {
		s.passiveState.noRTT.Add(1)
		return false
	}
	// Closed or very long downloads may still provide RTT. Never treat
	// missing/timed-out delivery counters as a zero-rate measurement.
	if sample.Delivery != nil && baseline.Delivery != nil {
		sample.DeliveryBaseline = baseline.Delivery
		if model.ValidateEdgeClientNetworkSample(&sample) != nil {
			sample.DeliveryBaseline = nil
		}
	}
	if model.ValidateEdgeClientNetworkSample(&sample) != nil {
		s.passiveState.invalid.Add(1)
		return false
	}
	bucket := s.passiveBucketLocked(now)
	if bucket == nil {
		s.passiveState.bucketLimited.Add(1)
		return false
	}
	count := bucket.prefixCounts[sample.Scope]
	if count >= frontnetwork.PassivePrefixLimit || len(bucket.samples) >= frontnetwork.PassiveBucketLimit {
		// Keep diversity under saturation: a dominant peer prefix cannot
		// take all slots simply by arriving first. Within equally represented
		// prefixes use a random process-seeded priority rather than recency.
		if count >= frontnetwork.PassivePrefixLimit {
			s.passiveState.prefixLimited.Add(1)
		} else {
			s.passiveState.bucketLimited.Add(1)
		}
		victim, victimCount, victimScore := -1, -1, uint64(0)
		for i, old := range bucket.samples {
			if count >= frontnetwork.PassivePrefixLimit && old.Scope != sample.Scope {
				continue
			}
			n := bucket.prefixCounts[old.Scope]
			score := maphash.String(passiveSampleSeed, old.ConnectionID)
			if n > victimCount || n == victimCount && score > victimScore {
				victim, victimCount, victimScore = i, n, score
			}
		}
		if victim < 0 {
			return false
		}
		old := bucket.samples[victim]
		// Inserting other scopes changes both populations. Only prefer a
		// replacement unconditionally when it improves prefix diversity.
		improves := old.Scope != sample.Scope && victimCount > count+1
		if !improves && (old.Scope != sample.Scope && victimCount <= count ||
			maphash.String(passiveSampleSeed, sample.ConnectionID) >= victimScore) {
			return false
		}
		bucket.prefixCounts[old.Scope]--
		bucket.samples[victim] = sample
	} else {
		bucket.samples = append(bucket.samples, sample)
	}
	bucket.prefixCounts[sample.Scope]++
	if download, _ := model.EdgeClientDeliveryMetrics(&sample); download != nil {
		s.passiveState.delivery.Add(1)
	}
	s.passiveState.recorded.Add(1)
	return true
}

func (s *Service) recordPassiveForwarding(ok bool, now time.Time) {
	s.passiveMu.Lock()
	defer s.passiveMu.Unlock()
	if b := s.passiveBucketLocked(now); b != nil {
		if ok {
			b.forwardOK++
		} else {
			b.forwardError++
		}
	}
}

func (s *Service) passiveStatsLocked(now time.Time) frontnetwork.PassiveStats {
	p := &s.passiveState
	stats := frontnetwork.PassiveStats{
		Eligible: p.eligible.Load(), NoPayload: p.noPayload.Load(), NoRTT: p.noRTT.Load(),
		Invalid: p.invalid.Load(), LockBusy: p.lockBusy.Load(), PrefixLimited: p.prefixLimited.Load(),
		BucketLimited: p.bucketLimited.Load(), ActiveLimited: p.activeLimited.Load(),
		ActiveSamples: p.activeSamples.Load(), Delivery: p.delivery.Load(), Recorded: p.recorded.Load(),
	}
	recent := now.UTC().Truncate(frontnetwork.PassiveInterval)
	for _, b := range s.passive {
		if b.start.Equal(recent) || b.start.Equal(recent.Add(-frontnetwork.PassiveInterval)) {
			stats.RecentForwardOK += b.forwardOK
			stats.RecentForwardError += b.forwardError
		}
	}
	return stats
}

func (s *Service) writePassiveNetworkMetrics(w io.Writer, labels string) {
	s.passiveMu.Lock()
	stats := s.passiveStatsLocked(time.Now())
	s.passiveMu.Unlock()
	fmt.Fprintln(w, "# TYPE fugue_edge_front_passive_events_total counter")
	for _, event := range []struct {
		kind  string
		count uint64
	}{{"eligible", stats.Eligible}, {"no_payload", stats.NoPayload}, {"no_rtt", stats.NoRTT},
		{"invalid", stats.Invalid}, {"lock_busy", stats.LockBusy}, {"prefix_limited", stats.PrefixLimited},
		{"bucket_limited", stats.BucketLimited}, {"active_limited", stats.ActiveLimited},
		{"recorded", stats.Recorded}, {"active_sample", stats.ActiveSamples}, {"delivery", stats.Delivery}} {
		fmt.Fprintf(w, "fugue_edge_front_passive_events_total{%s,kind=%q} %d\n", labels, event.kind, event.count)
	}
}

func (s *Service) handlePassiveNetworkSamples(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "private, no-store")
	if r.Method != http.MethodGet || r.URL.RawQuery != "" && r.URL.RawQuery != frontnetwork.PassiveStatsQuery {
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
	if r.URL.RawQuery == frontnetwork.PassiveStatsQuery {
		stats := s.passiveStatsLocked(latest)
		batch.Stats = &stats
	}
	for _, bucket := range s.passive {
		if !bucket.start.Before(oldest) && !bucket.start.After(latest) {
			batch.Samples = append(batch.Samples, bucket.samples...)
		}
	}
	s.passiveMu.Unlock()
	writeJSON(w, http.StatusOK, batch)
}
