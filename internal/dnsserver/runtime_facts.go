package dnsserver

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"sort"
	"time"

	"fugue/internal/dnsfacts"
	"fugue/internal/httpx"
	"fugue/internal/platformconfig"
)

func dnsRuntimeFacts(st *dnsServingState, now time.Time) (dnsfacts.Snapshot, error) {
	if st == nil || st.payload.Plan == nil || st.payload.Policy.DNSReadiness == nil || st.payload.Policy.MaxStaleSeconds <= 0 || st.checkedAt.IsZero() || st.checkedAt.After(now) || st.record.AppliedAt.IsZero() || st.record.AppliedAt.After(now) || len(st.facts) > 4096 {
		return dnsfacts.Snapshot{}, errors.New("DNS serving observations unavailable")
	}
	digest, err := platformconfig.Digest(st.payload.Plan)
	if err != nil {
		return dnsfacts.Snapshot{}, err
	}
	until := st.record.AppliedAt.Add(time.Duration(st.payload.Policy.MaxStaleSeconds) * time.Second)
	out := dnsfacts.Snapshot{Schema: dnsfacts.Schema, RouteSources: st.record.RouteSources, NodeID: st.record.NodeID, EdgeGroupID: st.record.GroupID,
		Assignment: st.record.Candidate.Assignment, ParentDigest: st.record.Parent.ContentHash,
		RouteArtifactID: st.routeID, PlanDigest: digest, ObservedAt: st.checkedAt,
		EvaluatedAt: now, CheckpointValidUntil: until, Facts: []dnsfacts.Probe{}}
	valid := validDNSReadinessFacts(st.payload.Plan, st.payload.Policy.DNSReadiness, st.facts, now)
	filtered := make([]dnsReadinessFact, 0, len(st.facts))
	for _, fact := range st.facts {
		_, fresh := valid[fact.ProbeID]
		fact.Ready = fresh && now.Before(until) && dnsFactMatchesRelease(fact, st.record.Parent, st.record.Candidate, st.routeID, st.payload)
		filtered = append(filtered, fact)
		// Raw probe errors may contain hostnames or upstream details. The shared
		// response carries only opaque requirements and their original proof.
		out.Facts = append(out.Facts, dnsfacts.Probe{ProbeID: fact.ProbeID, Ready: fact.Ready, Proof: fact.Proof, ProbeHistory: append([]time.Time(nil), fact.ProbeHistory...)})
	}
	sort.Slice(out.Facts, func(i, j int) bool { return out.Facts[i].ProbeID < out.Facts[j].ProbeID })
	status := summarizeDNSReadiness(st.payload.Plan, st.payload.Policy.DNSReadiness, filtered, digest, st.checkedAt, now)
	out.Ready = st.record.wholePositive() && now.Before(until) && status.ReadyRecords == status.Records
	if out.Ready {
		// A transport handoff needs an answer from this exact checkpoint, not
		// just a fresh route quorum. A signed selector can exclude the only
		// healthy targets, or the live listener can be answering from a
		// successor bridge whose proofs this checkpoint cannot export.
		state, err := buildDNSServingState(st.record, st.payload, st.routeID, st.record.NodeID, st.record.GroupID, filtered, st.checkedAt)
		out.Ready = err == nil && checkDNSServingSnapshot(state, now, false) == nil
	}
	return out, nil
}

func (s *Service) handleRuntimeFacts(w http.ResponseWriter, r *http.Request) {
	fail := func(reason string) {
		w.Header().Set("X-Fugue-DNS-Observation-Rejection", reason)
		w.Header().Set("Cache-Control", "private, no-store")
		httpx.WriteError(w, http.StatusServiceUnavailable, "DNS serving observations unavailable")
	}
	initial := s.currentDNSServingSelection()
	hosted := s.hostedDNS.Load()
	st := initial.state
	if st == nil {
		fail("state_missing")
		return
	}
	if r.Context().Err() != nil {
		fail("request_cancelled")
		return
	}
	// Validate against the current trust keys even if this checkpoint was
	// accepted before a key was revoked. No assignment or cache is changed.
	payload, routeID, err := s.verifiedDNSCheckpointPayload(st.record)
	rejection := ""
	switch {
	case err != nil:
		rejection = "checkpoint_validation"
	case routeID != st.routeID:
		rejection = "route_identity"
	case !reflect.DeepEqual(payload.Plan, st.payload.Plan):
		rejection = "plan_mismatch"
	case !reflect.DeepEqual(payload.Policy, st.payload.Policy):
		rejection = "policy_mismatch"
	case !st.record.Positive:
		rejection = "checkpoint_not_positive"
	case st.record.NodeID != s.Config.DNSNodeID || st.record.GroupID != s.Config.EdgeGroupID:
		rejection = "node_identity"
	}
	if rejection != "" {
		fail(rejection)
		return
	}
	observed, err := dnsRuntimeFacts(st, time.Now().UTC())
	if err != nil {
		fail("fact_snapshot")
		return
	}
	listenerFailed := s.listenerFailed.Load()
	observed.Ready = observed.Ready && !listenerFailed
	if hosted != nil {
		// Keep independently usable observations for the other hosts, but
		// never certify a complete global transport handoff using them.
		observed.Ready = false
		owned := map[string]bool{}
		for _, w := range hosted.keys {
			if w.packet != nil {
				owned[w.packet.Delivery.Host] = true
			}
			for host := range w.shared {
				owned[host] = true
			}
		}
		omit := map[string]bool{}
		for _, p := range st.payload.Plan.Probes {
			if owned[p.Hostname] {
				omit[p.ID] = true
			}
		}
		for i, f := range observed.Facts {
			if omit[f.ProbeID] {
				observed.Facts[i].Ready = false
				observed.Facts[i].ProbeHistory = nil
			}
		}
	}
	raw, err := json.Marshal(observed)
	if err != nil {
		fail("response_encoding")
		return
	}
	if len(raw) > 32<<20 {
		fail("response_size")
		return
	}
	compressed := acceptsRuntimeFactsGzip(r.Header)
	if compressed {
		raw, err = gzipRuntimeFacts(raw)
	}
	switch {
	case err != nil:
		rejection = "response_encoding"
	case r.Context().Err() != nil:
		rejection = "request_cancelled"
	default:
		current := s.currentDNSServingSelection()
		if current.state != st {
			rejection = "state_replaced"
			baseWriter, selection, renewal := dnsServingReplacement(initial, current)
			w.Header().Set(dnsfacts.ReplacementBaseHeader, baseWriter)
			w.Header().Set(dnsfacts.ReplacementSelectionHeader, selection)
			w.Header().Set(dnsfacts.ReplacementRenewalHeader, renewal)
		} else if s.hostedDNS.Load() != hosted {
			rejection = "scoped_selection_changed"
		} else if s.listenerFailed.Load() != listenerFailed {
			rejection = "listener_changed"
		}
	}
	if rejection != "" {
		fail(rejection)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	w.Header().Add("Vary", "Accept-Encoding")
	if compressed {
		w.Header().Set("Content-Encoding", "gzip")
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}
