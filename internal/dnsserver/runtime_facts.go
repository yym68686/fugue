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
	out := dnsfacts.Snapshot{Schema: dnsfacts.Schema, NodeID: st.record.NodeID, EdgeGroupID: st.record.GroupID,
		Assignment: st.record.Candidate.Assignment, ParentDigest: st.record.Parent.ContentHash,
		RouteArtifactID: st.routeID, PlanDigest: digest, ObservedAt: st.checkedAt,
		EvaluatedAt: now, CheckpointValidUntil: until, Facts: []dnsfacts.Probe{}}
	valid := validDNSReadinessFacts(st.payload.Plan, st.payload.Policy.DNSReadiness, st.facts, now)
	filtered := make([]dnsReadinessFact, 0, len(st.facts))
	for _, fact := range st.facts {
		_, fresh := valid[fact.ProbeID]
		fact.Ready = fresh && now.Before(until) && dnsProofMatchesRelease(fact.Proof, st.record.Parent, st.record.Candidate, st.routeID)
		filtered = append(filtered, fact)
		// Raw probe errors may contain hostnames or upstream details. The shared
		// response carries only opaque requirements and their original proof.
		out.Facts = append(out.Facts, dnsfacts.Probe{ProbeID: fact.ProbeID, Ready: fact.Ready, Proof: fact.Proof})
	}
	sort.Slice(out.Facts, func(i, j int) bool { return out.Facts[i].ProbeID < out.Facts[j].ProbeID })
	status := summarizeDNSReadiness(st.payload.Plan, st.payload.Policy.DNSReadiness, filtered, digest, st.checkedAt, now)
	out.Ready = now.Before(until) && status.ReadyRecords == status.Records
	return out, nil
}

func (s *Service) handleRuntimeFacts(w http.ResponseWriter, r *http.Request) {
	fail := func() {
		httpx.WriteError(w, http.StatusServiceUnavailable, "DNS serving observations unavailable")
	}
	st := s.platformServing.Load()
	if st == nil || r.Context().Err() != nil {
		fail()
		return
	}
	// Validate against the current trust keys even if this checkpoint was
	// accepted before a key was revoked. No assignment or cache is changed.
	payload, routeID, err := s.verifyDNSServingRelease(st.record.Parent, st.record.Candidate)
	if err != nil || routeID != st.routeID || !reflect.DeepEqual(payload.Plan, st.payload.Plan) || !reflect.DeepEqual(payload.Policy, st.payload.Policy) || !st.record.Positive || st.record.NodeID != s.Config.DNSNodeID || st.record.GroupID != s.Config.EdgeGroupID {
		fail()
		return
	}
	observed, err := dnsRuntimeFacts(st, time.Now().UTC())
	if err != nil {
		fail()
		return
	}
	listenerFailed := s.listenerFailed.Load()
	observed.Ready = observed.Ready && !listenerFailed
	raw, err := json.Marshal(observed)
	if err != nil || len(raw) > 8<<20 || r.Context().Err() != nil || s.platformServing.Load() != st || s.listenerFailed.Load() != listenerFailed {
		fail()
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "private, no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}
