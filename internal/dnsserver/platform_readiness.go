package dnsserver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"sync"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformconsumer"
	"fugue/internal/routeprobe"
)

type DNSReadinessStatus struct {
	PlanDigest   string    `json:"plan_digest"`
	Probes       int       `json:"probes"`
	ReadyProbes  int       `json:"ready_probes"`
	FailedProbes int       `json:"failed_probes"`
	Records      int       `json:"records"`
	ReadyRecords int       `json:"ready_records"`
	CheckedAt    time.Time `json:"checked_at"`
	FreshUntil   time.Time `json:"fresh_until"`
	Serving      bool      `json:"serving"`
}

type dnsReadinessFact struct {
	ProbeID string           `json:"probe_id"`
	Ready   bool             `json:"ready"`
	Reason  string           `json:"reason,omitempty"`
	Proof   routeprobe.Proof `json:"proof"`
}

type dnsReadinessReceipt struct {
	ArtifactID            string             `json:"artifact_id"`
	ArtifactDigest        string             `json:"artifact_digest"`
	ReleaseSetID          string             `json:"release_set_id"`
	ExpectedConsumerSetID string             `json:"expected_consumer_set_id"`
	FencingToken          int64              `json:"fencing_token"`
	NodeID                string             `json:"node_id"`
	Status                DNSReadinessStatus `json:"status"`
	Facts                 []dnsReadinessFact `json:"facts"`
}

type dnsReadinessProbeFunc func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error)

func (s *Service) observePlatformDNSReadiness(ctx context.Context, c dnsPlatformCandidate, a model.PlatformConsumerAssignment) (*dnsReadinessReceipt, error) {
	var payload struct {
		Plan   *platformconfig.DNSReadinessPlan `json:"readiness_plan"`
		Policy platformconfig.PolicySnapshot    `json:"policy"`
	}
	raw, err := json.Marshal(c.Artifact.Content)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	if payload.Plan == nil {
		return nil, nil
	}
	if err = platformconfig.ValidateDNSReadinessPlan(payload.Plan, payload.Policy.DNSReadiness); err != nil {
		return nil, err
	}
	planDigest, err := platformconfig.Digest(payload.Plan)
	if err != nil {
		return nil, err
	}
	receipt := &dnsReadinessReceipt{ArtifactID: c.Artifact.ID, ArtifactDigest: c.Artifact.ContentHash, ReleaseSetID: a.ReleaseSetID, ExpectedConsumerSetID: a.ExpectedConsumerSetID, FencingToken: a.FencingToken, NodeID: s.Config.DNSNodeID}
	cachePath := s.Config.CachePath + ".platform-dns-readiness.json"
	if raw, err := platformconsumer.ReadFile(cachePath, 8<<20); err == nil {
		var previous dnsReadinessReceipt
		if json.Unmarshal(raw, &previous) != nil || previous.Status.CheckedAt.IsZero() {
			return nil, errors.New("DNS readiness facts cache is corrupt")
		}
		if previous.ArtifactID == receipt.ArtifactID && previous.ArtifactDigest == receipt.ArtifactDigest && previous.ReleaseSetID == receipt.ReleaseSetID && previous.ExpectedConsumerSetID == receipt.ExpectedConsumerSetID && previous.FencingToken == receipt.FencingToken && previous.NodeID == receipt.NodeID && previous.Status.PlanDigest == planDigest && previous.Status.CheckedAt.Before(time.Now()) && time.Since(previous.Status.CheckedAt) < time.Duration(payload.Policy.DNSReadiness.ProbeIntervalSeconds)*time.Second {
			// Recompute readiness using the original proof deadlines even on a cache
			// hit. Re-reading a receipt never renews facts.
			previous.Status = summarizeDNSReadiness(payload.Plan, payload.Policy.DNSReadiness, previous.Facts, planDigest, previous.Status.CheckedAt, time.Now().UTC())
			return &previous, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("DNS readiness facts cache unavailable")
	}
	facts := collectDNSReadinessFacts(ctx, payload.Plan, payload.Policy.DNSReadiness, routeprobe.Probe)
	checkedAt := time.Now().UTC()
	receipt.Facts = facts
	receipt.Status = summarizeDNSReadiness(payload.Plan, payload.Policy.DNSReadiness, facts, planDigest, checkedAt, checkedAt)
	return receipt, nil
}

func collectDNSReadinessFacts(ctx context.Context, plan *platformconfig.DNSReadinessPlan, policy *platformconfig.DNSReadinessPolicy, probe dnsReadinessProbeFunc) []dnsReadinessFact {
	ctx, cancel := context.WithTimeout(ctx, min(20*time.Second, time.Duration(policy.ProbeIntervalSeconds)*time.Second))
	defer cancel()
	facts := make([]dnsReadinessFact, len(plan.Probes))
	jobs := make(chan int, len(plan.Probes))
	for i, p := range plan.Probes {
		facts[i] = dnsReadinessFact{ProbeID: p.ID, Reason: "not_observed"}
		jobs <- i
	}
	close(jobs)
	var wg sync.WaitGroup
	for worker := 0; worker < min(policy.MaxConcurrency, len(plan.Probes)); worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if ctx.Err() != nil {
					return
				}
				requirement := plan.Probes[i]
				proof, err := probe(ctx, requirement.Hostname, requirement.Path, requirement.Address, requirement.State, time.Duration(policy.ProbeTimeoutSeconds)*time.Second)
				fact := dnsReadinessFact{ProbeID: requirement.ID, Proof: proof}
				now := time.Now().UTC()
				switch {
				case err != nil:
					fact.Reason = "probe_failed"
				case proof.Digest != requirement.RouteDigest:
					fact.Reason = "route_digest_mismatch"
				case proof.EdgeID != requirement.EdgeID || proof.GroupID != requirement.EdgeGroupID:
					fact.Reason = "endpoint_identity_mismatch"
				case proof.State != requirement.State:
					fact.Reason = "route_state_mismatch"
				case proof.Version == "" || proof.CheckedAt.IsZero() || proof.CheckedAt.After(now) || !proof.ValidUntil.After(now):
					fact.Reason = "proof_not_fresh"
				default:
					expiry := proof.CheckedAt.Add(time.Duration(policy.FactFreshnessSeconds) * time.Second)
					if expiry.Before(fact.Proof.ValidUntil) {
						fact.Proof.ValidUntil = expiry
					}
					if fact.Proof.ValidUntil.After(now) {
						fact.Ready = true
					} else {
						fact.Reason = "proof_not_fresh"
					}
				}
				facts[i] = fact
			}
		}()
	}
	wg.Wait()
	return facts
}

func summarizeDNSReadiness(plan *platformconfig.DNSReadinessPlan, policy *platformconfig.DNSReadinessPolicy, facts []dnsReadinessFact, digest string, checkedAt, now time.Time) DNSReadinessStatus {
	status := DNSReadinessStatus{PlanDigest: digest, Probes: len(plan.Probes), Records: len(plan.Records), CheckedAt: checkedAt}
	validFacts := validDNSReadinessFacts(plan, policy, facts, now)
	valid := map[string]bool{}
	for id, fact := range validFacts {
		valid[id] = true
		if status.FreshUntil.IsZero() || fact.Proof.ValidUntil.Before(status.FreshUntil) {
			status.FreshUntil = fact.Proof.ValidUntil
		}
	}
	status.ReadyProbes = len(valid)
	status.FailedProbes = status.Probes - status.ReadyProbes
	for _, record := range plan.Records {
		edges, v4, v6 := map[string]bool{}, map[string]bool{}, map[string]bool{}
		for _, target := range record.Targets {
			ready := len(target.ProbeIDs) > 0
			for _, id := range target.ProbeIDs {
				ready = ready && valid[id]
			}
			if !ready {
				continue
			}
			edges[target.EdgeID] = true
			if target.Family == "A" {
				v4[target.EdgeID] = true
			} else if target.Family == "AAAA" {
				v6[target.EdgeID] = true
			}
		}
		if len(edges) >= record.MinimumHealthyEdges && (!record.RequireDualStack || (len(v4) >= record.MinimumHealthyEdges && len(v6) >= record.MinimumHealthyEdges)) {
			status.ReadyRecords++
		}
	}
	return status
}

func sortedDNSReadinessFacts(facts []dnsReadinessFact) []dnsReadinessFact {
	out := append([]dnsReadinessFact(nil), facts...)
	sort.Slice(out, func(i, j int) bool { return out[i].ProbeID < out[j].ProbeID })
	return out
}

func validDNSReadinessFacts(plan *platformconfig.DNSReadinessPlan, policy *platformconfig.DNSReadinessPolicy, facts []dnsReadinessFact, now time.Time) map[string]dnsReadinessFact {
	valid := map[string]dnsReadinessFact{}
	requirements := map[string]platformconfig.DNSReadinessProbe{}
	for _, probe := range plan.Probes {
		requirements[probe.ID] = probe
	}
	duplicate := map[string]bool{}
	for _, fact := range facts {
		if _, ok := duplicate[fact.ProbeID]; ok {
			duplicate[fact.ProbeID] = true
		} else {
			duplicate[fact.ProbeID] = false
		}
	}
	for _, fact := range facts {
		requirement, exists := requirements[fact.ProbeID]
		if !exists || duplicate[fact.ProbeID] || fact.Proof.Digest != requirement.RouteDigest || fact.Proof.EdgeID != requirement.EdgeID || fact.Proof.GroupID != requirement.EdgeGroupID || fact.Proof.State != requirement.State || fact.Proof.Version == "" || fact.Proof.ValidUntil.After(fact.Proof.CheckedAt.Add(time.Duration(policy.FactFreshnessSeconds)*time.Second)) {
			continue
		}
		if !fact.Ready || fact.Proof.CheckedAt.IsZero() || fact.Proof.CheckedAt.After(now) || !fact.Proof.ValidUntil.After(now) {
			continue
		}
		valid[fact.ProbeID] = fact
	}

	return valid
}
