package dnsserver

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/routeartifact"
	"fugue/internal/tlsissuance"
)

// Constructed from ONE authenticated recipient packet. An alias RR may
// have a distinct name; HTTPS still proves the domain that owns its routes.
// A decision digest is not a synthetic global release binding.
type hostedDNSDecision struct {
	Host, Decision         string
	SelectedAt, ValidUntil time.Time
}

// Only accept a packet reopened against this node's own current assignment.
// The signed parent supplies the probe budget. An observation is private
// to this attempt and does not publish DNS records or manufacture history.
func (s *Service) observeHostedDNSMember(ctx context.Context, p routeartifact.HostedSelectedPacket, parent *dnsServingState, workers int) (map[routeartifact.HostedRecipient][]dnsReadinessFact, error) {
	now := time.Now().UTC()
	if parent == nil || s.currentDNSServingState() != parent || !parent.record.Positive || len(p.Delivery.Parts) == 0 || p.Delivery.Parts[0].Recipient.Node != s.Config.DNSNodeID || p.Delivery.Parts[0].Recipient.Kind != model.PlatformArtifactKindDNSAnswerBundle || parent.payload.Policy.DNSReadiness == nil {
		return nil, errors.New("DNS host observation has no current physical parent")
	}
	h := hostedDNSDecision{p.Delivery.Host, p.Decision, p.SelectedAt, p.ValidUntil}
	policy := parent.payload.Policy.DNSReadiness
	required := map[string]pc.DNSReadinessProbe{}
	for _, part := range p.Delivery.Parts {
		unit := part.Content.DNS
		if unit == nil || part.Recipient.Group != s.Config.EdgeGroupID || len(unit.Views) != 1 {
			return nil, errors.New("empty or foreign DNS part")
		}
		if _, e := h.validFacts(unit.Views[0], &unit.Plan, policy, nil, now); e != nil {
			return nil, e
		}
		for _, pr := range unit.Plan.Probes {
			if old, ok := required[pr.ID]; ok && !reflect.DeepEqual(old, pr) {
				return nil, errors.New("ambiguous DNS probe")
			}
			required[pr.ID] = pr
		}
	}
	if workers < 1 || workers > policy.MaxConcurrency || len(required) > policy.MaxProbes {
		return nil, errors.New("DNS host probe set exceeds its signed budget")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(policy.ProbeIntervalSeconds)*time.Second)
	defer cancel()
	jobs := make(chan pc.DNSReadinessProbe)
	facts := make(map[string]dnsReadinessFact, len(required))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range min(len(required), workers) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pr := range jobs {
				if ctx.Err() != nil {
					return
				}
				proof, e := dnsNetworkObserver.ProbeHosted(ctx, pr.Hostname, pr.Path, pr.Address, pr.State, p.Decision, time.Duration(policy.ProbeTimeoutSeconds)*time.Second)
				f := dnsReadinessFact{ProbeID: pr.ID, Proof: proof, probeObservedAt: time.Now().UTC(), Ready: e == nil}
				if e != nil {
					f.Reason = "host_probe_failed"
				}
				mu.Lock()
				facts[pr.ID] = f
				mu.Unlock()
			}
		}()
	}
send:
	for _, pr := range required {
		select {
		case jobs <- pr:
		case <-ctx.Done():
			break send
		}
	}
	close(jobs)
	wg.Wait()
	// Keep completed negatives when a sibling probe timed out; a partial
	// sweep must not silently bridge two successful recovery observations.
	out := map[routeartifact.HostedRecipient][]dnsReadinessFact{}
	for _, part := range p.Delivery.Parts {
		unit := part.Content.DNS
		var rows []dnsReadinessFact
		for _, pr := range unit.Plan.Probes {
			f, ok := facts[pr.ID]
			if !ok {
				f = dnsReadinessFact{ProbeID: pr.ID, Reason: "host_probe_not_completed"}
			}
			rows = append(rows, f)
		}
		val, e := h.validFacts(unit.Views[0], &unit.Plan, policy, rows, time.Now().UTC())
		if e != nil {
			return nil, e
		}
		for i, f := range rows {
			if normalized, ok := val[f.ProbeID]; ok {
				rows[i] = normalized
			} else {
				rows[i].Ready = false
				rows[i].Reason = "host_proof_not_current"
			}
		}
		out[part.Recipient] = rows
	}
	if !p.ValidUntil.After(time.Now()) || s.currentDNSServingState() != parent {
		return nil, errors.New("DNS host selection changed during observation")
	}
	return out, ctx.Err()
}

// Only called after re-reading the identical privately addressed command.
// This is a volatile physical observation history, never a saved readiness
// receipt. Replaying one fact in two zones cannot count as two successes.
func (s *Service) hostedDNSRecovery(p routeartifact.HostedSelectedPacket, policy *pc.DNSReadinessPolicy, facts map[routeartifact.HostedRecipient][]dnsReadinessFact, now time.Time) {
	sched := dnsReadinessScheduler{historyStore: &s.hostedDNSHistory}
	seen := map[string]dnsReadinessFact{}
	for r, rows := range facts {
		for i, f := range rows {
			if first, ok := seen[f.ProbeID]; ok {
				rows[i] = first
				continue
			}
			key := p.Delivery.Host + "\x00" + p.Parent + "\x00" + p.Decision + "\x00" + f.ProbeID
			sched.observeProbeHistoryKey(key, &f, policy, now)
			rows[i] = f
			seen[f.ProbeID] = f
		}
		facts[r] = rows
	}
}

func (h hostedDNSDecision) validFacts(view pc.DNSQueryView, plan *pc.DNSReadinessPlan, policy *pc.DNSReadinessPolicy, facts []dnsReadinessFact, now time.Time) (map[string]dnsReadinessFact, error) {
	bad := errors.New("host DNS readiness lacks an exact live decision or signed dependencies")
	if !tlsissuance.ValidHostname(h.Host) || !platformproducer.ValidDigest(h.Decision) || h.SelectedAt.IsZero() || h.SelectedAt.After(now) || !h.ValidUntil.After(now) || policy == nil || plan == nil || len(plan.Probes) == 0 || pc.ValidateDNSReadinessPlan(plan, policy) != nil {
		return nil, bad
	}
	records := map[string]bool{}
	for _, r := range plan.Records {
		records[r.Hostname] = true
	}
	for _, r := range view.Records {
		if !records[r.Name] || r.Type != "A" && r.Type != "AAAA" || len(r.Candidates) == 0 {
			return nil, bad
		}
	}
	required := map[string]pc.DNSReadinessProbe{}
	for _, r := range plan.Probes {
		if r.Hostname != h.Host || r.StaticEdge != nil || r.PreviousAuthority != nil || r.CellPublicationDigest != "" {
			return nil, bad
		}
		required[r.ID] = r
	}
	valid := map[string]dnsReadinessFact{}
	seen := map[string]bool{}
	for _, f := range facts {
		if seen[f.ProbeID] {
			delete(valid, f.ProbeID)
			continue
		}
		seen[f.ProbeID] = true
		r, ok := required[f.ProbeID]
		p := f.Proof
		if !ok || !f.Ready || p.Kind != "hosted-route" || p.Version != h.Decision || p.TrafficRelease != nil || p.AppTrafficDigest != "" || !p.BundleValidUntil.IsZero() || p.EdgeID != r.EdgeID || p.GroupID != r.EdgeGroupID || p.State != r.State || p.Digest != r.RouteDigest || p.CheckedAt.Before(h.SelectedAt) || p.CheckedAt.After(now) || !p.ValidUntil.After(now) {
			continue
		}
		// Bound the returned TTL by all three independent deadlines. Do
		// not move the instant of observation forward when re-reading.
		until := p.CheckedAt.Add(time.Duration(pc.DNSReadinessFactMaxAge(r, policy)) * time.Second)
		if h.ValidUntil.Before(until) {
			until = h.ValidUntil
		}
		if until.Before(f.Proof.ValidUntil) {
			f.Proof.ValidUntil = until
		}
		if f.Proof.ValidUntil.After(now) {
			valid[f.ProbeID] = f
		}
	}
	return valid, nil
}

func (h hostedDNSDecision) targetReady(target pc.DNSReadinessTarget, valid map[string]dnsReadinessFact) bool {
	if len(target.ProbeIDs) == 0 {
		return false
	}
	for _, id := range target.ProbeIDs {
		f, ok := valid[id]
		if !ok || f.Proof.Version != h.Decision || f.Proof.GroupID != target.EdgeGroupID || f.Proof.EdgeID != target.EdgeID {
			return false
		}
	}
	return true
}
