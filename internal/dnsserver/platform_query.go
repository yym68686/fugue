package dnsserver

import (
	"encoding/json"
	"errors"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"github.com/miekg/dns"
)

type DNSQueryStatus struct {
	ViewDigest      string    `json:"view_digest"`
	Records         int       `json:"records"`
	EligibleRecords int       `json:"eligible_records"`
	Questions       int       `json:"questions"`
	Answers         int       `json:"answers"`
	EvaluatedAt     time.Time `json:"evaluated_at"`
	Serving         bool      `json:"serving"`
}

type dnsQueryReceipt struct {
	ArtifactID            string                `json:"artifact_id"`
	ArtifactDigest        string                `json:"artifact_digest"`
	ReleaseSetID          string                `json:"release_set_id"`
	ExpectedConsumerSetID string                `json:"expected_consumer_set_id"`
	FencingToken          int64                 `json:"fencing_token"`
	NodeID                string                `json:"node_id"`
	Status                DNSQueryStatus        `json:"status"`
	Zones                 []dnsQueryZoneReceipt `json:"zones"`
}

type dnsQueryZoneReceipt struct {
	Zone    string                `json:"zone"`
	Records []model.EdgeDNSRecord `json:"records"`
}

func (s *Service) evaluatePlatformDNSQueries(c dnsPlatformCandidate, a model.PlatformConsumerAssignment, readiness *dnsReadinessReceipt) (*dnsQueryReceipt, error) {
	var payload struct {
		Views  []platformconfig.DNSQueryView    `json:"query_views"`
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
	if len(payload.Views) == 0 {
		return nil, nil
	}
	if readiness == nil || payload.Plan == nil || payload.Policy.DNSReadiness == nil || readiness.ArtifactID != c.Artifact.ID || readiness.ArtifactDigest != c.Artifact.ContentHash || readiness.ReleaseSetID != a.ReleaseSetID || readiness.ExpectedConsumerSetID != a.ExpectedConsumerSetID || readiness.FencingToken != a.FencingToken || readiness.NodeID != s.Config.DNSNodeID {
		return nil, errors.New("DNS query facts do not match candidate release")
	}
	now := time.Now().UTC()
	receipt := &dnsQueryReceipt{ArtifactID: c.Artifact.ID, ArtifactDigest: c.Artifact.ContentHash, ReleaseSetID: a.ReleaseSetID, ExpectedConsumerSetID: a.ExpectedConsumerSetID, FencingToken: a.FencingToken, NodeID: s.Config.DNSNodeID, Status: DNSQueryStatus{EvaluatedAt: now}, Zones: []dnsQueryZoneReceipt{}}
	owned := []platformconfig.DNSQueryView{}
	for _, view := range payload.Views {
		if view.NodeID != s.Config.DNSNodeID {
			continue
		}
		if view.EdgeGroupID != s.Config.EdgeGroupID {
			return nil, errors.New("DNS query view group differs")
		}
		owned = append(owned, view)
		records, err := materializeDNSQueries(view, payload.Plan, payload.Policy.DNSReadiness, readiness.Facts, now)
		if err != nil {
			return nil, err
		}
		receipt.Zones = append(receipt.Zones, dnsQueryZoneReceipt{Zone: view.Zone, Records: records})
		for _, record := range records {
			receipt.Status.Records++
			if len(record.Values) > 0 {
				receipt.Status.EligibleRecords++
			}
			// Exercise the existing production selector using a bounded global/client
			// geography matrix, then pack real wire answers. This is local execution,
			// not a public serving probe or a manufactured positive ACK.
			hints := []dnsGeoHint{{}}
			hintKeys := map[string]bool{}
			for _, c := range record.Candidates {
				key := c.Country + "\x00" + c.Region
				if hintKeys[key] || len(hints) >= 16 {
					continue
				}
				hintKeys[key] = true
				hints = append(hints, dnsGeoHint{Country: c.Country, Region: c.Region, Source: "ecs"})
			}
			for _, hint := range hints {
				answers, err := executeDNSQueryRecord(record, hint, now)
				if err != nil {
					return nil, err
				}
				receipt.Status.Questions++
				receipt.Status.Answers += len(answers)
			}
		}
	}
	if len(owned) == 0 {
		return nil, errors.New("DNS consumer has no query view")
	}
	receipt.Status.ViewDigest, err = platformconfig.Digest(owned)
	if err != nil {
		return nil, err
	}
	return receipt, nil
}

func materializeDNSQueries(view platformconfig.DNSQueryView, plan *platformconfig.DNSReadinessPlan, policy *platformconfig.DNSReadinessPolicy, facts []dnsReadinessFact, now time.Time) ([]model.EdgeDNSRecord, error) {
	raw, err := json.Marshal(view.Records)
	if err != nil {
		return nil, err
	}
	var records []model.EdgeDNSRecord
	if err = json.Unmarshal(raw, &records); err != nil {
		return nil, err
	}
	valid := validDNSReadinessFacts(plan, policy, facts, now)
	byHost := map[string]platformconfig.DNSReadinessRecord{}
	for _, r := range plan.Records {
		byHost[r.Hostname] = r
	}
	for i := range records {
		r := &records[i]
		requirement, dynamic := byHost[r.Name]
		dynamic = dynamic && (r.Type == "A" || r.Type == "AAAA")
		if !dynamic {
			filtered, err := filterDNSRecordValues(*r, now)
			if err != nil {
				return nil, err
			}
			// filterDNSRecordValues preserves legacy audit metadata. A
			// detached query projection will be encoded again, so it must
			// retain expiry references only for the surviving values.
			if len(filtered.ValueExpirations) > 0 {
				expirations := map[string]time.Time{}
				for _, value := range filtered.Values {
					if until, ok := filtered.ValueExpirations[value]; ok {
						expirations[value] = until
					}
				}
				filtered.ValueExpirations = expirations
			}
			*r = filtered
			continue
		}
		targets := map[string]platformconfig.DNSReadinessTarget{}
		deadlines := map[string]time.Time{}
		edges, v4, v6 := map[string]bool{}, map[string]bool{}, map[string]bool{}
		for _, target := range requirement.Targets {
			ready := len(target.ProbeIDs) > 0
			until := time.Time{}
			for _, id := range target.ProbeIDs {
				f, ok := valid[id]
				if !ok {
					ready = false
					break
				}
				if until.IsZero() || f.Proof.ValidUntil.Before(until) {
					until = f.Proof.ValidUntil
				}
			}
			if !ready || until.Sub(now) < time.Second {
				continue
			}
			targets[target.Address] = target
			deadlines[target.Address] = until
			edges[target.EdgeID] = true
			if target.Family == "A" {
				v4[target.EdgeID] = true
			} else if target.Family == "AAAA" {
				v6[target.EdgeID] = true
			}
		}
		// Cached answers must expire before the proof quorum can disappear,
		// including a different node whose evidence expires before this answer.
		for _, until := range deadlines {
			r.TTL = min(r.TTL, int(until.Sub(now).Seconds()))
		}
		quorum := len(edges) >= requirement.MinimumHealthyEdges && (!requirement.RequireDualStack || (len(v4) >= requirement.MinimumHealthyEdges && len(v6) >= requirement.MinimumHealthyEdges))
		filter := func(in []model.EdgeDNSAnswerCandidate) []model.EdgeDNSAnswerCandidate {
			out := []model.EdgeDNSAnswerCandidate{}
			for _, c := range in {
				target, ok := targets[c.IP]
				if !quorum || !ok || target.EdgeID != c.EdgeID || target.EdgeGroupID != c.EdgeGroupID || target.Family != r.Type {
					continue
				}
				c.Healthy, c.RouteReady, c.TLSReady, c.DNSEligible = true, true, true, true
				c.ServingGeneration = valid[target.ProbeIDs[0]].Proof.Version
				out = append(out, c)
			}
			return out
		}
		r.Candidates = filter(r.Candidates)
		r.Values = nil
		for _, c := range r.Candidates {
			r.Values = append(r.Values, c.IP)
			r.TTL = min(r.TTL, int(deadlines[c.IP].Sub(now).Seconds()))
		}
		sort.Strings(r.Values)
		for j := range r.ScopedCandidates {
			r.ScopedCandidates[j].Candidates = filter(r.ScopedCandidates[j].Candidates)
		}
	}
	return records, nil
}

func executeDNSQueryRecord(record model.EdgeDNSRecord, hint dnsGeoHint, now time.Time) ([]dns.RR, error) {
	if hint.Source == "ecs" && !record.AnswerPolicy.ECSEnabled {
		hint = dnsGeoHint{Source: "ecs_disabled"}
	}
	if len(record.Values) == 0 {
		return nil, nil
	}
	if len(record.Candidates) > 0 {
		candidates, _, _ := edgeDNSAnswerCandidateDecision(record, hint, now, nil, nil)
		record.Values = nil
		for _, c := range candidates {
			record.Values = append(record.Values, c.IP)
		}
	}
	answers := rrForEdgeDNSRecordAt(record, record.Name, now)
	if len(answers) != len(record.Values) {
		return nil, errors.New("DNS query wire materialization lost an authorized answer")
	}
	response := new(dns.Msg)
	response.Answer = answers
	if _, err := response.Pack(); err != nil {
		return nil, errors.New("DNS query answers cannot be encoded")
	}
	return answers, nil
}
