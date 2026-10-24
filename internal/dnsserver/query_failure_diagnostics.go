package dnsserver

import (
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/lkgcache"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

const dnsQueryFailureLimit = 2 << 20

type dnsQueryFailureWitness struct {
	Schema         string                            `json:"schema"`
	Digest         string                            `json:"digest,omitempty"`
	ArtifactID     string                            `json:"artifact_id"`
	ArtifactDigest string                            `json:"artifact_digest"`
	Assignment     model.PlatformConsumerAssignment  `json:"assignment"`
	ObservedAt     time.Time                         `json:"observed_at"`
	View           platformconfig.DNSQueryView       `json:"view"`
	Plan           platformconfig.DNSReadinessPlan   `json:"plan"`
	Policy         platformconfig.DNSReadinessPolicy `json:"policy"`
	Facts          []dnsReadinessFact                `json:"facts"`
	Materialized   model.EdgeDNSRecord               `json:"materialized"`
	Hint           dnsGeoHint                        `json:"hint"`
	Error          string                            `json:"error"`
}

// Capture only the failing record's exact inputs. This witness is diagnostic,
// never a positive checkpoint, proof, public query or serving observation.
func (s *Service) retainDNSQueryFailure(c dnsPlatformCandidate, a model.PlatformConsumerAssignment, view platformconfig.DNSQueryView, record model.EdgeDNSRecord, plan *platformconfig.DNSReadinessPlan, policy *platformconfig.DNSReadinessPolicy, facts []dnsReadinessFact, at time.Time, hint dnsGeoHint, cause error) {
	if s.Config.CachePath == "" || plan == nil || policy == nil || cause == nil {
		return
	}
	witness := dnsQueryFailureWitness{Schema: "fugue.dns-query-failure/v1", ArtifactID: c.Artifact.ID, ArtifactDigest: c.Artifact.ContentHash, Assignment: a, ObservedAt: at, View: view, Policy: *policy, Materialized: record, Hint: hint, Error: cause.Error()}
	witness.View.Records = nil
	for _, r := range view.Records {
		if r.Name == record.Name && r.Type == record.Type {
			witness.View.Records = append(witness.View.Records, r)
		}
	}
	if len(witness.View.Records) != 1 {
		return
	}
	ids := map[string]bool{}
	for _, r := range plan.Records {
		if r.Hostname == record.Name {
			witness.Plan.Records = append(witness.Plan.Records, r)
			for _, target := range r.Targets {
				for _, id := range target.ProbeIDs {
					ids[id] = true
				}
			}
		}
	}
	for _, p := range plan.Probes {
		if ids[p.ID] {
			witness.Plan.Probes = append(witness.Plan.Probes, p)
		}
	}
	for _, f := range facts {
		if ids[f.ProbeID] {
			witness.Facts = append(witness.Facts, f)
		}
	}
	digest, err := platformconfig.Digest(witness)
	if err != nil {
		return
	}
	witness.Digest = digest
	raw, err := json.Marshal(witness)
	if err != nil || len(raw) > dnsQueryFailureLimit {
		if s.Logger != nil {
			s.Logger.Print("DNS query failure evidence exceeds bounded local capture")
		}
		return
	}
	if err = lkgcache.AtomicWriteFile(s.Config.CachePath+".query-failure.json", raw, 0600); err != nil {
		if s.Logger != nil {
			s.Logger.Print("DNS query failure evidence persistence unavailable")
		}
		return
	}
	if s.Logger != nil {
		s.Logger.Printf("DNS query failure evidence retained; digest=%s candidates=%d/%d facts=%d", digest, len(record.Candidates), len(witness.View.Records[0].Candidates), len(witness.Facts))
	}
}

// ReplayDNSQueryFailure reproduces record materialization and the selector's
// validation error at the original time. A match is not serving authorization;
// the referenced signed artifact and source snapshot remain independent checks.
func ReplayDNSQueryFailure(raw []byte) (bool, error) {
	if len(raw) > dnsQueryFailureLimit {
		return false, errors.New("DNS query witness exceeds bound")
	}
	var w dnsQueryFailureWitness
	if json.Unmarshal(raw, &w) != nil || w.Schema != "fugue.dns-query-failure/v1" || w.ObservedAt.IsZero() || len(w.View.Records) != 1 || w.Error == "" {
		return false, errors.New("invalid DNS query witness")
	}
	want := w.Digest
	w.Digest = ""
	got, err := platformconfig.Digest(w)
	if err != nil || want != got {
		return false, errors.New("DNS query witness digest mismatch")
	}
	records, err := materializeDNSQueries(w.View, &w.Plan, &w.Policy, w.Facts, w.ObservedAt)
	if err != nil || len(records) != 1 {
		return false, errors.New("DNS query materialization failed")
	}
	actualDigest, actualErr := platformconfig.Digest(records[0])
	expectedDigest, expectedErr := platformconfig.Digest(w.Materialized)
	if actualErr != nil || expectedErr != nil || actualDigest != expectedDigest {
		return false, errors.New("DNS query materialization differs")
	}
	_, err = executeDNSQueryRecord(records[0], w.Hint, w.ObservedAt)
	return err != nil && err.Error() == w.Error, nil
}
