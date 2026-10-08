package dnsserver

import (
	"testing"
	"time"

	"fugue/internal/model"
	"github.com/miekg/dns"
)

func TestQualityDNSPrimarySinceComesFromAnsweredPhysicalArtifact(t *testing.T) {
	for _, name := range []string{"signed_primary", "legacy", "old_physical_artifact", "readiness_fallback"} {
		t.Run(name, func(t *testing.T) {
			state, now := decisionTestState(t)
			primarySince := now.Add(-20 * time.Minute)
			payload := state.payload
			for viewIndex := range payload.Queries {
				for recordIndex := range payload.Queries[viewIndex].Records {
					record := &payload.Queries[viewIndex].Records[recordIndex]
					if record.Name != "target.example.test" || name == "legacy" {
						continue
					}
					record.AnswerPolicy = physicalSelectionTestRecord(now).AnswerPolicy
					selection := record.AnswerPolicy.PhysicalSelection
					selection.PrimarySince = &primarySince
					if name == "old_physical_artifact" {
						selection.PrimarySince = nil
					}
					if name == "readiness_fallback" {
						selection.PrimaryEdgeID = "unavailable-edge"
						selection.OrderedEdgeIDs = append([]string{selection.PrimaryEdgeID}, selection.OrderedEdgeIDs...)
					}
				}
			}
			var err error
			state, err = buildDNSServingState(state.record, payload, "route", "dns-a", "edge-group-a", state.facts, now)
			if err != nil {
				t.Fatal(err)
			}
			request := new(dns.Msg)
			request.SetQuestion("target.example.test.", dns.TypeA)
			receipt := captureDecision(t, state, request, "198.51.100.2:1234", now)
			evidence, err := QualityEvidenceFromDNSDecision(receipt, now, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if name == "signed_primary" {
				if evidence.PrimarySince == nil || !evidence.PrimarySince.Equal(primarySince) {
					t.Fatal("lost signed assignment history", evidence)
				}
			} else if evidence.PrimarySince != nil {
				t.Fatal("invented physical assignment history", name, evidence)
			}
		})
	}
}

func TestPhysicalSelectionPrimarySinceValidationAndClone(t *testing.T) {
	now := time.Now().UTC()
	selection := physicalSelectionTestRecord(now).AnswerPolicy.PhysicalSelection
	primarySince := now.Add(-time.Minute)
	selection.PrimarySince = &primarySince
	if err := model.ValidateDNSPhysicalSelection(selection); err != nil {
		t.Fatal(err)
	}
	clone := model.CloneDNSPhysicalSelection(selection)
	*clone.PrimarySince = now.Add(time.Second)
	if !selection.PrimarySince.Equal(primarySince) || model.ValidateDNSPhysicalSelection(clone) == nil {
		t.Fatal("assignment was aliased or a future assignment was accepted")
	}
	*clone.PrimarySince = time.Time{}
	if model.ValidateDNSPhysicalSelection(clone) == nil {
		t.Fatal("zero assignment time accepted as known")
	}
}

func qualityEvidenceFixture(t *testing.T) (DNSDecisionReceipt, time.Time) {
	t.Helper()
	state, now := decisionTestState(t)
	request := new(dns.Msg)
	request.SetQuestion("target.example.test.", dns.TypeA)
	return captureDecision(t, state, request, "198.51.100.2:1234", now), now
}

func TestQualityDNSBindingUsesActualPublicationNotDesired(t *testing.T) {
	receipt, now := qualityEvidenceFixture(t)
	receipt.Publication.Desired = &DNSDecisionPublication{Digest: "not-serving"}
	receipt.EvidenceDigest = dnsDecisionDigest(receipt)
	evidence, err := QualityEvidenceFromDNSDecision(receipt, now, time.Minute)
	if err != nil || evidence.EdgeID == "" || evidence.Scope != "global" || len(evidence.Candidates) != 2 || len(evidence.Proofs) != 4 || evidence.Publication.Digest != "sha256:loaded" {
		t.Fatal(evidence, err)
	}
	if _, err := QualityEvidenceFromDNSDecision(receipt, now.Add(31*time.Second), time.Minute); err == nil {
		t.Fatal("accepted expired route proof because receipt was recent")
	}
}

func TestQualityDNSBindingRejectsFailedWritesAndTampering(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*DNSDecisionReceipt)
	}{
		{"write_failed", func(receipt *DNSDecisionReceipt) { receipt.WriteSucceeded = false }},
		{"answer_missing", func(receipt *DNSDecisionReceipt) { receipt.AnswerPublication = nil }},
		{"receipt_future", func(receipt *DNSDecisionReceipt) { receipt.ObservedAt = receipt.ObservedAt.Add(time.Hour) }},
		{"receipt_stale", func(receipt *DNSDecisionReceipt) { receipt.ObservedAt = receipt.ObservedAt.Add(-time.Hour) }},
		{"wire_mismatch", func(receipt *DNSDecisionReceipt) { receipt.RRSet = []string{"forged"} }},
		{"candidate_mismatch", func(receipt *DNSDecisionReceipt) { receipt.Records[0].Answered[0].EdgeID = "forged" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt, now := qualityEvidenceFixture(t)
			test.edit(&receipt)
			receipt.EvidenceDigest = dnsDecisionDigest(receipt)
			if _, err := QualityEvidenceFromDNSDecision(receipt, now, time.Minute); err == nil {
				t.Fatal("accepted invalid actual answer")
			}
		})
	}
}
