package dnsserver

import (
	"testing"
	"time"

	"github.com/miekg/dns"
)

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
