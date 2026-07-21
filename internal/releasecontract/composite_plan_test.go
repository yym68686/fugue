package releasecontract

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestCompositeReleasePlanNeutralContractRoundTrip(t *testing.T) {
	plan, err := NewCompositeReleasePlan(compositePlanFixture())
	if err != nil {
		t.Fatal(err)
	}
	if got := []Domain{plan.BaseVersions[0].Domain, plan.BaseVersions[1].Domain}; !reflect.DeepEqual(got, []Domain{DomainAuthoritativeDNS, DomainControlPlane}) {
		t.Fatalf("version vector = %v", got)
	}
	encoded, err := MarshalCompositeReleasePlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeAndVerifyCompositeReleasePlan(bytes.NewReader(encoded), plan.Digest)
	if err != nil || !reflect.DeepEqual(decoded, plan) {
		t.Fatalf("decoded=%#v err=%v", decoded, err)
	}

	duplicate := bytes.Replace(encoded, []byte(`"kind":`), []byte(`"kind":"duplicate","kind":`), 1)
	if _, err := DecodeAndVerifyCompositeReleasePlan(bytes.NewReader(duplicate), plan.Digest); err == nil {
		t.Fatal("duplicate JSON field was accepted")
	}
	unknown := bytes.Replace(encoded, []byte(`"kind":`), []byte(`"unknown":true,"kind":`), 1)
	if _, err := DecodeAndVerifyCompositeReleasePlan(bytes.NewReader(unknown), plan.Digest); err == nil {
		t.Fatal("unknown JSON field was accepted")
	}
	caseChanged := bytes.Replace(encoded, []byte(`"kind":`), []byte(`"Kind":`), 1)
	if _, err := DecodeAndVerifyCompositeReleasePlan(bytes.NewReader(caseChanged), plan.Digest); err == nil {
		t.Fatal("case-insensitive JSON field was accepted")
	}
	malformedUnicode := bytes.Replace(encoded, []byte(`"control-plane"`), []byte(`"\ud800"`), 1)
	if _, err := DecodeAndVerifyCompositeReleasePlan(bytes.NewReader(malformedUnicode), plan.Digest); err == nil {
		t.Fatal("malformed Unicode escape was accepted")
	}
	trailing := append(append([]byte(nil), encoded...), []byte(`{}`)...)
	if _, err := DecodeAndVerifyCompositeReleasePlan(bytes.NewReader(trailing), plan.Digest); err == nil {
		t.Fatal("trailing JSON value was accepted")
	}
}

func TestDomainVocabularyAndAdaptersAreComplete(t *testing.T) {
	want := []Domain{DomainNodeLocal, DomainAuthoritativeDNS, DomainControlPlane, DomainImageCache, DomainBackup}
	if got := KnownDomains(); !reflect.DeepEqual(got, want) {
		t.Fatalf("domains = %v", got)
	}
	for index, domain := range want {
		parsed, err := ParseDomain(string(domain))
		rank, ranked := DomainRank(domain)
		adapter, bound := AdapterForDomain(domain)
		if err != nil || parsed != domain || !ranked || rank != index || !bound || adapter == "" {
			t.Fatalf("domain=%q parsed=%q err=%v rank=%d ranked=%t adapter=%q bound=%t", domain, parsed, err, rank, ranked, adapter, bound)
		}
	}
}

func compositePlanFixture() CompositeReleasePlan {
	dnsAdapter, _ := AdapterForDomain(DomainAuthoritativeDNS)
	controlAdapter, _ := AdapterForDomain(DomainControlPlane)
	return CompositeReleasePlan{
		BaseCommit: "1111111111111111111111111111111111111111", TargetCommit: "2222222222222222222222222222222222222222",
		ImageActivationPlanDigest: contractDigest("a"), Generation: "7", FencingEpoch: "11",
		BaseVersions: []DomainVersion{
			{Domain: DomainControlPlane, Version: contractDigest("b")},
			{Domain: DomainAuthoritativeDNS, Version: contractDigest("c")},
		},
		TargetVersions: []DomainVersion{
			{Domain: DomainControlPlane, Version: contractDigest("d")},
			{Domain: DomainAuthoritativeDNS, Version: contractDigest("e")},
		},
		Steps: []CompositeReleaseStep{
			{
				ID: "authoritative-dns", Domain: DomainAuthoritativeDNS, Adapter: dnsAdapter,
				ActivationIDs: []string{"activate-dns"}, BaseVersion: contractDigest("c"), TargetVersion: contractDigest("e"),
				ForwardRenderedDigest: contractDigest("f"), ReverseRenderedDigest: contractDigest("0"),
				Observation:           CompositeObservationPolicy{HealthEvidenceDigest: contractDigest("1"), MinimumSamples: "5", WindowSeconds: "120"},
				RollbackBudgetSeconds: "300",
			},
			{
				ID: "control-plane", Domain: DomainControlPlane, Adapter: controlAdapter, DependsOn: []string{"authoritative-dns"},
				ActivationIDs: []string{"activate-api"}, BaseVersion: contractDigest("b"), TargetVersion: contractDigest("d"),
				ForwardRenderedDigest: contractDigest("2"), ReverseRenderedDigest: contractDigest("3"),
				Observation:           CompositeObservationPolicy{HealthEvidenceDigest: contractDigest("4"), MinimumSamples: "5", WindowSeconds: "120"},
				RollbackBudgetSeconds: "300",
			},
		},
	}
}

func contractDigest(digit string) string {
	return "sha256:" + strings.Repeat(digit, 64)
}
