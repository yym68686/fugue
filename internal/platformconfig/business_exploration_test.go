package platformconfig

import (
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"
)

func businessPolicyForTest(now time.Time) model.BusinessEdgeExplorationPolicy {
	return model.BusinessEdgeExplorationPolicy{Version: model.BusinessEdgeExplorationVersion, IntentID: "intent-a", Mode: "observe", BaselineEdgeID: "edge-a", CandidateEdgeIDs: []string{"edge-a", "edge-b"},
		InitialDNSPublications: map[string]model.BusinessDNSPublicationAnchor{"dns-a": {Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", FencingToken: 1}},
		StartsAt:               now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour), ExplorationBasisPoints: 100, MaximumCandidateBasisPoints: 1000, MaximumStepBasisPoints: 50,
		BucketSeconds: 300, RequiredBuckets: 3, MinimumConnectionsPerBucket: 3, CooldownSeconds: 600, EvidenceMaxAgeSeconds: 600,
		MinimumCohortCoverageBasisPoints: 9000, RTTAdvantageRatio: .15, DeliveryAdvantageRatio: .2, MaximumRTTRegressionRatio: .1,
		MaximumDeliveryRegressionRatio: .1, MaximumRetransmissionIncrease: .01, StopRTTMS: 3000, StopRetransmissionRate: .2}
}

func TestBusinessDNSQueryRequiresExplicitPolicyAndExactPath(t *testing.T) {
	policy := physicalPolicyFixture()
	policy.PhysicalRoutes[0].Policy.Version = model.PhysicalComparableDeliveryPolicyVersion
	exploration := businessPolicyForTest(time.Now().UTC())
	policy.PhysicalRoutes[0].BusinessExploration = &exploration
	policy.PhysicalRoutes[0].PathPrefix = "/"
	raw, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	var decoded DNSQueryPolicy
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	copy := NormalizePolicySnapshot(PolicySnapshot{DNSQueryPolicy: &policy})
	copy.DNSQueryPolicy.PhysicalRoutes[0].BusinessExploration.CandidateEdgeIDs[0] = "other"
	if policy.PhysicalRoutes[0].BusinessExploration.CandidateEdgeIDs[0] != "edge-a" {
		t.Fatal("normalization aliases exploration intent")
	}
	for _, change := range []func(*DNSQueryPolicy){
		func(value *DNSQueryPolicy) { value.PhysicalRoutes[0].PathPrefix = "" },
		func(value *DNSQueryPolicy) { value.PhysicalRoutes[0].BusinessExploration = nil },
		func(value *DNSQueryPolicy) { value.PhysicalRoutes[0].Policy.WindowSeconds = 600 },
		func(value *DNSQueryPolicy) { value.PhysicalRoutes[0].BusinessExploration.ExplorationBasisPoints = 5000 },
	} {
		var candidate DNSQueryPolicy
		if err := json.Unmarshal(raw, &candidate); err != nil {
			t.Fatal(err)
		}
		change(&candidate)
		if ValidateDNSQueryPolicy(&candidate) == nil {
			t.Fatal("accepted incomplete exploration intent")
		}
	}
}

func TestBusinessDNSCompilerRequiresMatchingProducerIntent(t *testing.T) {
	policy := businessPolicyForTest(time.Now().UTC())
	record := model.EdgeDNSRecord{Name: "app.example.test", Type: "A", AnswerPolicy: model.DNSAnswerPolicy{
		PhysicalSelection: &model.DNSPhysicalSelection{BusinessAllocation: &model.DNSBusinessAllocation{Policy: policy}}}}
	if validateDNSBusinessIntent(record, PolicySnapshot{}) == nil {
		t.Fatal("accepted allocation without independent producer intent")
	}
	declared := PolicySnapshot{DNSQueryPolicy: &DNSQueryPolicy{PhysicalRoutes: []PhysicalQualityRoute{{Hostname: record.Name, PathPrefix: "/", BusinessExploration: &policy}}}}
	if err := validateDNSBusinessIntent(record, declared); err != nil {
		t.Fatal(err)
	}
	record.AnswerPolicy.PhysicalSelection.BusinessAllocation.Policy.Mode = "adaptive"
	if validateDNSBusinessIntent(record, declared) == nil {
		t.Fatal("artifact silently enabled automatic promotion")
	}
	record.AnswerPolicy.PhysicalSelection = nil
	if validateDNSBusinessIntent(record, declared) == nil {
		t.Fatal("missing business allocation accepted")
	}
}
