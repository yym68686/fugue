package dnsserver

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"
	"time"

	"fugue/internal/dnsobservation"
	"fugue/internal/model"
)

func TestObservationPolicyAuthenticatesTheExactServingCheckpoint(t *testing.T) {
	service, state, now := runtimeFactsFixture(t)
	if err := service.signDNSCheckpoint(&state.record); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(state.record)
	snapshot, err := dnsRuntimeFacts(state, now)
	if err != nil {
		t.Fatal(err)
	}
	queries := []dnsobservation.Query{{Hostname: "app.example.test", Type: "A"}}
	before, _ := json.Marshal([]any{state.record, snapshot})
	policy, err := VerifiedObservationPolicy(service.Config, raw, snapshot, queries, now)
	if err != nil || !reflect.DeepEqual(policy.Records[0].Allowed, []string{"8.8.8.8"}) || policy.Assignment.ContentHash != snapshot.Assignment.ContentHash || !policy.ValidUntil.Equal(now.Add(10*time.Minute)) {
		t.Fatal(policy, err)
	}
	// Authorization must not disappear with a failed proof, or imply readiness:
	// this command exports signed membership, not actual selected DNS answers.
	snapshot.Ready = false
	for i := range snapshot.Facts {
		snapshot.Facts[i].Ready = false
	}
	if _, err := VerifiedObservationPolicy(service.Config, raw, snapshot, queries, now); err != nil {
		t.Fatal(err)
	}
	snapshot, _ = dnsRuntimeFacts(state, now)
	for _, scenario := range []string{"signature", "key", "node", "fence", "parent", "plan", "future", "stale", "lease", "expired", "queries"} {
		t.Run(scenario, func(t *testing.T) {
			changed, configuration, encoded, at, qs := snapshot, service.Config, slices.Clone(raw), now, slices.Clone(queries)
			switch scenario {
			case "signature":
				encoded[len(encoded)-3] ^= 1
			case "key":
				configuration.BundleRevokedKeyIDs = []string{configuration.BundleSigningKeyID}
			case "node":
				changed.NodeID = "foreign"
			case "fence":
				changed.Assignment.FencingToken++
			case "parent":
				changed.ParentDigest = "foreign"
			case "plan":
				changed.PlanDigest = "foreign"
			case "future":
				changed.EvaluatedAt = now.Add(time.Second)
			case "stale":
				changed.EvaluatedAt = now.Add(-time.Hour)
			case "lease":
				changed.CheckpointValidUntil = now.Add(24 * time.Hour)
			case "expired":
				at = now.Add(24 * time.Hour)
			case "queries":
				qs = append(qs, qs[0])
			}
			if _, err := VerifiedObservationPolicy(configuration, encoded, changed, qs, at); err == nil {
				t.Fatal("untrusted policy accepted")
			}
		})
	}
	after, _ := json.Marshal([]any{state.record, snapshot})
	if string(before) != string(after) {
		t.Fatal("export mutated serving inputs")
	}
}

func TestObservationPolicyRecoveryMembershipIsNotObservedAnswerLearning(t *testing.T) {
	now := time.Now().UTC()
	record := model.EdgeDNSRecord{Type: "A", Values: []string{"192.0.2.99"}, AnswerPolicy: model.DNSAnswerPolicy{
		PolicyKind: model.DNSAnswerPolicyKindPhysicalOrder, AllowedEdgeGroups: []string{"group-a"},
		PhysicalOrder: &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a"}},
		Availability:  &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}},
		Candidates: []model.EdgeDNSAnswerCandidate{{EdgeID: "edge-a", EdgeGroupID: "group-a", IP: "192.0.2.1"},
			{EdgeID: "edge-b", EdgeGroupID: "group-a", IP: "192.0.2.2", Healthy: false, RouteReady: false},
			{EdgeID: "edge-c", EdgeGroupID: "group-a", IP: "192.0.2.3"},
			{EdgeID: "edge-b", EdgeGroupID: "foreign", IP: "192.0.2.4"}}}
	values, err := observationRecordAddresses(record, now)
	if err != nil || !reflect.DeepEqual(values, []string{"192.0.2.1", "192.0.2.2"}) {
		t.Fatal(values, err)
	}
	// A recovered authorized candidate was never removed by transient health;
	// neither an observed value nor an unlisted edge expands its authorization.
	record.Candidates[1].Healthy = true
	after, _ := observationRecordAddresses(record, now)
	if !reflect.DeepEqual(values, after) {
		t.Fatal("health changed membership")
	}
	record.AnswerPolicy.Availability = nil
	values, _ = observationRecordAddresses(record, now)
	if !reflect.DeepEqual(values, []string{"192.0.2.1"}) {
		t.Fatal(values)
	}
}

func TestObservationPolicyStaticExpirationsAndAddressFamilies(t *testing.T) {
	now := time.Now().UTC()
	record := model.EdgeDNSRecord{Type: "A", Values: []string{"192.0.2.1", "192.0.2.2"}, ValueExpirations: map[string]time.Time{"192.0.2.1": now}}
	values, err := observationRecordAddresses(record, now)
	if err != nil || !reflect.DeepEqual(values, []string{"192.0.2.2"}) {
		t.Fatal(values, err)
	}
	record.Values = []string{"2001:db8::1"}
	if _, err := observationRecordAddresses(record, now); err == nil {
		t.Fatal("wrong family accepted")
	}
}

func TestObservationPolicyQualityAndRedundantMembershipStayWithinTheirPools(t *testing.T) {
	now := time.Now().UTC()
	record := physicalSelectionTestRecord(now)
	record.AnswerPolicy.PhysicalSelection.OrderedEdgeIDs = []string{"edge-b"}
	values, err := observationRecordAddresses(record, now)
	if err != nil || !reflect.DeepEqual(values, []string{"192.0.2.11"}) {
		t.Fatal(values, err)
	}
	record.AnswerPolicy.Availability = &model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityRedundantVersion,
		OrderedEdgeIDs: []string{"edge-a", "unranked"}, MaxAnswers: 2, RecoverySuccesses: 3, RecoveryMinSeconds: 60}
	values, err = observationRecordAddresses(record, now)
	if err != nil || !reflect.DeepEqual(values, []string{"192.0.2.10", "192.0.2.12"}) {
		t.Fatal(values, err)
	}
	record.AnswerPolicy.AllowedEdgeGroups = []string{"shared"}
	values, err = observationRecordAddresses(record, now)
	if err != nil || !reflect.DeepEqual(values, []string{"192.0.2.10"}) {
		t.Fatal(values, err)
	}
}
