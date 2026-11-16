package dnsserver

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/dnsfacts"
	"fugue/internal/dnsobservation"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
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

func TestObservationPolicyRejectionIdentifiesTheExactUnchangedGate(t *testing.T) {
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
	type testCase struct {
		reason string
		change func(*dnsServingCheckpoint, *dnsfacts.Snapshot, *[]dnsobservation.Query, *time.Time)
	}
	cases := []testCase{
		{"query_count", func(_ *dnsServingCheckpoint, _ *dnsfacts.Snapshot, q *[]dnsobservation.Query, _ *time.Time) { *q = nil }},
		{"query_count", func(_ *dnsServingCheckpoint, _ *dnsfacts.Snapshot, q *[]dnsobservation.Query, _ *time.Time) {
			*q = make([]dnsobservation.Query, 33)
		}},
		{"checkpoint_future", func(c *dnsServingCheckpoint, _ *dnsfacts.Snapshot, _ *[]dnsobservation.Query, _ *time.Time) {
			c.AppliedAt = now.Add(time.Nanosecond)
		}},
		{"checkpoint_expired", func(c *dnsServingCheckpoint, _ *dnsfacts.Snapshot, _ *[]dnsobservation.Query, at *time.Time) {
			*at = c.AppliedAt.Add(time.Duration(state.payload.Policy.MaxStaleSeconds) * time.Second)
		}},
		{"snapshot_schema", func(_ *dnsServingCheckpoint, s *dnsfacts.Snapshot, _ *[]dnsobservation.Query, _ *time.Time) {
			s.Schema = "private-input"
		}},
		{"snapshot_node", func(_ *dnsServingCheckpoint, s *dnsfacts.Snapshot, _ *[]dnsobservation.Query, _ *time.Time) {
			s.NodeID = "private-input"
		}},
		{"snapshot_group", func(_ *dnsServingCheckpoint, s *dnsfacts.Snapshot, _ *[]dnsobservation.Query, _ *time.Time) {
			s.EdgeGroupID = "private-input"
		}},
		{"snapshot_parent", func(_ *dnsServingCheckpoint, s *dnsfacts.Snapshot, _ *[]dnsobservation.Query, _ *time.Time) {
			s.ParentDigest = "private-input"
		}},
		{"snapshot_plan", func(_ *dnsServingCheckpoint, s *dnsfacts.Snapshot, _ *[]dnsobservation.Query, _ *time.Time) {
			s.PlanDigest = "private-input"
		}},
		{"checkpoint_deadline", func(_ *dnsServingCheckpoint, s *dnsfacts.Snapshot, _ *[]dnsobservation.Query, _ *time.Time) {
			s.CheckpointValidUntil = s.CheckpointValidUntil.Add(time.Nanosecond)
		}},
		{"evaluation_missing", func(_ *dnsServingCheckpoint, s *dnsfacts.Snapshot, _ *[]dnsobservation.Query, _ *time.Time) {
			s.EvaluatedAt = time.Time{}
		}},
		{"evaluation_future", func(_ *dnsServingCheckpoint, s *dnsfacts.Snapshot, _ *[]dnsobservation.Query, _ *time.Time) {
			s.EvaluatedAt = now.Add(time.Nanosecond)
		}},
		{"evaluation_stale", func(_ *dnsServingCheckpoint, s *dnsfacts.Snapshot, _ *[]dnsobservation.Query, _ *time.Time) {
			s.EvaluatedAt = now.Add(-time.Duration(state.payload.Policy.DNSReadiness.ProbeIntervalSeconds)*time.Second - time.Nanosecond)
		}},
	}
	assignmentChanges := []struct {
		reason string
		change func(*model.PlatformConsumerAssignment)
	}{
		{"assignment_expected_set", func(a *model.PlatformConsumerAssignment) { a.ExpectedConsumerSetID = "private-input" }},
		{"assignment_release_set", func(a *model.PlatformConsumerAssignment) { a.ReleaseSetID = "private-input" }},
		{"assignment_release", func(a *model.PlatformConsumerAssignment) { a.ArtifactReleaseID = "private-input" }},
		{"assignment_kind", func(a *model.PlatformConsumerAssignment) { a.ArtifactKind = "private-input" }},
		{"assignment_scope", func(a *model.PlatformConsumerAssignment) { a.ScopeKey = "private-input" }},
		{"assignment_generation", func(a *model.PlatformConsumerAssignment) { a.ExpectedGeneration = "private-input" }},
		{"assignment_revision", func(a *model.PlatformConsumerAssignment) { a.Revision++ }},
		{"assignment_artifact", func(a *model.PlatformConsumerAssignment) { a.ArtifactID = "private-input" }},
		{"assignment_content", func(a *model.PlatformConsumerAssignment) { a.ContentHash = "private-input" }},
		{"assignment_sequence", func(a *model.PlatformConsumerAssignment) { a.GenerationSequence++ }},
		{"assignment_channel", func(a *model.PlatformConsumerAssignment) { a.ReleaseChannel = "private-input" }},
		{"assignment_fence", func(a *model.PlatformConsumerAssignment) { a.FencingToken++ }},
		{"assignment_protocol", func(a *model.PlatformConsumerAssignment) { a.ExpectedProtocolVersion = "private-input" }},
		{"assignment_schema", func(a *model.PlatformConsumerAssignment) { a.ExpectedSchemaVersion = "private-input" }},
		{"assignment_capabilities", func(a *model.PlatformConsumerAssignment) { a.CompatibilityCapabilities = []string{"private-input"} }},
		{"assignment_heartbeat_deadline", func(a *model.PlatformConsumerAssignment) {
			a.HeartbeatDeadline = a.HeartbeatDeadline.Add(time.Nanosecond)
		}},
		{"assignment_convergence_deadline", func(a *model.PlatformConsumerAssignment) {
			a.ConvergenceDeadline = a.ConvergenceDeadline.Add(time.Nanosecond)
		}},
	}
	for _, assignment := range assignmentChanges {
		cases = append(cases, testCase{assignment.reason, func(_ *dnsServingCheckpoint, s *dnsfacts.Snapshot, _ *[]dnsobservation.Query, _ *time.Time) {
			assignment.change(&s.Assignment)
		}})
	}
	for _, tc := range cases {
		t.Run(tc.reason, func(t *testing.T) {
			checkpoint, changed, qs, at := state.record, snapshot, slices.Clone(queries), now
			tc.change(&checkpoint, &changed, &qs, &at)
			if err := service.signDNSCheckpoint(&checkpoint); err != nil {
				t.Fatal(err)
			}
			encoded, _ := json.Marshal(checkpoint)
			before, _ := json.Marshal([]any{checkpoint, changed, qs})
			policy, err := VerifiedObservationPolicy(service.Config, encoded, changed, qs, at)
			if err == nil || err.Error() != "DNS observation policy rejected: "+tc.reason || !reflect.DeepEqual(policy, dnsobservation.Policy{}) {
				t.Fatal("incorrect rejection or policy leaked on failure", tc.reason, err)
			}
			if strings.Contains(err.Error(), "private-input") {
				t.Fatal("rejection exposed private input")
			}
			// Compare to the exact pre-diagnostic predicate, independently.
			c, p, _, decodeErr := service.decodeDNSCheckpointPayload(encoded)
			if decodeErr != nil {
				t.Fatal("test failed before the gate under investigation", decodeErr)
			}
			until := c.AppliedAt.Add(time.Duration(p.Policy.MaxStaleSeconds) * time.Second)
			plan, digestErr := platformconfig.Digest(p.Plan)
			wasRejected := digestErr != nil || len(qs) < 1 || len(qs) > 32 || c.AppliedAt.After(at) || !until.After(at) ||
				changed.Schema != dnsfacts.Schema || changed.NodeID != c.NodeID || changed.EdgeGroupID != c.GroupID ||
				changed.ParentDigest != c.Parent.ContentHash || changed.PlanDigest != plan ||
				!reflect.DeepEqual(changed.Assignment, c.Candidate.Assignment) || !changed.CheckpointValidUntil.Equal(until) ||
				changed.EvaluatedAt.IsZero() || changed.EvaluatedAt.After(at) || at.Sub(changed.EvaluatedAt) > time.Duration(p.Policy.DNSReadiness.ProbeIntervalSeconds)*time.Second
			if !wasRejected {
				t.Fatal("diagnostic changed the original acceptance predicate")
			}
			after, _ := json.Marshal([]any{checkpoint, changed, qs})
			if string(before) != string(after) {
				t.Fatal("diagnostic mutated verification inputs")
			}
		})
	}
	for _, age := range []time.Duration{0, time.Duration(state.payload.Policy.DNSReadiness.ProbeIntervalSeconds) * time.Second} {
		changed := snapshot
		changed.EvaluatedAt = now.Add(-age)
		if _, err := VerifiedObservationPolicy(service.Config, raw, changed, queries, now); err != nil {
			t.Fatal("existing inclusive freshness boundary changed", age, err)
		}
	}
	// Multiple failures preserve first-gate ordering, not input-dependent text.
	changed := snapshot
	changed.Schema, changed.NodeID = "private-input", "private-input"
	if _, err := VerifiedObservationPolicy(service.Config, raw, changed, queries, now); err == nil || err.Error() != "DNS observation policy rejected: snapshot_schema" {
		t.Fatal("rejection order changed", err)
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
