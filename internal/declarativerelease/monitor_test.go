package declarativerelease

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func verifiedMonitorFixture(t *testing.T) (Plan, ArtifactReceipt, RenderedManifests, ExecutionPlan, ExecutionResult, Observation, Observation) {
	t.Helper()
	plan, receipt, rendered, lkg, forward := executionFixture(t)
	prepareCluster := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}}
	prepared, err := PrepareExecution(context.Background(), prepareCluster, plan, "api", receipt, rendered, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	executeCluster := &fakeCluster{observations: []Observation{lkg}, health: []Observation{forward}}
	terminal := Execute(context.Background(), executeCluster, plan, prepared, rendered.Forward, rendered.LKG)
	if terminal.Status != "verified" {
		t.Fatalf("terminal fixture is not verified: %+v", terminal)
	}
	return plan, receipt, rendered, prepared, terminal, lkg, forward
}

func TestMonitorRecordBindsVerifiedForwardAndExactLKG(t *testing.T) {
	plan, receipt, rendered, prepared, terminal, _, _ := verifiedMonitorFixture(t)
	record, err := NewMonitorRecord(plan, receipt, prepared, terminal, rendered.Forward, rendered.LKG)
	if err != nil {
		t.Fatal(err)
	}
	if err := record.Validate(plan, receipt, prepared, terminal, rendered.Forward, rendered.LKG); err != nil {
		t.Fatal(err)
	}
	tampered := record
	tampered.LKGManifestDigest = "sha256:" + strings.Repeat("9", 64)
	if err := tampered.Validate(plan, receipt, prepared, terminal, rendered.Forward, rendered.LKG); err == nil {
		t.Fatal("tampered monitor LKG binding was accepted")
	}
	failed := terminal
	failed.Status = "compensated"
	if _, err := NewMonitorRecord(plan, receipt, prepared, failed, rendered.Forward, rendered.LKG); err == nil {
		t.Fatal("non-verified terminal result was persisted as an active monitor record")
	}
}

func TestRecordedExecutionPlanRetainsDigestValidationWithoutTransientFreshnessWindow(t *testing.T) {
	plan, _, rendered, prepared, _, _, _ := verifiedMonitorFixture(t)
	prepared.PreparedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	prepared.PlanDigest = ""
	unsigned, err := CanonicalJSON(prepared)
	if err != nil {
		t.Fatal(err)
	}
	prepared.PlanDigest = digestOf(unsigned)
	raw, err := CanonicalJSON(prepared)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRecordedExecutionPlan(bytes.NewReader(raw), plan, rendered.Forward, rendered.LKG); err != nil {
		t.Fatalf("durable verified plan was rejected solely because its prewrite time is old: %v", err)
	}
	if _, err := DecodeExecutionPlan(bytes.NewReader(raw), plan, rendered.Forward, rendered.LKG); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("transient executor accepted an old prewrite plan: %v", err)
	}
}

func TestMonitorStateRequiresThreeConsecutiveFailuresAndResetsOnHealth(t *testing.T) {
	plan, receipt, rendered, prepared, terminal, _, _ := verifiedMonitorFixture(t)
	record, err := NewMonitorRecord(plan, receipt, prepared, terminal, rendered.Forward, rendered.LKG)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 10, 18, 0, 0, 0, time.UTC)
	var state MonitorState
	for count := 1; count <= MonitorFailureThreshold; count++ {
		var rollback bool
		state, rollback, err = NewMonitorState(record, state, false, "route unavailable\nwith control byte", now.Add(time.Duration(count)*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if rollback != (count == MonitorFailureThreshold) || state.ConsecutiveFailures != count || strings.ContainsAny(state.LastReason, "\r\n") {
			t.Fatalf("failure %d state=%+v rollback=%t", count, state, rollback)
		}
	}
	state, rollback, err := NewMonitorState(record, state, true, "", now.Add(4*time.Minute))
	if err != nil || rollback || state.ConsecutiveFailures != 0 || state.LastHealthyAt == "" {
		t.Fatalf("healthy reset state=%+v rollback=%t err=%v", state, rollback, err)
	}
}

func TestMonitorStateDoesNotCountPublicRouteOnlyFailureTowardRollback(t *testing.T) {
	plan, receipt, rendered, prepared, terminal, _, _ := verifiedMonitorFixture(t)
	record, err := NewMonitorRecord(plan, receipt, prepared, terminal, rendered.Forward, rendered.LKG)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 10, 18, 0, 0, 0, time.UTC)
	state, rollback, err := NewMonitorObservationState(record, MonitorState{}, false, false, "public route canary timed out", now)
	if err != nil || rollback || state.ConsecutiveFailures != 0 || state.LastReason == "" || state.LastHealthyAt != "" {
		t.Fatalf("public-only failure state=%+v rollback=%t err=%v", state, rollback, err)
	}
	for count := 1; count <= MonitorFailureThreshold; count++ {
		state, rollback, err = NewMonitorObservationState(record, state, false, true, "component-local readiness failed", now.Add(time.Duration(count)*time.Minute))
		if err != nil || rollback != (count == MonitorFailureThreshold) || state.ConsecutiveFailures != count {
			t.Fatalf("local failure %d state=%+v rollback=%t err=%v", count, state, rollback, err)
		}
	}
	state, rollback, err = NewMonitorObservationState(record, state, false, true, "component-local readiness still failed", now.Add(4*time.Minute))
	if err != nil || !rollback || state.ConsecutiveFailures != MonitorFailureThreshold {
		t.Fatalf("threshold state was not capped: state=%+v rollback=%t err=%v", state, rollback, err)
	}
}

func TestContinuousRollbackRestoresOnlyRecordedComponentLKG(t *testing.T) {
	plan, receipt, rendered, prepared, terminal, lkg, forward := verifiedMonitorFixture(t)
	if _, err := NewMonitorRecord(plan, receipt, prepared, terminal, rendered.Forward, rendered.LKG); err != nil {
		t.Fatal(err)
	}
	cluster := &fakeCluster{observations: []Observation{forward}, health: []Observation{lkg}}
	result := RestoreMonitoredLKG(context.Background(), cluster, plan, prepared, rendered.Forward, rendered.LKG, plan.Releases[0])
	if result.Status != "compensated" || result.Reason != "continuous-health-threshold-lkg-restored" || result.LKGApplyCount != 1 || cluster.applies != 1 || cluster.deleteCreated != 1 {
		t.Fatalf("continuous rollback result=%+v applies=%d deleteCreated=%d", result, cluster.applies, cluster.deleteCreated)
	}
	other := plan.Releases[0]
	other.ComponentID = "controller"
	result = RestoreMonitoredLKG(context.Background(), cluster, plan, prepared, rendered.Forward, rendered.LKG, other)
	if result.Status != "recovery-required" || result.Reason != "continuous-rollback-plan-invalid" {
		t.Fatalf("cross-component rollback was accepted: %+v", result)
	}
}
