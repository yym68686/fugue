package declarativerelease

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type fakeCluster struct {
	observations         []Observation
	observationErrors    []error
	observedManifests    [][]byte
	cas                  []Observation
	casManifests         [][]byte
	degraded             []Observation
	verifiedTargets      []TargetIdentity
	verifyErrors         []error
	dryRuns              int
	applies              int
	applyErrors          []error
	deleteCreated        int
	health               []Observation
	healthErrors         []error
	healthTargets        []TargetIdentity
	healthPrewrite       []bool
	healthPreservedRoute []bool
	converged            [][]byte
	convergedErrors      []error
	monitorConverged     [][]byte
	monitorErrors        []error
	rollbackDriftChecks  int
	rollbackDriftErrors  []error
}

func (fake *fakeCluster) Observe(_ context.Context, _ PlanRelease, _ TargetIdentity, manifest []byte) (Observation, error) {
	fake.observedManifests = append(fake.observedManifests, append([]byte(nil), manifest...))
	if len(fake.observationErrors) > 0 {
		err := fake.observationErrors[0]
		fake.observationErrors = fake.observationErrors[1:]
		if err != nil {
			return Observation{}, err
		}
	}
	if len(fake.observations) == 0 {
		return Observation{}, errors.New("no observation")
	}
	value := fake.observations[0]
	fake.observations = fake.observations[1:]
	return value, nil
}

func (fake *fakeCluster) ObserveCAS(ctx context.Context, release PlanRelease, manifest []byte) (Observation, error) {
	fake.casManifests = append(fake.casManifests, append([]byte(nil), manifest...))
	if len(fake.cas) == 0 {
		return fake.Observe(ctx, release, TargetIdentity{}, manifest)
	}
	value := fake.cas[0]
	fake.cas = fake.cas[1:]
	return value, nil
}

func (fake *fakeCluster) ObserveDegraded(context.Context, PlanRelease, []byte) (Observation, error) {
	if len(fake.degraded) == 0 {
		return Observation{}, errors.New("no degraded observation")
	}
	value := fake.degraded[0]
	fake.degraded = fake.degraded[1:]
	return value, nil
}

func (fake *fakeCluster) VerifyTarget(_ context.Context, target TargetIdentity) error {
	fake.verifiedTargets = append(fake.verifiedTargets, target)
	if len(fake.verifyErrors) == 0 {
		return nil
	}
	err := fake.verifyErrors[0]
	fake.verifyErrors = fake.verifyErrors[1:]
	return err
}

func (fake *fakeCluster) DryRunApply(context.Context, PlanRelease, []byte) error {
	fake.dryRuns++
	return nil
}

func (fake *fakeCluster) Apply(context.Context, PlanRelease, TargetIdentity, []byte) error {
	fake.applies++
	if len(fake.applyErrors) == 0 {
		return nil
	}
	err := fake.applyErrors[0]
	fake.applyErrors = fake.applyErrors[1:]
	return err
}

func (fake *fakeCluster) Delete(context.Context, PlanRelease, []byte, Observation) error {
	fake.applies++
	return nil
}

func (fake *fakeCluster) DeleteCreated(context.Context, PlanRelease, []byte, Observation, Observation) error {
	fake.deleteCreated++
	return nil
}

func (fake *fakeCluster) DeleteCreatedForOwnershipTakeover(context.Context, PlanRelease, []byte, []byte, Observation, Observation) error {
	fake.deleteCreated++
	return nil
}

func (fake *fakeCluster) ClearOwnershipTakeoverForwardOnlyFields(context.Context, PlanRelease, []byte, []byte, Observation) error {
	return nil
}

func (fake *fakeCluster) WaitHealthy(ctx context.Context, _ PlanRelease, target TargetIdentity, _ []byte) (Observation, error) {
	fake.healthTargets = append(fake.healthTargets, target)
	fake.healthPrewrite = append(fake.healthPrewrite, IsPrewritePredecessorHealthWait(ctx))
	fake.healthPreservedRoute = append(fake.healthPreservedRoute, IsPreservedRouteHealthWait(ctx))
	var value Observation
	if len(fake.health) > 0 {
		value = fake.health[0]
		fake.health = fake.health[1:]
	}
	if len(fake.healthErrors) == 0 {
		return value, nil
	}
	err := fake.healthErrors[0]
	fake.healthErrors = fake.healthErrors[1:]
	return value, err
}

func (fake *fakeCluster) Converged(_ context.Context, _ PlanRelease, manifest []byte) error {
	fake.converged = append(fake.converged, append([]byte(nil), manifest...))
	if len(fake.convergedErrors) > 0 {
		err := fake.convergedErrors[0]
		fake.convergedErrors = fake.convergedErrors[1:]
		return err
	}
	return nil
}

func (fake *fakeCluster) MonitorConverged(_ context.Context, _ PlanRelease, manifest []byte) error {
	fake.monitorConverged = append(fake.monitorConverged, append([]byte(nil), manifest...))
	if len(fake.monitorErrors) > 0 {
		err := fake.monitorErrors[0]
		fake.monitorErrors = fake.monitorErrors[1:]
		return err
	}
	return nil
}

func (fake *fakeCluster) VerifyOwnershipConverged(context.Context, PlanRelease, []byte) error {
	return nil
}

func (fake *fakeCluster) ValidateEmergencyRollbackDrift(_ context.Context, _ PlanRelease, _ []byte, current Observation) (Observation, error) {
	fake.rollbackDriftChecks++
	if len(fake.rollbackDriftErrors) == 0 {
		return current, nil
	}
	err := fake.rollbackDriftErrors[0]
	fake.rollbackDriftErrors = fake.rollbackDriftErrors[1:]
	return Observation{}, err
}

func executionFixture(t *testing.T) (Plan, ArtifactReceipt, RenderedManifests, Observation, Observation) {
	t.Helper()
	return executionFixtureForPlan(t, boundAPIPlan(t))
}

func retryExecutionFixture(t *testing.T) (Plan, ArtifactReceipt, RenderedManifests, Observation, Observation) {
	t.Helper()
	plan := boundAPIPlan(t)
	plan.PlanDigest = ""
	plan.Releases[0].IntentGeneration = 2
	plan.Releases[0].RetrySameLKG = true
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestOf(unsigned)
	return executionFixtureForPlan(t, plan)
}

func executionFixtureForPlan(t *testing.T, plan Plan) (Plan, ArtifactReceipt, RenderedManifests, Observation, Observation) {
	t.Helper()
	verification := RegistryVerification{
		Image:          "ghcr.io/example/fugue-api@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		IndexDigest:    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ManifestDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ConfigDigest:   "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		OCIRevision:    testSHA2, Platform: "linux/amd64", Verification: "registry_manifest_config_and_layer_get",
		BlobCount: 2, LayerProbeCount: 1, RequestCount: 5, TotalLayerBytes: 100,
	}
	receipt, err := MaterializeArtifactReceipt(plan, "api", verification)
	if err != nil {
		t.Fatal(err)
	}
	base := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"replicas":2,"strategy":{"type":"RollingUpdate"},"template":{"metadata":{},"spec":{"containers":[{"image":"ghcr.io/example/fugue-api:old","name":"api"}]}}}}],"kind":"ComponentResourceSet"}`)
	rendered, err := RenderManifests(plan, "api", receipt, bytesReader(base), bytesReader(base))
	if err != nil {
		t.Fatal(err)
	}
	lkg := stableObservation("1", "10", "ghcr.io/example/fugue-api@"+testDigest, testSHA1)
	forward := stableObservation("1", "11", receipt.ImmutableRef, testSHA2)
	return plan, receipt, rendered, lkg, forward
}

func casOnlyObservation(observation Observation) Observation {
	return Observation{
		Present: observation.Present, Primary: observation.Primary, UID: observation.UID,
		ResourceVersion: observation.ResourceVersion, Generation: observation.Generation,
		Resources: append([]ResourceObservation(nil), observation.Resources...),
	}
}

func stableObservation(uid, rv, image, revision string) Observation {
	return Observation{
		Present: true,
		Primary: ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api"},
		UID:     uid, ResourceVersion: rv, Generation: 5, ObservedGeneration: 5,
		Desired: 2, Updated: 2, Ready: 2, Available: 2,
		ImageRef: image, ImageID: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		ConfigSHA: revision, ManifestSHA: revision, OCIRevision: revision,
		TemplateDigest: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
		HealthDigest:   "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		FieldManagers:  []string{"fugue-api-declarative"},
		Resources: []ResourceObservation{{
			Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api"},
			Present:  true, UID: uid, ResourceVersion: rv, Generation: 5,
			ObjectDigest:  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			FieldManagers: []string{"fugue-api-declarative"},
		}},
	}
}

func TestExecuteVerifiesForwardAndReconcilesCommitUnknown(t *testing.T) {
	plan, receipt, rendered, lkg, forward := executionFixture(t)
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil || fake.dryRuns != 2 {
		t.Fatalf("prepare: %v dryRuns=%d", err, fake.dryRuns)
	}
	fake.observations = []Observation{lkg}
	fake.applyErrors = []error{errors.New("transport closed")}
	fake.health = []Observation{forward}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "verified" || result.Reason != "forward-commit-unknown-reconciled" || result.ForwardApplyCount != 1 || result.LKGApplyCount != 0 || fake.applies != 1 {
		t.Fatalf("unexpected execution result: %+v applies=%d", result, fake.applies)
	}
}

func TestExecuteRetainsApplyFailureWhenNoProductionWriteOccurred(t *testing.T) {
	plan, receipt, rendered, lkg, _ := executionFixture(t)
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	fake.observations = []Observation{lkg, lkg}
	fake.applyErrors = []error{errors.New("stage edge Worker candidate: HTTP 409 (sequence_conflict)")}
	fake.health = []Observation{lkg}
	fake.healthErrors = []error{errors.New("forward health was not reached")}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "failed-no-write" || result.Reason != "forward-apply-rejected-before-commit" ||
		result.FailureClass != "forward_apply" || result.FailureDetail != "stage edge Worker candidate: HTTP 409 (sequence_conflict)" ||
		result.ForwardApplyCount != 1 || result.LKGApplyCount != 0 || fake.applies != 1 {
		t.Fatalf("no-write apply failure lost its cause: result=%+v applies=%d", result, fake.applies)
	}
}

func TestFailedEdgeGroupTransitionSkipsMixedIdentityHealthCheck(t *testing.T) {
	applyErr := errors.New("wait Guardian authority switch: transition timed out")
	partial := stableObservation("1", "11", "ghcr.io/example/fugue-edge@"+testDigest, testSHA1)
	release := PlanRelease{Transition: &Transition{Type: "edge-group-ab", EdgeGroupAB: &EdgeGroupABTransition{}}}
	fake := &fakeCluster{
		cas:          []Observation{partial},
		healthErrors: []error{errors.New("false live image provenance mismatch")},
	}

	observed, healthErr, convergedErr := observeForwardResult(context.Background(), fake, release, TargetIdentity{
		Present: true, ImageRef: "ghcr.io/example/fugue-edge@sha256:" + strings.Repeat("b", 64), ConfigSHA: testSHA2,
	}, []byte(`{"kind":"ComponentResourceSet"}`), applyErr)

	if !errors.Is(healthErr, applyErr) || convergedErr != nil || observed.ResourceVersion != partial.ResourceVersion {
		t.Fatalf("transition failure was not preserved: observed=%+v health=%v converged=%v", observed, healthErr, convergedErr)
	}
	if len(fake.healthTargets) != 0 || len(fake.converged) != 0 {
		t.Fatalf("mixed edge transition ran generic target checks: health=%d converged=%d", len(fake.healthTargets), len(fake.converged))
	}
	if detail := forwardFailureDetail(applyErr, errors.New("secondary health error"), errors.New("secondary convergence error")); detail != applyErr.Error() {
		t.Fatalf("apply failure was masked in terminal detail: %q", detail)
	}
}

func TestPrepareObservesTheLivePredecessorAgainstTheLKGManifest(t *testing.T) {
	plan, receipt, rendered, lkg, _ := executionFixture(t)
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}}
	if _, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	if len(fake.observedManifests) != 2 {
		t.Fatalf("observations=%d want=2", len(fake.observedManifests))
	}
	for index, manifest := range fake.observedManifests {
		if !bytes.Equal(manifest, rendered.LKG) {
			t.Fatalf("observation %d used the forward manifest to validate the live predecessor", index)
		}
	}
}

func TestPrepareBindsAbsentForwardOnlyResourceWithoutWeakeningPredecessorCAS(t *testing.T) {
	plan, receipt, rendered, lkg, _ := executionFixture(t)
	forwardSet, err := DecodeResourceSet(bytes.NewReader(rendered.Forward))
	if err != nil {
		t.Fatal(err)
	}
	forwardSet.Items = append(forwardSet.Items, map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]any{"name": "fugue-api-extra", "namespace": "fugue-system"},
		"spec":     map[string]any{"ports": []any{map[string]any{"port": json.Number("443")}}, "selector": map[string]any{"app": "api"}},
	})
	rendered.Forward, err = CanonicalJSON(forwardSet)
	if err != nil {
		t.Fatal(err)
	}
	rendered.ForwardDigest = digestOf(rendered.Forward)
	forwardCAS := casOnlyObservation(lkg)
	forwardCAS.ResourceVersion = "11"
	forwardCAS.Resources[0].ResourceVersion = "11"
	forwardCAS.Resources = append(forwardCAS.Resources, ResourceObservation{
		Identity: ResourceIdentity{APIVersion: "v1", Kind: "Service", Namespace: "fugue-system", Name: "fugue-api-extra"},
	})
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}, cas: []Observation{forwardCAS}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil || fake.dryRuns != 2 || prepared.Prewrite.ResourceVersion != "11" || len(prepared.Prewrite.Resources) != 2 || prepared.Prewrite.Resources[1].Present {
		t.Fatalf("forward-only absent CAS was not bound: plan=%+v dryRuns=%d err=%v", prepared, fake.dryRuns, err)
	}
}

func TestPrepareRejectsExistingOrDriftedForwardOnlyResourceCAS(t *testing.T) {
	plan, receipt, rendered, lkg, _ := executionFixture(t)
	forwardSet, err := DecodeResourceSet(bytes.NewReader(rendered.Forward))
	if err != nil {
		t.Fatal(err)
	}
	extraIdentity := ResourceIdentity{APIVersion: "v1", Kind: "Service", Namespace: "fugue-system", Name: "fugue-api-extra"}
	forwardSet.Items = append(forwardSet.Items, map[string]any{
		"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": extraIdentity.Name, "namespace": extraIdentity.Namespace},
		"spec": map[string]any{"ports": []any{map[string]any{"port": json.Number("443")}}},
	})
	rendered.Forward, err = CanonicalJSON(forwardSet)
	if err != nil {
		t.Fatal(err)
	}
	rendered.ForwardDigest = digestOf(rendered.Forward)
	for name, mutate := range map[string]func(*Observation){
		"existing": func(observation *Observation) {
			observation.Resources = append(observation.Resources, ResourceObservation{Identity: extraIdentity, Present: true, UID: "foreign-uid", ResourceVersion: "7", ObjectDigest: testDigest, FieldManagers: []string{"foreign-manager"}})
		},
		"predecessor-drift": func(observation *Observation) {
			observation.Resources[0].ObjectDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
			observation.Resources = append(observation.Resources, ResourceObservation{Identity: extraIdentity})
		},
	} {
		t.Run(name, func(t *testing.T) {
			fresh := casOnlyObservation(lkg)
			mutate(&fresh)
			fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}, cas: []Observation{fresh}}
			if _, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0)); err == nil || fake.dryRuns != 0 {
				t.Fatalf("unsafe forward-only CAS was accepted: dryRuns=%d err=%v", fake.dryRuns, err)
			}
		})
	}
}

func TestPrepareRejectsAnLKGRestartDuringPrewriteValidation(t *testing.T) {
	plan, receipt, rendered, lkg, _ := executionFixture(t)
	changed := lkg
	changed.HealthDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	fake := &fakeCluster{observations: []Observation{lkg, changed}, health: []Observation{lkg}}
	if _, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0)); err == nil || !strings.Contains(err.Error(), "health changed") {
		t.Fatalf("changing LKG health witness was accepted: %v", err)
	}
}

func TestPrepareComparesPodHealthUsingTheSameObservationSemantics(t *testing.T) {
	plan, receipt, rendered, lkg, _ := executionFixture(t)
	probeAugmented := lkg
	probeAugmented.HealthDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{probeAugmented}}
	if _, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0)); err != nil {
		t.Fatalf("probe-augmented health witness was compared with the pod-only witness: %v", err)
	}
}

func TestPrepareRecoversTheDeclaredLKGAfterATransientUnreadyObservation(t *testing.T) {
	plan, receipt, rendered, lkg, _ := executionFixture(t)
	fake := &fakeCluster{
		observationErrors: []error{errors.New("ready workload pod count mismatch")},
		observations:      []Observation{lkg, lkg},
		health:            []Observation{lkg},
	}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil || prepared.AlreadyConverged || fake.dryRuns != 2 {
		t.Fatalf("transiently unready LKG did not recover: plan=%+v dryRuns=%d err=%v", prepared, fake.dryRuns, err)
	}
	if len(fake.healthTargets) != 1 || fake.healthTargets[0].OCIRevision != plan.Releases[0].ExpectedPreviousOCIRevision {
		t.Fatalf("prepare waited on the forward target before the declared LKG: %+v", fake.healthTargets)
	}
}

func TestPrepareFailedAtomSuccessorAdvancesFromTypedReadyCountMismatch(t *testing.T) {
	plan := boundAPIPlan(t)
	plan.PlanDigest = ""
	plan.Releases[0].SupersedesFailedConfigSHA = strings.Repeat("f", 40)
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestOf(unsigned)
	plan, receipt, rendered, lkg, forward := executionFixtureForPlan(t, plan)
	if plan.Releases[0].RetrySameLKG || plan.Releases[0].SupersedesFailedConfigSHA == "" {
		t.Fatalf("fixture is not a failed-atom successor: %+v", plan.Releases[0])
	}
	degraded := casOnlyObservation(lkg)
	fake := &fakeCluster{
		observations: []Observation{lkg},
		healthErrors: []error{
			fmt.Errorf("%w: ready workload pod count mismatch: got=0 want=1", ErrDegradedPredecessorHealth),
		},
		cas: []Observation{degraded, degraded},
	}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Now().UTC())
	if err != nil || !prepared.DegradedPredecessor || prepared.AlreadyConverged || len(fake.verifiedTargets) != 1 || len(fake.healthTargets) != 1 {
		t.Fatalf("independent degraded predecessor: plan=%+v verified=%d health_waits=%d err=%v", prepared, len(fake.verifiedTargets), len(fake.healthTargets), err)
	}
	if fake.healthTargets[0] != prepared.LKG || !fake.healthPrewrite[0] || fake.verifiedTargets[0] != prepared.LKG || len(fake.casManifests) != 2 || len(fake.converged) != 1 || fake.dryRuns != 2 {
		t.Fatalf("independent degraded predecessor was not exact-CAS verified: health=%+v marked=%+v verified=%+v cas=%d converged=%d dryRuns=%d", fake.healthTargets, fake.healthPrewrite, fake.verifiedTargets, len(fake.casManifests), len(fake.converged), fake.dryRuns)
	}
	encoded, err := CanonicalJSON(prepared)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err = DecodeExecutionPlan(bytes.NewReader(encoded), plan, rendered.Forward, rendered.LKG)
	if err != nil {
		t.Fatalf("decode failed-atom execution plan: %v", err)
	}
	current := casOnlyObservation(lkg)
	current.ResourceVersion = "11"
	current.Resources[0].ResourceVersion = "11"
	fake.cas = []Observation{current}
	fake.health = []Observation{forward}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "verified" || result.Reason != "forward-verified" || result.ForwardApplyCount != 1 || fake.applies != 1 {
		t.Fatalf("failed-atom degraded predecessor did not execute: result=%+v applies=%d", result, fake.applies)
	}
}

func TestDegradedPredecessorPreservesOnlyPreexistingPublicRouteFailure(t *testing.T) {
	plan := boundAPIPlan(t)
	plan.PlanDigest = ""
	plan.Releases[0].SupersedesFailedConfigSHA = strings.Repeat("f", 40)
	plan.Releases[0].Health = append(plan.Releases[0].Health, HealthProbe{
		Type: "public-route-http", Name: "edge-group-country-de", Address: "192.0.2.10:443",
		Host: "fugue.pro", Path: "/healthz", Expected: "ok",
	})
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestOf(unsigned)
	plan, receipt, rendered, lkg, forward := executionFixtureForPlan(t, plan)
	degraded := casOnlyObservation(lkg)
	fake := &fakeCluster{
		observations: []Observation{lkg},
		healthErrors: []error{
			fmt.Errorf("%w: %w: public route canary response is invalid", ErrDegradedPredecessorHealth, ErrPublicRouteHealth),
		},
		cas: []Observation{degraded, degraded},
	}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Now().UTC())
	if err != nil || !prepared.DegradedPredecessor || !prepared.DegradedRoute {
		t.Fatalf("prepare route-degraded predecessor: plan=%+v err=%v", prepared, err)
	}
	encoded, err := CanonicalJSON(prepared)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err = DecodeExecutionPlan(bytes.NewReader(encoded), plan, rendered.Forward, rendered.LKG)
	if err != nil {
		t.Fatalf("decode route-degraded execution plan: %v", err)
	}
	current := casOnlyObservation(lkg)
	current.ResourceVersion = "11"
	current.Resources[0].ResourceVersion = "11"
	fake.cas = []Observation{current}
	fake.health = []Observation{forward}
	fake.healthErrors = []error{fmt.Errorf("%w: public route canary response is invalid", ErrPublicRouteHealth)}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "verified" || result.Reason != "forward-verified-with-preserved-route-degradation" ||
		result.ForwardApplyCount != 1 || result.LKGApplyCount != 0 || fake.applies != 1 ||
		len(fake.healthPreservedRoute) == 0 || !fake.healthPreservedRoute[len(fake.healthPreservedRoute)-1] {
		t.Fatalf("route-degraded predecessor was not preserved exactly: result=%+v applies=%d route_waits=%v", result, fake.applies, fake.healthPreservedRoute)
	}
}

func TestExecutionPlanRejectsUnprovenDegradedRoute(t *testing.T) {
	plan, receipt, rendered, lkg, _ := retryExecutionFixture(t)
	degraded := casOnlyObservation(lkg)
	fake := &fakeCluster{cas: []Observation{degraded, degraded}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Now().UTC())
	if err != nil || !prepared.DegradedPredecessor {
		t.Fatalf("prepare degraded predecessor: plan=%+v err=%v", prepared, err)
	}
	prepared.DegradedRoute = true
	prepared.PlanDigest = ""
	unsigned, err := CanonicalJSON(prepared)
	if err != nil {
		t.Fatal(err)
	}
	prepared.PlanDigest = digestOf(unsigned)
	if err := prepared.Validate(plan, rendered.Forward, rendered.LKG); err == nil || !strings.Contains(err.Error(), "degraded route recovery is not authorized") {
		t.Fatalf("execution plan accepted route recovery without a public route probe: %v", err)
	}
}

func TestPrepareFailedAtomSuccessorUsesTypedPredecessorWaitAfterObservationError(t *testing.T) {
	plan := boundAPIPlan(t)
	plan.PlanDigest = ""
	plan.Releases[0].SupersedesFailedConfigSHA = strings.Repeat("f", 40)
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestOf(unsigned)
	plan, receipt, rendered, lkg, _ := executionFixtureForPlan(t, plan)
	degraded := casOnlyObservation(lkg)
	fake := &fakeCluster{
		observationErrors: []error{errors.New("transient predecessor observation error")},
		healthErrors: []error{
			fmt.Errorf("%w: ready workload pod count mismatch: got=0 want=1", ErrDegradedPredecessorHealth),
		},
		cas: []Observation{degraded, degraded},
	}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Now().UTC())
	if err != nil || !prepared.DegradedPredecessor || len(fake.healthTargets) != 1 || !fake.healthPrewrite[0] || len(fake.verifiedTargets) != 1 || fake.dryRuns != 2 {
		t.Fatalf("typed predecessor wait did not enter degraded selector: prepared=%+v health=%+v marked=%+v verified=%d dryRuns=%d err=%v",
			prepared, fake.healthTargets, fake.healthPrewrite, len(fake.verifiedTargets), fake.dryRuns, err)
	}
}

func TestPrepareFailedAtomSuccessorAdoptsOwnedManifestDriftFromHealthyLKG(t *testing.T) {
	plan := boundAPIPlan(t)
	plan.PlanDigest = ""
	plan.Releases[0].SupersedesFailedConfigSHA = strings.Repeat("f", 40)
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestOf(unsigned)
	plan, receipt, rendered, lkg, _ := executionFixtureForPlan(t, plan)
	owned := casOnlyObservation(lkg)
	owned.ImageRef = lkg.ImageRef
	owned.ConfigSHA = lkg.ConfigSHA
	owned.ManifestSHA = lkg.ManifestSHA
	owned.OCIRevision = lkg.OCIRevision
	owned.TemplateDigest = lkg.TemplateDigest
	owned.FieldManagers = append([]string(nil), lkg.FieldManagers...)
	fake := &fakeCluster{
		observations:    []Observation{lkg},
		health:          []Observation{lkg},
		degraded:        []Observation{owned, owned},
		convergedErrors: []error{errors.New("declared predecessor manifest has not converged"), nil},
	}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Now().UTC())
	if err != nil || !prepared.DegradedPredecessor || prepared.AlreadyConverged || prepared.Prewrite.ImageRef != lkg.ImageRef || fake.dryRuns != 2 {
		t.Fatalf("healthy owned predecessor drift was not adopted: prepared=%+v dryRuns=%d err=%v", prepared, fake.dryRuns, err)
	}
	if len(fake.healthTargets) != 1 || !fake.healthPrewrite[0] || len(fake.converged) != 2 || len(fake.degraded) != 0 {
		t.Fatalf("healthy owned predecessor evidence was incomplete: health=%+v marked=%+v converged=%d degraded_remaining=%d",
			fake.healthTargets, fake.healthPrewrite, len(fake.converged), len(fake.degraded))
	}
}

func TestPrepareFailedAtomSuccessorRecoversExactFailedWorkloadBeforeLKGObservation(t *testing.T) {
	plan := boundAPIPlan(t)
	plan.PlanDigest = ""
	failedSHA := strings.Repeat("f", 40)
	plan.Releases[0].SupersedesFailedConfigSHA = failedSHA
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestOf(unsigned)
	plan, receipt, rendered, lkg, _ := executionFixtureForPlan(t, plan)
	failed := lkg
	failed.ConfigSHA = failedSHA
	failed.ManifestSHA = failedSHA
	failed.OCIRevision = failedSHA
	failed.ImageRef = plan.Releases[0].Artifact.Repository + "@" + testDigest
	owned := casOnlyObservation(failed)
	owned.ImageRef = failed.ImageRef
	owned.ConfigSHA = failedSHA
	owned.ManifestSHA = failedSHA
	owned.OCIRevision = failedSHA
	owned.TemplateDigest = failed.TemplateDigest
	owned.FieldManagers = append([]string(nil), failed.FieldManagers...)
	fake := &fakeCluster{
		observations:    []Observation{failed},
		degraded:        []Observation{owned, owned},
		convergedErrors: []error{nil},
	}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Now().UTC())
	if err != nil || !prepared.DegradedPredecessor || prepared.AlreadyConverged || prepared.Prewrite.OCIRevision != failedSHA || fake.dryRuns != 2 {
		t.Fatalf("exact failed workload was not recovered: prepared=%+v dry_runs=%d err=%v", prepared, fake.dryRuns, err)
	}
	if len(fake.healthTargets) != 0 || len(fake.converged) != 1 || len(fake.degraded) != 0 {
		t.Fatalf("failed workload recovery used unexpected evidence: health=%+v converged=%d degraded_remaining=%d",
			fake.healthTargets, len(fake.converged), len(fake.degraded))
	}
}

func TestMatchesSupersededFailedAtomRejectsPartialIdentity(t *testing.T) {
	failedSHA := strings.Repeat("f", 40)
	release := PlanRelease{
		SupersedesFailedConfigSHA: failedSHA,
		Artifact:                  Artifact{Repository: "ghcr.io/example/fugue-api"},
	}
	exact := Observation{
		Present: true, ConfigSHA: failedSHA, ManifestSHA: failedSHA, OCIRevision: failedSHA,
		ImageRef: release.Artifact.Repository + "@" + testDigest,
	}
	if !exact.MatchesSupersededFailedAtom(release) {
		t.Fatal("exact superseded failed atom was rejected")
	}
	for name, mutate := range map[string]func(*Observation){
		"config":   func(value *Observation) { value.ConfigSHA = strings.Repeat("e", 40) },
		"manifest": func(value *Observation) { value.ManifestSHA = strings.Repeat("e", 40) },
		"revision": func(value *Observation) { value.OCIRevision = strings.Repeat("e", 40) },
		"tag":      func(value *Observation) { value.ImageRef = "ghcr.io/example/fugue-api:latest" },
		"repo":     func(value *Observation) { value.ImageRef = "ghcr.io/example/other@" + testDigest },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := exact
			mutate(&candidate)
			if candidate.MatchesSupersededFailedAtom(release) {
				t.Fatal("partial failed atom identity was accepted")
			}
		})
	}
}

func TestPrepareOrdinarySuccessorRejectsOwnedManifestDriftFromHealthyLKG(t *testing.T) {
	plan, receipt, rendered, lkg, _ := executionFixture(t)
	owned := casOnlyObservation(lkg)
	owned.ImageRef = lkg.ImageRef
	owned.ConfigSHA = lkg.ConfigSHA
	owned.ManifestSHA = lkg.ManifestSHA
	owned.OCIRevision = lkg.OCIRevision
	owned.TemplateDigest = lkg.TemplateDigest
	owned.FieldManagers = append([]string(nil), lkg.FieldManagers...)
	fake := &fakeCluster{
		observations:    []Observation{lkg},
		health:          []Observation{lkg},
		degraded:        []Observation{owned, owned},
		convergedErrors: []error{errors.New("declared predecessor manifest has not converged")},
	}
	if _, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Now().UTC()); err == nil || !strings.Contains(err.Error(), "has not converged") {
		t.Fatalf("ordinary successor accepted healthy predecessor manifest drift: %v", err)
	}
	if len(fake.degraded) != 2 || fake.dryRuns != 0 {
		t.Fatalf("ordinary successor entered owned recovery: degraded_remaining=%d dryRuns=%d", len(fake.degraded), fake.dryRuns)
	}
}

func TestPrepareFailedAtomSuccessorRejectsUntypedReadyCountMismatch(t *testing.T) {
	plan, receipt, rendered, lkg, _ := executionFixture(t)
	fake := &fakeCluster{
		observations: []Observation{lkg},
		healthErrors: []error{
			errors.New("ready workload pod count mismatch: got=0 want=1"),
		},
	}
	if _, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0)); err == nil || !strings.Contains(err.Error(), "ready workload pod count mismatch") {
		t.Fatalf("untyped predecessor error was accepted: %v", err)
	}
	if len(fake.verifiedTargets) != 0 || len(fake.casManifests) != 0 || fake.dryRuns != 0 {
		t.Fatalf("untyped predecessor error entered degraded recovery: verified=%d cas=%d dryRuns=%d", len(fake.verifiedTargets), len(fake.casManifests), fake.dryRuns)
	}
}

func TestExecuteDegradedPredecessorRejectsIndependentNonRetryWithoutFailedAtom(t *testing.T) {
	plan, receipt, rendered, lkg, _ := executionFixture(t)
	degraded := casOnlyObservation(lkg)
	fake := &fakeCluster{
		observations: []Observation{lkg},
		healthErrors: []error{
			fmt.Errorf("%w: ready workload pod count mismatch: got=0 want=1", ErrDegradedPredecessorHealth),
		},
		cas: []Observation{degraded, degraded},
	}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil || !prepared.DegradedPredecessor || plan.Releases[0].RetrySameLKG || plan.Releases[0].SupersedesFailedConfigSHA != "" {
		t.Fatalf("fixture is not an ordinary independent non-retry: plan=%+v prepared=%+v err=%v", plan.Releases[0], prepared, err)
	}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "recovery-required" || result.Reason != "execution-plan-invalid" || result.ForwardApplyCount != 0 || fake.applies != 0 {
		t.Fatalf("ordinary independent non-retry entered degraded execution: result=%+v applies=%d", result, fake.applies)
	}
}

func TestPrepareAndExecuteRecoverAnExactDegradedSameLKGPredecessor(t *testing.T) {
	plan, receipt, rendered, lkg, forward := retryExecutionFixture(t)
	degraded := casOnlyObservation(lkg)
	fake := &fakeCluster{cas: []Observation{degraded, degraded}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil || !prepared.DegradedPredecessor || prepared.AlreadyConverged || fake.dryRuns != 2 || len(fake.verifiedTargets) != 1 {
		t.Fatalf("prepare degraded predecessor: plan=%+v dryRuns=%d verified=%d err=%v", prepared, fake.dryRuns, len(fake.verifiedTargets), err)
	}
	if fake.verifiedTargets[0] != prepared.LKG || len(fake.converged) != 1 {
		t.Fatalf("degraded predecessor identity was not verified exactly: verified=%+v converged=%d", fake.verifiedTargets, len(fake.converged))
	}
	current := casOnlyObservation(lkg)
	current.ResourceVersion = "11"
	current.Resources[0].ResourceVersion = "11"
	fake.cas = []Observation{current}
	fake.health = []Observation{forward}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "verified" || result.Reason != "forward-verified" || result.ForwardApplyCount != 1 || fake.applies != 1 {
		t.Fatalf("degraded predecessor did not execute with a fresh CAS: result=%+v applies=%d", result, fake.applies)
	}
}

func TestPrepareAndExecuteRecoverAnOwnedDegradedPriorAttempt(t *testing.T) {
	plan, receipt, rendered, lkg, forward := retryExecutionFixture(t)
	legacyCAS := casOnlyObservation(lkg)
	prior := casOnlyObservation(lkg)
	prior.ImageRef = "ghcr.io/example/fugue-api@sha256:" + strings.Repeat("9", 64)
	prior.ConfigSHA = strings.Repeat("9", 40)
	prior.ManifestSHA = prior.ConfigSHA
	prior.OCIRevision = prior.ConfigSHA
	prior.TemplateDigest = "sha256:" + strings.Repeat("8", 64)
	prior.FieldManagers = []string{plan.Releases[0].Workload.FieldManager}
	fake := &fakeCluster{
		cas:             []Observation{legacyCAS},
		degraded:        []Observation{prior, prior},
		convergedErrors: []error{errors.New("live is not the declared LKG"), nil},
	}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil || !prepared.DegradedPredecessor || prepared.Prewrite.ImageRef != prior.ImageRef || fake.dryRuns != 2 {
		t.Fatalf("prepare owned degraded predecessor: plan=%+v dryRuns=%d err=%v", prepared, fake.dryRuns, err)
	}
	current := prior
	current.ResourceVersion = "11"
	current.Resources = append([]ResourceObservation(nil), prior.Resources...)
	current.Resources[0].ResourceVersion = "11"
	fake.degraded = []Observation{current}
	fake.convergedErrors = []error{nil}
	fake.health = []Observation{forward}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "verified" || result.Reason != "forward-verified" || result.ForwardApplyCount != 1 || fake.applies != 1 {
		t.Fatalf("owned degraded predecessor did not execute: result=%+v applies=%d", result, fake.applies)
	}
}

func TestPrepareOwnedDegradedPriorAttemptRejectsOwnershipAndSpecDrift(t *testing.T) {
	plan, receipt, rendered, lkg, _ := retryExecutionFixture(t)
	legacyCAS := casOnlyObservation(lkg)
	prior := casOnlyObservation(lkg)
	prior.ImageRef = "ghcr.io/example/fugue-api@sha256:" + strings.Repeat("9", 64)
	prior.ConfigSHA = strings.Repeat("9", 40)
	prior.ManifestSHA = prior.ConfigSHA
	prior.OCIRevision = prior.ConfigSHA
	prior.TemplateDigest = "sha256:" + strings.Repeat("8", 64)
	fake := &fakeCluster{
		cas:             []Observation{legacyCAS},
		degraded:        []Observation{prior},
		convergedErrors: []error{errors.New("live is not the declared LKG")},
	}
	if _, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0)); err == nil || !strings.Contains(err.Error(), "field manager") {
		t.Fatalf("unowned degraded predecessor was accepted: %v", err)
	}

	prior.FieldManagers = []string{plan.Releases[0].Workload.FieldManager}
	drift := prior
	drift.Resources = append([]ResourceObservation(nil), prior.Resources...)
	drift.Resources[0].ObjectDigest = "sha256:" + strings.Repeat("7", 64)
	fake = &fakeCluster{
		cas:             []Observation{legacyCAS},
		degraded:        []Observation{prior, drift},
		convergedErrors: []error{errors.New("live is not the declared LKG"), nil},
	}
	if _, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0)); err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("changing owned predecessor was accepted: %v", err)
	}
}

func TestPrepareDegradedPredecessorFailsClosedOnArtifactOrSpecDrift(t *testing.T) {
	plan, receipt, rendered, lkg, _ := retryExecutionFixture(t)
	degraded := casOnlyObservation(lkg)
	fake := &fakeCluster{verifyErrors: []error{errors.New("wrong OCI revision")}}
	if _, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0)); err == nil || !strings.Contains(err.Error(), "verify degraded predecessor artifact") || fake.dryRuns != 0 {
		t.Fatalf("unverified predecessor was accepted: dryRuns=%d err=%v", fake.dryRuns, err)
	}
	drift := degraded
	drift.Resources = append([]ResourceObservation(nil), degraded.Resources...)
	drift.Resources[0].ObjectDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	fake = &fakeCluster{cas: []Observation{degraded, drift}}
	if _, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0)); err == nil || !strings.Contains(err.Error(), "identity changed") || fake.dryRuns != 0 {
		t.Fatalf("changing predecessor spec was accepted: dryRuns=%d err=%v", fake.dryRuns, err)
	}
}

func TestExecuteDegradedPredecessorRejectsSpecDriftBeforeApply(t *testing.T) {
	plan, receipt, rendered, lkg, _ := retryExecutionFixture(t)
	degraded := casOnlyObservation(lkg)
	fake := &fakeCluster{cas: []Observation{degraded, degraded}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	drift := casOnlyObservation(lkg)
	drift.Resources[0].ObjectDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	fake.cas = []Observation{drift}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "failed-no-write" || result.Reason != "prewrite-cas-drift" || result.ForwardApplyCount != 0 || fake.applies != 0 {
		t.Fatalf("degraded predecessor drift reached apply: result=%+v applies=%d", result, fake.applies)
	}
}

func TestPrepareAndExecuteReconcileAForwardThatAlreadyConverged(t *testing.T) {
	plan, receipt, rendered, _, forward := executionFixture(t)
	fake := &fakeCluster{observations: []Observation{forward}, health: []Observation{forward}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil || !prepared.AlreadyConverged || fake.dryRuns != 2 {
		t.Fatalf("prepare converged target: plan=%+v dryRuns=%d err=%v", prepared, fake.dryRuns, err)
	}
	fake.observations = []Observation{forward}
	fake.health = []Observation{forward}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "verified" || result.Reason != "forward-already-converged" || result.ForwardApplyCount != 0 || result.LKGApplyCount != 0 || fake.applies != 0 {
		t.Fatalf("already-converged target was not reconciled read-only: %+v applies=%d", result, fake.applies)
	}
}

func TestReadOnlyExecutionReconcileVerifiesForwardWithoutMutation(t *testing.T) {
	plan, receipt, rendered, lkg, forward := executionFixture(t)
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	fake.observations = []Observation{forward}
	fake.health = []Observation{forward}
	result := ReconcileExecution(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "verified" || result.Reason != "forward-reconciled-after-executor-failure" || fake.applies != 0 {
		t.Fatalf("forward reconcile mutated or failed: %+v applies=%d", result, fake.applies)
	}
}

func TestReadOnlyExecutionReconcileReportsLKGWithoutRetryingForward(t *testing.T) {
	plan, receipt, rendered, lkg, _ := executionFixture(t)
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	fake.observations = []Observation{lkg}
	fake.cas = []Observation{lkg}
	fake.health = []Observation{lkg}
	result := ReconcileExecution(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "failed-no-write" || result.Reason != "executor-failure-no-write-observed" || fake.applies != 0 {
		t.Fatalf("LKG reconcile retried or misclassified: %+v applies=%d", result, fake.applies)
	}
}

func TestForwardOnlyResourcesBlockLKGTerminalReconcileUntilDeleted(t *testing.T) {
	identity := ResourceIdentity{APIVersion: "v1", Kind: "Service", Namespace: "fugue-system", Name: "api-tls"}
	before := Observation{Resources: []ResourceObservation{{Identity: identity}}}
	current := Observation{Resources: []ResourceObservation{{Identity: identity, Present: true, UID: "uid", ResourceVersion: "10"}}}
	if !forwardOnlyResourcesRemain(before, current) {
		t.Fatal("forward-only resource was ignored during LKG reconcile")
	}
	current.Resources[0].Present = false
	current.Resources[0].UID = ""
	current.Resources[0].ResourceVersion = ""
	if forwardOnlyResourcesRemain(before, current) {
		t.Fatal("absent forward-only resource blocked LKG reconcile")
	}
}

func TestExecuteCompensatesUnhealthyForwardExactlyOnce(t *testing.T) {
	plan, receipt, rendered, lkg, forward := executionFixture(t)
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	unhealthy := forward
	unhealthy.Ready = 1
	fake.observations = []Observation{lkg}
	fake.health = []Observation{unhealthy, lkg}
	fake.healthErrors = []error{errors.New("rollout unhealthy"), nil}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "compensated" || result.ForwardApplyCount != 1 || result.LKGApplyCount != 1 || fake.applies != 2 {
		t.Fatalf("unexpected compensation result: %+v applies=%d", result, fake.applies)
	}
}

func TestExecuteUsesCASOnlyObservationToCompensateAnUnhealthyObject(t *testing.T) {
	plan, receipt, rendered, lkg, forward := executionFixture(t)
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	fake.observations = []Observation{lkg}
	fake.health = []Observation{{}, lkg}
	fake.healthErrors = []error{errors.New("failed workload has no healthy pod"), nil}
	fake.cas = []Observation{casOnlyObservation(lkg), forward}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "compensated" || result.LKGApplyCount != 1 || fake.applies != 2 || fake.deleteCreated != 1 || len(fake.cas) != 0 {
		t.Fatalf("failed workload was not UID/RV-bound for compensation: %+v applies=%d", result, fake.applies)
	}
}

func TestExecuteFailsClosedOnPrewriteDriftWithoutApply(t *testing.T) {
	plan, receipt, rendered, lkg, _ := executionFixture(t)
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	drift := lkg
	drift.ResourceVersion = "12"
	fake.observations = []Observation{drift}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "failed-no-write" || result.Reason != "prewrite-cas-drift" || fake.applies != 0 {
		t.Fatalf("prewrite drift did not stop before write: %+v", result)
	}
}

func TestExecuteClassifiesPrewriteObservationFailureAsNoWrite(t *testing.T) {
	plan, receipt, rendered, lkg, _ := executionFixture(t)
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	fake.observationErrors = []error{errors.New("transient observation failure")}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "failed-no-write" || result.Reason != "prewrite-cas-drift" ||
		result.ForwardApplyCount != 0 || result.LKGApplyCount != 0 || fake.applies != 0 {
		t.Fatalf("prewrite observation failure retained a false recovery fence: result=%+v applies=%d", result, fake.applies)
	}
}

func TestExecuteRecapturesOnlyResourceCASBeforeFirstWrite(t *testing.T) {
	plan, receipt, rendered, lkg, forward := executionFixture(t)
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	fake.cas = []Observation{casOnlyObservation(lkg)}
	fake.observationErrors = []error{errors.New("full observation must not be repeated")}
	fake.health = []Observation{forward}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "verified" || result.Reason != "forward-verified" || fake.applies != 1 || len(fake.observationErrors) != 1 {
		t.Fatalf("execute repeated full prewrite observation: result=%+v applies=%d observationErrors=%d", result, fake.applies, len(fake.observationErrors))
	}
}

func TestExecuteRetainsRecoveryRequiredWhenLKGUnproven(t *testing.T) {
	plan, receipt, rendered, lkg, forward := executionFixture(t)
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	unhealthy := forward
	unhealthy.Ready = 0
	fake.observations = []Observation{lkg}
	fake.health = []Observation{unhealthy, unhealthy}
	fake.healthErrors = []error{errors.New("forward unhealthy"), errors.New("LKG unknown")}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "recovery-required" || result.Reason != "lkg-unproven" ||
		!strings.Contains(result.FailureDetail, "LKG health: LKG unknown") || fake.applies != 2 {
		t.Fatalf("unproven rollback was not retained: %+v", result)
	}
}

func TestFirstInstallCompensatesToUIDBoundAbsentLKG(t *testing.T) {
	registry := testRegistry()
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{"cmd/fugue-api/main.go", "deploy/releases/api/intent.json"})
	if err != nil {
		t.Fatal(err)
	}
	plan, err = BindIntents(registry, plan, map[string]Intent{"api": {
		APIVersion: IntentAPIVersion, Kind: IntentKind, Component: "api", Generation: 1,
		ExpectedPreviousPresent: false, Rollback: "previous-git-lkg",
	}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	verification := RegistryVerification{
		Image:       "ghcr.io/example/fugue-api@sha256:" + string(bytes.Repeat([]byte{'b'}, 64)),
		IndexDigest: "sha256:" + string(bytes.Repeat([]byte{'b'}, 64)), ManifestDigest: "sha256:" + string(bytes.Repeat([]byte{'c'}, 64)),
		ConfigDigest: "sha256:" + string(bytes.Repeat([]byte{'d'}, 64)), OCIRevision: testSHA2, Platform: "linux/amd64",
		Verification: "registry_manifest_config_and_layer_get", BlobCount: 2, LayerProbeCount: 1, RequestCount: 5, TotalLayerBytes: 10,
	}
	receipt, err := MaterializeArtifactReceipt(plan, "api", verification)
	if err != nil {
		t.Fatal(err)
	}
	base := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"replicas":2,"strategy":{"type":"RollingUpdate"},"template":{"metadata":{},"spec":{"containers":[{"image":"placeholder","name":"api"}]}}}}],"kind":"ComponentResourceSet"}`)
	rendered, err := RenderManifests(plan, "api", receipt, bytes.NewReader(base), nil)
	if err != nil {
		t.Fatal(err)
	}
	absent := Observation{
		Present:   false,
		Primary:   ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api"},
		Resources: []ResourceObservation{{Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api"}}},
	}
	fake := &fakeCluster{observations: []Observation{absent}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil || fake.dryRuns != 1 || prepared.LKG.Present {
		t.Fatalf("prepare first install: plan=%+v dryRuns=%d err=%v", prepared, fake.dryRuns, err)
	}
	if len(fake.observedManifests) != 1 || !bytes.Equal(fake.observedManifests[0], rendered.Forward) {
		t.Fatalf("first-install CAS did not use the forward resource schema: %d observations", len(fake.observedManifests))
	}
	forward := stableObservation("new-uid", "10", receipt.ImmutableRef, testSHA2)
	forward.Ready = 0
	fake.observations = []Observation{absent, absent}
	fake.health = []Observation{forward}
	fake.healthErrors = []error{errors.New("new component unhealthy")}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "compensated" || result.LKGApplyCount != 1 || result.Final.Present {
		t.Fatalf("first install did not return to absent LKG: %+v", result)
	}
}

func TestBindManifestCASPreservesDesiredSpec(t *testing.T) {
	manifest := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"replicas":2}}],"kind":"ComponentResourceSet"}`)
	observation := stableObservation("uid-1", "42", "ghcr.io/example/fugue-api@"+testDigest, testSHA1)
	bound, err := BindManifestCAS(manifest, observation)
	if err != nil || !bytes.Contains(bound, []byte(`"resourceVersion":"42"`)) || !bytes.Contains(bound, []byte(`"uid":"uid-1"`)) {
		t.Fatalf("resource-set CAS binding failed: %v %s", err, bound)
	}
	observation.ResourceVersion = "0"
	observation.Resources[0].ResourceVersion = "0"
	if _, err := BindManifestCAS(manifest, observation); err == nil {
		t.Fatal("invalid resourceVersion was accepted")
	}
}

func TestAbsentLKGAllowsOnlyExplicitRetainedPVC(t *testing.T) {
	primary := ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control"}
	observation := Observation{
		Present: false, Primary: primary,
		Resources: []ResourceObservation{
			{Identity: primary},
			{Identity: ResourceIdentity{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: "fugue-system", Name: "edge-control-state"},
				Present: true, RetainOnRollback: true, UID: "pvc-uid", ResourceVersion: "9", ObjectDigest: "sha256:" + string(bytes.Repeat([]byte{'a'}, 64)), FieldManagers: []string{"fugue-edge-control-declarative"}},
		},
	}
	if err := observation.ValidateMustBeStable(); err != nil || !observation.Matches(TargetIdentity{Present: false}, PlanRelease{}, true) {
		t.Fatalf("retained PVC did not preserve absent workload LKG: %v %+v", err, observation)
	}
	observation.Resources[1].RetainOnRollback = false
	if observation.Matches(TargetIdentity{Present: false}, PlanRelease{}, true) {
		t.Fatal("ordinary present resource was accepted as an absent LKG")
	}
}

func bytesReader(value []byte) *bytes.Reader { return bytes.NewReader(value) }
