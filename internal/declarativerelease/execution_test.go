package declarativerelease

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type fakeCluster struct {
	observations      []Observation
	observationErrors []error
	cas               []Observation
	verifiedTargets   []TargetIdentity
	verifyErrors      []error
	dryRuns           int
	applies           int
	applyErrors       []error
	deleteCreated     int
	health            []Observation
	healthErrors      []error
	healthTargets     []TargetIdentity
	converged         [][]byte
}

func (fake *fakeCluster) Observe(context.Context, PlanRelease, TargetIdentity, []byte) (Observation, error) {
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

func (fake *fakeCluster) ObserveCAS(context.Context, PlanRelease, []byte) (Observation, error) {
	if len(fake.cas) == 0 {
		return Observation{}, errors.New("no CAS observation")
	}
	value := fake.cas[0]
	fake.cas = fake.cas[1:]
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

func (fake *fakeCluster) WaitHealthy(_ context.Context, _ PlanRelease, target TargetIdentity, _ []byte) (Observation, error) {
	fake.healthTargets = append(fake.healthTargets, target)
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
	return nil
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
	plan.Releases[0].BootstrapLKGPath = "deploy/releases/api/lkg.json"
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
	if result.Reason != "prewrite-cas-drift" || result.ForwardApplyCount != 0 || fake.applies != 0 {
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
	fake.cas = []Observation{forward}
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
	if result.Reason != "prewrite-cas-drift" || fake.applies != 0 {
		t.Fatalf("prewrite drift did not stop before write: %+v", result)
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
	if result.Status != "recovery-required" || result.Reason != "lkg-unproven" || fake.applies != 2 {
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
