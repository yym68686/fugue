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
	casManifests      [][]byte
	degraded          []Observation
	verifiedTargets   []TargetIdentity
	verifyErrors      []error
	dryRuns           int
	dryRunAdoptions   int
	dryRunTakeovers   int
	adoptions         []Observation
	adoptionErrors    []error
	takeovers         []Observation
	takeoverErrors    []error
	applies           int
	applyErrors       []error
	deleteCreated     int
	health            []Observation
	healthErrors      []error
	healthTargets     []TargetIdentity
	converged         [][]byte
	convergedErrors   []error
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

func (fake *fakeCluster) VerifyBootstrapTarget(_ context.Context, _ PlanRelease, target TargetIdentity) error {
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

func (fake *fakeCluster) DryRunOwnershipAdoption(context.Context, PlanRelease, OwnershipAdoptionPlan, []byte) error {
	fake.dryRunAdoptions++
	return nil
}

func (fake *fakeCluster) DryRunOwnershipTakeover(context.Context, PlanRelease, OwnershipAdoptionPlan, TargetIdentity, []byte) error {
	fake.dryRunTakeovers++
	return nil
}

func (fake *fakeCluster) AdoptOwnership(context.Context, PlanRelease, OwnershipAdoptionPlan, TargetIdentity, []byte) (Observation, error) {
	var observation Observation
	if len(fake.adoptions) > 0 {
		observation = fake.adoptions[0]
		fake.adoptions = fake.adoptions[1:]
	}
	if len(fake.adoptionErrors) == 0 {
		return observation, nil
	}
	err := fake.adoptionErrors[0]
	fake.adoptionErrors = fake.adoptionErrors[1:]
	return observation, err
}

func (fake *fakeCluster) TakeoverOwnership(context.Context, PlanRelease, OwnershipAdoptionPlan, TargetIdentity, []byte) (Observation, error) {
	var observation Observation
	if len(fake.takeovers) > 0 {
		observation = fake.takeovers[0]
		fake.takeovers = fake.takeovers[1:]
	}
	if len(fake.takeoverErrors) == 0 {
		return observation, nil
	}
	err := fake.takeoverErrors[0]
	fake.takeoverErrors = fake.takeoverErrors[1:]
	return observation, err
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
	if len(fake.convergedErrors) > 0 {
		err := fake.convergedErrors[0]
		fake.convergedErrors = fake.convergedErrors[1:]
		return err
	}
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
	lkgInput := base
	release := plan.Releases[0]
	if release.MigrationState == "adopting" && release.OwnershipAdoption != nil && release.HeterogeneousBootstrapLKG {
		set, decodeErr := DecodeResourceSet(bytes.NewReader(base))
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if patchErr := patchResourceSet(&set, release, release.Artifact.Repository+"@"+release.ExpectedPreviousImageDigest,
			release.ExpectedPreviousConfigSHA, release.ExpectedPreviousManifestSHA, release.ExpectedPreviousOCIRevision,
			plan.PlanDigest, receipt.ReceiptDigest); patchErr != nil {
			t.Fatal(patchErr)
		}
		lkgInput, err = CanonicalJSON(set)
		if err != nil {
			t.Fatal(err)
		}
	}
	rendered, err := RenderManifests(plan, "api", receipt, bytesReader(base), bytesReader(lkgInput))
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

func TestPrepareBindsExplicitOwnershipAdoptionToLKGAndLiveCAS(t *testing.T) {
	plan := boundAPIPlan(t)
	plan.PlanDigest = ""
	release := &plan.Releases[0]
	release.MigrationState = "adopting"
	release.BootstrapLKGPath = "deploy/releases/api/lkg.json"
	release.OwnershipAdoption = &OwnershipAdoption{
		LegacyFieldManager: "helm",
		Resources: []OwnershipAdoptionScope{{
			Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api"},
			Fields:   []string{"/spec/template/metadata/annotations/fugue.pro~1source-commit", "/spec/template/spec/containers/name=api/image"},
		}},
	}
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestOf(unsigned)
	plan, receipt, rendered, lkg, _ := executionFixtureForPlan(t, plan)
	rendered.LKG = bytes.ReplaceAll(rendered.LKG, []byte("ghcr.io/example/fugue-api:old"), []byte(lkg.ImageRef))
	rendered.LKGDigest = digestOf(rendered.LKG)
	if _, err := BootstrapPredecessorConvergenceManifest(rendered.LKG, *release); err != nil {
		set, decodeErr := DecodeResourceSet(bytes.NewReader(rendered.LKG))
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		primary, primaryErr := set.Primary(release.Workload)
		if primaryErr != nil {
			t.Fatal(primaryErr)
		}
		image, _ := workloadContainerImage(primary, release.Workload.Container, "container")
		t.Fatalf("bootstrap retry fixture identity: image=%q expected=%q err=%v", image, release.Artifact.Repository+"@"+release.ExpectedPreviousImageDigest, err)
	}
	lkg.FieldManagers = []string{"helm"}
	lkg.Resources[0].FieldManagers = []string{"helm"}
	probeAugmented := lkg
	probeAugmented.HealthDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{probeAugmented}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if fake.dryRunAdoptions != 1 || fake.dryRuns != 0 {
		t.Fatalf("adoption prepare used the ordinary force scope: adoption=%d ordinary=%d", fake.dryRunAdoptions, fake.dryRuns)
	}
	adoption := prepared.OwnershipAdoption
	if adoption == nil || adoption.Component != "api" || adoption.UID != lkg.UID ||
		adoption.ResourceVersion != lkg.ResourceVersion || adoption.Generation != lkg.Generation ||
		adoption.LegacyFieldManager != "helm" || adoption.BootstrapLKGDigest != prepared.LKG.ManifestDigest ||
		adoption.ImageRef != prepared.LKG.ImageRef || adoption.ConfigSHA != prepared.LKG.ConfigSHA ||
		len(adoption.Resources) != 1 || len(adoption.Resources[0].Fields) != 2 {
		t.Fatalf("ownership adoption was not exactly bound: %+v", adoption)
	}
	prepared.OwnershipAdoption.ResourceVersion = "999"
	prepared.PlanDigest = ""
	tampered, err := CanonicalJSON(prepared)
	if err != nil {
		t.Fatal(err)
	}
	prepared.PlanDigest = digestOf(tampered)
	if err := prepared.Validate(plan, rendered.Forward, rendered.LKG); err == nil || !strings.Contains(err.Error(), "ownership adoption identity") {
		t.Fatalf("ownership adoption CAS drift was accepted: %v", err)
	}
}

func TestBindOwnershipAdoptionRecognizesAnExactExistingDeclarativeManager(t *testing.T) {
	plan := boundAPIPlan(t)
	release := plan.Releases[0]
	release.MigrationState = "adopting"
	release.BootstrapLKGPath = "deploy/releases/api/lkg.json"
	release.OwnershipAdoption = &OwnershipAdoption{
		LegacyFieldManager: "helm",
		Resources: []OwnershipAdoptionScope{{
			Identity: ResourceIdentity{APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind, Namespace: release.Workload.Namespace, Name: release.Workload.Name},
			Fields:   []string{"/spec/template"},
		}},
	}
	lkg := TargetIdentity{
		Present: true, ImageRef: release.Artifact.Repository + "@" + release.ExpectedPreviousImageDigest,
		ConfigSHA: release.ExpectedPreviousConfigSHA, ManifestSHA: release.ExpectedPreviousManifestSHA,
		OCIRevision: release.ExpectedPreviousOCIRevision, ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	prewrite := stableObservation("uid", "10", lkg.ImageRef, lkg.ConfigSHA)
	prewrite.FieldManagers = []string{release.Workload.FieldManager, "helm"}
	prewrite.Resources[0].FieldManagers = []string{release.Workload.FieldManager, "helm"}
	adoption, err := bindOwnershipAdoption(release, lkg, prewrite)
	if err != nil || adoption == nil || !adoption.AlreadyConverged {
		t.Fatalf("exact existing declarative ownership was not resumable: adoption=%+v err=%v", adoption, err)
	}
	prewrite.Resources[0].FieldManagers = []string{"helm"}
	if _, err := bindOwnershipAdoption(release, lkg, prewrite); err == nil {
		t.Fatal("partially adopted ownership was accepted")
	}
}

func TestPrepareAlreadyAdoptedOwnershipDryRunsForwardWithoutForce(t *testing.T) {
	plan := boundAPIPlan(t)
	plan.PlanDigest = ""
	release := &plan.Releases[0]
	release.MigrationState = "adopting"
	release.BootstrapLKGPath = "deploy/releases/api/lkg.json"
	release.OwnershipAdoption = &OwnershipAdoption{
		LegacyFieldManager: "helm",
		Resources: []OwnershipAdoptionScope{{
			Identity: ResourceIdentity{APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind, Namespace: release.Workload.Namespace, Name: release.Workload.Name},
			Fields:   []string{"/spec/template"},
		}},
	}
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestOf(unsigned)
	plan, receipt, rendered, lkg, _ := executionFixtureForPlan(t, plan)
	lkg.FieldManagers = []string{release.Workload.FieldManager, "helm"}
	lkg.Resources[0].FieldManagers = []string{release.Workload.FieldManager, "helm"}
	fake := &fakeCluster{
		observations: []Observation{lkg, lkg}, health: []Observation{lkg},
		cas: []Observation{casOnlyObservation(lkg)},
	}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil || prepared.OwnershipAdoption == nil || !prepared.OwnershipAdoption.AlreadyConverged ||
		fake.dryRunAdoptions != 0 || fake.dryRunTakeovers != 1 || fake.dryRuns != 1 {
		t.Fatalf("already-adopted prepare did not use bounded ordinary dry-runs: plan=%+v fake=%+v err=%v", prepared.OwnershipAdoption, fake, err)
	}
}

func TestExecuteAlreadyAdoptedOwnershipPerformsReviewedTakeoverBeforeForward(t *testing.T) {
	plan := boundAPIPlan(t)
	plan.PlanDigest = ""
	release := &plan.Releases[0]
	release.MigrationState = "adopting"
	release.BootstrapLKGPath = "deploy/releases/api/lkg.json"
	release.OwnershipAdoption = &OwnershipAdoption{
		LegacyFieldManager: "helm",
		Resources: []OwnershipAdoptionScope{{
			Identity: ResourceIdentity{APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind, Namespace: release.Workload.Namespace, Name: release.Workload.Name},
			Fields:   []string{"/spec/template"},
		}},
	}
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestOf(unsigned)
	plan, receipt, rendered, lkg, forward := executionFixtureForPlan(t, plan)
	lkg.FieldManagers = []string{release.Workload.FieldManager, "helm"}
	lkg.Resources[0].FieldManagers = []string{release.Workload.FieldManager, "helm"}
	fake := &fakeCluster{
		observations: []Observation{lkg, lkg}, health: []Observation{lkg},
		cas: []Observation{casOnlyObservation(lkg)},
	}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil || prepared.OwnershipAdoption == nil || !prepared.OwnershipAdoption.AlreadyConverged {
		t.Fatalf("prepare resumed adoption: plan=%+v err=%v", prepared.OwnershipAdoption, err)
	}
	takeover := casOnlyObservation(lkg)
	takeover.ResourceVersion = "12"
	takeover.Generation++
	takeover.Resources[0].ResourceVersion = "12"
	takeover.Resources[0].Generation++
	takeover.Resources[0].ObjectDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	takeover.Resources[0].FieldManagers = []string{release.Workload.FieldManager, "helm"}
	fake.cas = []Observation{casOnlyObservation(lkg)}
	fake.takeovers = []Observation{takeover}
	fake.health = []Observation{forward}
	fake.dryRuns = 0
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "verified" || result.Reason != "forward-verified" || fake.dryRuns != 1 || fake.applies != 1 || len(fake.takeovers) != 0 {
		t.Fatalf("reviewed ownership takeover did not precede ordinary forward: result=%+v fake=%+v", result, fake)
	}
}

func TestExecuteCompensatesAnUnverifiedOwnershipTakeover(t *testing.T) {
	plan := boundAPIPlan(t)
	plan.PlanDigest = ""
	release := &plan.Releases[0]
	release.MigrationState = "adopting"
	release.BootstrapLKGPath = "deploy/releases/api/lkg.json"
	release.OwnershipAdoption = &OwnershipAdoption{LegacyFieldManager: "helm", Resources: []OwnershipAdoptionScope{{
		Identity: ResourceIdentity{APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind, Namespace: release.Workload.Namespace, Name: release.Workload.Name},
		Fields:   []string{"/spec/template"},
	}}}
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestOf(unsigned)
	plan, receipt, rendered, lkg, _ := executionFixtureForPlan(t, plan)
	lkg.FieldManagers = []string{release.Workload.FieldManager, "helm"}
	lkg.Resources[0].FieldManagers = []string{release.Workload.FieldManager, "helm"}
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}, cas: []Observation{casOnlyObservation(lkg)}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil || prepared.OwnershipAdoption == nil || !prepared.OwnershipAdoption.AlreadyConverged {
		t.Fatalf("prepare resumed adoption: plan=%+v err=%v", prepared.OwnershipAdoption, err)
	}
	partial := casOnlyObservation(lkg)
	partial.ResourceVersion = "12"
	partial.Generation++
	partial.Resources = append([]ResourceObservation(nil), lkg.Resources...)
	partial.Resources[0].ResourceVersion = "12"
	partial.Resources[0].Generation++
	partial.Resources[0].ObjectDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	partial.Resources[0].FieldManagers = []string{release.Workload.FieldManager, "helm"}
	fake.cas = []Observation{casOnlyObservation(lkg)}
	fake.takeovers = []Observation{partial}
	fake.takeoverErrors = []error{errors.New("post-write takeover verification failed")}
	fake.health = []Observation{lkg}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "compensated" || result.Reason != "ownership-takeover-lkg-restored" || result.ForwardApplyCount != 0 ||
		result.LKGApplyCount != 1 || fake.applies != 1 || fake.deleteCreated != 1 {
		t.Fatalf("unverified takeover did not restore LKG: result=%+v fake=%+v", result, fake)
	}
}

func TestExecuteAdoptsReviewedLKGFieldsBeforeOrdinaryForwardApply(t *testing.T) {
	plan := boundAPIPlan(t)
	plan.PlanDigest = ""
	release := &plan.Releases[0]
	release.MigrationState = "adopting"
	release.BootstrapLKGPath = "deploy/releases/api/lkg.json"
	release.OwnershipAdoption = &OwnershipAdoption{
		LegacyFieldManager: "helm",
		Resources: []OwnershipAdoptionScope{{
			Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api"},
			Fields:   []string{"/spec/template"},
		}},
	}
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestOf(unsigned)
	plan, receipt, rendered, lkg, forward := executionFixtureForPlan(t, plan)
	lkg.FieldManagers = []string{"helm"}
	lkg.Resources[0].FieldManagers = []string{"helm"}
	fake := &fakeCluster{observations: []Observation{lkg, lkg}, health: []Observation{lkg}}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	adopted := lkg
	adopted.ResourceVersion = "11"
	adopted.FieldManagers = []string{"fugue-api-declarative", "helm"}
	adopted.Resources = append([]ResourceObservation(nil), lkg.Resources...)
	adopted.Resources[0].ResourceVersion = "11"
	adopted.Resources[0].FieldManagers = []string{"fugue-api-declarative", "helm"}
	fake.observations = []Observation{lkg}
	fake.adoptions = []Observation{adopted}
	fake.cas = []Observation{casOnlyObservation(lkg), casOnlyObservation(adopted)}
	fake.health = []Observation{forward}
	result := Execute(context.Background(), fake, plan, prepared, rendered.Forward, rendered.LKG)
	if result.Status != "verified" || result.Reason != "forward-verified" || fake.dryRuns != 1 || fake.applies != 1 ||
		len(fake.casManifests) != 2 || !bytes.Equal(fake.casManifests[0], rendered.LKG) || !bytes.Equal(fake.casManifests[1], rendered.Forward) {
		t.Fatalf("adoption did not converge through ordinary apply: result=%+v dryRuns=%d applies=%d", result, fake.dryRuns, fake.applies)
	}
}

func TestForwardResourceCASExtensionAllowsOnlyAbsentNewResources(t *testing.T) {
	base := casOnlyObservation(stableObservation("1", "10", "ghcr.io/example/api@"+testDigest, testSHA1))
	extra := ResourceObservation{Identity: ResourceIdentity{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "fugue-system", Name: "api"}}
	expanded := base
	expanded.Resources = append(append([]ResourceObservation(nil), base.Resources...), extra)
	if !expanded.ExtendsResourceCAS(base) {
		t.Fatal("absent forward-only resource was rejected")
	}
	expanded.Resources[len(expanded.Resources)-1] = ResourceObservation{
		Identity: extra.Identity, Present: true, UID: "new", ResourceVersion: "11", Generation: 0,
		ObjectDigest:  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		FieldManagers: []string{"other"},
	}
	if expanded.ExtendsResourceCAS(base) {
		t.Fatal("pre-existing forward-only resource was accepted")
	}
}

func TestOwnershipTakeoverCASAllowsOnlyUIDPreservingScopedMovement(t *testing.T) {
	base := casOnlyObservation(stableObservation("1", "10", "ghcr.io/example/api@"+testDigest, testSHA1))
	manager := "fugue-api-declarative"
	plan := OwnershipAdoptionPlan{Resources: []OwnershipAdoptionResourcePlan{{Identity: base.Resources[0].Identity}}}
	current := base
	current.ResourceVersion = "11"
	current.Generation++
	current.Resources = append([]ResourceObservation(nil), base.Resources...)
	current.Resources[0].ResourceVersion = "11"
	current.Resources[0].Generation++
	current.Resources[0].ObjectDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	current.Resources[0].FieldManagers = []string{manager, "helm"}
	current.Resources = append(current.Resources, ResourceObservation{Identity: ResourceIdentity{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "fugue-system", Name: "api"}})
	if !current.CompletesOwnershipTakeover(base, manager, plan) {
		t.Fatal("UID-preserving reviewed takeover was rejected")
	}
	current.Resources[0].UID = "replaced"
	if current.CompletesOwnershipTakeover(base, manager, plan) {
		t.Fatal("resource replacement was accepted as ownership takeover")
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

func TestBootstrapArtifactCompatibilityRequiresExplicitAdoptionAndExactLKG(t *testing.T) {
	release := PlanRelease{
		MigrationState: "adopting", RetrySameLKG: true, HeterogeneousBootstrapLKG: true,
		BootstrapLKGPath: "deploy/releases/edge-worker-de/lkg.json", ExpectedPreviousPresent: true,
		ExpectedPreviousConfigSHA: testSHA1, ExpectedPreviousManifestSHA: testSHA1, ExpectedPreviousOCIRevision: testSHA1,
		ExpectedPreviousImageDigest: testDigest, OwnershipAdoption: &OwnershipAdoption{LegacyFieldManager: "helm"},
	}
	lkg := TargetIdentity{Present: true, ImageRef: "ghcr.io/example/fugue-edge@" + testDigest, ConfigSHA: testSHA1, ManifestSHA: testSHA1, OCIRevision: testSHA1}
	if !allowsBootstrapArtifactVerification(release, lkg) {
		t.Fatal("exact explicit adoption bootstrap was rejected")
	}
	for name, mutate := range map[string]func(*PlanRelease, *TargetIdentity){
		"independent":    func(value *PlanRelease, _ *TargetIdentity) { value.MigrationState = "independent" },
		"no ownership":   func(value *PlanRelease, _ *TargetIdentity) { value.OwnershipAdoption = nil },
		"ordinary retry": func(value *PlanRelease, _ *TargetIdentity) { value.RetrySameLKG = false },
		"wrong digest": func(_ *PlanRelease, target *TargetIdentity) {
			target.ImageRef = "ghcr.io/example/fugue-edge@sha256:" + strings.Repeat("c", 64)
		},
	} {
		copyRelease, copyLKG := release, lkg
		mutate(&copyRelease, &copyLKG)
		if allowsBootstrapArtifactVerification(copyRelease, copyLKG) {
			t.Fatalf("%s gained bootstrap artifact compatibility", name)
		}
	}
}

func TestPrepareBootstrapRetryUsesHealthyLegacyObservationInsteadOfOwnedDegraded(t *testing.T) {
	plan := boundAPIPlan(t)
	plan.PlanDigest = ""
	release := &plan.Releases[0]
	release.IntentGeneration = 2
	release.RetrySameLKG = true
	release.MigrationState = "adopting"
	release.HeterogeneousBootstrapLKG = true
	release.BootstrapLKGPath = "deploy/releases/api/lkg.json"
	release.OwnershipAdoption = &OwnershipAdoption{LegacyFieldManager: "helm", Resources: []OwnershipAdoptionScope{{
		Identity: ResourceIdentity{APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind, Namespace: release.Workload.Namespace, Name: release.Workload.Name},
		Fields:   []string{"/spec/template"},
	}}}
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanDigest = digestOf(unsigned)
	plan, receipt, rendered, lkg, _ := executionFixtureForPlan(t, plan)
	lkg.FieldManagers = []string{"helm"}
	lkg.Resources[0].FieldManagers = []string{"helm"}
	fake := &fakeCluster{
		observationErrors: []error{errors.New("forward absent")},
		observations:      []Observation{lkg, lkg},
		health:            []Observation{lkg},
	}
	prepared, err := PrepareExecution(context.Background(), fake, plan, "api", receipt, rendered, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if prepared.OwnershipAdoption == nil || len(fake.degraded) != 0 || len(fake.cas) != 0 || len(fake.verifiedTargets) != 1 || fake.dryRunAdoptions != 1 {
		t.Fatalf("bootstrap retry did not remain in the explicit adoption path: prepared=%+v fake=%+v", prepared.OwnershipAdoption, fake)
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
