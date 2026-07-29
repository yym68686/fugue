package releasecontrol

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/componentmanifest"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestReconcileComponentPlanPersistsIdempotentLaneLocalShadowStatus(t *testing.T) {
	t.Parallel()

	stateStore, artifact := testValidatedComponentPlanArtifact(t)
	spec := ComponentPlanSpec{
		ArtifactID: artifact.ID, ContentHash: artifact.ContentHash, Generation: artifact.Generation,
	}
	principal := testReleaseControlPrincipal()
	first, err := ReconcileComponentPlan(context.Background(), stateStore, spec, principal)
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	second, err := ReconcileComponentPlan(context.Background(), stateStore, spec, principal)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("idempotent reconcile changed status:\n first=%+v\nsecond=%+v", first, second)
	}
	if err := VerifyComponentPlanStatus(first); err != nil {
		t.Fatalf("verify status: %v", err)
	}
	active, release, found, err := stateStore.GetActivePlatformArtifact(
		model.PlatformArtifactKindComponentReleasePlan,
		artifact.ScopeKey,
		model.PlatformArtifactReleaseChannelShadow,
	)
	if err != nil || !found || active.ID != artifact.ID || release.ID != first.ReleaseID {
		t.Fatalf("shadow status is not active: active=%+v release=%+v found=%v err=%v", active, release, found, err)
	}
	_, _, found, err = stateStore.GetActivePlatformArtifact(
		model.PlatformArtifactKindComponentReleasePlan,
		artifact.ScopeKey,
		model.PlatformArtifactReleaseChannelFull,
	)
	if err != nil || found {
		t.Fatalf("reconcile created a full release: found=%v err=%v", found, err)
	}
	messages, err := stateStore.ListPlatformReleaseMessages(
		model.PlatformArtifactKindComponentReleasePlan,
		artifact.ScopeKey,
		time.Time{},
		10,
	)
	if err != nil || len(messages) != 1 || messages[0].ReleaseID != first.ReleaseID {
		t.Fatalf("idempotent reconcile wrote duplicate messages: messages=%+v err=%v", messages, err)
	}
}

func TestReconcileComponentPlanConvergesConcurrentWorkersToOneRelease(t *testing.T) {
	t.Parallel()

	stateStore, artifact := testValidatedComponentPlanArtifact(t)
	spec := ComponentPlanSpec{ArtifactID: artifact.ID, ContentHash: artifact.ContentHash, Generation: artifact.Generation}
	const workers = 12
	start := make(chan struct{})
	statuses := make(chan ComponentPlanStatus, workers)
	errors := make(chan error, workers)
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			status, err := ReconcileComponentPlan(context.Background(), stateStore, spec, testReleaseControlPrincipal())
			statuses <- status
			errors <- err
		}()
	}
	close(start)
	group.Wait()
	close(statuses)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatalf("concurrent reconcile: %v", err)
		}
	}
	var expected ComponentPlanStatus
	for status := range statuses {
		if expected.ReleaseID == "" {
			expected = status
			continue
		}
		if !reflect.DeepEqual(status, expected) {
			t.Fatalf("concurrent workers diverged:\n got=%+v\nwant=%+v", status, expected)
		}
	}
	messages, err := stateStore.ListPlatformReleaseMessages(
		model.PlatformArtifactKindComponentReleasePlan,
		artifact.ScopeKey,
		time.Time{},
		workers+1,
	)
	if err != nil || len(messages) != 1 || messages[0].ReleaseID != expected.ReleaseID {
		t.Fatalf("concurrent reconcile did not converge to one message: messages=%+v err=%v", messages, err)
	}
}

func TestReconcileComponentPlanFailsClosedBeforePublishing(t *testing.T) {
	t.Parallel()

	stateStore, artifact := testValidatedComponentPlanArtifact(t)
	valid := ComponentPlanSpec{ArtifactID: artifact.ID, ContentHash: artifact.ContentHash, Generation: artifact.Generation}
	for name, test := range map[string]struct {
		mutate    func(*ComponentPlanSpec)
		principal model.Principal
	}{
		"artifact id": {
			mutate:    func(spec *ComponentPlanSpec) { spec.ArtifactID = "other-artifact" },
			principal: testReleaseControlPrincipal(),
		},
		"content hash": {
			mutate: func(spec *ComponentPlanSpec) {
				spec.ContentHash = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			},
			principal: testReleaseControlPrincipal(),
		},
		"generation": {
			mutate:    func(spec *ComponentPlanSpec) { spec.Generation = "git-3333333333333333333333333333333333333333" },
			principal: testReleaseControlPrincipal(),
		},
		"principal": {
			mutate:    func(*ComponentPlanSpec) {},
			principal: model.Principal{ActorType: model.ActorTypeSystem, ActorID: "unprivileged"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			spec := valid
			test.mutate(&spec)
			if _, err := ReconcileComponentPlan(context.Background(), stateStore, spec, test.principal); err == nil {
				t.Fatal("reconcile unexpectedly succeeded")
			}
		})
	}
	_, _, found, err := stateStore.GetActivePlatformArtifact(
		model.PlatformArtifactKindComponentReleasePlan,
		artifact.ScopeKey,
		model.PlatformArtifactReleaseChannelShadow,
	)
	if err != nil || found {
		t.Fatalf("failed reconcile published shadow state: found=%v err=%v", found, err)
	}
}

func TestReconcileComponentPlanRejectsCanceledContextAndCorruptStoreOutput(t *testing.T) {
	t.Parallel()

	stateStore, artifact := testValidatedComponentPlanArtifact(t)
	spec := ComponentPlanSpec{ArtifactID: artifact.ID, ContentHash: artifact.ContentHash, Generation: artifact.Generation}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReconcileComponentPlan(ctx, stateStore, spec, testReleaseControlPrincipal()); err == nil {
		t.Fatal("canceled reconcile unexpectedly succeeded")
	}

	corrupt := &corruptComponentPlanStore{artifact: artifact}
	if _, err := ReconcileComponentPlan(context.Background(), corrupt, spec, testReleaseControlPrincipal()); err == nil {
		t.Fatal("corrupt store output unexpectedly succeeded")
	}
	if corrupt.calls != 1 || corrupt.request.ReleaseChannel != model.PlatformArtifactReleaseChannelShadow ||
		corrupt.request.SoftOverride || corrupt.request.ForcePublish || corrupt.request.KernelBreakGlass != nil {
		t.Fatalf("reconciler constructed unsafe release request: calls=%d request=%+v", corrupt.calls, corrupt.request)
	}
}

func TestVerifyComponentPlanStatusDetectsMutation(t *testing.T) {
	t.Parallel()

	stateStore, artifact := testValidatedComponentPlanArtifact(t)
	status, err := ReconcileComponentPlan(
		context.Background(),
		stateStore,
		ComponentPlanSpec{ArtifactID: artifact.ID, ContentHash: artifact.ContentHash, Generation: artifact.Generation},
		testReleaseControlPrincipal(),
	)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for name, mutate := range map[string]func(*ComponentPlanStatus){
		"production mutation": func(status *ComponentPlanStatus) { status.ProductionMutationAllowed = true },
		"idempotency drift": func(status *ComponentPlanStatus) {
			status.IdempotencyKey = "component-shadow/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		},
		"scope drift": func(status *ComponentPlanStatus) { status.ScopeKey = "component-release-plan:other" },
	} {
		t.Run(name, func(t *testing.T) {
			mutated := status
			mutate(&mutated)
			mutated.Digest = DigestComponentPlanStatus(mutated)
			if err := VerifyComponentPlanStatus(mutated); err == nil {
				t.Fatal("rehashed malformed status unexpectedly passed")
			}
		})
	}
}

type corruptComponentPlanStore struct {
	artifact model.PlatformArtifact
	request  model.PlatformArtifactReleaseRequest
	calls    int
}

func (fake *corruptComponentPlanStore) GetPlatformArtifact(string) (model.PlatformArtifact, error) {
	return fake.artifact, nil
}

func (fake *corruptComponentPlanStore) ReleasePlatformArtifact(
	_ string,
	req model.PlatformArtifactReleaseRequest,
	_ model.Principal,
) (model.PlatformArtifact, model.PlatformArtifactRelease, model.PlatformReleaseMessage, *model.PlatformLKGSnapshot, error) {
	fake.calls++
	fake.request = req
	release := model.PlatformArtifactRelease{
		ID:             "release-corrupt",
		ArtifactID:     fake.artifact.ID,
		ArtifactKind:   fake.artifact.ArtifactKind,
		Scope:          fake.artifact.Scope,
		ScopeKey:       fake.artifact.ScopeKey,
		Generation:     fake.artifact.Generation,
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		Status:         model.PlatformArtifactReleaseStatusActive,
		LaneKey:        "corrupt",
		FencingToken:   1,
		Version:        1,
		IdempotencyKey: req.IdempotencyKey,
	}
	message := model.PlatformReleaseMessage{
		ReleaseID: release.ID, ArtifactID: fake.artifact.ID, ArtifactKind: fake.artifact.ArtifactKind,
		Scope: fake.artifact.Scope, ScopeKey: fake.artifact.ScopeKey, Generation: fake.artifact.Generation,
		ReleaseChannel: release.ReleaseChannel, MessageType: model.PlatformReleaseMessageTypeRelease,
	}
	return fake.artifact, release, message, nil, nil
}

func testValidatedComponentPlanArtifact(t *testing.T) (*store.Store, model.PlatformArtifact) {
	t.Helper()
	manifestFile, err := os.Open(filepath.Join("..", "..", "docs", "architecture", "component-ownership-v1.yaml"))
	if err != nil {
		t.Fatalf("open component ownership manifest: %v", err)
	}
	defer manifestFile.Close()
	manifest, err := componentmanifest.Load(manifestFile)
	if err != nil {
		t.Fatalf("load component ownership manifest: %v", err)
	}
	changePlan, err := componentmanifest.PlanChanges(manifest, []string{"internal/releasecontrol/component_plan_reconciler.go"})
	if err != nil {
		t.Fatalf("plan release-control change: %v", err)
	}
	coordinationPlan, err := componentmanifest.BuildShadowCoordinationPlan(changePlan)
	if err != nil {
		t.Fatalf("build coordination plan: %v", err)
	}
	envelope, err := componentmanifest.BuildShadowArtifactEnvelope(
		manifest,
		changePlan,
		coordinationPlan,
		"1111111111111111111111111111111111111111",
		"2222222222222222222222222222222222222222",
	)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	identity, err := envelope.ArtifactIdentity()
	if err != nil {
		t.Fatalf("derive artifact identity: %v", err)
	}
	content, err := envelope.Content()
	if err != nil {
		t.Fatalf("encode envelope: %v", err)
	}
	stateStore := store.New(filepath.Join(t.TempDir(), "state.json"))
	stateStore.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring(
		"release-control-test-signing-key",
		"release-control-test",
		"",
		"",
		nil,
	))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	artifact, err := stateStore.CreatePlatformArtifact(model.PlatformArtifact{
		ArtifactKind: model.PlatformArtifactKindComponentReleasePlan,
		Scope: model.PlatformArtifactScope{
			ScopeType: identity.ScopeType,
			Key:       identity.ScopeKey,
		},
		Generation:         identity.Generation,
		Content:            content,
		CompatibilityFloor: componentmanifest.ShadowArtifactSchemaVersionV1,
	})
	if err != nil {
		t.Fatalf("create artifact: %v", err)
	}
	artifact, err = stateStore.ValidatePlatformArtifact(artifact.ID, []model.PlatformArtifactValidationResult{{
		Name: "component.release-plan", Pass: true, Severity: model.RobustnessSeverityBlockPublish,
	}})
	if err != nil || artifact.Status != model.PlatformArtifactStatusValidated {
		t.Fatalf("validate artifact: artifact=%+v err=%v", artifact, err)
	}
	return stateStore, artifact
}

func testReleaseControlPrincipal() model.Principal {
	return model.Principal{
		ActorType: model.ActorTypeSystem,
		ActorID:   "release-control-shadow",
		Scopes:    map[string]struct{}{"platform.admin": {}},
	}
}

var _ ComponentPlanStore = (*store.Store)(nil)
var _ ComponentPlanStore = (*corruptComponentPlanStore)(nil)
