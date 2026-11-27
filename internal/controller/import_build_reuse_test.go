package controller

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

func TestGitHubSyncReusesVerifiedBuildAfterDeployAdmissionFailure(t *testing.T) {
	for _, scenario := range []string{"verified", "billing-restored", "missing-blob", "digest-changed", "inputs-changed"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			state := newStoreTestDatabase(t, "")
			if err := state.Init(); err != nil {
				t.Fatal(err)
			}
			tenant, err := state.CreateTenant("Tenant")
			if err != nil {
				t.Fatal(err)
			}
			project, err := state.CreateProject(tenant.ID, "service", "")
			if err != nil {
				t.Fatal(err)
			}
			source := model.AppSource{Type: model.AppSourceTypeGitHubPublic, RepoURL: "https://github.com/example/service", RepoBranch: "main", CommitSHA: "previous", BuildStrategy: model.AppBuildStrategyDockerfile}
			app, err := state.CreateImportedApp(tenant.ID, project.ID, "service", "", model.AppSpec{Image: "registry.example/service:previous", Replicas: 1, RuntimeID: model.DefaultManagedRuntimeID, Env: map[string]string{"BUILD_MODE": "release"}}, source, model.AppRoute{})
			if err != nil {
				t.Fatal(err)
			}
			svc := &Service{Store: state, Config: config.ControllerConfig{GitHubSyncTimeout: time.Second, ImageStoreMode: "distributed"}, Logger: log.New(io.Discard, "", 0), latestGitHubCommit: func(context.Context, string, string, string) (string, string, error) { return "next", "main", nil }}
			digest := "sha256:" + strings.Repeat("a", 64)
			verified := func(_ context.Context, endpoint, ref string) (destinationImageCacheVerification, error) {
				repo, target, ok := managedImageRepoTarget(ref)
				if !ok {
					t.Fatal("invalid ref", ref)
				}
				return destinationImageCacheVerification{Repo: repo, Target: target, Available: true, CanonicalDigest: digest, ReferencedBlobs: []string{"sha256:" + strings.Repeat("b", 64)}, ReferencedBlobBytes: 128}, nil
			}
			svc.verifyDestinationImageCache = verified
			source.CommitSHA = "next"
			originalSpec := cloneImportSpec(app.Spec)
			fingerprint := importBuildReuseFingerprint(source, originalSpec, svc.effectiveImportBuilderPolicy(svc.importBuilderMemoryCeilingBytes(tenant.ID)))
			var saved model.Operation
			for i := 0; i < 3; i++ {
				op, err := state.CreateOperation(model.Operation{TenantID: tenant.ID, AppID: app.ID, Type: model.OperationTypeImport, RequestedByType: model.ActorTypeBootstrap, RequestedByID: model.OperationRequestedByGitHubSyncController, DesiredSource: &source, DesiredSpec: &originalSpec})
				if err != nil {
					t.Fatal(err)
				}
				if i == 2 {
					finalSpec := cloneImportSpec(originalSpec)
					finalSpec.Image = "registry.example/service:build"
					if scenario == "billing-restored" {
						resources := model.DefaultManagedAppResources()
						finalSpec.Resources = &resources
					}
					finalSource := source
					finalSource.ResolvedImageRef = finalSpec.Image
					op, err = state.RecordImportBuildResult(op, finalSpec, finalSource, &finalSource)
					if err != nil {
						t.Fatal(err)
					}
					saved = op
					if err := svc.saveImportBuildLocator(ctx, app, op, fingerprint, importImageDestination{CacheEndpoint: "http://192.0.2.10:5000"}); err != nil {
						t.Fatal(err)
					}
				}
				if _, err = state.FailOperation(op.ID, "queue deploy after import: billing balance depleted"); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "billing-restored" {
				if _, err := state.SetTenantBillingBalance(tenant.ID, 0, map[string]string{"note": "fixture depleted"}); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					if err := svc.syncGitHubApps(ctx); err != nil {
						t.Fatal(err)
					}
				}
				ops, _ := state.ListOperations("", true)
				if len(ops) != 3 {
					t.Fatal("unfunded release built again", len(ops))
				}
				if _, err := state.TopUpTenantBilling(tenant.ID, 1000, "fund saved release"); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "missing-blob":
				svc.verifyDestinationImageCache = func(context.Context, string, string) (destinationImageCacheVerification, error) {
					return destinationImageCacheVerification{}, errors.New("blob unavailable")
				}
			case "digest-changed":
				digest = "sha256:" + strings.Repeat("c", 64)
			case "inputs-changed":
				fingerprint = importBuildReuseFingerprint(source, model.AppSpec{Env: map[string]string{"BUILD_MODE": "debug"}}, sourceimport.BuilderPodPolicy{})
				reused, err := svc.reusableGitHubBuild(ctx, app, fingerprint)
				if err != nil || reused != nil {
					t.Fatal("reused changed input", reused, err)
				}
				return
			}
			if err := svc.syncGitHubApps(ctx); err != nil {
				t.Fatal(err)
			}
			ops, err := state.ListOperations("", true)
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "verified" && scenario != "billing-restored" {
				if len(ops) != 3 {
					t.Fatal("failed verification queued work", len(ops))
				}
				return
			}
			if len(ops) != 4 {
				t.Fatal("saved build did not resume", len(ops))
			}
			queued := ops[3]
			if queued.Type != model.OperationTypeDeploy || !strings.HasSuffix(queued.DesiredSpec.Image, "@"+digest) || queued.DesiredOriginSource.CommitSHA != "next" {
				t.Fatal("rebuild or wrong release queued", queued)
			}
			if err := svc.syncGitHubApps(ctx); err != nil {
				t.Fatal(err)
			}
			ops, _ = state.ListOperations("", true)
			if len(ops) != 4 {
				t.Fatal("duplicate actionable release")
			}
			evidence, err := state.ListOperationEvidence(model.OperationEvidenceFilter{OperationID: saved.ID, PlatformAdmin: true, Types: []string{"build_storage_footprint"}})
			if err != nil || len(evidence) != 1 {
				t.Fatal("missing graph footprint", len(evidence), err)
			}
		})
	}
}

func TestGitHubRetryTracksOriginAndIgnoresSystemResets(t *testing.T) {
	origin := &model.AppSource{Type: model.AppSourceTypeGitHubPublic, CommitSHA: "commit"}
	ops := []model.Operation{}
	for i := 0; i < 3; i++ {
		ops = append(ops, model.Operation{AppID: "app", Type: model.OperationTypeDeploy, Status: model.OperationStatusFailed, RequestedByID: model.OperationRequestedByGitHubSyncController, DesiredSource: &model.AppSource{Type: model.AppSourceTypeDockerImage}, DesiredOriginSource: origin})
	}
	ops = append(ops, model.Operation{AppID: "app", Type: model.OperationTypeImport, Status: model.OperationStatusCompleted, RequestedByType: model.ActorTypeSystem, RequestedByID: "fugue-controller/reconcile", DesiredOriginSource: origin})
	state := collectTrackedGitHubCommitStates(ops)[trackedGitHubCommitStateKey("app", "commit")]
	if state.consecutiveAutomaticFailures != 3 {
		t.Fatal("system operation erased retry budget", state)
	}
	ops = append(ops, model.Operation{AppID: "app", Type: model.OperationTypeImport, RequestedByType: model.ActorTypeAPIKey, RequestedByID: "user-key", DesiredOriginSource: origin})
	if got := collectTrackedGitHubCommitStates(ops)[trackedGitHubCommitStateKey("app", "commit")]; got.consecutiveAutomaticFailures != 0 {
		t.Fatal("manual intent did not reset budget", got)
	}
}

func TestBuildReuseFingerprintIncludesBuildSelection(t *testing.T) {
	source := model.AppSource{CommitSHA: "commit"}
	spec := model.AppSpec{Env: map[string]string{"SECRET": "private"}}
	policy := sourceimport.BuilderPodPolicy{}
	first := importBuildReuseFingerprint(source, spec, policy)
	for _, change := range []string{"suffix", "dependencies", "environment"} {
		next := model.CloneAppSource(&source)
		config := cloneImportSpec(spec)
		switch change {
		case "suffix":
			next.ImageNameSuffix = "worker"
		case "dependencies":
			next.ComposeDependsOn = []string{"database"}
		case "environment":
			config.Env["SECRET"] = "changed"
		}
		if importBuildReuseFingerprint(*next, config, policy) == first {
			t.Fatal("changed build selection reused", change)
		}
	}
	if strings.Contains(first, "private") {
		t.Fatal("secret escaped hash")
	}
}

func TestBuildResultHashBindsConfigurationAndIgnoresProgress(t *testing.T) {
	spec := model.AppSpec{Image: "registry.example/service:build", Env: map[string]string{"MODE": "release"}}
	op := model.Operation{DesiredSpec: &spec, ConfigBaseSpec: &model.AppSpec{}, DesiredSource: &model.AppSource{Type: model.AppSourceTypeGitHubPublic, CommitSHA: "commit", ResolvedImageRef: spec.Image}}
	first := importBuildResultHash(op)
	op.Status = model.OperationStatusFailed
	op.ResultMessage = "billing blocked"
	op.UpdatedAt = time.Now()
	if importBuildResultHash(op) != first {
		t.Fatal("operation progress invalidated saved build")
	}
	changed := cloneImportSpec(spec)
	changed.Env["MODE"] = "debug"
	op.DesiredSpec = &changed
	if importBuildResultHash(op) == first {
		t.Fatal("different saved configuration reused old digest")
	}
}

func TestGitHubReleaseDecisionDeduplicatesPolling(t *testing.T) {
	state := newStoreTestDatabase(t, "")
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	svc := &Service{Store: state, Logger: log.New(&output, "", 0)}
	app := model.App{ID: "app", TenantID: "tenant"}
	for i := 0; i < 3; i++ {
		svc.recordGitHubReleaseDecision(context.Background(), app, "commit", "fingerprint", "blocked_deploy_admission", trackedGitHubCommitState{}, 7)
	}
	if strings.Count(output.String(), "github release decision app=") != 1 {
		t.Fatal("polling emitted duplicate evidence", output.String())
	}
}
