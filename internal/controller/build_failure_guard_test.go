package controller

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/buildadmission"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

func TestPermanentBuildFailureSurvivesRestartAndChangedInputsResume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "postgres-fixture")
	s := &Service{Store: newStoreTestDatabase(t, path)}
	if err := s.Store.Init(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	app := model.App{ID: "app-fixture", TenantID: "tenant-fixture"}
	source := model.AppSource{Type: model.AppSourceTypeGitHubPublic, RepoURL: "https://github.com/example/service", CommitSHA: strings.Repeat("a", 40), BuildStrategy: model.AppBuildStrategyDockerfile, DockerfilePath: "Dockerfile"}
	spec := model.AppSpec{Env: map[string]string{"BUILD_MODE": "release"}}
	policy := sourceimport.EffectiveBuilderPolicy(sourceimport.BuilderPodPolicy{}, 6<<30)
	fingerprint := buildadmission.InputFingerprint(source, spec, policy)
	if err := s.buildAdmission().RecordPermanentFailure(ctx, app, "operation-fixture", fingerprint, &sourceimport.BuilderResourceFailure{Resource: "memory", LimitBytes: 6 << 30}); err != nil {
		t.Fatal(err)
	}
	// A new controller instance reads the durable fact, not an in-memory cache.
	s = &Service{Store: newStoreTestDatabase(t, path)}
	ledger, err := s.buildAdmission().ReadFailureLedger(ctx, app)
	if err != nil {
		t.Fatal(err)
	}
	var rejected *sourceimport.BuilderResourceFailure
	if !errors.As(buildadmission.CachedRejection(ledger, fingerprint), &rejected) || !rejected.Preflight {
		t.Fatal("identical inputs were not rejected", ledger)
	}
	for _, change := range []string{"commit", "configuration", "quota", "policy"} {
		src, config, nextPolicy := source, spec, policy
		switch change {
		case "commit":
			src.CommitSHA = strings.Repeat("b", 40)
		case "configuration":
			src.DockerfilePath = "Dockerfile.release"
		case "quota":
			nextPolicy = sourceimport.EffectiveBuilderPolicy(sourceimport.BuilderPodPolicy{}, 8<<30)
		case "policy":
			nextPolicy.HeavyEphemeralLimitCeiling = "32Gi"
		}
		next := buildadmission.InputFingerprint(src, config, nextPolicy)
		if next == fingerprint || buildadmission.CachedRejection(ledger, next) != nil || !buildadmission.PolicyChangedSinceFailure(ledger, model.Operation{ID: "operation-fixture"}, next) {
			t.Fatal("changed inputs could not resume", change)
		}
	}
	other, err := s.buildAdmission().ReadFailureLedger(ctx, model.App{ID: app.ID, TenantID: "different-tenant"})
	if err != nil || len(other.Failures) != 0 {
		t.Fatal("failure history crossed tenant scope", other, err)
	}
}
