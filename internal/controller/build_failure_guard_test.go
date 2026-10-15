package controller

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

func TestPermanentBuildFailureSurvivesRestartAndChangedInputsResume(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	s := &Service{Store: store.New(path)}
	if err := s.Store.Init(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	app := model.App{ID: "app-fixture", TenantID: "tenant-fixture"}
	source := model.AppSource{Type: model.AppSourceTypeGitHubPublic, RepoURL: "https://github.com/example/service", CommitSHA: strings.Repeat("a", 40), BuildStrategy: model.AppBuildStrategyDockerfile, DockerfilePath: "Dockerfile"}
	spec := model.AppSpec{Env: map[string]string{"BUILD_MODE": "release"}}
	policy := sourceimport.EffectiveBuilderPolicy(sourceimport.BuilderPodPolicy{}, 6<<30)
	fingerprint := buildInputFingerprint(source, spec, policy)
	if err := s.recordPermanentBuildFailure(ctx, app, "operation-fixture", fingerprint, &sourceimport.BuilderResourceFailure{Resource: "memory", LimitBytes: 6 << 30}); err != nil {
		t.Fatal(err)
	}
	// A new controller instance reads the durable fact, not an in-memory cache.
	s = &Service{Store: store.New(path)}
	ledger, err := s.readBuildFailureLedger(ctx, app)
	if err != nil {
		t.Fatal(err)
	}
	var rejected *sourceimport.BuilderResourceFailure
	if !errors.As(cachedBuildRejection(ledger, fingerprint), &rejected) || !rejected.Preflight {
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
		next := buildInputFingerprint(src, config, nextPolicy)
		if next == fingerprint || cachedBuildRejection(ledger, next) != nil || !buildPolicyChangedSinceFailure(ledger, model.Operation{ID: "operation-fixture"}, next) {
			t.Fatal("changed inputs could not resume", change)
		}
	}
	other, err := s.readBuildFailureLedger(ctx, model.App{ID: app.ID, TenantID: "different-tenant"})
	if err != nil || len(other.Failures) != 0 {
		t.Fatal("failure history crossed tenant scope", other, err)
	}
}

func TestBuildFingerprintIgnoresOutputAndCredentialMetadata(t *testing.T) {
	source := model.AppSource{CommitSHA: strings.Repeat("a", 40)}
	policy := sourceimport.EffectiveBuilderPolicy(sourceimport.BuilderPodPolicy{}, 6<<30)
	spec := model.AppSpec{Env: map[string]string{"TOKEN": "test-secret"}}
	first := buildInputFingerprint(source, spec, policy)
	source.RepoAuthToken, source.ResolvedImageRef, source.DetectedStack = "new-credential", "output-image", "detected"
	spec.Image, spec.RestartToken = "runtime-image", "restart-only"
	if got := buildInputFingerprint(source, spec, policy); got != first || strings.Contains(got, "test-secret") {
		t.Fatal("volatile metadata changed or leaked into fingerprint", got)
	}
	spec.Env = map[string]string{"TOKEN": "changed-secret"}
	if buildInputFingerprint(source, spec, policy) == first {
		t.Fatal("changed configuration did not invalidate guard")
	}
}
