package store

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"fugue/internal/model"
)

func TestPlatformReleaseStateMachinePostgres(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run platform release Postgres integration test")
	}
	if !strings.Contains(databaseURL, "fugue-pgtest") && !strings.Contains(databaseURL, "fugue_test") {
		t.Fatalf("refusing to run platform release integration test against non-test database URL %q", databaseURL)
	}

	s := New("", databaseURL)
	if err := s.Init(); err != nil {
		t.Fatalf("init postgres store: %v", err)
	}

	scopeKey := "pg-release-" + model.NewID("scope")
	scope := model.PlatformArtifactScope{ScopeType: "test", Key: scopeKey}
	createValidated := func(generation string) model.PlatformArtifact {
		t.Helper()
		artifact, err := s.CreatePlatformArtifact(model.PlatformArtifact{
			ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle,
			Scope:        scope,
			Generation:   generation,
			Content: map[string]any{
				"records": []any{map[string]any{"name": generation + ".example.test"}},
			},
		})
		if err != nil {
			t.Fatalf("create artifact %s: %v", generation, err)
		}
		artifact, err = s.ValidatePlatformArtifact(artifact.ID, []model.PlatformArtifactValidationResult{{
			Name:     "schema",
			Pass:     true,
			Severity: model.RobustnessSeverityInfo,
		}})
		if err != nil {
			t.Fatalf("validate artifact %s: %v", generation, err)
		}
		return artifact
	}

	stable := createValidated("stable-" + model.NewID("gen"))
	first := createValidated("first-" + model.NewID("gen"))
	second := createValidated("second-" + model.NewID("gen"))
	seedVerifiedPlatformLKG(t, s, stable)

	type releaseResult struct {
		artifact model.PlatformArtifact
		release  model.PlatformArtifactRelease
		err      error
	}
	start := make(chan struct{})
	results := make(chan releaseResult, 2)
	var wg sync.WaitGroup
	for _, artifact := range []model.PlatformArtifact{first, second} {
		artifact := artifact
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			releasedArtifact, release, _, _, err := s.ReleasePlatformArtifact(artifact.ID, model.PlatformArtifactReleaseRequest{
				ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
				IdempotencyKey: "concurrent-" + artifact.Generation,
			}, testPlatformPrincipal())
			results <- releaseResult{artifact: releasedArtifact, release: release, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	releases := []releaseResult{}
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent release failed: %v", result.err)
		}
		releases = append(releases, result)
	}
	if len(releases) != 2 {
		t.Fatalf("expected two serialized release results, got %d", len(releases))
	}
	if releases[0].release.FencingToken == releases[1].release.FencingToken {
		t.Fatalf("concurrent releases reused fencing token: %+v", releases)
	}

	activeArtifact, activeRelease, found, err := s.GetActivePlatformArtifact(
		model.PlatformArtifactKindDNSAnswerBundle,
		scopeKey,
		model.PlatformArtifactReleaseChannelFull,
	)
	if err != nil {
		t.Fatalf("get active release: %v", err)
	}
	if !found {
		t.Fatal("expected one active release")
	}
	var activeCount int
	if err := s.db.QueryRow(`
SELECT COUNT(*)
FROM fugue_platform_artifact_releases
WHERE artifact_kind = $1 AND scope_key = $2 AND release_channel = $3 AND status = $4`,
		model.PlatformArtifactKindDNSAnswerBundle,
		scopeKey,
		model.PlatformArtifactReleaseChannelFull,
		model.PlatformArtifactReleaseStatusActive,
	).Scan(&activeCount); err != nil {
		t.Fatalf("count active releases: %v", err)
	}
	if activeCount != 1 {
		t.Fatalf("unique release lane allowed %d active releases", activeCount)
	}

	var staleRelease model.PlatformArtifactRelease
	for _, result := range releases {
		if result.release.ID != activeRelease.ID {
			staleRelease = result.release
			break
		}
	}
	if staleRelease.ID == "" {
		t.Fatalf("could not identify stale release: active=%+v releases=%+v", activeRelease, releases)
	}
	if _, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(
		staleRelease.ID,
		completePlatformVerificationRequest(staleRelease.FencingToken, false),
		testPlatformPrincipal(),
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale fenced release verification must fail, got %v", err)
	}

	beforeVerify, err := s.GetPlatformLKG(model.PlatformArtifactKindDNSAnswerBundle, scopeKey)
	if err != nil {
		t.Fatalf("get LKG before verification: %v", err)
	}
	if beforeVerify == nil || beforeVerify.Generation != stable.Generation {
		t.Fatalf("serving-unverified release overwrote stable LKG: %+v", beforeVerify)
	}
	verifyRequest := completePlatformVerificationRequest(activeRelease.FencingToken, false)
	_, verifiedRelease, _, verifiedLKG, err := s.VerifyPlatformArtifactReleaseLKG(activeRelease.ID, verifyRequest, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("verify active release: %v", err)
	}
	if verifiedRelease.VerificationState != model.PlatformArtifactVerificationStateVerified ||
		verifiedLKG == nil ||
		verifiedLKG.Generation != activeArtifact.Generation {
		t.Fatalf("unexpected verified release state: release=%+v lkg=%+v", verifiedRelease, verifiedLKG)
	}
	_, repeatedRelease, _, repeatedLKG, err := s.VerifyPlatformArtifactReleaseLKG(activeRelease.ID, verifyRequest, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("repeat identical verification must be idempotent: %v", err)
	}
	if repeatedRelease.Version != verifiedRelease.Version ||
		repeatedLKG == nil ||
		repeatedLKG.ID != verifiedLKG.ID {
		t.Fatalf("repeat verification mutated state: first=%+v/%+v repeated=%+v/%+v", verifiedRelease, verifiedLKG, repeatedRelease, repeatedLKG)
	}

	retryArtifact := createValidated("retry-" + model.NewID("gen"))
	retryRequest := model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		IdempotencyKey: "retry-same-release",
	}
	_, firstRetry, _, _, err := s.ReleasePlatformArtifact(retryArtifact.ID, retryRequest, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("first idempotent release: %v", err)
	}
	_, secondRetry, _, _, err := s.ReleasePlatformArtifact(retryArtifact.ID, retryRequest, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("second idempotent release: %v", err)
	}
	if firstRetry.ID != secondRetry.ID || firstRetry.FencingToken != secondRetry.FencingToken {
		t.Fatalf("idempotent retry created a new release: first=%+v second=%+v", firstRetry, secondRetry)
	}

	for iteration := 0; iteration < 5; iteration++ {
		verifyCandidate := createValidated("verify-race-" + model.NewID("gen"))
		_, releaseToVerify, _, _, err := s.ReleasePlatformArtifact(verifyCandidate.ID, model.PlatformArtifactReleaseRequest{
			ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
			IdempotencyKey: "verify-race-release-" + verifyCandidate.Generation,
		}, testPlatformPrincipal())
		if err != nil {
			t.Fatalf("iteration %d release verification candidate: %v", iteration, err)
		}
		nextCandidate := createValidated("next-race-" + model.NewID("gen"))

		startRace := make(chan struct{})
		verifyResult := make(chan error, 1)
		nextReleaseResult := make(chan releaseResult, 1)
		go func() {
			<-startRace
			_, _, _, _, verifyErr := s.VerifyPlatformArtifactReleaseLKG(
				releaseToVerify.ID,
				completePlatformVerificationRequest(releaseToVerify.FencingToken, false),
				testPlatformPrincipal(),
			)
			verifyResult <- verifyErr
		}()
		go func() {
			<-startRace
			releasedArtifact, nextRelease, _, _, releaseErr := s.ReleasePlatformArtifact(nextCandidate.ID, model.PlatformArtifactReleaseRequest{
				ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
				IdempotencyKey: "next-race-release-" + nextCandidate.Generation,
			}, testPlatformPrincipal())
			nextReleaseResult <- releaseResult{artifact: releasedArtifact, release: nextRelease, err: releaseErr}
		}()
		close(startRace)

		verifyErr := <-verifyResult
		nextResult := <-nextReleaseResult
		if verifyErr != nil && !errors.Is(verifyErr, ErrConflict) {
			t.Fatalf("iteration %d concurrent verification returned non-conflict error: %v", iteration, verifyErr)
		}
		if nextResult.err != nil {
			t.Fatalf("iteration %d concurrent next release failed: %v", iteration, nextResult.err)
		}
		_, _, _, _, err = s.VerifyPlatformArtifactReleaseLKG(
			nextResult.release.ID,
			completePlatformVerificationRequest(nextResult.release.FencingToken, false),
			testPlatformPrincipal(),
		)
		if err != nil {
			t.Fatalf("iteration %d verify next active release: %v", iteration, err)
		}
	}
}
