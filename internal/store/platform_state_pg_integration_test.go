package store

import (
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
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
	configureTestPlatformArtifactSigning(s)
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
	conflicts := 0
	for result := range results {
		if result.err != nil {
			if errors.Is(result.err, ErrConflict) {
				conflicts++
				continue
			}
			t.Fatalf("concurrent release failed with unexpected error: %v", result.err)
		}
		releases = append(releases, result)
	}
	if len(releases) < 1 || len(releases) > 2 || len(releases)+conflicts != 2 {
		t.Fatalf("expected monotonic serialization to accept one or two releases, releases=%d conflicts=%d", len(releases), conflicts)
	}
	if len(releases) == 2 && releases[0].release.FencingToken == releases[1].release.FencingToken {
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
	if activeArtifact.ID != second.ID {
		t.Fatalf("monotonic release lane must end on the highest sequence, active=%+v second=%+v", activeArtifact, second)
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

	if len(releases) == 2 {
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

	if _, err := s.db.Exec(`
DELETE FROM fugue_platform_lkg_snapshot_history
WHERE artifact_kind = $1 AND scope_key = $2 AND generation = $3`,
		verifiedLKG.ArtifactKind, verifiedLKG.ScopeKey, verifiedLKG.Generation); err != nil {
		t.Fatalf("simulate mixed-version current-only LKG write: %v", err)
	}
	mixedVersionCandidate := createValidated("mixed-version-" + model.NewID("gen"))
	_, mixedVersionRelease, _, _, err := s.ReleasePlatformArtifact(mixedVersionCandidate.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		IdempotencyKey: "mixed-version-history-recovery",
	}, testPlatformPrincipal())
	if err != nil {
		t.Fatalf("release mixed-version history recovery candidate: %v", err)
	}
	if _, _, _, _, err := s.VerifyPlatformArtifactReleaseLKG(
		mixedVersionRelease.ID,
		completePlatformVerificationRequest(mixedVersionRelease.FencingToken, false),
		testPlatformPrincipal(),
	); err != nil {
		t.Fatalf("verify mixed-version history recovery candidate: %v", err)
	}
	var recoveredPrevious int
	if err := s.db.QueryRow(`
SELECT COUNT(*)
FROM fugue_platform_lkg_snapshot_history
WHERE artifact_kind = $1 AND scope_key = $2 AND generation = $3`,
		verifiedLKG.ArtifactKind, verifiedLKG.ScopeKey, verifiedLKG.Generation).Scan(&recoveredPrevious); err != nil {
		t.Fatalf("count recovered previous LKG history: %v", err)
	}
	if recoveredPrevious != 1 {
		t.Fatalf("mixed-version current LKG was not archived before overwrite: count=%d", recoveredPrevious)
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

	currentLKG, err := s.GetPlatformLKG(model.PlatformArtifactKindDNSAnswerBundle, scopeKey)
	if err != nil {
		t.Fatalf("get current LKG after release races: %v", err)
	}
	history, err := s.ListPlatformLKGHistory(model.PlatformArtifactKindDNSAnswerBundle, scopeKey, 100)
	if err != nil {
		t.Fatalf("list PostgreSQL LKG history: %v", err)
	}
	if len(history) < platformLKGHistoryDefaultLimit {
		t.Fatalf("expected at least %d retained PostgreSQL LKG generations, got %+v", platformLKGHistoryDefaultLimit, history)
	}
	if currentLKG == nil || history[0].ID != currentLKG.ID {
		t.Fatalf("current PostgreSQL LKG must be the newest retained generation: current=%+v history=%+v", currentLKG, history)
	}
}

func TestPlatformOverrideAuditPostgres(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run platform override Postgres integration test")
	}
	if !strings.Contains(databaseURL, "fugue-pgtest") && !strings.Contains(databaseURL, "fugue_test") {
		t.Fatalf("refusing to run platform override integration test against non-test database URL %q", databaseURL)
	}

	s := New("", databaseURL)
	configureTestPlatformArtifactSigning(s)
	if err := s.Init(); err != nil {
		t.Fatalf("init postgres store: %v", err)
	}
	scopeKey := "pg-override-" + model.NewID("scope")
	scope := model.PlatformArtifactScope{ScopeType: "test", Key: scopeKey}
	createDraft := func(generation string) model.PlatformArtifact {
		t.Helper()
		artifact, err := s.CreatePlatformArtifact(model.PlatformArtifact{
			ArtifactKind: model.PlatformArtifactKindTrafficSafetyPolicy,
			Scope:        scope,
			Generation:   generation,
			Content:      map[string]any{"min_healthy_edges": 1},
		})
		if err != nil {
			t.Fatalf("create draft artifact %s: %v", generation, err)
		}
		return artifact
	}

	softArtifact := createDraft("soft-" + model.NewID("generation"))
	_, softRelease, _, _, err := s.ReleasePlatformArtifact(softArtifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		SoftOverride:   true,
		Reason:         "postgres soft override transaction",
	}, testPlatformSoftOverridePrincipal())
	if err != nil {
		t.Fatalf("release postgres soft override: %v", err)
	}

	kernelArtifact := createDraft("kernel-" + model.NewID("generation"))
	expiresAt := time.Now().UTC().Add(5 * time.Minute)
	_, kernelRelease, _, _, err := s.ReleasePlatformArtifact(kernelArtifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		KernelBreakGlass: &model.PlatformKernelBreakGlassRequest{
			ExpiresAt:          expiresAt,
			Confirmation:       platformsafety.KernelBreakGlassConfirmation,
			TargetConfirmation: kernelArtifact.ID,
		},
		Reason: "postgres kernel break-glass transaction",
	}, testPlatformKernelBreakGlassPrincipal())
	if err != nil {
		t.Fatalf("release postgres kernel break-glass: %v", err)
	}
	if kernelRelease.OverrideExpiresAt == nil ||
		!kernelRelease.OverrideExpiresAt.Equal(expiresAt) ||
		!stringSliceContains(kernelRelease.BypassedInvariants, platformsafety.InvariantFullPinnedRollback) {
		t.Fatalf("unexpected persisted kernel release: %+v", kernelRelease)
	}

	events, err := s.ListAuditEvents("", true, 0)
	if err != nil {
		t.Fatalf("list postgres audit events: %v", err)
	}
	foundTargets := map[string]bool{}
	for _, event := range events {
		if event.ChainID == platformsafety.PlatformSafetyAuditChainID {
			foundTargets[event.TargetID] = true
		}
	}
	if !foundTargets[softRelease.ID] || !foundTargets[kernelRelease.ID] {
		t.Fatalf("override release audit events missing: soft=%s kernel=%s events=%+v", softRelease.ID, kernelRelease.ID, events)
	}
	if err := platformsafety.VerifyTamperEvidentAuditChain(
		events,
		platformsafety.PlatformSafetyAuditChainID,
		s.platformArtifactSigningKeyring(),
	); err != nil {
		t.Fatalf("postgres platform safety audit chain did not verify: %v", err)
	}

	failedArtifact := createDraft("audit-failure-" + model.NewID("generation"))
	s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring(
		"",
		"",
		"platform-artifact-test-signing-key",
		"platform-artifact-test",
		nil,
	))
	if _, _, _, _, err := s.ReleasePlatformArtifact(failedArtifact.ID, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelGray,
		CanaryRuleRef:  "edge=test",
		SoftOverride:   true,
		Reason:         "postgres audit signing failure",
	}, testPlatformSoftOverridePrincipal()); !errors.Is(err, platformsafety.ErrPlatformSigningKeyUnavailable) {
		t.Fatalf("expected postgres audit signing failure, got %v", err)
	}
	if _, _, found, err := s.GetActivePlatformArtifact(
		failedArtifact.ArtifactKind,
		failedArtifact.ScopeKey,
		model.PlatformArtifactReleaseChannelGray,
	); err != nil {
		t.Fatalf("get postgres active artifact after failed release: %v", err)
	} else if found {
		t.Fatal("postgres release committed despite audit signing failure")
	}
}
