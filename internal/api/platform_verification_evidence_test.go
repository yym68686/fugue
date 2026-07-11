package api

import (
	"net/http"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestPlatformVerificationEvidenceWireSupportsLegacyAndFourState(t *testing.T) {
	t.Parallel()

	legacyPass := true
	legacyEvidence, legacyStates, err := (platformArtifactVerificationEvidenceHTTPRequest{
		ConsumerConvergence:        &legacyPass,
		LocalProbe:                 &legacyPass,
		PublicSynthetic:            &legacyPass,
		WatchWindow:                &legacyPass,
		BaselineMonotonic:          &legacyPass,
		DatabaseRollbackCompatible: &legacyPass,
	}).modelEvidence()
	if err != nil || !legacyEvidence.ConsumerConvergence || len(nonPassingPlatformVerificationEvidence(legacyStates)) != 0 {
		t.Fatalf("legacy passing evidence must remain accepted: evidence=%+v states=%+v err=%v", legacyEvidence, legacyStates, err)
	}

	stateOnly := passingPlatformVerificationEvidenceHTTPRequest()
	stateEvidence, stateValues, err := stateOnly.modelEvidence()
	if err != nil || !stateEvidence.ConsumerConvergence || !stateEvidence.LocalProbe ||
		!stateEvidence.PublicSynthetic || !stateEvidence.WatchWindow ||
		!stateEvidence.BaselineMonotonic || !stateEvidence.DatabaseRollbackCompatible ||
		len(nonPassingPlatformVerificationEvidence(stateValues)) != 0 {
		t.Fatalf("state-only passing evidence must be accepted: evidence=%+v states=%+v err=%v", stateEvidence, stateValues, err)
	}

	stateOnly.LocalProbeState = model.InvariantEvidenceStateStale
	stateEvidence, stateValues, err = stateOnly.modelEvidence()
	if err != nil || stateEvidence.LocalProbe {
		t.Fatalf("stale evidence must remain non-passing without becoming invalid: evidence=%+v err=%v", stateEvidence, err)
	}
	if got := strings.Join(nonPassingPlatformVerificationEvidence(stateValues), ","); got != "local_probe=stale" {
		t.Fatalf("unexpected non-passing evidence: %q", got)
	}

	legacyFalse := false
	stateOnly.LocalProbeState = model.InvariantEvidenceStatePass
	stateOnly.LocalProbe = &legacyFalse
	if _, _, err := stateOnly.modelEvidence(); err == nil {
		t.Fatal("contradictory explicit state and legacy boolean must fail closed")
	}
}

func TestVerifyPlatformArtifactReleaseLKGAcceptsStateOnlyEvidence(t *testing.T) {
	t.Parallel()

	_, server, _, platformAdminKey, _, _ := setupAppDomainTestServerWithDomains(t, "fugue.pro")
	create := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts", platformAdminKey, model.PlatformArtifactCreateRequest{
		ArtifactKind: model.PlatformArtifactKindEdgeRankingPolicy,
		Scope:        model.PlatformArtifactScope{ScopeType: "global"},
		Generation:   "state_only_verification_gen_1",
		Content:      map[string]any{"weights": map[string]any{"ttfb": 1}},
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("create artifact: status=%d body=%s", create.Code, create.Body.String())
	}
	var created model.PlatformArtifactResponse
	mustDecodeJSON(t, create, &created)
	validate := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/validate", platformAdminKey, map[string]any{"dry_run": false})
	if validate.Code != http.StatusOK {
		t.Fatalf("validate artifact: status=%d body=%s", validate.Code, validate.Body.String())
	}
	release := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifacts/"+created.Artifact.ID+"/release", platformAdminKey, model.PlatformArtifactReleaseRequest{
		ReleaseChannel: model.PlatformArtifactReleaseChannelShadow,
		IdempotencyKey: "state-only-verification-release",
		Reason:         "verify state-only migration",
	})
	if release.Code != http.StatusOK {
		t.Fatalf("release artifact: status=%d body=%s", release.Code, release.Body.String())
	}
	var released model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, release, &released)

	stale := passingPlatformVerificationEvidenceHTTPRequest()
	stale.LocalProbeState = model.InvariantEvidenceStateStale
	blocked := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifact-releases/"+released.Release.ID+"/verify-lkg", platformAdminKey, platformArtifactVerifyLKGHTTPRequest{
		FencingToken:    released.Release.FencingToken,
		Reason:          "stale evidence must hold promotion",
		AllowInitialLKG: true,
		Evidence:        stale,
	})
	if blocked.Code != http.StatusConflict || !strings.Contains(blocked.Body.String(), "local_probe=stale") {
		t.Fatalf("stale state must block with attributed conflict: status=%d body=%s", blocked.Code, blocked.Body.String())
	}

	verified := performJSONRequest(t, server, http.MethodPost, "/v1/admin/artifact-releases/"+released.Release.ID+"/verify-lkg", platformAdminKey, platformArtifactVerifyLKGHTTPRequest{
		FencingToken:    released.Release.FencingToken,
		Reason:          "all explicit evidence passed",
		AllowInitialLKG: true,
		Evidence:        passingPlatformVerificationEvidenceHTTPRequest(),
	})
	if verified.Code != http.StatusOK {
		t.Fatalf("state-only verification must pass: status=%d body=%s", verified.Code, verified.Body.String())
	}
	var response model.PlatformArtifactReleaseResponse
	mustDecodeJSON(t, verified, &response)
	if response.LKG == nil || response.Release.VerificationState != model.PlatformArtifactVerificationStateVerified {
		t.Fatalf("state-only verification did not promote verified LKG: %+v", response)
	}
}

func passingPlatformVerificationEvidenceHTTPRequest() platformArtifactVerificationEvidenceHTTPRequest {
	return platformArtifactVerificationEvidenceHTTPRequest{
		ConsumerConvergenceState: model.InvariantEvidenceStatePass,
		LocalProbeState:          model.InvariantEvidenceStatePass,
		PublicSyntheticState:     model.InvariantEvidenceStatePass,
		WatchWindowState:         model.InvariantEvidenceStatePass,
		BaselineMonotonicState:   model.InvariantEvidenceStatePass,
		DatabaseRollbackState:    model.InvariantEvidenceStatePass,
		EvidenceRefs:             []string{"test:state-only"},
	}
}
