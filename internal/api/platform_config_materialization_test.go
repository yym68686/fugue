package api

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestPlatformCaptureAndMaterializeWithoutHTTPRequest(t *testing.T) {
	state, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	principal := model.Principal{ActorType: model.ActorTypeBootstrap, ActorID: "configuration-producer", Scopes: map[string]struct{}{"platform.admin": {}}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := server.capturePlatformIntent(ctx, principal); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled capture did not stop", err)
	}
	projection, err := server.capturePlatformIntent(context.Background(), principal)
	if err != nil {
		t.Fatal(err)
	}
	if projection.BusinessSnapshotRevision == "" || projection.MigrationReady {
		t.Fatal("capture lost provenance or claimed promotion readiness")
	}
	_, err = platformconfig.Compile(platformconfig.CompileRequest{Intent: projection.Intent, Policy: projection.Policy, RuntimeSnapshot: projection.RuntimeSnapshot})
	if err == nil || !strings.Contains(err.Error(), "insufficient route-ready and TLS-ready edges") {
		t.Fatal("capture without DNS readiness became compilable", err)
	}
	// A complete fixed input exercises the persistence path independently of the
	// fixture's deliberately unavailable runtime observations.
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: "materialization-test", Routes: []platformconfig.RouteIntent{{Hostname: "app.example.test", UpstreamURL: "http://origin:8080", Enabled: true}}},
		Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: "materialization-test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.materializePlatformCompilation(ctx, compiled, principal, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled materialization did not stop", err)
	}
	after, err := state.ListPlatformArtifacts(model.PlatformArtifactFilter{})
	if err != nil || len(before) != len(after) {
		t.Fatal("canceled workflow wrote artifacts", err)
	}
	// Recover a partial immutable write without generating another route identity.
	partialInput := compiled.RouteArtifact
	partialInput.CreatedByType, partialInput.CreatedByID = principal.ActorType, principal.ActorID
	partial, _, err := state.EnsurePlatformArtifact(partialInput)
	if err != nil {
		t.Fatal(err)
	}
	first, err := server.materializePlatformCompilation(context.Background(), compiled, principal, nil)
	if err != nil || first.RouteArtifact.ID != partial.ID {
		t.Fatal("partial compiler output was not recovered", err)
	}
	replay, err := server.materializePlatformCompilation(context.Background(), compiled, principal, &platformConfigStoredInputs{Intent: first.IntentArtifact, Policy: first.PolicyArtifact})
	if err != nil || replay.ReleaseArtifact.ID != first.ReleaseArtifact.ID || replay.RouteArtifact.ID != first.RouteArtifact.ID || replay.DNSArtifact.ID != first.DNSArtifact.ID || replay.TLSArtifact.ID != first.TLSArtifact.ID {
		t.Fatal("fixed-input replay changed artifact identities", err)
	}
	if !reflect.DeepEqual(replay.IntentArtifact, first.IntentArtifact) || !reflect.DeepEqual(replay.PolicyArtifact, first.PolicyArtifact) {
		t.Fatal("replay rewrote the original inputs")
	}
	for _, artifact := range []model.PlatformArtifact{replay.IntentArtifact, replay.PolicyArtifact, replay.RouteArtifact, replay.DNSArtifact, replay.TLSArtifact, replay.ReleaseArtifact} {
		if artifact.Status != model.PlatformArtifactStatusValidated || state.VerifyPlatformArtifactIntegrity(artifact) != nil {
			t.Fatal("materialized artifact is not validated and signed", artifact.ArtifactKind)
		}
		if lkg, err := state.GetPlatformLKG(artifact.ArtifactKind, artifact.ScopeKey); err != nil || lkg != nil {
			t.Fatal("compilation altered LKG", artifact.ArtifactKind, err)
		}
	}
	if result := server.validateReleaseSetReferences(replay.ReleaseArtifact); !result.Pass {
		t.Fatal("materialization broke the complete release set", result.Message)
	}
	messages, err := state.ListPlatformReleaseMessages(model.PlatformArtifactKindReleaseSet, first.ReleaseArtifact.ScopeKey, time.Time{}, 100)
	if err != nil || len(messages) != 0 {
		t.Fatal("compilation published a release", err)
	}
}
