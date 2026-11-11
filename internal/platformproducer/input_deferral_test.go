package platformproducer

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func inputDeferralFixture(t *testing.T) (InputDeferral, model.PlatformArtifact) {
	t.Helper()
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	p := InputDeferral{SchemaVersion: InputDeferralSchema, Generation: "finite-alpha", Action: "defer", Request: CaptureDeferralRequest{
		SchemaVersion: CaptureDeferralSchema, OperationID: "finite-alpha", RequestedAt: at, ExpiresAt: at.Add(30 * time.Minute),
		Authorities: []CaptureDeferralAuthority{{Scope: Scope, Policy: model.PlatformPublicationPrecondition{ArtifactID: "policy-alpha", ContentHash: "sha256:" + strings.Repeat("a", 64), ReleaseID: "release-alpha", FencingToken: 3}}},
	}}
	p.RequestDigest, _ = p.Request.Digest()
	raw, _ := json.Marshal(p)
	a := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, ScopeKey: InputDeferralScope, Generation: p.Generation}
	if err := json.Unmarshal(raw, &a.Content); err != nil {
		t.Fatal(err)
	}
	return p, a
}

func TestInputDeferralAbsoluteBoundAndTerminalRelease(t *testing.T) {
	p, a := inputDeferralFixture(t)
	decoded, err := DecodeInputDeferral(a)
	if err != nil || decoded.RequestDigest != p.RequestDigest {
		t.Fatal(err)
	}
	b := p.Request.Authorities[0]
	for _, offset := range []time.Duration{0, time.Minute, 30*time.Minute - time.Nanosecond, 30 * time.Minute, time.Hour} {
		got, err := decoded.Defers(p.Request.RequestedAt.Add(offset), b.Scope, b.Policy)
		if err != nil || got != (offset < 30*time.Minute) {
			t.Fatal("absolute deadline changed on observation/restart", offset, got, err)
		}
	}
	if _, err := p.Defers(p.Request.RequestedAt.Add(-time.Second), b.Scope, b.Policy); err == nil {
		t.Fatal("clock uncertainty granted admission")
	}
	changed := b.Policy
	changed.FencingToken++
	if _, err := p.Defers(p.Request.RequestedAt, b.Scope, changed); err == nil {
		t.Fatal("authority drift granted admission")
	}
	if got, err := p.Defers(p.Request.RequestedAt, Scope+":cell-other", b.Policy); err != nil || got {
		t.Fatal("control spread to another scope", err)
	}
	released := p
	released.Action, released.Generation = "release", p.Generation+":release"
	if err := ValidateInputDeferralTransition(released, &p, p.Request.RequestedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got, err := released.Defers(p.Request.RequestedAt.Add(time.Minute), b.Scope, b.Policy); err != nil || got {
		t.Fatal("release retained restriction", err)
	}
	if err := ValidateInputDeferralTransition(released, &p, p.Request.RequestedAt.Add(-time.Hour)); err != nil {
		t.Fatal("clock uncertainty prevented exact terminal release", err)
	}
	if got, err := released.Defers(p.Request.RequestedAt.Add(-time.Hour), b.Scope, b.Policy); err != nil || got {
		t.Fatal("clock uncertainty reactivated released restriction", err)
	}
	if ValidateInputDeferralTransition(p, &released, p.Request.RequestedAt.Add(time.Minute)) == nil {
		t.Fatal("early release was reactivated")
	}
	extended := p
	extended.Request.ExpiresAt = extended.Request.ExpiresAt.Add(time.Second)
	if _, err := extended.Request.Digest(); err == nil {
		t.Fatal("1800 second limit was relaxed")
	}
	changedRequest := p
	changedRequest.RequestDigest = "sha256:" + strings.Repeat("b", 64)
	if ValidateInputDeferralTransition(changedRequest, &p, p.Request.RequestedAt) == nil {
		t.Fatal("same operation was rebound")
	}
	if ValidateInputDeferralTransition(p, nil, p.Request.ExpiresAt) == nil {
		t.Fatal("expired control was enabled")
	}
}

func TestInputDeferralStrictSignedIdentity(t *testing.T) {
	_, original := inputDeferralFixture(t)
	for _, scenario := range []string{"unknown", "scope", "kind", "generation", "digest", "action", "oversize"} {
		t.Run(scenario, func(t *testing.T) {
			raw, _ := json.Marshal(original)
			var a model.PlatformArtifact
			_ = json.Unmarshal(raw, &a)
			switch scenario {
			case "unknown":
				a.Content["enabled"] = true
			case "scope":
				a.ScopeKey = Scope
			case "kind":
				a.ArtifactKind = model.PlatformArtifactKindReleaseSet
			case "generation":
				a.Generation += "-replay"
			case "digest":
				a.Content["request_digest"] = "sha256:" + strings.Repeat("b", 64)
			case "action":
				a.Content["action"] = "extend"
			case "oversize":
				a.Content["unused"] = strings.Repeat("x", MaxInputDeferralBytes)
			}
			if _, err := DecodeInputDeferral(a); err == nil {
				t.Fatal("invalid control accepted")
			}
		})
	}
}
