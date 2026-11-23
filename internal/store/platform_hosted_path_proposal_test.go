package store

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	pc "fugue/internal/platformconfig"
)

func TestHostedProposalPureDraftCannotBecomeAnAssignment(t *testing.T) {
	s := newPlatformArtifactEnsureTestStore(t)
	now := time.Now().UTC()
	hash := "sha256:" + strings.Repeat("a", 64)
	p := pc.HostedScopeProposal{Schema: pc.HostedProposalSchema, Host: "site.example.test",
		Base:      pc.PreviousTrafficPublicationReference{ReleaseSetID: "base", ReleaseSetDigest: hash, ReleaseID: "full", FencingToken: 3, RouteArtifactID: "routes", RouteArtifactDigest: hash, TLSArtifactID: "certs", TLSArtifactDigest: hash, DNSArtifactID: "answers", DNSArtifactDigest: hash},
		Authority: pc.HostedProposalAuthority{Producer: model.PlatformPublicationPrecondition{ArtifactID: "policy", ContentHash: hash, ReleaseID: "producer", FencingToken: 4}, LKGID: "lkg", EvidenceHash: hash, ExpiresAt: now.Add(time.Hour)},
		Changes:   []pc.HostedReadinessTargetEdit{{Record: "site.example.test"}}}
	rows := pc.HostedDispatchRows{Owner: model.EdgeTLSAllowlistEntry{Hostname: p.Host, TenantID: "tenant-a", AppID: "app-a"}, Routes: []model.EdgeRouteIntent{{Hostname: p.Host, AppID: "app-a"}}}
	p.Before, p.Prepared, p.After = rows, rows, rows
	draft, err := hostedProposalDraft(p, now.Add(-time.Minute), now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreatePlatformArtifact(draft); err == nil {
		t.Fatal("public create accepted")
	}
	if _, _, err = s.EnsurePlatformArtifact(draft); err == nil {
		t.Fatal("generic ensure accepted")
	}
	a, created, err := s.ensurePlatformArtifact(draft)
	if err != nil || !created || s.VerifyPlatformArtifactIntegrity(a) != nil {
		t.Fatal("signed draft", err)
	}
	retried, err := hostedProposalDraft(p, now, now)
	if err != nil {
		t.Fatal(err)
	}
	b, created, err := s.ensurePlatformArtifact(retried)
	if err != nil || created || a.ID != b.ID || !a.CreatedAt.Equal(b.CreatedAt) {
		t.Fatal("refreshed evidence on retry", err)
	}
	for _, id := range []string{a.ID, a.Generation} {
		if _, err = s.ValidatePlatformArtifact(id, []model.PlatformArtifactValidationResult{{Name: "test", Pass: true}}); err == nil {
			t.Fatal("generic validation accepted")
		}
		for _, channel := range []string{"shadow", "gray", "full"} {
			if _, _, _, _, err = s.ReleasePlatformArtifact(id, model.PlatformArtifactReleaseRequest{ReleaseChannel: channel, SoftOverride: true, Reason: "test"}, model.Principal{}); err == nil {
				t.Fatal("generic release accepted")
			}
			if _, _, _, _, err = s.RollbackPlatformArtifact(id, model.PlatformArtifactRollbackRequest{ReleaseChannel: channel, ToGeneration: a.Generation, Reason: "test"}, model.Principal{}); err == nil {
				t.Fatal("generic rollback accepted")
			}
		}
	}
	if _, err = hostedProposalDraft(p, now, p.Authority.ExpiresAt); err == nil {
		t.Fatal("expired authority accepted")
	}
}
