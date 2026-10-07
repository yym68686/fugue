package celldns

import (
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"testing"
	"time"
)

// SourceSnapshot preserves exact producer and lane identities, without granting
// readiness. Runtime tests supply their own real/negative proof observations.
func SourceSnapshot(t testing.TB, req platformconfig.CompileRequest, dns model.PlatformArtifact, policies []model.PlatformArtifact, releases []model.PlatformArtifactRelease, now time.Time) model.PlatformDNSRouteSourceSnapshot {
	t.Helper()
	out := model.PlatformDNSRouteSourceSnapshot{DNSArtifactID: dns.ID, DNSArtifactDigest: dns.ContentHash, ObservedAt: now}
	add := func(parent, route, tls model.PlatformArtifact, r model.PlatformArtifactRelease, b *model.PlatformPublicationPrecondition) {
		var policy model.PlatformArtifact
		var activation model.PlatformArtifactRelease
		for _, a := range policies {
			if a.ID == b.ArtifactID {
				policy = a
			}
		}
		for _, a := range releases {
			if a.ID == b.ReleaseID {
				activation = a
			}
		}
		out.Scopes = append(out.Scopes, model.PlatformDNSRouteSourceScope{ScopeKey: parent.ScopeKey, Lanes: []model.PlatformDNSRouteSourceLane{{ReleaseChannel: r.ReleaseChannel, FencingToken: r.FencingToken, Version: 1, ActiveReleaseID: r.ID}}, Publications: []model.PlatformDNSRouteSourcePublication{{Selections: []string{r.ReleaseChannel}, Parent: parent, Route: route, TLS: tls, Release: r, ProducerPolicy: policy, ProducerRelease: activation}}})
	}
	for _, p := range req.CellRoutePublications {
		add(p.Parent, p.Route, p.TLS, Publication(p), p.ProducerPolicy)
	}
	if p := req.PreviousTrafficPublication; p != nil {
		add(p.Parent, p.Route, p.TLS, PreviousPublication(*p), p.ProducerPolicy)
	}
	var err error
	hashed := out
	hashed.ObservedAt = time.Time{}
	out.SelectionDigest, err = platformconfig.Digest(hashed)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
