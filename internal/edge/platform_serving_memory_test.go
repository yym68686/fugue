package edge

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/routeartifact"
)

func servingMemoryFixture(tb testing.TB) (*Service, model.EdgeRouteBundle, model.EdgeRouteIntentSnapshot) {
	tb.Helper()
	req := platformconfig.CompileRequest{
		Intent: platformconfig.PlatformIntent{Generation: "intent", Scope: "global"},
		Policy: platformconfig.PolicySnapshot{Generation: "policy", Scope: "global"},
	}
	for i := range 180 {
		req.Intent.Routes = append(req.Intent.Routes, platformconfig.RouteIntent{
			Hostname: fmt.Sprintf("route-%03d.example.test", i), Enabled: true,
			UpstreamURL: "http://origin:8080", TLSPolicy: model.EdgeRouteTLSPolicyPlatform,
		})
	}
	compiled, err := platformconfig.Compile(req)
	if err != nil {
		tb.Fatal(err)
	}
	projection, err := routeartifact.Project(compiled.RouteArtifact)
	if err != nil {
		tb.Fatal(err)
	}
	bundle, err := routeartifact.MaterializeSnapshotForGroup(projection, "edge-group-test")
	if err != nil {
		tb.Fatal(err)
	}
	now := time.Now().UTC()
	bundle.GeneratedAt, bundle.ValidUntil = now, now.Add(time.Hour)
	s := NewService(config.EdgeConfig{EdgeGroupID: "edge-group-test", CachePath: filepath.Join(tb.TempDir(), "routes.json"), CaddyEnabled: true}, nil)
	s.recordSyncSuccess(bundle, "", now, false)
	s.recordCaddyApply(bundle.Version, len(bundle.Routes), "config", nil)
	raw, err := json.Marshal(cacheFile{Version: cacheFileVersion, Bundle: bundle})
	if err != nil {
		tb.Fatal(err)
	}
	if err = os.WriteFile(s.Config.CachePath, raw, 0600); err != nil {
		tb.Fatal(err)
	}
	return s, bundle, projection
}

// One observation checks the live executor and disk before probing, before
// persisting its receipt and before each of the two authenticated reports.
func BenchmarkServingValidationCycle(b *testing.B) {
	s, bundle, projection := servingMemoryFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		verified, err := preparePlatformServingProjection(projection, s.Config.EdgeGroupID)
		if err != nil {
			b.Fatal(err)
		}
		for range 4 {
			if err := s.validatePlatformServingBundle(bundle, verified); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func TestServingProjectionReuseRechecksLiveState(t *testing.T) {
	for _, change := range []string{"bundle", "index", "disk", "duplicate"} {
		t.Run(change, func(t *testing.T) {
			s, bundle, projection := servingMemoryFixture(t)
			verified, err := preparePlatformServingProjection(projection, s.Config.EdgeGroupID)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.validatePlatformServingBundle(bundle, verified); err != nil {
				t.Fatal(err)
			}
			switch change {
			case "bundle":
				s.bundle.Routes[0].UpstreamURL = "http://changed:8080"
			case "index":
				other := bundle
				other.Routes = append([]model.EdgeRouteBinding(nil), bundle.Routes...)
				other.Routes[0].UpstreamURL = "http://changed:8080"
				s.routeIndex.Store(buildEdgeRouteIndex(other, s.Config.EdgeGroupID, routePublicationMetadata{}))
			case "disk":
				cached := cacheFile{Version: cacheFileVersion, Bundle: bundle}
				cached.Bundle.Signature = "corrupted-with-same-version"
				raw, _ := json.Marshal(cached)
				if err = os.WriteFile(s.Config.CachePath, raw, 0600); err != nil {
					t.Fatal(err)
				}
			case "duplicate":
				s.bundle.Routes[1] = s.bundle.Routes[0]
			}
			if err = s.validatePlatformServingBundle(bundle, verified); err == nil {
				t.Fatal("changed live state reused positive evidence")
			}
		})
	}
}
