package store

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func TestRetainedTrafficAdmissionFencesBaselineAndExpiry(t *testing.T) {
	now := time.Now().UTC()
	for _, scenario := range []string{"valid", "expired", "wrong source", "new full fence", "new LKG", "extended lease", "revoked source", "forged route", "foreign member"} {
		t.Run(scenario, func(t *testing.T) {
			s := New(filepath.Join(t.TempDir(), "state.json"))
			configureTestPlatformArtifactSigning(s)
			if err := s.Init(); err != nil {
				t.Fatal(err)
			}
			save := func(kind string, content map[string]any) model.PlatformArtifact {
				t.Helper()
				a, err := s.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: kind, Scope: model.PlatformArtifactScope{ScopeType: "global"}, Content: content})
				if err != nil {
					t.Fatal(err)
				}
				a, err = s.ValidatePlatformArtifact(a.ID, []model.PlatformArtifactValidationResult{{Name: "synthetic", Pass: true}})
				if err != nil {
					t.Fatal(err)
				}
				return a
			}
			route := platformconfig.CompiledRoute{RouteIntent: platformconfig.RouteIntent{TenantID: "tenant-a", AppID: "app-a", Hostname: "app.example.test", Enabled: true}}
			source := save(model.PlatformArtifactKindEdgeRouteBundle, map[string]any{"routes": []platformconfig.CompiledRoute{route}})
			sourceParent := save(model.PlatformArtifactKindReleaseSet, map[string]any{"artifact_ids": []any{source.ID}})
			baseline := model.PlatformArtifactRelease{ID: "full-old", FencingToken: 7}
			lkg := model.PlatformLKGSnapshot{ArtifactID: sourceParent.ID, ContentHash: sourceParent.ContentHash, ExpiresAt: now.Add(time.Hour)}
			part := platformconfig.RetainedTrafficPartition{TenantID: route.TenantID, AppID: route.AppID, Source: model.PlatformPublicationPrecondition{ArtifactID: lkg.ArtifactID, ContentHash: lkg.ContentHash, ReleaseID: baseline.ID, FencingToken: baseline.FencingToken}, RouteArtifactID: source.ID, RouteArtifactDigest: source.ContentHash, ExpiresAt: now.Add(time.Minute), Routes: []platformconfig.CompiledRoute{route}}
			switch scenario {
			case "expired":
				part.ExpiresAt = now
			case "wrong source":
				part.Source.ArtifactID = "foreign"
			case "new full fence":
				baseline.FencingToken++
			case "new LKG":
				lkg.ArtifactID = "parent-new"
			case "extended lease":
				part.ExpiresAt = lkg.ExpiresAt.Add(time.Second)
			case "revoked source":
				source.Status = model.PlatformArtifactStatusRejected
			case "forged route":
				part.Routes[0].Hostname = "forged.example.test"
			case "foreign member":
				source = save(model.PlatformArtifactKindEdgeRouteBundle, map[string]any{"routes": []platformconfig.CompiledRoute{route}, "foreign": true})
				part.RouteArtifactID, part.RouteArtifactDigest = source.ID, source.ContentHash
			}
			raw, _ := json.Marshal([]platformconfig.RetainedTrafficPartition{part})
			child := model.PlatformArtifact{ID: "routes-new", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, Metadata: map[string]string{platformconfig.RetainedTrafficPartitionsMetadata: string(raw)}}
			parent := model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: source.ScopeKey, Content: map[string]any{"artifact_ids": []any{child.ID}}}
			state := &model.State{PlatformArtifacts: []model.PlatformArtifact{child, source, sourceParent}}
			err := validateRetainedTrafficBaseline(state, parent, baseline, lkg, now, s.platformArtifactSigningKeyring())
			if scenario == "valid" && err != nil || scenario != "valid" && !errors.Is(err, ErrConflict) {
				t.Fatal("baseline gate", err)
			}
			if scenario == "expired" {
				if err := validateLeasedTrafficAdmission(state, parent, "full", "", s.platformArtifactSigningKeyring(), now); !errors.Is(err, ErrConflict) {
					t.Fatal("manual serving admission bypassed expiry", err)
				}
			}
			if err := validateLeasedTrafficAdmission(state, child, "full", "", s.platformArtifactSigningKeyring(), now); !errors.Is(err, ErrConflict) {
				t.Fatal("standalone child bypassed complete parent", err)
			}
		})
	}
}
