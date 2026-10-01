package celldns

import (
	"encoding/json"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func SourceAuthorizedRequest(t testing.TB, transition bool) (platformconfig.CompileRequest, []model.PlatformArtifact, []model.PlatformArtifactRelease) {
	t.Helper()
	req := Request(t)
	if transition {
		req = TransitionRequest(t)
	}
	var artifacts []model.PlatformArtifact
	var releases []model.PlatformArtifactRelease
	bind := func(parent *model.PlatformArtifact) *model.PlatformPublicationPrecondition {
		name := strings.ReplaceAll(parent.ScopeKey, ":", "-")
		scope, err := platformproducer.PolicyScopeForTarget(parent.ScopeKey)
		if err != nil {
			t.Fatal(err)
		}
		policy := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "producer-" + name, Mode: "serving", InputSource: "business-static-intent", TargetScope: parent.ScopeKey, IntervalSeconds: 30, RefreshSeconds: 120, StaticIntentArtifactID: "static-" + name, StaticIntentDigest: "sha256:" + strings.Repeat("a", 64), DNSPolicyArtifactID: "projection-" + name, DNSPolicyDigest: "sha256:" + strings.Repeat("b", 64), RequireApplicationDomains: true, RequireRouteDefaults: true, Serving: &platformproducer.ServingPolicy{CanaryRuleRef: "cohort=complete", GrayMinSeconds: 1, FullMinSeconds: 1, RolloutTimeoutSeconds: 60}}
		if parent.ScopeKey == "global" {
			policy.RequireDNSQueryPolicy = true
		} else {
			policy.AuthorityCellID = strings.TrimPrefix(parent.ScopeKey, "authority-cell:")
			policy.PublicationRole = platformconfig.PublicationRoleCellRoutes
		}
		raw, _ := json.Marshal(policy)
		var content map[string]any
		json.Unmarshal(raw, &content)
		artifact := Sign(t, model.PlatformArtifact{SchemaVersion: model.PlatformArtifactSchemaVersionV1, ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Scope: model.PlatformArtifactScope{Key: scope}, Generation: policy.Generation, Content: content}, "producer-policy-"+name, 1)
		if _, err := platformproducer.Decode(artifact); err != nil {
			t.Fatal(err)
		}
		release := model.PlatformArtifactRelease{ID: "producer-release-" + name, ArtifactID: artifact.ID, ArtifactKind: artifact.ArtifactKind, ScopeKey: scope, Generation: artifact.Generation, ReleaseChannel: "shadow", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: req.CreatedAt, LaneKey: platformsafety.ReleaseLaneKey(artifact.ArtifactKind, scope, "shadow")}
		artifacts, releases = append(artifacts, artifact), append(releases, release)
		parent.Metadata[platformconfig.ProducerPolicyReleaseMetadata] = release.ID
		*parent = Sign(t, *parent, parent.ID, parent.GenerationSequence)
		req.Intent.DNSRouteSources = append(req.Intent.DNSRouteSources, platformconfig.DNSRouteSourceAuthorization{ScopeKey: parent.ScopeKey, PolicyArtifactID: artifact.ID, PolicyDigest: artifact.ContentHash})
		return &model.PlatformPublicationPrecondition{ArtifactID: artifact.ID, ContentHash: artifact.ContentHash, ReleaseID: release.ID, FencingToken: release.FencingToken}
	}
	for i := range req.CellRoutePublications {
		p := &req.CellRoutePublications[i]
		p.ProducerPolicy = bind(&p.Parent)
	}
	if req.PreviousTrafficPublication != nil {
		p := req.PreviousTrafficPublication
		p.ProducerPolicy = bind(&p.Parent)
	}
	return req, artifacts, releases
}
