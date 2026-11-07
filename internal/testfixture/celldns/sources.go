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
	return sourceAuthorizedRequestInputs(t, transition, nil, nil)
}

// CompletePublication retains the actual normalized compile results and pinned
// producer source artifacts used to build this synthetic graph. Database tests
// can persist those dependencies rather than forge pins or drop signed edges.
func CompletePublication(t testing.TB, transition, authorized bool) (req platformconfig.CompileRequest, policies []model.PlatformArtifact, releases []model.PlatformArtifactRelease, inputs []platformconfig.CompileResult, sources []model.PlatformArtifact) {
	t.Helper()
	if authorized {
		req, policies, releases = sourceAuthorizedRequestInputs(t, transition, &inputs, &sources)
	} else if transition {
		req = transitionRequestInputs(t, false, nil, &inputs)
	} else {
		req = requestWithInputs(t, &inputs)
	}
	return
}

func sourceAuthorizedRequestInputs(t testing.TB, transition bool, inputs *[]platformconfig.CompileResult, sources *[]model.PlatformArtifact) (platformconfig.CompileRequest, []model.PlatformArtifact, []model.PlatformArtifactRelease) {
	t.Helper()
	var req platformconfig.CompileRequest
	if transition {
		req = transitionRequestInputs(t, false, nil, inputs)
	} else {
		req = requestWithInputs(t, inputs)
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
		if sources != nil {
			// Real signed source objects replace the historical fixture's hash
			// placeholders; existing callers keep their original fixture shape.
			var source *platformconfig.CompileResult
			for i := range *inputs {
				c := &(*inputs)[i]
				if c.Lineage.InputSnapshotDigest == parent.Metadata["input_snapshot_digest"] && c.PolicyArtifact.Scope.Key == parent.ScopeKey {
					source = c
				}
			}
			if source == nil {
				t.Fatal("compiled producer source unavailable")
			}
			static := Sign(t, source.IntentArtifact, policy.StaticIntentArtifactID, 1)
			projection := Sign(t, source.PolicyArtifact, policy.DNSPolicyArtifactID, 1)
			source.IntentArtifact, source.PolicyArtifact = static, projection
			policy.StaticIntentDigest, policy.DNSPolicyDigest = static.ContentHash, projection.ContentHash
			*sources = append(*sources, static, projection)
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
