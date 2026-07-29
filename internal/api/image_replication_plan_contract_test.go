package api

import (
	"testing"

	"fugue/internal/model"
)

func TestImageReplicationPlanArtifactRequiresVersionedEnvelope(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		content map[string]any
		scope   model.PlatformArtifactScope
		pass    bool
	}{
		"v1 envelope": {
			content: map[string]any{
				"apiVersion": model.ImagePlaneAPIVersionV1,
				"kind":       model.ImageReplicationPlanKind,
				"spec":       map[string]any{"nodeID": "worker-a", "images": []any{}},
			},
			scope: model.PlatformArtifactScope{ScopeType: "node", Key: "node:worker-a", NodeID: "worker-a"},
			pass:  true,
		},
		"additive v1 field": {
			content: map[string]any{
				"apiVersion": model.ImagePlaneAPIVersionV1,
				"kind":       model.ImageReplicationPlanKind,
				"spec":       map[string]any{"nodeID": "worker-a", "images": []any{}},
				"metadata":   map[string]any{"source": "shadow"},
			},
			scope: model.PlatformArtifactScope{ScopeType: "node", Key: "node:worker-a", NodeID: "worker-a"},
			pass:  true,
		},
		"missing version": {
			content: map[string]any{"kind": "ImageReplicationPlan", "spec": map[string]any{"nodeID": "worker-a", "images": []any{}}},
			scope:   model.PlatformArtifactScope{ScopeType: "node", Key: "node:worker-a", NodeID: "worker-a"},
		},
		"wrong version": {
			content: map[string]any{"apiVersion": "image-plane.fugue.dev/v2", "kind": "ImageReplicationPlan", "spec": map[string]any{"nodeID": "worker-a", "images": []any{}}},
			scope:   model.PlatformArtifactScope{ScopeType: "node", Key: "node:worker-a", NodeID: "worker-a"},
		},
		"wrong kind": {
			content: map[string]any{"apiVersion": "image-plane.fugue.dev/v1", "kind": "ImageInventory", "spec": map[string]any{"nodeID": "worker-a", "images": []any{}}},
			scope:   model.PlatformArtifactScope{ScopeType: "node", Key: "node:worker-a", NodeID: "worker-a"},
		},
		"missing spec": {
			content: map[string]any{"apiVersion": model.ImagePlaneAPIVersionV1, "kind": model.ImageReplicationPlanKind},
			scope:   model.PlatformArtifactScope{ScopeType: "node", Key: "node:worker-a", NodeID: "worker-a"},
		},
		"cross-node payload": {
			content: map[string]any{"apiVersion": model.ImagePlaneAPIVersionV1, "kind": model.ImageReplicationPlanKind, "spec": map[string]any{"nodeID": "worker-b", "images": []any{}}},
			scope:   model.PlatformArtifactScope{ScopeType: "node", Key: "node:worker-a", NodeID: "worker-a"},
		},
		"images is not array": {
			content: map[string]any{"apiVersion": model.ImagePlaneAPIVersionV1, "kind": model.ImageReplicationPlanKind, "spec": map[string]any{"nodeID": "worker-a", "images": map[string]any{}}},
			scope:   model.PlatformArtifactScope{ScopeType: "node", Key: "node:worker-a", NodeID: "worker-a"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			result := platformArtifactInvariantValidation(model.PlatformArtifact{
				ArtifactKind: model.PlatformArtifactKindImageReplicationPlan,
				Scope:        test.scope,
				ScopeKey:     test.scope.Key,
				Content:      test.content,
			})
			if result.Pass != test.pass {
				t.Fatalf("versioned image plan validation pass=%t, want %t: %+v", result.Pass, test.pass, result)
			}
		})
	}
}
