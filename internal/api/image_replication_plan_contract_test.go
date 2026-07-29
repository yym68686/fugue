package api

import (
	"testing"

	"fugue/internal/model"
)

func TestImageReplicationPlanArtifactRequiresVersionedEnvelope(t *testing.T) {
	t.Parallel()

	for name, test := range map[string]struct {
		content map[string]any
		pass    bool
	}{
		"v1 envelope": {
			content: map[string]any{
				"apiVersion": model.ImagePlaneAPIVersionV1,
				"kind":       model.ImageReplicationPlanKind,
			},
			pass: true,
		},
		"additive v1 field": {
			content: map[string]any{
				"apiVersion": model.ImagePlaneAPIVersionV1,
				"kind":       model.ImageReplicationPlanKind,
				"metadata":   map[string]any{"source": "shadow"},
			},
			pass: true,
		},
		"missing version": {content: map[string]any{"kind": "ImageReplicationPlan"}},
		"wrong version": {
			content: map[string]any{"apiVersion": "image-plane.fugue.dev/v2", "kind": "ImageReplicationPlan"},
		},
		"wrong kind": {
			content: map[string]any{"apiVersion": "image-plane.fugue.dev/v1", "kind": "ImageInventory"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			result := platformArtifactInvariantValidation(model.PlatformArtifact{
				ArtifactKind: model.PlatformArtifactKindImageReplicationPlan,
				Content:      test.content,
			})
			if result.Pass != test.pass {
				t.Fatalf("versioned image plan validation pass=%t, want %t: %+v", result.Pass, test.pass, result)
			}
		})
	}
}
