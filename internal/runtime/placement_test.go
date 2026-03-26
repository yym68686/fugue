package runtime

import (
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestSchedulingForRuntimeManagedOwnedKeepsRuntimeSpecificConstraints(t *testing.T) {
	t.Parallel()

	got := SchedulingForRuntime(model.Runtime{
		ID:       "runtime_owned",
		TenantID: "tenant_demo",
		Type:     model.RuntimeTypeManagedOwned,
	})

	wantSelector := map[string]string{
		RuntimeIDLabelKey: "runtime_owned",
		TenantIDLabelKey:  "tenant_demo",
	}
	if !reflect.DeepEqual(got.NodeSelector, wantSelector) {
		t.Fatalf("expected node selector %v, got %v", wantSelector, got.NodeSelector)
	}
	if len(got.Tolerations) != 1 {
		t.Fatalf("expected one toleration, got %d", len(got.Tolerations))
	}
	if got.Tolerations[0].Key != TenantTaintKey || got.Tolerations[0].Value != "tenant_demo" {
		t.Fatalf("unexpected managed-owned toleration: %#v", got.Tolerations[0])
	}
}

func TestPlacementNodeSelectorForLabelsNormalizesLocationAliases(t *testing.T) {
	t.Parallel()

	got := PlacementNodeSelectorForLabels(map[string]string{
		"region":       "ap-northeast-1",
		"zone":         "ap-northeast-1a",
		"country_code": "JP",
	})

	want := map[string]string{
		RegionLabelKey:              "ap-northeast-1",
		ZoneLabelKey:                "ap-northeast-1a",
		LocationCountryCodeLabelKey: "jp",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected placement selector %v, got %v", want, got)
	}
}

func TestSchedulingForRuntimeManagedSharedUsesPlacementLabelsOnlyWhenPresent(t *testing.T) {
	t.Parallel()

	withoutPlacement := SchedulingForRuntime(model.Runtime{
		ID:   "runtime_shared_default",
		Type: model.RuntimeTypeManagedShared,
	})
	if len(withoutPlacement.NodeSelector) != 0 || len(withoutPlacement.Tolerations) != 0 {
		t.Fatalf("expected unlabeled managed-shared runtime to stay unconstrained, got %#v", withoutPlacement)
	}

	withPlacement := SchedulingForRuntime(model.Runtime{
		ID:   "runtime_shared_tokyo",
		Type: model.RuntimeTypeManagedShared,
		Labels: map[string]string{
			RegionLabelKey: "ap-northeast-1",
		},
	})
	wantSelector := map[string]string{
		RegionLabelKey: "ap-northeast-1",
	}
	if !reflect.DeepEqual(withPlacement.NodeSelector, wantSelector) {
		t.Fatalf("expected shared runtime placement selector %v, got %v", wantSelector, withPlacement.NodeSelector)
	}
	if len(withPlacement.Tolerations) != 0 {
		t.Fatalf("expected no tolerations for managed-shared placement, got %#v", withPlacement.Tolerations)
	}
}
