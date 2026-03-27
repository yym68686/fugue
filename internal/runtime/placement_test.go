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

func TestSchedulingForRuntimeManagedOwnedSharedPoolDropsTenantTaint(t *testing.T) {
	t.Parallel()

	got := SchedulingForRuntime(model.Runtime{
		ID:       "runtime_owned",
		TenantID: "tenant_demo",
		Type:     model.RuntimeTypeManagedOwned,
		PoolMode: model.RuntimePoolModeInternalShared,
	})

	wantSelector := map[string]string{
		RuntimeIDLabelKey: "runtime_owned",
		TenantIDLabelKey:  "tenant_demo",
	}
	if !reflect.DeepEqual(got.NodeSelector, wantSelector) {
		t.Fatalf("expected node selector %v, got %v", wantSelector, got.NodeSelector)
	}
	if len(got.Tolerations) != 0 {
		t.Fatalf("expected shared-pool managed-owned runtime to drop tenant tolerations, got %#v", got.Tolerations)
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
	wantDefaultSelector := map[string]string{
		SharedPoolLabelKey: SharedPoolLabelValue,
	}
	if !reflect.DeepEqual(withoutPlacement.NodeSelector, wantDefaultSelector) || len(withoutPlacement.Tolerations) != 0 {
		t.Fatalf("expected unlabeled managed-shared runtime to require shared-pool selector, got %#v", withoutPlacement)
	}

	withPlacement := SchedulingForRuntime(model.Runtime{
		ID:   "runtime_shared_tokyo",
		Type: model.RuntimeTypeManagedShared,
		Labels: map[string]string{
			RegionLabelKey: "ap-northeast-1",
		},
	})
	wantSelector := map[string]string{
		SharedPoolLabelKey: SharedPoolLabelValue,
		RegionLabelKey:     "ap-northeast-1",
	}
	if !reflect.DeepEqual(withPlacement.NodeSelector, wantSelector) {
		t.Fatalf("expected shared runtime placement selector %v, got %v", wantSelector, withPlacement.NodeSelector)
	}
	if len(withPlacement.Tolerations) != 0 {
		t.Fatalf("expected no tolerations for managed-shared placement, got %#v", withPlacement.Tolerations)
	}
}

func TestJoinNodeLabelsAndTaintsReflectSharedPoolMembership(t *testing.T) {
	t.Parallel()

	dedicated := model.Runtime{
		ID:       "runtime_owned",
		TenantID: "tenant_demo",
		Type:     model.RuntimeTypeManagedOwned,
	}
	if got := JoinNodeLabels(dedicated); !reflect.DeepEqual(got, []string{
		RuntimeIDLabelKey + "=runtime_owned",
		TenantIDLabelKey + "=tenant_demo",
		NodeModeLabelKey + "=" + model.RuntimeTypeManagedOwned,
	}) {
		t.Fatalf("unexpected dedicated join labels: %#v", got)
	}
	if got := JoinNodeTaints(dedicated); !reflect.DeepEqual(got, []string{
		TenantTaintKey + "=tenant_demo:NoSchedule",
	}) {
		t.Fatalf("unexpected dedicated join taints: %#v", got)
	}

	shared := dedicated
	shared.PoolMode = model.RuntimePoolModeInternalShared
	if got := JoinNodeLabels(shared); !reflect.DeepEqual(got, []string{
		RuntimeIDLabelKey + "=runtime_owned",
		TenantIDLabelKey + "=tenant_demo",
		NodeModeLabelKey + "=" + model.RuntimeTypeManagedOwned,
		SharedPoolLabelKey + "=" + SharedPoolLabelValue,
	}) {
		t.Fatalf("unexpected shared-pool join labels: %#v", got)
	}
	if got := JoinNodeTaints(shared); len(got) != 0 {
		t.Fatalf("expected shared-pool join taints to be empty, got %#v", got)
	}
}
