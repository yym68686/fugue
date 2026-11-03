package runtime

import (
	"encoding/json"
	"reflect"
	"testing"

	"fugue/internal/model"
)

func TestAppNodeLossPolicyOnlyChangesExplicitAppPodTolerations(t *testing.T) {
	zero := int64(0)
	custom := int64(42)
	base := SchedulingConstraints{NodeSelector: map[string]string{"pool": "authorized"}, Tolerations: []Toleration{{Key: "tenant", Operator: "Equal", Value: "example", Effect: "NoSchedule"}, {Key: "node.kubernetes.io/unreachable", Operator: "Exists", Effect: "NoExecute", TolerationSeconds: &custom}}}
	for _, tc := range []struct {
		name    string
		policy  *model.AppNodeLossPolicy
		seconds *int64
	}{
		{"wait", &model.AppNodeLossPolicy{Mode: model.AppNodeLossWait}, nil},
		{"replace-default", &model.AppNodeLossPolicy{Mode: model.AppNodeLossReplace}, func() *int64 { x := int64(300); return &x }()},
		{"replace-zero", &model.AppNodeLossPolicy{Mode: model.AppNodeLossReplace, TolerationSeconds: &zero}, &zero},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := model.App{ID: "app_example", TenantID: "tenant_example", Name: "service", Spec: model.AppSpec{Image: "registry.example/service:one", Replicas: 1, NodeLoss: tc.policy}}
			obj := buildAppDeploymentObject("tenant_example", app, nil, base, nil)
			raw, _ := json.Marshal(obj)
			var deployment struct {
				Spec struct {
					Replicas int `json:"replicas"`
					Template struct {
						Spec struct {
							NodeSelector map[string]string `json:"nodeSelector"`
							Tolerations  []Toleration      `json:"tolerations"`
						} `json:"spec"`
					} `json:"template"`
				} `json:"spec"`
			}
			if err := json.Unmarshal(raw, &deployment); err != nil {
				t.Fatal(err)
			}
			if deployment.Spec.Replicas != 1 || !reflect.DeepEqual(deployment.Spec.Template.Spec.NodeSelector, base.NodeSelector) {
				t.Fatal("node loss policy changed authorized capacity/placement")
			}
			tols := deployment.Spec.Template.Spec.Tolerations
			if len(tols) != 3 || !reflect.DeepEqual(tols[0], base.Tolerations[0]) {
				t.Fatal("unrelated tenant tolerations changed", tols)
			}
			for _, tol := range tols[1:] {
				if tol.Effect != "NoExecute" || !reflect.DeepEqual(tol.TolerationSeconds, tc.seconds) {
					t.Fatal("wrong node eviction timeout", tol)
				}
			}
			if len(base.Tolerations) != 2 || *base.Tolerations[1].TolerationSeconds != 42 {
				t.Fatal("render mutated shared scheduling")
			}
		})
	}
	for _, policy := range []*model.AppNodeLossPolicy{nil, {Mode: model.AppNodeLossLegacy}} {
		if !reflect.DeepEqual(AppNodeLossScheduling(policy, base), base) {
			t.Fatal("legacy workloads changed during upgrade")
		}
	}
	clone := cloneSchedulingConstraints(base)
	*clone.Tolerations[1].TolerationSeconds = 0
	if *base.Tolerations[1].TolerationSeconds != 42 {
		t.Fatal("scheduling clone aliases timeout")
	}
}
