package model

import "testing"

func TestAppSpecDeepCopyDetachesNestedCollectionsWithoutNormalization(t *testing.T) {
	spec := AppSpec{Image: " image ", Command: []string{"run"}, Env: map[string]string{"A": "1"},
		NetworkPolicy:     &AppNetworkPolicySpec{Egress: &AppNetworkPolicyDirectionSpec{AllowApps: []AppNetworkPolicyAppPeer{{AppID: "b", Ports: []int{80}}}}},
		Data:              &AppDataMaterializationSpec{Workspaces: []AppDataWorkspaceMaterialization{{Assets: []string{"a"}}}},
		PersistentStorage: &AppPersistentStorageSpec{Mounts: []AppPersistentStorageMount{{Path: "/data"}}},
		Continuity:        &AppContinuityPolicy{ZeroDowntime: &AppZeroDowntimePolicy{Enabled: true, Canary: &AppRolloutCanarySpec{StepWeights: []int{1, 5}}}},
	}
	copy := spec.DeepCopy()
	if copy.Image != spec.Image {
		t.Fatalf("deep copy normalized image: %q", copy.Image)
	}
	copy.Command[0] = "changed"
	copy.Env["A"] = "2"
	copy.NetworkPolicy.Egress.AllowApps[0].Ports[0] = 443
	copy.Data.Workspaces[0].Assets[0] = "changed"
	copy.PersistentStorage.Mounts[0].Path = "/other"
	copy.Continuity.ZeroDowntime.Canary.StepWeights[0] = 99
	if spec.Command[0] != "run" || spec.Env["A"] != "1" || spec.NetworkPolicy.Egress.AllowApps[0].Ports[0] != 80 || spec.Data.Workspaces[0].Assets[0] != "a" || spec.PersistentStorage.Mounts[0].Path != "/data" || spec.Continuity.ZeroDowntime.Canary.StepWeights[0] != 1 {
		t.Fatal("mutating copy changed source spec")
	}
}
