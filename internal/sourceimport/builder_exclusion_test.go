package sourceimport

import (
	"testing"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestBuilderExplicitExclusionOverridesSharedFallback(t *testing.T) {
	t.Parallel()
	policy := defaultBuilderPodPolicy()
	demand, err := builderDemandForProfile(policy, builderWorkloadProfileHeavy)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{runtime.BuildNodeLabelKey, runtime.BuilderRoleLabelKey, "example.test/build-permission"} {
		t.Run(key, func(t *testing.T) {
			current := policy
			if key == "example.test/build-permission" {
				current.BuildNodeLabelKey = key
			}
			isolated := builderTestNode("storage-node", "storage-node", current, "8000m", "32Gi", "120Gi", "500m", "1Gi", "2Gi")
			isolated.Labels[runtime.SharedPoolLabelKey] = runtime.SharedPoolLabelValue
			isolated.Labels[runtime.NodeModeLabelKey] = model.RuntimeTypeManagedOwned
			isolated.Labels[key] = "false"
			spare := builderTestNode("build-node", "build-node", current, "4000m", "8Gi", "30Gi", "250m", "1Gi", "2Gi")
			got := selectBuilderCandidates(current, builderWorkloadProfileHeavy, demand, []builderNodeSnapshot{isolated, spare}, nil, nil)
			if len(got) != 1 || got[0].Node.Name != spare.Name {
				t.Fatalf("explicit exclusion ignored: %+v", got)
			}
		})
	}
}
