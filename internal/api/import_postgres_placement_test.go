package api

import (
	"fugue/internal/model"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
)

func TestSelectImportedPostgresStorageClassUsesTargetCapacityAndPolicy(t *testing.T) {
	zero, available, small := resource.MustParse("0"), resource.MustParse("20Gi"), resource.MustParse("512Mi")
	target := &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/hostname": "node-target"}}
	other := &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/hostname": "node-other"}}
	for _, tc := range []struct {
		name             string
		defaultCapacity  *resource.Quantity
		fallbackTopology *metav1.LabelSelector
		approved         bool
		want             string
	}{
		{"exhausted default", &zero, target, true, "replicated"},
		{"insufficient default", &small, target, true, "replicated"},
		{"default available", &available, target, true, "local"},
		{"default unknown", nil, target, true, "local"},
		{"fallback other node", &zero, other, true, "local"},
		{"fallback unapproved", &zero, target, false, "local"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := selectImportedPostgresStorageClass("local", map[string]string{"kubernetes.io/hostname": "node-target"}, []importStorageCapacity{{StorageClassName: "local", Capacity: tc.defaultCapacity, NodeTopology: target}, {StorageClassName: "replicated", Capacity: &available, NodeTopology: tc.fallbackTopology}}, map[string]bool{"replicated": tc.approved}, resource.MustParse("1Gi"))
			if got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}
func TestImportedPostgresExplicitStorageClassDoesNotQueryCluster(t *testing.T) {
	s := &Server{newClusterNodeClient: func() (*clusterNodeClient, error) { t.Fatal("explicit storage must not be changed"); return nil, nil }}
	spec := &model.AppPostgresSpec{StorageClassName: "explicit"}
	s.applyNewImportedPostgresPlacement(spec, "runtime-example")
	if spec.StorageClassName != "explicit" {
		t.Fatal("changed explicit storage")
	}
}
