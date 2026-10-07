package api

import (
	"context"
	"encoding/json"
	"fugue/internal/model"
	corev1 "k8s.io/api/core/v1"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLocalPVControlPlaneCountsBoundClaimEvenWithoutHostLV(t *testing.T) {
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/nodes/worker" {
			w.Write([]byte(`{"metadata":{"name":"worker","labels":{"kubernetes.io/hostname":"worker"}}}`))
			return
		}
		if r.URL.Path == "/api/v1/persistentvolumes" {
			w.Write([]byte(`{"items":[{"metadata":{"name":"pvc-unreported"},"spec":{"csi":{"driver":"local.csi.openebs.io","volumeAttributes":{"volgroup":"vg"}},"claimRef":{"name":"data","namespace":"app"},"nodeAffinity":{"required":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["worker"]}]}]}}},"status":{"phase":"Bound"}}]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer kube.Close()
	s := &Server{newClusterNodeClient: func() (*clusterNodeClient, error) {
		return &clusterNodeClient{baseURL: kube.URL, client: kube.Client()}, nil
	}}
	in := model.LocalPVInventory{ClusterNodeName: "worker", VGName: "vg", BoundPVCount: -1, UnsafeReasons: []string{"kubectl_pv_unavailable", "lvm_tools_unavailable_or_vg_missing"}}
	s.enrichLocalPVBindings(context.Background(), &in)
	if !in.BoundPVCountKnown || in.BoundPVCount != 1 || len(in.BoundPVCRefs) != 1 || containsString(in.UnsafeReasons, "kubectl_pv_unavailable") {
		t.Fatalf("incorrect enrichment: %+v", in)
	}
	if !containsString(in.UnsafeReasons, "lvm_tools_unavailable_or_vg_missing") {
		t.Fatal("enrichment erased host failure")
	}
}
func TestLocalPVAffinityUncertaintyCannotProveExclusion(t *testing.T) {
	var pv corev1.PersistentVolume
	var node corev1.Node
	json.Unmarshal([]byte(`{"metadata":{"name":"worker"}}`), &node)
	json.Unmarshal([]byte(`{"spec":{"nodeAffinity":{"required":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"unknown","operator":"In","values":["zone"]}]}]}}}}`), &pv)
	if !localPVPossiblyTargetsNode(pv, node) {
		t.Fatal("unknown affinity hid binding")
	}
}
func TestLocalPVFailedReadCannotBecomeKnownZero(t *testing.T) {
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unavailable", 503) }))
	defer kube.Close()
	s := &Server{newClusterNodeClient: func() (*clusterNodeClient, error) {
		return &clusterNodeClient{baseURL: kube.URL, client: kube.Client()}, nil
	}}
	in := model.LocalPVInventory{ClusterNodeName: "worker", BoundPVCount: 0, BoundPVCountKnown: true}
	s.enrichLocalPVBindings(context.Background(), &in)
	if in.BoundPVCount >= 0 || in.BoundPVCountKnown {
		t.Fatalf("read failure invented zero: %+v", in)
	}
}
