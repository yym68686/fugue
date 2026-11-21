package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/model/edgecohort"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestManagedEdgeCapabilityRejectsOldOrIncompleteGuardian(t *testing.T) {
	replicas := int32(1)
	deployment := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Generation: 2, Annotations: map[string]string{}}, Spec: appsv1.DeploymentSpec{Replicas: &replicas}, Status: appsv1.DeploymentStatus{ObservedGeneration: 2, ReadyReplicas: 1, UpdatedReplicas: 1}}
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("preflight performed mutation")
		}
		_ = json.NewEncoder(w).Encode(deployment)
	}))
	defer kube.Close()
	s := &Server{controlPlaneNamespace: "fixture-system", newClusterNodeClient: func() (*clusterNodeClient, error) {
		return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL}, nil
	}}
	if s.validateManagedEdgeCohortCapability(context.Background()) == nil {
		t.Fatal("old Guardian accepted")
	}
	deployment.Annotations[edgecohort.CapabilityAnnotation] = edgecohort.CapabilityV1
	if err := s.validateManagedEdgeCohortCapability(context.Background()); err != nil {
		t.Fatal(err)
	}
	deployment.Status.UpdatedReplicas = 0
	if s.validateManagedEdgeCohortCapability(context.Background()) == nil {
		t.Fatal("incomplete rollout accepted")
	}
}

func TestEdgeAdmissionRespectsAndReleasesAuthorityLease(t *testing.T) {
	other := "another-release"
	seconds := int32(60)
	now := metav1.NewMicroTime(time.Now().UTC())
	lease := coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{UID: "fixture-uid", ResourceVersion: "1"}, Spec: coordinationv1.LeaseSpec{HolderIdentity: &other, RenewTime: &now, LeaseDurationSeconds: &seconds}}
	writes := 0
	fenceStatus := http.StatusNotFound
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/configmaps/") {
			if r.Method != http.MethodGet || lease.Spec.HolderIdentity == nil || !strings.HasPrefix(*lease.Spec.HolderIdentity, "edge-admission") {
				t.Error("journal must be read under the admission lease")
			}
			if strings.Contains(r.URL.Path, "transition-prepared-") && fenceStatus == http.StatusOK {
				_, _ = w.Write([]byte(`{"metadata":{"labels":{"fugue.pro/authority-kind":"front-repair-journal"}}}`))
				return
			}
			if fenceStatus != http.StatusOK {
				w.WriteHeader(fenceStatus)
				return
			}
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Method == "PUT" {
			writes++
			var next coordinationv1.Lease
			if json.NewDecoder(r.Body).Decode(&next) != nil || next.ResourceVersion != lease.ResourceVersion {
				w.WriteHeader(409)
				return
			}
			lease = next
			lease.ResourceVersion += "x"
		}
		_ = json.NewEncoder(w).Encode(lease)
	}))
	defer kube.Close()
	c := &clusterNodeClient{client: kube.Client(), baseURL: kube.URL}
	if _, err := acquireEdgeEnrollmentLease(context.Background(), c, "fixture-system", "edge-group-fixture"); err == nil || writes != 0 {
		t.Fatal("active release lock was changed")
	}
	empty := ""
	lease.Spec.HolderIdentity = &empty
	for _, status := range []int{http.StatusOK, http.StatusForbidden} {
		fenceStatus = status
		before := writes
		if release, err := acquireEdgeEnrollmentLease(context.Background(), c, "fixture-system", "edge-group-fixture"); err == nil || release != nil {
			t.Fatalf("journal response status %d did not block membership change", status)
		}
		if writes-before != 2 || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "" {
			t.Fatalf("lease leaked after blocking admission: journal status %d", status)
		}
	}
	fenceStatus, writes = http.StatusNotFound, 0
	release, err := acquireEdgeEnrollmentLease(context.Background(), c, "fixture-system", "edge-group-fixture")
	if err != nil {
		t.Fatal(err)
	}
	if err = release(); err != nil {
		t.Fatal(err)
	}
	if writes != 2 || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "" {
		t.Fatal("lease was not released")
	}
}
