package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInspectManagedPostgresResizeCapabilityProvesDiscoveryAndAuthorization(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		switch requests {
		case 1:
			if r.Method != http.MethodGet || r.URL.Path != kubeCoreV1DiscoveryPath {
				t.Fatalf("unexpected discovery request %s %s", r.Method, r.URL.Path)
			}
			_, _ = w.Write([]byte(`{"groupVersion":"v1","resources":[{"name":"pods","namespaced":true,"kind":"Pod","verbs":["get","list"]},{"name":"pods/resize","namespaced":true,"kind":"Pod","verbs":["get","patch","update"]}]}`))
		case 2:
			if r.Method != http.MethodPost || r.URL.Path != kubeSelfSubjectAccessReviewPath {
				t.Fatalf("unexpected authorization request %s %s", r.Method, r.URL.Path)
			}
			if got := r.Header.Get("Content-Type"); got != "application/json" {
				t.Fatalf("unexpected authorization content type %q", got)
			}
			var review kubeSelfSubjectAccessReview
			if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
				t.Fatalf("decode authorization review: %v", err)
			}
			expected := managedPostgresResizeResourceAttributes("tenant-a")
			if review.APIVersion != "authorization.k8s.io/v1" || review.Kind != "SelfSubjectAccessReview" || review.Spec.ResourceAttributes != expected {
				t.Fatalf("unexpected authorization review: %+v", review)
			}
			if review.Status != nil {
				t.Fatalf("authorization request must omit status, got %+v", review.Status)
			}
			allowed := true
			review.Status = &kubeSubjectAccessReviewStatus{Allowed: &allowed}
			if err := json.NewEncoder(w).Encode(review); err != nil {
				t.Fatalf("encode authorization response: %v", err)
			}
		default:
			t.Fatalf("unexpected extra Kubernetes request %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)

	client := &kubeClient{client: server.Client(), baseURL: server.URL, bearerToken: "test", namespace: "controller-system"}
	capability, err := client.inspectManagedPostgresResizeCapability(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("inspect resize capability: %v", err)
	}
	if !capability.Available() || capability.Namespace != "tenant-a" || capability.Reason != "available" {
		t.Fatalf("unexpected resize capability: %+v", capability)
	}
	if requests != 2 {
		t.Fatalf("expected exact discovery and authorization requests, got %d", requests)
	}
}

func TestInspectManagedPostgresResizeCapabilityDoesNotReviewUnavailableResource(t *testing.T) {
	tests := []struct {
		name       string
		resource   string
		wantReason string
		wantFound  bool
	}{
		{
			name:       "subresource absent",
			resource:   `{"groupVersion":"v1","resources":[{"name":"pods","namespaced":true,"kind":"Pod","verbs":["get"]}]}`,
			wantReason: "resize_subresource_not_discovered",
		},
		{
			name:       "patch verb absent",
			resource:   `{"groupVersion":"v1","resources":[{"name":"pods/resize","namespaced":true,"kind":"Pod","verbs":["get","update"]}]}`,
			wantReason: "resize_patch_verb_not_discovered",
			wantFound:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				if r.Method != http.MethodGet || r.URL.Path != kubeCoreV1DiscoveryPath {
					t.Fatalf("unexpected Kubernetes request %s %s", r.Method, r.URL.Path)
				}
				_, _ = w.Write([]byte(tt.resource))
			}))
			t.Cleanup(server.Close)

			client := &kubeClient{client: server.Client(), baseURL: server.URL, bearerToken: "test", namespace: "tenant-a"}
			capability, err := client.inspectManagedPostgresResizeCapability(context.Background(), "")
			if err != nil {
				t.Fatalf("inspect resize capability: %v", err)
			}
			if capability.Available() || capability.Reason != tt.wantReason || capability.ResizeSubresourceDiscovered != tt.wantFound {
				t.Fatalf("unexpected unavailable capability: %+v", capability)
			}
			if requests != 1 {
				t.Fatalf("authorization review must not run for unavailable discovery; requests=%d", requests)
			}
		})
	}
}

func TestInspectManagedPostgresResizeCapabilityReportsAuthorizationDenial(t *testing.T) {
	server := newManagedPostgresResizeCapabilityServer(t, func(review *kubeSelfSubjectAccessReview) {
		allowed := false
		denied := true
		review.Status = &kubeSubjectAccessReviewStatus{
			Allowed: &allowed,
			Denied:  &denied,
			Reason:  "RBAC denied",
		}
	})
	t.Cleanup(server.Close)

	client := &kubeClient{client: server.Client(), baseURL: server.URL, bearerToken: "test", namespace: "tenant-a"}
	capability, err := client.inspectManagedPostgresResizeCapability(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("inspect denied resize capability: %v", err)
	}
	if capability.Available() || capability.PatchAuthorized || capability.Reason != "resize_patch_authorization_denied" || capability.Message != "RBAC denied" {
		t.Fatalf("unexpected denied capability: %+v", capability)
	}
}

func TestInspectManagedPostgresResizeCapabilityRejectsMalformedEvidence(t *testing.T) {
	tests := []struct {
		name      string
		discovery string
		mutate    func(*kubeSelfSubjectAccessReview)
		wantError string
	}{
		{
			name:      "wrong discovery group version",
			discovery: `{"groupVersion":"v2","resources":[]}`,
			wantError: "groupVersion",
		},
		{
			name:      "invalid resize resource shape",
			discovery: `{"groupVersion":"v1","resources":[{"name":"pods/resize","namespaced":false,"kind":"Pod","verbs":["patch"]}]}`,
			wantError: "discovery shape is invalid",
		},
		{
			name:      "duplicate resize resources",
			discovery: `{"groupVersion":"v1","resources":[{"name":"pods/resize","namespaced":true,"kind":"Pod","verbs":["patch"]},{"name":"pods/resize","namespaced":true,"kind":"Pod","verbs":["patch"]}]}`,
			wantError: "duplicate pods/resize",
		},
		{
			name: "missing allowed decision",
			mutate: func(review *kubeSelfSubjectAccessReview) {
				review.Status = &kubeSubjectAccessReviewStatus{}
			},
			wantError: "omitted status.allowed",
		},
		{
			name: "changed review attributes",
			mutate: func(review *kubeSelfSubjectAccessReview) {
				allowed := true
				review.Status = &kubeSubjectAccessReviewStatus{Allowed: &allowed}
				review.Spec.ResourceAttributes.Resource = "deployments"
			},
			wantError: "changed the reviewed resource attributes",
		},
		{
			name: "authorization evaluation error",
			mutate: func(review *kubeSelfSubjectAccessReview) {
				allowed := true
				review.Status = &kubeSubjectAccessReviewStatus{
					Allowed:         &allowed,
					EvaluationError: "authorizer unavailable",
				}
			},
			wantError: "evaluation failed",
		},
		{
			name: "contradictory decision",
			mutate: func(review *kubeSelfSubjectAccessReview) {
				allowed := true
				denied := true
				review.Status = &kubeSubjectAccessReviewStatus{Allowed: &allowed, Denied: &denied}
			},
			wantError: "contradictory",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			discovery := tt.discovery
			if discovery == "" {
				discovery = `{"groupVersion":"v1","resources":[{"name":"pods/resize","namespaced":true,"kind":"Pod","verbs":["patch"]}]}`
			}
			server := newManagedPostgresResizeCapabilityServerWithDiscovery(t, discovery, tt.mutate)
			t.Cleanup(server.Close)
			client := &kubeClient{client: server.Client(), baseURL: server.URL, bearerToken: "test", namespace: "tenant-a"}
			_, err := client.inspectManagedPostgresResizeCapability(context.Background(), "tenant-a")
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("expected error containing %q, got %v", tt.wantError, err)
			}
		})
	}
}

func newManagedPostgresResizeCapabilityServer(
	t *testing.T,
	mutate func(*kubeSelfSubjectAccessReview),
) *httptest.Server {
	t.Helper()
	return newManagedPostgresResizeCapabilityServerWithDiscovery(
		t,
		`{"groupVersion":"v1","resources":[{"name":"pods/resize","namespaced":true,"kind":"Pod","verbs":["get","patch","update"]}]}`,
		mutate,
	)
}

func newManagedPostgresResizeCapabilityServerWithDiscovery(
	t *testing.T,
	discovery string,
	mutate func(*kubeSelfSubjectAccessReview),
) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == kubeCoreV1DiscoveryPath:
			_, _ = w.Write([]byte(discovery))
		case r.Method == http.MethodPost && r.URL.Path == kubeSelfSubjectAccessReviewPath:
			var review kubeSelfSubjectAccessReview
			if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
				t.Fatalf("decode authorization review: %v", err)
			}
			if mutate != nil {
				mutate(&review)
			}
			if err := json.NewEncoder(w).Encode(review); err != nil {
				t.Fatalf("encode authorization review: %v", err)
			}
		default:
			t.Fatalf("unexpected Kubernetes request %s %s", r.Method, r.URL.Path)
		}
	}))
}
