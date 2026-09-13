package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/platformcontrol"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPlatformConsumerIdentityExchangesOnlyLiveBoundPods(t *testing.T) {
	t.Parallel()
	const ns = "platform-system-test"
	for _, tc := range []struct {
		name   string
		status int
		mutate func(*authenticationv1.SelfSubjectReview, *corev1.Pod)
	}{
		{"authorized", 200, nil},
		{"anonymous subject", 401, func(r *authenticationv1.SelfSubjectReview, _ *corev1.Pod) {
			r.Status.UserInfo = authenticationv1.UserInfo{}
		}},
		{"unbound legacy token", 401, func(r *authenticationv1.SelfSubjectReview, _ *corev1.Pod) { r.Status.UserInfo.Extra = nil }},
		{"tenant namespace", 403, func(r *authenticationv1.SelfSubjectReview, _ *corev1.Pod) {
			r.Status.UserInfo.Username = "system:serviceaccount:tenant-test:dns-consumer"
		}},
		{"replaced pod", 403, func(_ *authenticationv1.SelfSubjectReview, p *corev1.Pod) { p.UID = "replacement-pod" }},
		{"deleted pod", 403, func(_ *authenticationv1.SelfSubjectReview, p *corev1.Pod) {
			v := metav1.Now()
			p.DeletionTimestamp = &v
		}},
		{"terminated pod", 403, func(_ *authenticationv1.SelfSubjectReview, p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded }},
		{"unscheduled pod", 403, func(_ *authenticationv1.SelfSubjectReview, p *corev1.Pod) { p.Spec.NodeName = "" }},
		{"changed service account", 403, func(_ *authenticationv1.SelfSubjectReview, p *corev1.Pod) {
			p.Spec.ServiceAccountName = "another-account"
		}},
		{"no capability policy", 403, func(_ *authenticationv1.SelfSubjectReview, p *corev1.Pod) { p.Annotations = nil }},
		{"unknown policy fields", 403, func(_ *authenticationv1.SelfSubjectReview, p *corev1.Pod) {
			p.Annotations[platformConsumerIdentityAnnotation] = `{"version":"v1","component":"dns-server","scope_key":"global","artifact_kinds":["dns_answer_bundle"],"node_id":"forged-node"}`
		}},
		{"unknown capability", 403, func(_ *authenticationv1.SelfSubjectReview, p *corev1.Pod) {
			p.Annotations[platformConsumerIdentityAnnotation] = `{"version":"v1","component":"dns-server","scope_key":"global","artifact_kinds":["dns_answer_bundle","unrecognized"]}`
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
			server.controlPlaneNamespace = ns
			ring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "component-test-key", Keys: map[string]string{"component-test-key": "component-test-secret"}}
			server.auth.PlatformComponentIdentityKeyring = ring
			review := authenticationv1.SelfSubjectReview{Status: authenticationv1.SelfSubjectReviewStatus{UserInfo: authenticationv1.UserInfo{Username: "system:serviceaccount:" + ns + ":dns-consumer", UID: "account-uid", Extra: map[string]authenticationv1.ExtraValue{"authentication.kubernetes.io/pod-name": {"dns-pod"}, "authentication.kubernetes.io/pod-uid": {"pod-uid"}}}}}
			pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "dns-pod", Namespace: ns, UID: "pod-uid", Annotations: map[string]string{platformConsumerIdentityAnnotation: `{"version":"v1","component":"dns-server","scope_key":"global","artifact_kinds":["dns_answer_bundle"]}`}}, Spec: corev1.PodSpec{NodeName: "physical-node", ServiceAccountName: "dns-consumer"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
			if tc.mutate != nil {
				tc.mutate(&review, &pod)
			}
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/apis/authentication.k8s.io/v1/selfsubjectreviews":
					var req authenticationv1.SelfSubjectReview
					if r.Method != "POST" || json.NewDecoder(r.Body).Decode(&req) != nil || r.Header.Get("Authorization") != "Bearer pod-bound-token" || req.Kind != "SelfSubjectReview" {
						t.Error("review must authenticate with submitted Pod token only")
					}
					if r.Header.Get("Impersonate-User") != "" {
						t.Error("impersonation header forwarded")
					}
					json.NewEncoder(w).Encode(review)
				case "/api/v1/namespaces/" + ns + "/pods/dns-pod":
					if r.Header.Get("Authorization") != "Bearer kube-verifier-token" {
						t.Error("Pod lookup must use API metadata reader")
					}
					json.NewEncoder(w).Encode(pod)
				default:
					t.Errorf("unexpected Kubernetes request: %s", r.URL.Path)
					w.WriteHeader(404)
				}
			}))
			defer kube.Close()
			server.newClusterNodeClient = func() (*clusterNodeClient, error) {
				return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL, bearerToken: "kube-verifier-token"}, nil
			}
			response := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/identity", "pod-bound-token", map[string]any{"node_id": "forged-node", "component": "edge-worker", "scope_key": "other-scope"})
			if response.Code != tc.status {
				t.Fatalf("exchange: wanted %d got %d %s", tc.status, response.Code, response.Body.String())
			}
			if tc.status == 200 {
				var body struct {
					Token     string    `json:"token"`
					ExpiresAt time.Time `json:"expires_at"`
					NodeID    string    `json:"node_id"`
				}
				mustDecodeJSON(t, response, &body)
				claims, err := platformcontrol.ParsePlatformComponentIdentity(ring, body.Token, time.Now().UTC())
				if err != nil || body.NodeID != "physical-node" || claims.NodeID != body.NodeID || claims.Component != "dns-server" || claims.ScopeKey != "global" || claims.CredentialID != "kubernetes:"+ns+":dns-consumer:pod-uid" || len(claims.ArtifactKinds) != 1 || claims.ArtifactKinds[0] != "dns_answer_bundle" || claims.ExpiresAtUnix-claims.IssuedAtUnix != 120 {
					t.Fatalf("claims not derived from live Pod: %+v %v", claims, err)
				}
				if response.Header().Get("Cache-Control") != "no-store" || body.ExpiresAt.Unix() != claims.ExpiresAtUnix {
					t.Fatal("credential expiry/cache controls missing")
				}
			}
			consumers, err := state.ListPlatformConsumers("dns_answer_bundle", "global")
			if err != nil || len(consumers) != 0 {
				t.Fatal("credential issuance created runtime facts")
			}
		})
	}
}

func TestPlatformConsumerIdentityVerifierFailureDoesNotLeakTokens(t *testing.T) {
	_, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	for _, code := range []int{401, 403, 500} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
				w.Write([]byte("sensitive-review-token"))
			}))
			defer kube.Close()
			server.newClusterNodeClient = func() (*clusterNodeClient, error) {
				return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL}, nil
			}
			r := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/identity", "bound-token", nil)
			expected := 401
			if code == 500 {
				expected = 503
			}
			if r.Code != expected || strings.Contains(r.Body.String(), "sensitive-review-token") {
				t.Fatalf("verifier must fail closed without secrets: %d %s", r.Code, r.Body.String())
			}
		})
	}
	noHeader := performJSONRequest(t, server, http.MethodPost, "/v1/platform-state/consumers/identity?token=bound-token", "", nil)
	if noHeader.Code != 401 {
		t.Fatalf("query token accepted: %d", noHeader.Code)
	}
}
