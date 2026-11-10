package api

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/store"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type pendingVerifierFunc func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error)

func (f pendingVerifierFunc) VerifyPendingRootSource(c context.Context, r inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
	return f(c, r)
}

type pendingWitnessFunc func(context.Context, inputarchive.PendingRootRequest) error

func (f pendingWitnessFunc) CheckPendingRootSource(c context.Context, r inputarchive.PendingRootRequest) error {
	return f(c, r)
}

type pendingAdmissionFunc func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootReservation, error)

func (f pendingAdmissionFunc) ReservePendingRoot(c context.Context, r inputarchive.PendingRootRequest) (inputarchive.PendingRootReservation, error) {
	return f(c, r)
}

type pendingIdentityFixture struct {
	a          PendingRootAuthority
	review     authenticationv1.TokenReview
	pod        corev1.Pod
	rs         appsv1.ReplicaSet
	deployment appsv1.Deployment
	account    corev1.ServiceAccount
}

func pendingDedicatedTestToken() string {
	return "eyJhbGciOiJFUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(`{"aud":["`+pendingRootAudience+`"]}`)) + ".c2ln"
}

func newPendingIdentityFixture() *pendingIdentityFixture {
	a := PendingRootAuthority{Owner: "edge-control/cell-alpha", Namespace: "platform-system-test", ServiceAccount: "neutral-producer", ServiceAccountUID: "account-uid", Deployment: "neutral-producer", DeploymentUID: "deployment-uid", SourceSHA: strings.Repeat("a", 40), ImageRef: "registry.test/edge-control@sha256:" + strings.Repeat("b", 64), PolicyRevision: "policy-1"}
	controller := true
	template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"fugue.io/edge-group-id": "cell-alpha"}, Annotations: map[string]string{"fugue.pro/oci-revision": a.SourceSHA}}, Spec: corev1.PodSpec{ServiceAccountName: a.ServiceAccount, Containers: []corev1.Container{{Name: "edge-control", Image: a.ImageRef}}}}
	selector := &metav1.LabelSelector{MatchLabels: map[string]string{"fugue.io/edge-group-id": "cell-alpha"}}
	f := &pendingIdentityFixture{a: a,
		review:     authenticationv1.TokenReview{Status: authenticationv1.TokenReviewStatus{Authenticated: true, Audiences: []string{pendingRootAudience}, User: authenticationv1.UserInfo{Username: "system:serviceaccount:" + a.Namespace + ":" + a.ServiceAccount, UID: a.ServiceAccountUID, Extra: map[string]authenticationv1.ExtraValue{"authentication.kubernetes.io/pod-name": {"producer-pod"}, "authentication.kubernetes.io/pod-uid": {"pod-uid"}}}}},
		pod:        corev1.Pod{ObjectMeta: *template.ObjectMeta.DeepCopy(), Spec: *template.Spec.DeepCopy(), Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "edge-control", ImageID: a.ImageRef}}}},
		rs:         appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "producer-rs", Namespace: a.Namespace, UID: "rs-uid", ResourceVersion: "10", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: a.Deployment, UID: types.UID(a.DeploymentUID), Controller: &controller}}}, Spec: appsv1.ReplicaSetSpec{Template: *template.DeepCopy(), Selector: selector.DeepCopy()}},
		deployment: appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: a.Deployment, Namespace: a.Namespace, UID: types.UID(a.DeploymentUID), ResourceVersion: "11", Annotations: map[string]string{"fugue.pro/production-config-sha": a.SourceSHA}}, Spec: appsv1.DeploymentSpec{Template: *template.DeepCopy(), Selector: selector.DeepCopy()}},
		account:    corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: a.ServiceAccount, Namespace: a.Namespace, UID: types.UID(a.ServiceAccountUID), ResourceVersion: "12"}},
	}
	f.pod.Name, f.pod.Namespace, f.pod.UID, f.pod.ResourceVersion, f.pod.Spec.NodeName = "producer-pod", a.Namespace, "pod-uid", "9", "neutral-node"
	f.pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: f.rs.Name, UID: f.rs.UID, Controller: &controller}}
	return f
}

func pendingTestServer(t *testing.T, f *pendingIdentityFixture, verifier inputarchive.PendingRootSourceVerifier, after func(string, int)) *Server {
	t.Helper()
	s := &Server{store: &store.Store{}, controlPlaneNamespace: f.a.Namespace} // any unintended DB access panics
	if verifier == nil {
		verifier = pendingVerifierFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
			return nil, errors.New("synthetic source denied")
		})
	}
	config := PendingRootTransportConfig{Authorities: []PendingRootAuthority{f.a}, SourceVerifiers: map[string]inputarchive.PendingRootSourceVerifier{f.a.Owner: verifier}, Admission: pendingAdmissionFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootReservation, error) {
		return nil, errors.New("synthetic allocation denied")
	})}
	if err := s.ConfigurePendingRootTransport(&config); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer private-api-reviewer" || r.Header.Get("Impersonate-User") != "" || r.Header.Get("X-Forwarded-For") != "" {
			t.Error("caller authority forwarded to Kubernetes")
		}
		counts[r.URL.Path]++
		if after != nil {
			after(r.URL.Path, counts[r.URL.Path])
		}
		w.Header().Set("Content-Type", "application/json")
		base, apps := "/api/v1/namespaces/"+f.a.Namespace, "/apis/apps/v1/namespaces/"+f.a.Namespace
		var value any
		switch r.URL.Path {
		case "/apis/authentication.k8s.io/v1/tokenreviews":
			var review authenticationv1.TokenReview
			if r.Method != "POST" || json.NewDecoder(r.Body).Decode(&review) != nil || review.Spec.Token != pendingDedicatedTestToken() || len(review.Spec.Audiences) != 1 || review.Spec.Audiences[0] != pendingRootAudience {
				t.Error("incorrect exclusive TokenReview")
			}
			value = &f.review
		case base + "/pods/producer-pod":
			value = &f.pod
		case base + "/serviceaccounts/neutral-producer":
			value = &f.account
		case apps + "/replicasets/producer-rs":
			value = &f.rs
		case apps + "/deployments/neutral-producer":
			value = &f.deployment
		default:
			t.Errorf("unexpected identity read: %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		if err := json.NewEncoder(w).Encode(value); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(kube.Close)
	s.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL, bearerToken: "private-api-reviewer"}, nil
	}
	return s
}

func TestPendingRootIdentityBindsExclusiveAudienceAndLiveOwnerChain(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		mutate func(*pendingIdentityFixture)
	}{
		{"valid", 200, func(*pendingIdentityFixture) {}},
		{"reader audience", 401, func(f *pendingIdentityFixture) { f.review.Status.Audiences = []string{"reader"} }},
		{"multiple audiences", 401, func(f *pendingIdentityFixture) {
			f.review.Status.Audiences = append(f.review.Status.Audiences, "reader")
		}},
		{"unverified", 401, func(f *pendingIdentityFixture) { f.review.Status.Authenticated = false }},
		{"unbound", 401, func(f *pendingIdentityFixture) { f.review.Status.User.Extra = nil }},
		{"tenant account", 403, func(f *pendingIdentityFixture) {
			f.review.Status.User.Username = "system:serviceaccount:tenant-test:neutral-producer"
		}},
		{"replaced account", 403, func(f *pendingIdentityFixture) { f.account.UID = "replacement" }},
		{"replaced pod", 403, func(f *pendingIdentityFixture) { f.pod.UID = "replacement" }},
		{"terminating pod", 403, func(f *pendingIdentityFixture) { now := metav1.Now(); f.pod.DeletionTimestamp = &now }},
		{"no controlling owner", 403, func(f *pendingIdentityFixture) { f.pod.OwnerReferences = nil }},
		{"replaced replica set", 403, func(f *pendingIdentityFixture) { f.rs.UID = "replacement" }},
		{"foreign deployment", 403, func(f *pendingIdentityFixture) { f.rs.OwnerReferences[0].UID = "foreign" }},
		{"replacement deployment", 403, func(f *pendingIdentityFixture) { f.deployment.UID = "replacement" }},
		{"changed source", 403, func(f *pendingIdentityFixture) { f.pod.Annotations["fugue.pro/oci-revision"] = strings.Repeat("c", 40) }},
		{"cross group", 403, func(f *pendingIdentityFixture) { f.pod.Labels["fugue.io/edge-group-id"] = "cell-beta" }},
		{"mutable image", 403, func(f *pendingIdentityFixture) { f.pod.Spec.Containers[0].Image = "registry.test/edge-control:latest" }},
		{"runtime image drift", 403, func(f *pendingIdentityFixture) {
			f.pod.Status.ContainerStatuses[0].ImageID = "sha256:" + strings.Repeat("c", 64)
		}},
		{"selector mismatch", 403, func(f *pendingIdentityFixture) { f.rs.Spec.Selector.MatchLabels["extra"] = "required" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPendingIdentityFixture()
			tc.mutate(f)
			s := pendingTestServer(t, f, nil, nil)
			p, status := s.verifyPendingRootPod(t.Context(), pendingDedicatedTestToken(), s.pendingRootTransport.Load())
			if status != tc.status || status == 200 && (p.authority != f.a || p.podUID != "pod-uid") {
				t.Fatal(status, p)
			}
		})
	}
	for _, suffix := range []string{"pods/producer-pod", "replicasets/producer-rs", "deployments/neutral-producer", "serviceaccounts/neutral-producer"} {
		t.Run("during auth "+suffix, func(t *testing.T) {
			f := newPendingIdentityFixture()
			s := pendingTestServer(t, f, nil, func(path string, count int) {
				if strings.HasSuffix(path, suffix) && count == 2 {
					f.pod.ResourceVersion = "changed"
					f.rs.ResourceVersion = "changed"
					f.deployment.ResourceVersion = "changed"
					f.account.ResourceVersion = "changed"
				}
			})
			if _, status := s.verifyPendingRootPod(t.Context(), pendingDedicatedTestToken(), s.pendingRootTransport.Load()); status != 403 {
				t.Fatal(status)
			}
		})
	}
}

type pendingDeadlineRecorder struct {
	*httptest.ResponseRecorder
	read, write time.Time
}

func (w *pendingDeadlineRecorder) SetReadDeadline(d time.Time) error  { w.read = d; return nil }
func (w *pendingDeadlineRecorder) SetWriteDeadline(d time.Time) error { w.write = d; return nil }

type pendingUnreadBody struct{ reads int }

func (b *pendingUnreadBody) Read([]byte) (int, error) { b.reads++; return 0, io.EOF }
func (*pendingUnreadBody) Close() error               { return nil }

func pendingHTTPTestRequest(s *Server, body io.Reader) *http.Request {
	host := "fugue-api-tls." + s.controlPlaneNamespace + ".svc"
	r := httptest.NewRequest("POST", "https://"+host+":8443"+pendingRootPath, body)
	r.TLS = &tls.ConnectionState{ServerName: host}
	r.Header.Set("Authorization", "Bearer "+pendingDedicatedTestToken())
	r.Header.Set("Content-Type", "application/json")
	return r
}

func pendingHTTPTestSource(owner string) inputarchive.PendingRootRequest {
	return inputarchive.PendingRootRequest{Schema: inputarchive.PendingRootRequestSchema, SourceSchema: "fugue.edge-control.pending-root-source.v1", Source: []byte(`{"original":"neutral"}`), Registration: inputarchive.PendingRootRegistration{Schema: inputarchive.PendingRootSchema, Owner: owner, Generation: "neutral-generation", SourceDigest: "sha256:" + strings.Repeat("a", 64), References: []inputarchive.HotReference{{Kind: "release", ID: "neutral-release"}}}}
}

func TestPendingRootHTTPRefusesBeforeBodyOrDatabaseAndSharesDeadline(t *testing.T) {
	f := newPendingIdentityFixture()
	verified := 0
	s := pendingTestServer(t, f, pendingVerifierFunc(func(_ context.Context, r inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
		verified++
		return pendingWitnessFunc(func(context.Context, inputarchive.PendingRootRequest) error { return nil }), nil
	}), nil)
	for _, tc := range []struct {
		name   string
		status int
		mutate func(*http.Request)
	}{
		{"public", 404, func(r *http.Request) { r.TLS = nil }},
		{"query", 404, func(r *http.Request) { r.URL.RawQuery = "token=foreign" }},
		{"wrong host", 404, func(r *http.Request) { r.Host = "public.test" }},
		{"no credential", 401, func(r *http.Request) { r.Header.Del("Authorization") }},
		{"reader purpose", 401, func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer eyJhbGciOiJFUzI1NiJ9."+base64.RawURLEncoding.EncodeToString([]byte(`{"aud":["reader"]}`))+".c2ln")
		}},
		{"mixed purpose hidden by review intersection", 401, func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer eyJhbGciOiJFUzI1NiJ9."+base64.RawURLEncoding.EncodeToString([]byte(`{"aud":["`+pendingRootAudience+`","reader"]}`))+".c2ln")
		}},
		{"two credentials", 401, func(r *http.Request) { r.Header.Add("Authorization", "Bearer other") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &pendingUnreadBody{}
			r := pendingHTTPTestRequest(s, b)
			tc.mutate(r)
			w := &pendingDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
			s.requirePendingRootPod(http.HandlerFunc(s.handleRetainPendingCompilerInputRoot)).ServeHTTP(w, r)
			if w.Code != tc.status || b.reads != 0 || verified != 0 {
				t.Fatal(w.Code, b.reads, verified)
			}
		})
	}
	raw, err := inputarchive.MarshalPendingRootHTTP(pendingHTTPTestSource(f.a.Owner))
	if err != nil {
		t.Fatal(err)
	}
	otherOwner, err := inputarchive.MarshalPendingRootHTTP(pendingHTTPTestSource("edge-control/cell-beta"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		status      int
		body        []byte
		contentType string
	}{
		{"allocation denial", 409, raw, "application/json"},
		{"cross-owner body", 400, otherOwner, "application/json"},
		{"duplicate", 400, bytes.Replace(raw, []byte(`"owner":`), []byte(`"owner":"foreign","owner":`), 1), "application/json"},
		{"content type", 415, raw, "application/octet-stream"},
		{"outer oversize", 413, bytes.Repeat([]byte{' '}, inputarchive.PendingRootHTTPBytes+1), "application/json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			deadline, _ := ctx.Deadline()
			r := pendingHTTPTestRequest(s, bytes.NewReader(tc.body)).WithContext(ctx)
			r.Header.Set("Content-Type", tc.contentType)
			w := &pendingDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
			s.requirePendingRootPod(http.HandlerFunc(s.handleRetainPendingCompilerInputRoot)).ServeHTTP(w, r)
			if w.Code != tc.status || w.Header().Get("Cache-Control") != "no-store" || !w.read.Equal(deadline) || !w.write.Equal(deadline) {
				t.Fatal(w.Code, w.Body.String(), w.read, w.write)
			}
		})
	}
	if verified != 1 {
		t.Fatal("invalid request reached source admission", verified)
	}
	if err := s.ConfigurePendingRootTransport(nil); err != nil {
		t.Fatal(err)
	}
	b := &pendingUnreadBody{}
	w := &pendingDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	s.requirePendingRootPod(http.HandlerFunc(s.handleRetainPendingCompilerInputRoot)).ServeHTTP(w, pendingHTTPTestRequest(s, b))
	if w.Code != 503 || b.reads != 0 {
		t.Fatal(w.Code, b.reads)
	}
}

func TestPendingRootAuthConfigurationIsCopiedAndRevocable(t *testing.T) {
	f := newPendingIdentityFixture()
	s := pendingTestServer(t, f, nil, nil)
	old := s.pendingRootTransport.Load()
	p := &pendingRootPrincipal{transport: old, authority: f.a, podUID: "pod-uid", deadline: time.Now().Add(time.Minute)}
	ctx := context.WithValue(t.Context(), pendingRootPrincipalKey{}, p)
	if err := p.check(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.ConfigurePendingRootTransport(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := old.VerifyPendingRootSource(ctx, pendingHTTPTestSource(f.a.Owner)); err == nil {
		t.Fatal("revoked principal retained authority")
	}
	if _, err := s.store.RetainPendingCompilerInputExternalRoot(ctx, pendingHTTPTestSource(f.a.Owner)); err == nil {
		t.Fatal("revoked provider accessed DB")
	}
	config := PendingRootTransportConfig{Authorities: []PendingRootAuthority{f.a}, SourceVerifiers: map[string]inputarchive.PendingRootSourceVerifier{f.a.Owner: pendingVerifierFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootSourceWitness, error) {
		return nil, store.ErrConflict
	})}, Admission: pendingAdmissionFunc(func(context.Context, inputarchive.PendingRootRequest) (inputarchive.PendingRootReservation, error) {
		return nil, store.ErrConflict
	})}
	if err := s.ConfigurePendingRootTransport(&config); err != nil {
		t.Fatal(err)
	}
	config.Authorities[0].Owner = "edge-control/cell-beta"
	delete(config.SourceVerifiers, f.a.Owner)
	if s.pendingRootTransport.Load().authorities[f.a.DeploymentUID] != f.a || s.pendingRootTransport.Load().verifiers[f.a.Owner] == nil {
		t.Fatal("caller changed installed authority")
	}
	config.Authorities = []PendingRootAuthority{f.a, f.a}
	if err := s.ConfigurePendingRootTransport(&config); err == nil {
		t.Fatal("duplicate authority accepted")
	}
}

func TestPendingRootKubernetesClientDoesNotFollowOrLeakFailures(t *testing.T) {
	for _, mode := range []string{"redirect", "oversize", "denial"} {
		t.Run(mode, func(t *testing.T) {
			followed := false
			target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed = true }))
			defer target.Close()
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "redirect":
					http.Redirect(w, r, target.URL, 307)
				case "oversize":
					w.Write(bytes.Repeat([]byte{' '}, (1<<20)+1))
				default:
					http.Error(w, "private-api-reviewer dedicated-pod-token", 500)
				}
			}))
			defer kube.Close()
			var result authenticationv1.TokenReview
			err := pendingRootKubernetesJSON(t.Context(), &clusterNodeClient{client: kube.Client(), baseURL: kube.URL, bearerToken: "private-api-reviewer"}, "GET", "/identity", nil, &result)
			if err == nil || followed || strings.Contains(err.Error(), "private-api-reviewer") || strings.Contains(err.Error(), "dedicated-pod-token") {
				t.Fatal("unsafe verification failure", err, followed)
			}
		})
	}
}
