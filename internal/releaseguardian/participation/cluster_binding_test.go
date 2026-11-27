package participation

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

func TestBoundKubernetesObserverMatchesApprovedIdentity(t *testing.T) {
	var requests atomic.Int32
	var namespaceUID atomic.Value
	namespaceUID.Store("ns-a")
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/namespaces/fixture" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(corev1.Namespace{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
			ObjectMeta: metav1.ObjectMeta{Name: "fixture", UID: types.UID(namespaceUID.Load().(string)), ResourceVersion: "10"},
		})
	}))
	defer s.Close()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})
	cfg := &rest.Config{Host: s.URL, TLSClientConfig: rest.TLSClientConfig{CAData: ca}}
	policy := productioncontrol.ParticipationPolicy{Schema: productioncontrol.BoundParticipationPolicySchema, Namespace: "fixture",
		Kubernetes: &productioncontrol.ParticipationKubernetesBinding{
			Endpoint: s.URL, CADigest: digest(s.Certificate().Raw), NamespaceUID: "ns-a",
		}}

	for _, tc := range []struct {
		name   string
		change func(*rest.Config, *productioncontrol.ParticipationPolicy)
	}{
		{"wrong endpoint", func(c *rest.Config, _ *productioncontrol.ParticipationPolicy) { c.Host = "https://wrong.invalid" }},
		{"wrong CA digest", func(_ *rest.Config, p *productioncontrol.ParticipationPolicy) {
			p.Kubernetes.CADigest = digest([]byte("other CA"))
		}},
		{"unapproved additional CA", func(c *rest.Config, _ *productioncontrol.ParticipationPolicy) {
			c.CAData = append(append([]byte(nil), c.CAData...), c.CAData...)
		}},
		{"hostname override", func(c *rest.Config, _ *productioncontrol.ParticipationPolicy) { c.ServerName = "other.invalid" }},
		{"legacy topology", func(_ *rest.Config, p *productioncontrol.ParticipationPolicy) {
			p.Schema, p.Kubernetes = productioncontrol.ParticipationPolicySchema, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := rest.CopyConfig(cfg)
			p := policy
			binding := *policy.Kubernetes
			p.Kubernetes = &binding
			tc.change(c, &p)
			if _, err := NewBoundKubernetesObserver(c, p); err == nil || requests.Load() != 0 {
				t.Fatalf("unapproved Kubernetes connection was accepted: %v", err)
			}
		})
	}
	observer, err := NewBoundKubernetesObserver(cfg, policy)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := observer.VerifyNamespace(ctx); err != nil {
		t.Fatal(err)
	}
	namespaceUID.Store("ns-recreated")
	if err := observer.VerifyNamespace(ctx); err == nil || requests.Load() != 2 {
		t.Fatalf("changed live namespace was not rejected: %v", err)
	}
}
