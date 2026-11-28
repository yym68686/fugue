package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/productioncontrol"
	"fugue/internal/storageaudit"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

func TestStorageAuditShadowRejectsDivergenceAndConfigMapReplacement(t *testing.T) {
	current := storageaudit.Policy{
		Version: 1, Mode: "audit", EffectiveFrom: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC),
		IntervalSeconds: 300, SourcePageRows: 64, HistoryDays: 7, MinimumVerifiedVersions: 3,
		UnusedUploadHours: 24, UnreferencedUploadDays: 30, WarningFreePercent: 20, PressureFreePercent: 15, CriticalFreePercent: 10,
	}
	target := current
	target.SourcePageRows = 96
	spec := storageAuditShadowSpec{
		Kubernetes: productioncontrol.ParticipationKubernetesBinding{NamespaceUID: "namespace-v1"},
		Namespace:  "control", ConfigMap: "storage", ConfigMapUID: "storage-v1",
	}
	for _, test := range []struct {
		name         string
		afterVersion string
		recreate     bool
		diverge      bool
		wantError    string
	}{
		{name: "same provider facts", afterVersion: "1"},
		{name: "concurrent Change", afterVersion: "2", wantError: "changed during"},
		{name: "configMap recreation", afterVersion: "2", recreate: true, wantError: "ConfigMap does not match"},
		{name: "database-only commit", afterVersion: "1", diverge: true, wantError: "policy disagree"},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy, err := json.Marshal(current)
			if err != nil {
				t.Fatal(err)
			}
			reads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Error("storage audit shadow sent a mutation")
					w.WriteHeader(http.StatusMethodNotAllowed)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v1/namespaces/control":
					_ = json.NewEncoder(w).Encode(v1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
						ObjectMeta: metav1.ObjectMeta{Name: "control", UID: "namespace-v1", ResourceVersion: "1"}})
				case "/api/v1/namespaces/control/configmaps/storage":
					meta := metav1.ObjectMeta{Namespace: "control", Name: "storage", UID: "storage-v1", ResourceVersion: "1"}
					reads++
					if reads == 2 {
						meta.ResourceVersion = test.afterVersion
						if test.recreate {
							meta.UID = "storage-v2"
						}
					}
					_ = json.NewEncoder(w).Encode(v1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
						ObjectMeta: meta, Data: map[string]string{storageAuditConfigKey: string(policy)}})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client, err := newStorageAuditShadowClient(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			report, err := readStorageAuditShadow(t.Context(), client, spec, target, func(context.Context) (storageaudit.OwnerObservation, error) {
				dbPolicy := current
				if test.diverge {
					dbPolicy = target
				}
				return storageaudit.OwnerObservation{
					Owner: "worker", Policy: dbPolicy, PolicyDigest: dbPolicy.Digest(),
					Activation: &storageaudit.OwnerActivation{PolicyDigest: current.Digest()},
				}, nil
			})
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) || report.Schema != "" {
					t.Fatalf("expected refusal %q, got %+v, %v", test.wantError, report, err)
				}
				return
			}
			if err != nil || report.ExecutionEvidence || report.ConfigMapResourceVersion != "1" ||
				report.Current != report.Rollback || report.Target.Digest != "sha256:"+target.Digest() || report.Rollback.Document != current {
				t.Fatalf("unexpected storage audit shadow: %+v, %v", report, err)
			}
			if _, err := client.ConfigMaps(spec.Namespace).Create(t.Context(), &v1.ConfigMap{}, metav1.CreateOptions{}); err == nil ||
				!strings.Contains(err.Error(), "permits only plain") {
				t.Fatalf("write reached the storage audit shadow transport: %v", err)
			}
		})
	}
}
