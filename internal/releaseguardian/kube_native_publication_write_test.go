package releaseguardian

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

func TestNativePublicationWriterNeverReplaysConfigMapWrite(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPut} {
		t.Run(method, func(t *testing.T) {
			var attempts atomic.Int32
			path := "/api/v1/namespaces/fixture/configmaps"
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: "fixture", Name: "release-pointer"},
				Data:       map[string]string{"desired.json": "fixture"},
			}
			if method == http.MethodPut {
				path += "/release-pointer"
				cm.ResourceVersion, cm.UID = "42", "fixture-uid"
			}
			var status atomic.Int32
			status.Store(http.StatusTooManyRequests)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				var observed corev1.ConfigMap
				if err := json.NewDecoder(r.Body).Decode(&observed); err != nil || r.Method != method ||
					r.URL.Path != path || observed.Name != cm.Name || observed.Namespace != cm.Namespace ||
					observed.UID != cm.UID || observed.ResourceVersion != cm.ResourceVersion {
					t.Errorf("wrong provider write: %s %s; resourceVersion=%q err=%v", r.Method, r.URL.Path, observed.ResourceVersion, err)
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "0")
				if status.Load() == http.StatusTooManyRequests {
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Status","status":"Failure","reason":"TooManyRequests","code":429}`))
					return
				}
				observed.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}
				observed.ResourceVersion = "43"
				_ = json.NewEncoder(w).Encode(observed)
			}))
			defer server.Close()
			client, err := coreclient.NewForConfigAndClient(&rest.Config{Host: server.URL}, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			writer := nativeConfigMapWriter{client.RESTClient()}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			write := func() (*corev1.ConfigMap, error) {
				if method == http.MethodPost {
					return writer.Create(ctx, cm, metav1.CreateOptions{})
				}
				return writer.Update(ctx, cm, metav1.UpdateOptions{})
			}
			if _, err := write(); !apierrors.IsTooManyRequests(err) || attempts.Load() != 1 {
				t.Fatalf("limit response did not end the attempt: requests=%d error=%v", attempts.Load(), err)
			}
			status.Store(http.StatusOK)
			if result, err := write(); err != nil || result.ResourceVersion != "43" || attempts.Load() != 2 {
				t.Fatalf("successful fresh request did not return provider version: attempts=%d err=%v", attempts.Load(), err)
			}
		})
	}
}
