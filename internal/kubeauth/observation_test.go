package kubeauth

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestKubernetesObservationClassifiesWithoutObjectOrQueryValues(t *testing.T) {
	for _, row := range []struct {
		path, resource, cache               string
		single, namespaced, watch, selector bool
	}{
		{"/api/v1/pods?resourceVersion=0", "pods", "any", false, false, false, false},
		{"/apis/apps/v1/namespaces/private-ns/deployments/private-app?labelSelector=secret%3Dprivate-selector", "deployments", "unspecified", true, true, false, true},
		{"/api/v1/namespaces/private-ns/pods?resourceVersion=private-revision&watch=true", "pods", "revision", false, true, true, false},
		{"/api/v1/secrets?resourceVersion=", "secrets", "latest", false, false, false, false},
		{"/healthz?token=private-token", "other", "unspecified", false, false, false, false},
	} {
		req, _ := http.NewRequest("GET", "https://control.invalid"+row.path, nil)
		got := classifyRequest(req)
		raw, _ := json.Marshal(got)
		if strings.Contains(string(raw), "private-") {
			t.Fatalf("observation leaked request values: %s", raw)
		}
		if got.Resource != row.resource || got.CacheMode != row.cache || got.SingleObject != row.single || got.Namespaced != row.namespaced || got.Watch != row.watch || got.LabelSelector != row.selector {
			t.Fatalf("incorrect classification: %+v", got)
		}
	}
}
