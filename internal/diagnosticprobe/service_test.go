package diagnosticprobe

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"fugue/internal/livediagnostics"
)

func TestServiceObservationUsesScopedDiscoveryAndRedactsSelectedFields(t *testing.T) {
	var address string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/namespaces/test-system/services/test-pipeline" {
			if r.Header.Get("Authorization") != "Bearer test-scope" {
				t.Error("discovery lacks scoped identity")
			}
			u, _ := url.Parse(address)
			port, _ := strconv.Atoi(u.Port())
			fmt.Fprintf(w, `{"spec":{"clusterIP":"127.0.0.1","ports":[{"name":"http","port":%d}]}}`, port)
			return
		}
		if r.URL.Path != "/status" || r.Header.Get("Authorization") != "" {
			t.Errorf("service received unexpected path or Kubernetes credentials: %s", r.URL.Path)
		}
		fmt.Fprint(w, `{"pipeline":{"received":9007199254740993,"queue_depth":4},"password":"hidden","unselected":"private"}`)
	}))
	defer server.Close()
	address = server.URL
	k := &kubeReader{client: server.Client(), publicClient: server.Client(), base: address, token: "test-scope"}
	c := Collector{Service: &Service{Namespace: "test-system", Name: "test-pipeline", Port: "http"}, Path: "/status", Fields: []string{"pipeline", "password"}}
	value, err := k.serviceJSON(context.Background(), livediagnostics.ProbeRequest{}, c)
	if err != nil {
		t.Fatal(err)
	}
	result := value.(map[string]any)
	if result["password"] != "[REDACTED]" || result["unselected"] != nil {
		t.Fatalf("unselected or sensitive data escaped: %+v", result)
	}
	if fmt.Sprint(result["pipeline"].(map[string]any)["received"]) != "9007199254740993" {
		t.Fatal("service counters lost precision")
	}
	for _, bad := range []string{"https://other.invalid/status", "//other.invalid/status", "/status?token=x"} {
		c.Path = bad
		if _, err := k.serviceJSON(context.Background(), livediagnostics.ProbeRequest{}, c); err == nil {
			t.Fatalf("unrestricted service URL accepted: %s", bad)
		}
	}
}
