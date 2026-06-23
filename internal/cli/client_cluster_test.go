package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestExecClusterPodExtendsHTTPTimeoutPastClientDefault(t *testing.T) {
	t.Parallel()

	var gotBody clusterExecRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/cluster/exec" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		time.Sleep(75 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"namespace":"kube-system","pod":"coredns-abc","command":["sh","-lc","echo ok"],"output":"ok\n","attempt_count":1}`))
	}))
	defer server.Close()

	client, err := newClientWithOptions(server.URL, "token", clientOptions{
		RequireToken:   true,
		RequestTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	result, err := client.ExecClusterPod(clusterExecRequest{
		Namespace: "kube-system",
		Pod:       "coredns-abc",
		Command:   []string{"sh", "-lc", "echo ok"},
		Timeout:   200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("exec cluster pod: %v", err)
	}
	if result.Output != "ok\n" {
		t.Fatalf("unexpected output %q", result.Output)
	}
	if gotBody.TimeoutMS != 200 {
		t.Fatalf("expected timeout_ms to be forwarded, got %+v", gotBody)
	}
	if client.httpClient.Timeout != 10*time.Millisecond {
		t.Fatalf("expected base client timeout to remain unchanged, got %s", client.httpClient.Timeout)
	}
}

func TestClusterExecControlPlaneRequestTimeoutIncludesRetries(t *testing.T) {
	t.Parallel()

	got := clusterExecControlPlaneRequestTimeout(clusterExecRequest{
		Retries:    4,
		RetryDelay: 500 * time.Millisecond,
		Timeout:    2 * time.Minute,
	})
	want := 5*2*time.Minute + 4*500*time.Millisecond + clusterExecHTTPTimeoutPadding
	if got != want {
		t.Fatalf("expected %s, got %s", want, got)
	}
}
