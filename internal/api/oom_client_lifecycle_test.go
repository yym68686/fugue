package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	fugueruntime "fugue/internal/runtime"
	"fugue/internal/store"
)

func TestOOMScanReusesNamespaceConnectionsAndClosesEachPass(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"complete", "cancel", "server-error"} {
		cancelDuringScan := mode == "cancel"
		t.Run(mode, func(t *testing.T) {
			stateStore := store.New(filepath.Join(t.TempDir(), "state.json"))
			if err := stateStore.Init(); err != nil {
				t.Fatal(err)
			}
			expected := map[string]map[string]bool{}
			for tenantIndex := 0; tenantIndex < 2; tenantIndex++ {
				tenant, err := stateStore.CreateTenant(fmt.Sprintf("Scan Tenant %d", tenantIndex))
				if err != nil {
					t.Fatal(err)
				}
				raiseManagedTestCap(t, stateStore, tenant.ID)
				project, err := stateStore.CreateProject(tenant.ID, "scan", "")
				if err != nil {
					t.Fatal(err)
				}
				namespace := fugueruntime.NamespaceForTenant(tenant.ID)
				expected[namespace] = map[string]bool{}
				for appIndex := 0; appIndex < 10; appIndex++ {
					app, err := stateStore.CreateApp(tenant.ID, project.ID, fmt.Sprintf("app-%d", appIndex), "", model.AppSpec{
						Image: "registry.example.test/workload:v1", RuntimeID: "runtime_managed_shared", Replicas: 1, Ports: []int{8080},
						Resources: &model.ResourceSpec{CPUMilliCores: 100, MemoryMebibytes: 64},
					})
					if err != nil {
						t.Fatal(err)
					}
					selector, _, err := runtimeLogTarget(app, "app")
					if err != nil {
						t.Fatal(err)
					}
					expected[namespace][selector] = true
				}
			}

			var mu sync.Mutex
			connections := map[net.Conn]http.ConnState{}
			createdConnections := 0
			requests := map[string]int{}
			var cancel context.CancelFunc
			requestCount := 0
			endpoint := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
				if len(parts) != 5 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "namespaces" || parts[4] != "pods" {
					t.Errorf("unexpected Kubernetes request path %q", r.URL.Path)
					http.Error(w, "unexpected path", http.StatusBadRequest)
					return
				}
				namespace, selector := parts[3], r.URL.Query().Get("labelSelector")
				if !expected[namespace][selector] || r.Header.Get("Authorization") != "Bearer synthetic-token" {
					t.Errorf("namespace, selector, or authorization changed")
					http.Error(w, "unexpected target", http.StatusForbidden)
					return
				}
				mu.Lock()
				requests[namespace+"/"+selector]++
				requestCount++
				shouldCancel := cancelDuringScan && requestCount == 3
				shouldFail := mode == "server-error" && requestCount == 3
				mu.Unlock()
				if shouldFail {
					http.Error(w, "temporary read failure", http.StatusServiceUnavailable)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"items":[]}`))
				if shouldCancel {
					cancel()
				}
			}))
			endpoint.Config.ConnState = func(c net.Conn, state http.ConnState) {
				mu.Lock()
				defer mu.Unlock()
				if state == http.StateNew {
					createdConnections++
				}
				connections[c] = state
			}
			endpoint.StartTLS()
			t.Cleanup(endpoint.Close)
			server := NewServer(stateStore, auth.New(stateStore, ""), nil, ServerConfig{})
			var transports []*http.Transport
			factoryCalls := map[string]int{}
			t.Cleanup(func() {
				for _, tr := range transports {
					tr.CloseIdleConnections()
				}
			})
			server.newLogsClient = func(namespace string) (appLogsClient, error) {
				if _, ok := expected[namespace]; !ok {
					t.Fatalf("client created for unexpected namespace %q", namespace)
				}
				factoryCalls[namespace]++
				transport := endpoint.Client().Transport.(*http.Transport).Clone()
				transport.IdleConnTimeout = time.Hour
				transports = append(transports, transport)
				return &kubeLogsClient{client: &http.Client{Transport: transport, Timeout: time.Second}, baseURL: endpoint.URL, bearerToken: "synthetic-token", namespace: namespace}, nil
			}
			passes := 2
			if cancelDuringScan {
				passes = 1
			}
			for pass := 1; pass <= passes; pass++ {
				ctx, stop := context.WithCancel(context.Background())
				cancel = stop
				server.applyOOMRightSizingOnce(ctx)
				stop()
				if len(transports) > 2*pass {
					t.Fatalf("created %d independent transports after %d passes; want at most one per namespace per pass", len(transports), pass)
				}
				deadline := time.Now().Add(2 * time.Second)
				for {
					mu.Lock()
					open := 0
					for _, state := range connections {
						if state != http.StateClosed {
							open++
						}
					}
					mu.Unlock()
					if open == 0 {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("scan returned with %d connections still open", open)
					}
					time.Sleep(time.Millisecond)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if cancelDuringScan {
				if requestCount != 3 {
					t.Fatalf("canceled scan made %d requests; want 3", requestCount)
				}
			} else {
				if createdConnections != 4 || len(transports) != 4 {
					t.Fatalf("two passes used %d connections / %d transports; want 4", createdConnections, len(transports))
				}
				if len(requests) != 20 {
					t.Fatalf("observed %d distinct app selectors; want 20", len(requests))
				}
				for key, count := range requests {
					if count != 2 {
						t.Fatalf("%s was queried %d times; want once per pass", key, count)
					}
				}
				for namespace, count := range factoryCalls {
					if count != 2 {
						t.Fatalf("%s client reused beyond a pass or duplicated: %d", namespace, count)
					}
				}
			}
		})
	}
}
