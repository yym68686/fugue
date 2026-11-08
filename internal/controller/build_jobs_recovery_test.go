package controller

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/store"
)

func TestCleanupZombieBuildJobsRecoversReservationsAfterPodsStop(t *testing.T) {
	state := store.New(t.TempDir() + "/state.json")
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	jobPresent, podRunning, leasePresent := true, true, true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		write := func(value any) {
			if err := json.NewEncoder(w).Encode(value); err != nil {
				t.Error(err)
			}
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "/jobs"):
			jobs := []any{}
			if jobPresent {
				jobs = append(jobs, map[string]any{"metadata": map[string]any{"name": "build-example", "labels": map[string]string{"fugue.pro/operation-id": "operation-missing"}}, "status": map[string]int{"active": 1}})
			}
			write(map[string]any{"items": jobs})
		case strings.HasSuffix(r.URL.Path, "/jobs/build-example") && r.Method == http.MethodDelete:
			jobPresent = false
			write(map[string]any{})
		case strings.HasSuffix(r.URL.Path, "/jobs/build-example"):
			w.WriteHeader(404)
		case strings.HasSuffix(r.URL.Path, "/leases"):
			leases := []any{}
			if leasePresent {
				leases = append(leases, map[string]any{
					"metadata": map[string]any{"name": "build-example", "uid": "lease-uid", "resourceVersion": "7", "annotations": map[string]string{"fugue.pro/builder-node-name": "worker-a"}},
					"spec":     map[string]any{"holderIdentity": "build-example", "renewTime": time.Now().Add(-5 * time.Minute).Format(time.RFC3339Nano), "leaseDurationSeconds": 2100},
				})
			}
			write(map[string]any{"items": leases})
		case strings.HasSuffix(r.URL.Path, "/pods"):
			phase := "Succeeded"
			if podRunning {
				phase = "Running"
			}
			write(map[string]any{"items": []any{map[string]any{"status": map[string]string{"phase": phase}}}})
		case strings.HasSuffix(r.URL.Path, "/nodes/worker-a"):
			write(map[string]any{"status": map[string]any{"conditions": []map[string]string{{"type": "Ready", "status": "True"}}}})
		case strings.HasSuffix(r.URL.Path, "/leases/build-example") && r.Method == http.MethodDelete:
			var options struct {
				Preconditions map[string]string `json:"preconditions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&options); err != nil {
				t.Error(err)
			}
			if options.Preconditions["uid"] != "lease-uid" || options.Preconditions["resourceVersion"] != "7" {
				t.Error("reservation deletion was not fenced")
			}
			if jobPresent || podRunning {
				t.Error("released capacity before the workload stopped")
			}
			leasePresent = false
			write(map[string]any{})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	svc := &Service{Store: state, Logger: log.New(io.Discard, "", 0), newKubeClient: func(string) (*kubeClient, error) {
		return &kubeClient{client: server.Client(), baseURL: server.URL, namespace: "test-system"}, nil
	}}
	if err := svc.cleanupZombieBuildJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if jobPresent || !leasePresent {
		t.Fatal("cleanup failed to preserve terminating workload capacity")
	}
	podRunning = false
	if err := svc.cleanupZombieBuildJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if leasePresent {
		t.Fatal("orphan reservation survived the next controller recovery sweep")
	}
}
