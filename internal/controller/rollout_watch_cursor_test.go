package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPodRolloutWatchContinuesListIncludingEmptyList(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "existing", true: "empty"}[empty], func(t *testing.T) {
			changed := make(chan struct{})
			watching := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("watch") != "true" {
					items := []any{}
					if !empty {
						items = append(items, map[string]any{"metadata": map[string]string{"name": "old", "resourceVersion": "17"}})
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]string{"resourceVersion": "42"}, "items": items})
					return
				}
				if r.URL.Query().Get("resourceVersion") != "42" {
					t.Errorf("watch lost collection cursor: %s", r.URL)
					return
				}
				close(watching)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte("{\"type\":\"BOOKMARK\"}\n"))
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
					return
				case <-changed:
				}
				_, _ = w.Write([]byte("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"new\",\"resourceVersion\":\"43\"}}}\n"))
			}))
			defer server.Close()
			client := &kubeClient{client: server.Client(), baseURL: server.URL}
			list, err := client.listPodSnapshotBySelector(context.Background(), "tenant-a", "app=demo")
			if err != nil {
				t.Fatal(err)
			}
			app := model.App{ID: "app-demo"}
			targets := managedAppPodRolloutWatchTargets("tenant-a", app, list.Metadata.ResourceVersion)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- client.waitForAnyObjectEvent(ctx, targets, time.Second) }()
			select {
			case <-watching:
			case <-ctx.Done():
				t.Fatal("watch did not start")
			}
			select {
			case err := <-done:
				t.Fatalf("unchanged list/bookmark caused wake: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			close(changed)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("real ADDED did not wake")
			}
		})
	}
}

func TestUnboundOrFailedWatchUsesPollingBudget(t *testing.T) {
	for _, rv := range []string{"", "0", "42"} {
		t.Run("rv="+rv, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); http.Error(w, "expired", http.StatusGone) }))
			defer server.Close()
			c := &kubeClient{client: server.Client(), baseURL: server.URL}
			start := time.Now()
			err := c.waitForAnyObjectEvent(context.Background(), []kubeWatchTarget{{apiPath: "/api/v1/pods", resourceVersion: rv}}, 40*time.Millisecond)
			if err != nil || time.Since(start) < 35*time.Millisecond {
				t.Fatalf("watch spun instead of bounded retry: %v %v", time.Since(start), err)
			}
			want := int32(0)
			if rv == "42" {
				want = 1
			}
			if calls.Load() != want {
				t.Fatalf("requests=%d want=%d", calls.Load(), want)
			}
		})
	}
}
