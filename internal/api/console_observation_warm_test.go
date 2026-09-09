package api

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestConsoleWarmPublishesCompleteObservationBeforePageRequest(t *testing.T) {
	kube := newManagedAppTestServer(t, map[string]any{"items": []any{}})
	defer kube.Close()
	var calls atomic.Int32
	s := &Server{managedAppStatusCache: newManagedAppStatusCache(4*time.Second, time.Second)}
	s.newManagedAppStatusClient = func() (*managedAppStatusClient, error) {
		calls.Add(1)
		return &managedAppStatusClient{client: kube.Client(), baseURL: kube.URL, bearerToken: "test"}, nil
	}
	// Volume refresh has no cluster client in this test; it must not prevent
	// publication of the independent managed-app observation.
	s.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{client: &http.Client{Timeout: time.Second}, baseURL: kube.URL}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startConsoleObservationWarmLoop(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if snapshot, ok, expired := s.managedAppStatusCache.getObservedList(); ok && !expired {
			if snapshot.clusterID == "" || !snapshot.ok {
				t.Fatal("warmer published incomplete observation")
			}
			cancel()
			before := calls.Load()
			time.Sleep(30 * time.Millisecond)
			if calls.Load() != before {
				t.Fatal("cancelled warmer restarted")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("warmer did not publish without a foreground request")
}
