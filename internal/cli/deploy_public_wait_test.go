package cli

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDeployWaitsForPublicRecoveryAndDoesNotRedeploy(t *testing.T) {
	old := deployWaitPollInterval
	deployWaitPollInterval = time.Millisecond
	defer func() { deployWaitPollInterval = old }()
	hits := 0
	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "HEAD" {
			t.Errorf("probe mutated route: %s", r.Method)
		}
		hits++
		if hits < 3 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(200)
		}
	}))
	defer edge.Close()
	var out, stderr bytes.Buffer
	cli := newCLI(&out, &stderr)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := cli.waitForPublicURL(ctx, edge.URL, edge.Client()); err != nil || hits != 3 {
		t.Fatalf("premature success: hits=%d err=%v", hits, err)
	}
	if !bytes.Contains(stderr.Bytes(), []byte("public_route_ready=false")) || !bytes.Contains(stderr.Bytes(), []byte("public_route_ready=true")) {
		t.Fatal(stderr.String())
	}
}
func TestPublicWaitDeadlineIsIndeterminateNotSuccessfulOrRemoteFailure(t *testing.T) {
	edge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer edge.Close()
	cli := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := cli.waitForPublicURL(ctx, edge.URL, edge.Client())
	if ExitCodeForError(err) != ExitCodeIndeterminate {
		t.Fatalf("wrong outcome: %v", err)
	}
}
