package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fugue/internal/model"
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

func TestPublicAppWaitRefreshesStaleObservationInsteadOfReportingSuccess(t *testing.T) {
	old := deployWaitPollInterval
	deployWaitPollInterval = time.Millisecond
	defer func() { deployWaitPollInterval = old }()
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "stale", true: "missing"}[missing], func(t *testing.T) {
			app := model.App{ID: "app_wait", Spec: model.AppSpec{Replicas: 1}, Route: &model.AppRoute{PublicURL: "http://127.0.0.1:1"}}
			if !missing {
				app.ObservedStatus = &model.AppObservedStatus{Fresh: false}
			}
			hits := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/v1/apps/app_wait" || r.URL.Query().Get("include_live_status") != "true" {
					t.Errorf("unexpected observation request: %s %s", r.Method, r.URL)
				}
				hits++
				current := app
				current.ObservedStatus = &model.AppObservedStatus{Fresh: hits >= 2, ObservedAt: time.Now()}
				_ = json.NewEncoder(w).Encode(map[string]any{"app": current})
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			client, err := newClientWithOptions(server.URL, "token", clientOptions{Context: ctx, ReadRetryCount: -1})
			if err != nil {
				t.Fatal(err)
			}
			cli := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
			// The loopback route is deliberately unreachable by the public-only
			// transport. A refreshed observation must still not count as public
			// availability, and the caller must retain the refreshed app facts.
			err = cli.waitForPublicApp(client, &app)
			if ExitCodeForError(err) != ExitCodeIndeterminate || hits != 2 || app.ObservedStatus == nil || !app.ObservedStatus.Fresh {
				t.Fatalf("premature success or lost observation: hits=%d app=%+v err=%v", hits, app, err)
			}
		})
	}
}

func TestPublicAppWaitMissingObservationHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &Client{context: ctx}
	app := model.App{ID: "app_wait", Spec: model.AppSpec{Replicas: 1}, Route: &model.AppRoute{PublicURL: "https://worker.example"}}
	cli := newCLI(&bytes.Buffer{}, &bytes.Buffer{})
	if err := cli.waitForPublicApp(client, &app); ExitCodeForError(err) != ExitCodeIndeterminate {
		t.Fatalf("missing observation counted as success: %v", err)
	}
}
