package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func TestRuntimeLogsStreamReturnsSSESnapshot(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	namespace := runtime.NamespaceForTenant(app.TenantID)
	selector, containerName, err := runtimeLogTarget(app, "app")
	if err != nil {
		t.Fatalf("runtime log target: %v", err)
	}

	fake := newFakeAppLogsClient()
	fake.setPods(selector, []kubePodInfo{fakePod("demo-6b9c9d7d8f-abc12", "Running", time.Date(2026, 3, 26, 1, 0, 0, 0, time.UTC), containerName)})
	fake.setLogLines(namespace, "demo-6b9c9d7d8f-abc12", containerName, false,
		"2026-03-26T01:00:00Z first line",
		"2026-03-26T01:00:01Z second line",
	)
	server.newLogsClient = func(namespace string) (appLogsClient, error) {
		return fake, nil
	}

	recorder := performStreamRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/runtime-logs/stream?follow=false", apiKey, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "text/event-stream" {
		t.Fatalf("expected text/event-stream content type, got %q", contentType)
	}

	events := parseSSEEvents(t, recorder.Body.String())
	if len(events) != 5 {
		t.Fatalf("expected 5 SSE events, got %d body=%s", len(events), recorder.Body.String())
	}
	if events[0].Event != "ready" || events[1].Event != "state" || events[2].Event != "log" || events[3].Event != "log" || events[4].Event != "end" {
		t.Fatalf("unexpected event sequence: %+v", events)
	}

	var state runtimeLogStreamStateEvent
	events[1].decode(t, &state)
	if len(state.Pods) != 1 || state.Pods[0] != "demo-6b9c9d7d8f-abc12" {
		t.Fatalf("expected pod state to include runtime pod, got %+v", state.Pods)
	}

	var first, second logStreamLogEvent
	events[2].decode(t, &first)
	events[3].decode(t, &second)
	if first.Line != "first line" || second.Line != "second line" {
		t.Fatalf("unexpected log lines: %+v %+v", first, second)
	}
	if first.Source.Pod != "demo-6b9c9d7d8f-abc12" || first.Source.Container != containerName {
		t.Fatalf("unexpected log source: %+v", first.Source)
	}

	var end logStreamEndEvent
	events[4].decode(t, &end)
	if end.Reason != "snapshot_complete" {
		t.Fatalf("expected snapshot_complete end reason, got %+v", end)
	}
	if _, err := parseLogStreamCursor(events[4].ID); err != nil {
		t.Fatalf("expected final SSE id to be a valid cursor, got %q err=%v", events[4].ID, err)
	}
}

func TestRuntimeLogsStreamResumesFromLastEventID(t *testing.T) {
	t.Parallel()

	_, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	namespace := runtime.NamespaceForTenant(app.TenantID)
	selector, containerName, err := runtimeLogTarget(app, "app")
	if err != nil {
		t.Fatalf("runtime log target: %v", err)
	}

	fake := newFakeAppLogsClient()
	fake.setPods(selector, []kubePodInfo{fakePod("demo-6b9c9d7d8f-def34", "Running", time.Date(2026, 3, 26, 2, 0, 0, 0, time.UTC), containerName)})
	fake.setLogLines(namespace, "demo-6b9c9d7d8f-def34", containerName, false,
		"2026-03-26T02:00:00Z line one",
		"2026-03-26T02:00:01Z line two",
	)
	server.newLogsClient = func(namespace string) (appLogsClient, error) {
		return fake, nil
	}

	first := performStreamRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/runtime-logs/stream?follow=false", apiKey, "")
	if first.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, first.Code, first.Body.String())
	}
	firstEvents := parseSSEEvents(t, first.Body.String())
	cursor := firstEvents[len(firstEvents)-1].ID
	if cursor == "" {
		t.Fatal("expected stream to emit an event id cursor")
	}

	fake.setLogLines(namespace, "demo-6b9c9d7d8f-def34", containerName, false,
		"2026-03-26T02:00:00Z line one",
		"2026-03-26T02:00:01Z line two",
		"2026-03-26T02:00:02Z line three",
	)

	second := performStreamRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/runtime-logs/stream?follow=false", apiKey, cursor)
	if second.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, second.Code, second.Body.String())
	}
	secondEvents := parseSSEEvents(t, second.Body.String())
	logCount := 0
	for _, event := range secondEvents {
		if event.Event != "log" {
			continue
		}
		logCount++
		var payload logStreamLogEvent
		event.decode(t, &payload)
		if payload.Line != "line three" {
			t.Fatalf("expected resumed stream to emit only the new line, got %+v", payload)
		}
	}
	if logCount != 1 {
		t.Fatalf("expected exactly one resumed log event, got %d body=%s", logCount, second.Body.String())
	}
}

func TestBuildLogsStreamEmitsStatusAndTerminalEnd(t *testing.T) {
	t.Parallel()

	s, server, apiKey, app := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})
	op, err := s.CreateOperation(model.Operation{
		TenantID:        app.TenantID,
		Type:            model.OperationTypeImport,
		RequestedByType: model.ActorTypeAPIKey,
		RequestedByID:   "tester",
		AppID:           app.ID,
		DesiredSpec:     &app.Spec,
		DesiredSource: &model.AppSource{
			Type:          model.AppSourceTypeGitHubPublic,
			RepoURL:       "https://github.com/example/demo",
			RepoBranch:    "main",
			BuildStrategy: model.AppBuildStrategyNixpacks,
		},
	})
	if err != nil {
		t.Fatalf("create import operation: %v", err)
	}
	claimed, found, err := s.ClaimNextPendingOperation()
	if err != nil {
		t.Fatalf("claim pending operation: %v", err)
	}
	if !found || claimed.ID != op.ID {
		t.Fatalf("expected to claim operation %s, got %+v found=%t", op.ID, claimed, found)
	}
	op, err = s.CompleteManagedOperation(op.ID, "/tmp/demo.yaml", "build finished")
	if err != nil {
		t.Fatalf("complete managed operation: %v", err)
	}

	fake := newFakeAppLogsClient()
	jobName := "build-demo-42"
	fake.setJobs("fugue.pro/operation-id="+op.ID, []kubeJobInfo{fakeJob(jobName, time.Date(2026, 3, 26, 3, 0, 0, 0, time.UTC), 0, 1, 0)})
	fake.setPods("job-name="+jobName, []kubePodInfo{fakePod("build-demo-42-abcd", "Succeeded", time.Date(2026, 3, 26, 3, 0, 1, 0, time.UTC), "builder")})
	fake.setLogLines("", "build-demo-42-abcd", "builder", false,
		"2026-03-26T03:00:00Z build step",
	)
	server.newLogsClient = func(namespace string) (appLogsClient, error) {
		return fake, nil
	}

	recorder := performStreamRequest(t, server, http.MethodGet, "/v1/apps/"+app.ID+"/build-logs/stream?follow=false&operation_id="+op.ID, apiKey, "")
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}

	events := parseSSEEvents(t, recorder.Body.String())
	if len(events) != 4 {
		t.Fatalf("expected 4 SSE events, got %d body=%s", len(events), recorder.Body.String())
	}
	if events[0].Event != "ready" || events[1].Event != "status" || events[2].Event != "log" || events[3].Event != "end" {
		t.Fatalf("unexpected event sequence: %+v", events)
	}

	var status buildLogStreamStatusEvent
	events[1].decode(t, &status)
	if status.OperationID != op.ID || status.OperationStatus != model.OperationStatusCompleted {
		t.Fatalf("unexpected build status payload: %+v", status)
	}
	if status.JobName != jobName || status.BuildStrategy != model.AppBuildStrategyNixpacks {
		t.Fatalf("unexpected build metadata in status: %+v", status)
	}

	var logEvent logStreamLogEvent
	events[2].decode(t, &logEvent)
	if logEvent.Line != "build step" {
		t.Fatalf("expected streamed build log, got %+v", logEvent)
	}

	var end logStreamEndEvent
	events[3].decode(t, &end)
	if end.Reason != "operation_completed" || end.OperationStatus != model.OperationStatusCompleted {
		t.Fatalf("unexpected end event: %+v", end)
	}
}

type fakeAppLogsClient struct {
	mu             sync.Mutex
	jobsBySelector map[string][]kubeJobInfo
	jobByName      map[string]kubeJobInfo
	podsBySelector map[string][]kubePodInfo
	logLines       map[string][]string
}

func newFakeAppLogsClient() *fakeAppLogsClient {
	return &fakeAppLogsClient{
		jobsBySelector: map[string][]kubeJobInfo{},
		jobByName:      map[string]kubeJobInfo{},
		podsBySelector: map[string][]kubePodInfo{},
		logLines:       map[string][]string{},
	}
}

func (f *fakeAppLogsClient) setJobs(selector string, jobs []kubeJobInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cloned := append([]kubeJobInfo(nil), jobs...)
	f.jobsBySelector[selector] = cloned
	for _, job := range cloned {
		f.jobByName[job.Metadata.Name] = job
	}
}

func (f *fakeAppLogsClient) setPods(selector string, pods []kubePodInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.podsBySelector[selector] = append([]kubePodInfo(nil), pods...)
}

func (f *fakeAppLogsClient) setLogLines(namespace, podName, container string, previous bool, lines ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logLines[f.logKey(namespace, podName, container, previous)] = append([]string(nil), lines...)
}

func (f *fakeAppLogsClient) listJobsBySelector(ctx context.Context, namespace, selector string) ([]kubeJobInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]kubeJobInfo(nil), f.jobsBySelector[selector]...), nil
}

func (f *fakeAppLogsClient) getJob(ctx context.Context, namespace, jobName string) (kubeJobInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	job, ok := f.jobByName[jobName]
	if !ok {
		return kubeJobInfo{}, &kubeStatusError{StatusCode: http.StatusNotFound, Message: "job not found"}
	}
	return job, nil
}

func (f *fakeAppLogsClient) listPodsBySelector(ctx context.Context, namespace, selector string) ([]kubePodInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]kubePodInfo(nil), f.podsBySelector[selector]...), nil
}

func (f *fakeAppLogsClient) readPodLogs(ctx context.Context, namespace, podName string, opts kubeLogOptions) (string, error) {
	body, err := f.streamPodLogs(ctx, namespace, podName, opts)
	if err != nil {
		return "", err
	}
	defer body.Close()
	payload, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(payload), "\n"), nil
}

func (f *fakeAppLogsClient) streamPodLogs(ctx context.Context, namespace, podName string, opts kubeLogOptions) (io.ReadCloser, error) {
	f.mu.Lock()
	lines := append([]string(nil), f.logLines[f.logKey(namespace, podName, opts.Container, opts.Previous)]...)
	f.mu.Unlock()

	if opts.SinceTime != nil && !opts.SinceTime.IsZero() {
		filtered := make([]string, 0, len(lines))
		for _, line := range lines {
			timestamp, _ := parseTimedLogLine(line)
			if timestamp.IsZero() || !timestamp.Before(opts.SinceTime.UTC()) {
				filtered = append(filtered, line)
			}
		}
		lines = filtered
	}
	if opts.TailLines > 0 && len(lines) > opts.TailLines {
		lines = lines[len(lines)-opts.TailLines:]
	}

	payload := strings.Join(lines, "\n")
	if payload != "" {
		payload += "\n"
	}
	return io.NopCloser(strings.NewReader(payload)), nil
}

func (f *fakeAppLogsClient) logKey(namespace, podName, container string, previous bool) string {
	return strings.Join([]string{namespace, podName, container, strconv.FormatBool(previous)}, "\x00")
}

func fakePod(name, phase string, createdAt time.Time, container string) kubePodInfo {
	pod := kubePodInfo{}
	pod.Metadata.Name = name
	pod.Metadata.CreationTimestamp = createdAt
	pod.Spec.Containers = []struct {
		Name string `json:"name"`
	}{{Name: container}}
	pod.Status.Phase = phase
	return pod
}

func fakeJob(name string, createdAt time.Time, active, succeeded, failed int) kubeJobInfo {
	job := kubeJobInfo{}
	job.Metadata.Name = name
	job.Metadata.CreationTimestamp = createdAt
	job.Status.Active = active
	job.Status.Succeeded = succeeded
	job.Status.Failed = failed
	return job
}

type sseEvent struct {
	ID    string
	Event string
	Data  string
}

func (e sseEvent) decode(t *testing.T, dst any) {
	t.Helper()
	if err := json.Unmarshal([]byte(e.Data), dst); err != nil {
		t.Fatalf("decode SSE event %q: %v data=%s", e.Event, err, e.Data)
	}
}

func parseSSEEvents(t *testing.T, body string) []sseEvent {
	t.Helper()
	chunks := strings.Split(body, "\n\n")
	events := make([]sseEvent, 0, len(chunks))
	for _, chunk := range chunks {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" || strings.HasPrefix(chunk, "retry:") {
			continue
		}
		event := sseEvent{}
		for _, line := range strings.Split(chunk, "\n") {
			switch {
			case strings.HasPrefix(line, "id: "):
				event.ID = strings.TrimSpace(strings.TrimPrefix(line, "id: "))
			case strings.HasPrefix(line, "event: "):
				event.Event = strings.TrimSpace(strings.TrimPrefix(line, "event: "))
			case strings.HasPrefix(line, "data: "):
				event.Data = strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			}
		}
		if event.Event != "" {
			events = append(events, event)
		}
	}
	return events
}

func performStreamRequest(t *testing.T, server *Server, method, target, apiKey, lastEventID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	if strings.TrimSpace(lastEventID) != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, req)
	return recorder
}
