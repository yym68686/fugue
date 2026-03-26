package api

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/runtime"
)

const (
	logStreamCursorVersion = 1
	logStreamRetryMS       = 3000
	logStreamScanBuffer    = 1024 * 1024
)

type logStreamTuning struct {
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
}

func defaultLogStreamTuning() logStreamTuning {
	return logStreamTuning{
		PollInterval:      2 * time.Second,
		HeartbeatInterval: 15 * time.Second,
	}
}

func (t logStreamTuning) normalized() logStreamTuning {
	if t.PollInterval <= 0 {
		t.PollInterval = 2 * time.Second
	}
	if t.HeartbeatInterval <= 0 {
		t.HeartbeatInterval = 15 * time.Second
	}
	return t
}

type logStreamCursor struct {
	Version int                              `json:"v"`
	Sources map[string]logStreamCursorSource `json:"sources,omitempty"`
}

type logStreamCursorSource struct {
	Timestamp string `json:"ts,omitempty"`
	Skip      int    `json:"skip,omitempty"`
}

func newLogStreamCursor() logStreamCursor {
	return logStreamCursor{
		Version: logStreamCursorVersion,
		Sources: map[string]logStreamCursorSource{},
	}
}

func parseLogStreamCursor(raw string) (logStreamCursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return newLogStreamCursor(), nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return logStreamCursor{}, fmt.Errorf("decode cursor: %w", err)
	}
	cursor := newLogStreamCursor()
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return logStreamCursor{}, fmt.Errorf("parse cursor: %w", err)
	}
	if cursor.Version != logStreamCursorVersion {
		return logStreamCursor{}, fmt.Errorf("unsupported cursor version %d", cursor.Version)
	}
	if cursor.Sources == nil {
		cursor.Sources = map[string]logStreamCursorSource{}
	}
	return cursor, nil
}

func logStreamCursorFromRequest(r *http.Request) (logStreamCursor, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("cursor"))
	if raw == "" {
		raw = strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	}
	return parseLogStreamCursor(raw)
}

func (c logStreamCursor) encode() string {
	if c.Version == 0 {
		c.Version = logStreamCursorVersion
	}
	if c.Sources == nil {
		c.Sources = map[string]logStreamCursorSource{}
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(payload)
}

func (c logStreamCursor) position(sourceID string) (time.Time, int, bool) {
	state, ok := c.Sources[sourceID]
	if !ok || strings.TrimSpace(state.Timestamp) == "" {
		return time.Time{}, 0, false
	}
	timestamp, err := time.Parse(time.RFC3339Nano, state.Timestamp)
	if err != nil {
		return time.Time{}, 0, false
	}
	return timestamp.UTC(), state.Skip, true
}

func (c *logStreamCursor) advance(sourceID string, timestamp time.Time) string {
	if c.Sources == nil {
		c.Sources = map[string]logStreamCursorSource{}
	}
	if timestamp.IsZero() {
		return c.encode()
	}
	timestamp = timestamp.UTC()
	current, ok := c.Sources[sourceID]
	if ok && current.Timestamp == timestamp.Format(time.RFC3339Nano) {
		current.Skip++
		c.Sources[sourceID] = current
		return c.encode()
	}
	c.Sources[sourceID] = logStreamCursorSource{
		Timestamp: timestamp.Format(time.RFC3339Nano),
		Skip:      1,
	}
	return c.encode()
}

type sseWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

func newSSEWriter(w http.ResponseWriter) (*sseWriter, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("streaming is not supported by this response writer")
	}
	headers := w.Header()
	headers.Set("Content-Type", "text/event-stream")
	headers.Set("Cache-Control", "no-cache")
	headers.Set("Connection", "keep-alive")
	headers.Set("X-Accel-Buffering", "no")
	return &sseWriter{w: w, flusher: flusher}, nil
}

func (w *sseWriter) writeRetry(ms int) error {
	if _, err := fmt.Fprintf(w.w, "retry: %d\n\n", ms); err != nil {
		return err
	}
	w.flusher.Flush()
	return nil
}

func (w *sseWriter) writeEvent(event, id string, payload any) error {
	if id != "" {
		if _, err := fmt.Fprintf(w.w, "id: %s\n", id); err != nil {
			return err
		}
	}
	if event != "" {
		if _, err := fmt.Fprintf(w.w, "event: %s\n", event); err != nil {
			return err
		}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w.w, "data: %s\n\n", body); err != nil {
		return err
	}
	w.flusher.Flush()
	return nil
}

type logStreamSourceSpec struct {
	Stream    string `json:"stream"`
	Namespace string `json:"namespace,omitempty"`
	Component string `json:"component,omitempty"`
	JobName   string `json:"job_name,omitempty"`
	Pod       string `json:"pod,omitempty"`
	Container string `json:"container,omitempty"`
	Previous  bool   `json:"previous,omitempty"`
	Phase     string `json:"phase,omitempty"`
}

func (s logStreamSourceSpec) ID() string {
	parts := []string{
		strings.TrimSpace(s.Stream),
		strings.TrimSpace(s.Component),
		strings.TrimSpace(s.JobName),
		strings.TrimSpace(s.Namespace),
		strings.TrimSpace(s.Pod),
		strings.TrimSpace(s.Container),
		fmt.Sprintf("previous=%t", s.Previous),
	}
	return strings.Join(parts, "/")
}

type logStreamLine struct {
	Source    logStreamSourceSpec
	Timestamp time.Time
	Line      string
}

type logStreamSourceDone struct {
	Source logStreamSourceSpec
	Err    error
}

type logStreamReadyEvent struct {
	Cursor string `json:"cursor"`
	Stream string `json:"stream"`
	Follow bool   `json:"follow"`
}

type buildLogStreamReadyEvent struct {
	logStreamReadyEvent
	OperationID   string `json:"operation_id"`
	BuildStrategy string `json:"build_strategy,omitempty"`
}

type runtimeLogStreamReadyEvent struct {
	logStreamReadyEvent
	Component string `json:"component"`
	Namespace string `json:"namespace"`
	Selector  string `json:"selector"`
	Container string `json:"container"`
	Previous  bool   `json:"previous,omitempty"`
}

type buildLogStreamStatusEvent struct {
	Cursor          string     `json:"cursor"`
	OperationID     string     `json:"operation_id"`
	OperationStatus string     `json:"operation_status"`
	JobName         string     `json:"job_name,omitempty"`
	Pods            []string   `json:"pods,omitempty"`
	BuildStrategy   string     `json:"build_strategy,omitempty"`
	ErrorMessage    string     `json:"error_message,omitempty"`
	ResultMessage   string     `json:"result_message,omitempty"`
	LastUpdatedAt   time.Time  `json:"last_updated_at"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
}

type runtimeLogStreamStateEvent struct {
	Cursor    string   `json:"cursor"`
	Component string   `json:"component"`
	Namespace string   `json:"namespace"`
	Selector  string   `json:"selector"`
	Container string   `json:"container"`
	Pods      []string `json:"pods,omitempty"`
	Follow    bool     `json:"follow"`
	Previous  bool     `json:"previous,omitempty"`
}

type logStreamLogEvent struct {
	Cursor    string              `json:"cursor"`
	Source    logStreamSourceSpec `json:"source"`
	Timestamp string              `json:"timestamp,omitempty"`
	Line      string              `json:"line"`
}

type logStreamHeartbeatEvent struct {
	Cursor string    `json:"cursor"`
	Time   time.Time `json:"time"`
}

type logStreamWarningEvent struct {
	Cursor  string               `json:"cursor"`
	Message string               `json:"message"`
	Source  *logStreamSourceSpec `json:"source,omitempty"`
}

type logStreamEndEvent struct {
	Cursor          string `json:"cursor"`
	Reason          string `json:"reason"`
	OperationStatus string `json:"operation_status,omitempty"`
}

type buildLogStatusState struct {
	OperationStatus string
	JobName         string
	PodsKey         string
	BuildStrategy   string
	ErrorMessage    string
	ResultMessage   string
	LastUpdatedAt   string
	CompletedAt     string
	StartedAt       string
}

func buildLogStatusStateFromOperation(op model.Operation, jobName string, pods []string) buildLogStatusState {
	return buildLogStatusState{
		OperationStatus: strings.TrimSpace(op.Status),
		JobName:         strings.TrimSpace(jobName),
		PodsKey:         strings.Join(pods, "\x00"),
		BuildStrategy:   strings.TrimSpace(buildStrategyFromOperation(op)),
		ErrorMessage:    strings.TrimSpace(op.ErrorMessage),
		ResultMessage:   strings.TrimSpace(op.ResultMessage),
		LastUpdatedAt:   op.UpdatedAt.UTC().Format(time.RFC3339Nano),
		CompletedAt:     formatOptionalTime(op.CompletedAt),
		StartedAt:       formatOptionalTime(op.StartedAt),
	}
}

func parseFollowQuery(raw string, defaultValue bool) (bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultValue, nil
	}
	return parseBoolQuery(raw)
}

func formatOptionalTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func discoverBuildLogSources(ctx context.Context, client appLogsClient, operationID string) (string, []logStreamSourceSpec, []string, error) {
	jobName, err := latestBuilderJobName(ctx, client, "", operationID)
	if err != nil || jobName == "" {
		return jobName, nil, nil, err
	}
	pods, err := client.listPodsBySelector(ctx, "", "job-name="+jobName)
	if err != nil {
		return jobName, nil, nil, err
	}
	sortPodsByCreation(pods)
	sources := make([]logStreamSourceSpec, 0)
	for _, pod := range pods {
		for _, container := range podContainerNames(pod, true) {
			sources = append(sources, logStreamSourceSpec{
				Stream:    "build",
				JobName:   jobName,
				Pod:       pod.Metadata.Name,
				Container: container,
				Phase:     pod.Status.Phase,
			})
		}
	}
	return jobName, sources, buildLogPodNames(pods), nil
}

func discoverRuntimeLogSources(ctx context.Context, client appLogsClient, namespace, selector, component, containerName, requestedPod string, previous bool) ([]logStreamSourceSpec, []string, error) {
	pods, err := client.listPodsBySelector(ctx, namespace, selector)
	if err != nil {
		return nil, nil, err
	}
	sortPodsByCreation(pods)
	pods = filterPodsByName(pods, requestedPod)
	sources := make([]logStreamSourceSpec, 0, len(pods))
	for _, pod := range pods {
		sources = append(sources, logStreamSourceSpec{
			Stream:    "runtime",
			Namespace: namespace,
			Component: component,
			Pod:       pod.Metadata.Name,
			Container: containerName,
			Previous:  previous,
			Phase:     pod.Status.Phase,
		})
	}
	return sources, runtimeLogPodNames(pods), nil
}

func parseTimedLogLine(raw string) (time.Time, string) {
	raw = strings.TrimRight(raw, "\r")
	parts := strings.SplitN(raw, " ", 2)
	if len(parts) != 2 {
		return time.Time{}, raw
	}
	timestamp, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		return time.Time{}, raw
	}
	return timestamp.UTC(), parts[1]
}

func isTerminalOperationStatus(status string) bool {
	status = strings.TrimSpace(strings.ToLower(status))
	return status == model.OperationStatusCompleted || status == model.OperationStatusFailed
}

func isTerminalPodPhase(phase string) bool {
	phase = strings.TrimSpace(strings.ToLower(phase))
	return phase == "succeeded" || phase == "failed"
}

func buildStreamEndReason(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case model.OperationStatusCompleted:
		return "operation_completed"
	case model.OperationStatusFailed:
		return "operation_failed"
	default:
		return "snapshot_complete"
	}
}

func pumpLogSource(ctx context.Context, client appLogsClient, source logStreamSourceSpec, follow bool, tailLines int, cursor logStreamCursor, lines chan<- logStreamLine, done chan<- logStreamSourceDone) {
	options := kubeLogOptions{
		Container:  source.Container,
		TailLines:  tailLines,
		Previous:   source.Previous,
		Follow:     follow,
		Timestamps: true,
	}
	cursorTimestamp, skipCount, ok := cursor.position(source.ID())
	if ok {
		sinceTime := cursorTimestamp.Add(-time.Nanosecond)
		options.SinceTime = &sinceTime
		options.TailLines = 0
	}
	body, err := client.streamPodLogs(ctx, source.Namespace, source.Pod, options)
	if err != nil {
		select {
		case done <- logStreamSourceDone{Source: source, Err: err}:
		case <-ctx.Done():
		}
		return
	}
	defer body.Close()

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), logStreamScanBuffer)
	for scanner.Scan() {
		timestamp, line := parseTimedLogLine(scanner.Text())
		if ok {
			if !timestamp.IsZero() {
				if timestamp.Before(cursorTimestamp) {
					continue
				}
				if timestamp.Equal(cursorTimestamp) && skipCount > 0 {
					skipCount--
					continue
				}
			} else if skipCount > 0 {
				skipCount--
				continue
			}
		}
		select {
		case lines <- logStreamLine{Source: source, Timestamp: timestamp, Line: line}:
		case <-ctx.Done():
			return
		}
	}

	err = scanner.Err()
	if ctx.Err() != nil {
		err = nil
	}
	select {
	case done <- logStreamSourceDone{Source: source, Err: err}:
	case <-ctx.Done():
	}
}

func (s *Server) handleStreamAppBuildLogs(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	op, err := s.resolveBuildOperation(app, strings.TrimSpace(r.URL.Query().Get("operation_id")))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	tailLines, err := parseTailLines(r.URL.Query().Get("tail_lines"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	follow, err := parseFollowQuery(r.URL.Query().Get("follow"), true)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	cursor, err := logStreamCursorFromRequest(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	client, err := s.newLogsClient("")
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	jobName, initialSources, initialPods, err := discoverBuildLogSources(r.Context(), client, op.ID)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	stream, err := newSSEWriter(w)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	cursorID := cursor.encode()
	if err := stream.writeRetry(logStreamRetryMS); err != nil {
		return
	}
	if err := stream.writeEvent("ready", cursorID, buildLogStreamReadyEvent{
		logStreamReadyEvent: logStreamReadyEvent{
			Cursor: cursorID,
			Stream: "build",
			Follow: follow,
		},
		OperationID:   op.ID,
		BuildStrategy: buildStrategyFromOperation(op),
	}); err != nil {
		return
	}

	statusEvent := buildLogStreamStatusEvent{
		Cursor:          cursorID,
		OperationID:     op.ID,
		OperationStatus: op.Status,
		JobName:         jobName,
		Pods:            initialPods,
		BuildStrategy:   buildStrategyFromOperation(op),
		ErrorMessage:    op.ErrorMessage,
		ResultMessage:   op.ResultMessage,
		LastUpdatedAt:   op.UpdatedAt,
		CompletedAt:     op.CompletedAt,
		StartedAt:       op.StartedAt,
	}
	if err := stream.writeEvent("status", cursorID, statusEvent); err != nil {
		return
	}

	tuning := s.logStreamTuning.normalized()
	ticker := time.NewTicker(tuning.PollInterval)
	heartbeatTicker := time.NewTicker(tuning.HeartbeatInterval)
	defer ticker.Stop()
	defer heartbeatTicker.Stop()

	lines := make(chan logStreamLine, 64)
	done := make(chan logStreamSourceDone, 32)
	active := map[string]context.CancelFunc{}
	sealed := map[string]struct{}{}
	currentOp := op
	currentJobName := jobName
	currentPods := append([]string(nil), initialPods...)
	lastStatus := buildLogStatusStateFromOperation(currentOp, currentJobName, currentPods)

	attachSources := func(sources []logStreamSourceSpec) {
		for _, source := range sources {
			sourceID := source.ID()
			if _, ok := active[sourceID]; ok {
				continue
			}
			if _, ok := sealed[sourceID]; ok {
				continue
			}
			sourceCtx, cancel := context.WithCancel(r.Context())
			active[sourceID] = cancel
			go pumpLogSource(sourceCtx, client, source, follow, tailLines, cursor, lines, done)
		}
	}
	attachSources(initialSources)

	if !follow && len(active) == 0 {
		_ = stream.writeEvent("end", cursorID, logStreamEndEvent{
			Cursor:          cursorID,
			Reason:          buildStreamEndReason(currentOp.Status),
			OperationStatus: currentOp.Status,
		})
		return
	}

	for {
		select {
		case line := <-lines:
			cursorID = cursor.advance(line.Source.ID(), line.Timestamp)
			event := logStreamLogEvent{
				Cursor: cursorID,
				Source: line.Source,
				Line:   line.Line,
			}
			if !line.Timestamp.IsZero() {
				event.Timestamp = line.Timestamp.UTC().Format(time.RFC3339Nano)
			}
			if err := stream.writeEvent("log", cursorID, event); err != nil {
				return
			}
		case result := <-done:
			if cancel, ok := active[result.Source.ID()]; ok {
				cancel()
				delete(active, result.Source.ID())
			}
			if result.Source.Previous || isTerminalPodPhase(result.Source.Phase) {
				sealed[result.Source.ID()] = struct{}{}
			}
			if result.Err != nil {
				if err := stream.writeEvent("warning", cursorID, logStreamWarningEvent{
					Cursor:  cursorID,
					Message: result.Err.Error(),
					Source:  &result.Source,
				}); err != nil {
					return
				}
			}
			if !follow && len(active) == 0 && len(lines) == 0 {
				_ = stream.writeEvent("end", cursorID, logStreamEndEvent{
					Cursor:          cursorID,
					Reason:          buildStreamEndReason(currentOp.Status),
					OperationStatus: currentOp.Status,
				})
				return
			}
			if isTerminalOperationStatus(currentOp.Status) && len(active) == 0 && len(lines) == 0 {
				_ = stream.writeEvent("end", cursorID, logStreamEndEvent{
					Cursor:          cursorID,
					Reason:          buildStreamEndReason(currentOp.Status),
					OperationStatus: currentOp.Status,
				})
				return
			}
		case <-ticker.C:
			nextOp, err := s.store.GetOperation(currentOp.ID)
			if err == nil {
				currentOp = nextOp
			} else if err := stream.writeEvent("warning", cursorID, logStreamWarningEvent{
				Cursor:  cursorID,
				Message: err.Error(),
			}); err != nil {
				return
			}

			nextJobName, sources, pods, err := discoverBuildLogSources(r.Context(), client, currentOp.ID)
			if err == nil {
				currentJobName = nextJobName
				currentPods = append(currentPods[:0], pods...)
				attachSources(sources)
			} else if err := stream.writeEvent("warning", cursorID, logStreamWarningEvent{
				Cursor:  cursorID,
				Message: err.Error(),
			}); err != nil {
				return
			}

			nextStatus := buildLogStatusStateFromOperation(currentOp, currentJobName, currentPods)
			if nextStatus != lastStatus {
				lastStatus = nextStatus
				if err := stream.writeEvent("status", cursorID, buildLogStreamStatusEvent{
					Cursor:          cursorID,
					OperationID:     currentOp.ID,
					OperationStatus: currentOp.Status,
					JobName:         currentJobName,
					Pods:            append([]string(nil), currentPods...),
					BuildStrategy:   buildStrategyFromOperation(currentOp),
					ErrorMessage:    currentOp.ErrorMessage,
					ResultMessage:   currentOp.ResultMessage,
					LastUpdatedAt:   currentOp.UpdatedAt,
					CompletedAt:     currentOp.CompletedAt,
					StartedAt:       currentOp.StartedAt,
				}); err != nil {
					return
				}
			}
			if !follow && len(active) == 0 && len(lines) == 0 {
				_ = stream.writeEvent("end", cursorID, logStreamEndEvent{
					Cursor:          cursorID,
					Reason:          buildStreamEndReason(currentOp.Status),
					OperationStatus: currentOp.Status,
				})
				return
			}
			if isTerminalOperationStatus(currentOp.Status) && len(active) == 0 && len(lines) == 0 {
				_ = stream.writeEvent("end", cursorID, logStreamEndEvent{
					Cursor:          cursorID,
					Reason:          buildStreamEndReason(currentOp.Status),
					OperationStatus: currentOp.Status,
				})
				return
			}
		case <-heartbeatTicker.C:
			if err := stream.writeEvent("heartbeat", cursorID, logStreamHeartbeatEvent{
				Cursor: cursorID,
				Time:   time.Now().UTC(),
			}); err != nil {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}

func (s *Server) handleStreamAppRuntimeLogs(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	runtimeObj, err := s.store.GetRuntime(app.Spec.RuntimeID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if runtimeObj.Type == model.RuntimeTypeExternalOwned {
		httpx.WriteError(w, http.StatusBadRequest, "runtime logs are only available for managed runtimes")
		return
	}

	component := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("component")))
	if component == "" {
		component = "app"
	}
	if component != "app" && component != "postgres" {
		httpx.WriteError(w, http.StatusBadRequest, "component must be app or postgres")
		return
	}

	tailLines, err := parseTailLines(r.URL.Query().Get("tail_lines"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	previous, err := parseBoolQuery(r.URL.Query().Get("previous"))
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	followDefault := !previous
	follow, err := parseFollowQuery(r.URL.Query().Get("follow"), followDefault)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	requestedPod := strings.TrimSpace(r.URL.Query().Get("pod"))
	cursor, err := logStreamCursorFromRequest(r)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	namespace := runtime.NamespaceForTenant(app.TenantID)
	client, err := s.newLogsClient(namespace)
	if err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	selector, containerName, err := runtimeLogTarget(app, component)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	initialSources, initialPods, err := discoverRuntimeLogSources(r.Context(), client, namespace, selector, component, containerName, requestedPod, previous)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(initialPods) == 0 && (requestedPod != "" || !follow) {
		httpx.WriteError(w, http.StatusNotFound, "no matching pods found")
		return
	}

	stream, err := newSSEWriter(w)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}
	cursorID := cursor.encode()
	if err := stream.writeRetry(logStreamRetryMS); err != nil {
		return
	}
	if err := stream.writeEvent("ready", cursorID, runtimeLogStreamReadyEvent{
		logStreamReadyEvent: logStreamReadyEvent{
			Cursor: cursorID,
			Stream: "runtime",
			Follow: follow,
		},
		Component: component,
		Namespace: namespace,
		Selector:  selector,
		Container: containerName,
		Previous:  previous,
	}); err != nil {
		return
	}
	if err := stream.writeEvent("state", cursorID, runtimeLogStreamStateEvent{
		Cursor:    cursorID,
		Component: component,
		Namespace: namespace,
		Selector:  selector,
		Container: containerName,
		Pods:      initialPods,
		Follow:    follow,
		Previous:  previous,
	}); err != nil {
		return
	}

	tuning := s.logStreamTuning.normalized()
	ticker := time.NewTicker(tuning.PollInterval)
	heartbeatTicker := time.NewTicker(tuning.HeartbeatInterval)
	defer ticker.Stop()
	defer heartbeatTicker.Stop()

	lines := make(chan logStreamLine, 64)
	done := make(chan logStreamSourceDone, 32)
	active := map[string]context.CancelFunc{}
	sealed := map[string]struct{}{}
	lastPodsKey := strings.Join(initialPods, "\x00")

	attachSources := func(sources []logStreamSourceSpec) {
		for _, source := range sources {
			sourceID := source.ID()
			if _, ok := active[sourceID]; ok {
				continue
			}
			if _, ok := sealed[sourceID]; ok {
				continue
			}
			sourceCtx, cancel := context.WithCancel(r.Context())
			active[sourceID] = cancel
			go pumpLogSource(sourceCtx, client, source, follow, tailLines, cursor, lines, done)
		}
	}
	attachSources(initialSources)

	if !follow && len(active) == 0 {
		_ = stream.writeEvent("end", cursorID, logStreamEndEvent{
			Cursor: cursorID,
			Reason: "snapshot_complete",
		})
		return
	}

	for {
		select {
		case line := <-lines:
			cursorID = cursor.advance(line.Source.ID(), line.Timestamp)
			event := logStreamLogEvent{
				Cursor: cursorID,
				Source: line.Source,
				Line:   line.Line,
			}
			if !line.Timestamp.IsZero() {
				event.Timestamp = line.Timestamp.UTC().Format(time.RFC3339Nano)
			}
			if err := stream.writeEvent("log", cursorID, event); err != nil {
				return
			}
		case result := <-done:
			if cancel, ok := active[result.Source.ID()]; ok {
				cancel()
				delete(active, result.Source.ID())
			}
			if result.Source.Previous || isTerminalPodPhase(result.Source.Phase) {
				sealed[result.Source.ID()] = struct{}{}
			}
			if result.Err != nil {
				if err := stream.writeEvent("warning", cursorID, logStreamWarningEvent{
					Cursor:  cursorID,
					Message: result.Err.Error(),
					Source:  &result.Source,
				}); err != nil {
					return
				}
			}
			if !follow && len(active) == 0 && len(lines) == 0 {
				_ = stream.writeEvent("end", cursorID, logStreamEndEvent{
					Cursor: cursorID,
					Reason: "snapshot_complete",
				})
				return
			}
		case <-ticker.C:
			sources, pods, err := discoverRuntimeLogSources(r.Context(), client, namespace, selector, component, containerName, requestedPod, previous)
			if err != nil {
				if err := stream.writeEvent("warning", cursorID, logStreamWarningEvent{
					Cursor:  cursorID,
					Message: err.Error(),
				}); err != nil {
					return
				}
				continue
			}
			podsKey := strings.Join(pods, "\x00")
			if podsKey != lastPodsKey {
				lastPodsKey = podsKey
				if err := stream.writeEvent("state", cursorID, runtimeLogStreamStateEvent{
					Cursor:    cursorID,
					Component: component,
					Namespace: namespace,
					Selector:  selector,
					Container: containerName,
					Pods:      pods,
					Follow:    follow,
					Previous:  previous,
				}); err != nil {
					return
				}
			}
			attachSources(sources)
		case <-heartbeatTicker.C:
			if err := stream.writeEvent("heartbeat", cursorID, logStreamHeartbeatEvent{
				Cursor: cursorID,
				Time:   time.Now().UTC(),
			}); err != nil {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}
