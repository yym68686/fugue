package edge

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
)

type edgeActiveRequestBodyBuffer struct {
	EdgeRequestID     string
	TraceID           string
	RequestID         string
	Host              string
	Method            string
	Path              string
	PathPrefix        string
	AppID             string
	TenantID          string
	RuntimeID         string
	RouteID           string
	RouteKind         string
	ClientIP          string
	ClientCountry     string
	ClientRegion      string
	ClientASN         string
	StartedAt         time.Time
	LastReadAt        time.Time
	ContentLength     int64
	BytesRead         int64
	ReadCalls         int64
	AvgBPS            int64
	MinWindowBPS      int64
	MinWindowObserved bool
	BodyReadBlock     time.Duration
	FileWrite         time.Duration
	FirstBodyByte     time.Duration
	LastBodyByte      time.Duration
	MaxReadGap        time.Duration
	SlowLogged        bool
}

type edgeActiveRequestBodyBufferSnapshot struct {
	EdgeRequestID     string
	TraceID           string
	RequestID         string
	Host              string
	Method            string
	Path              string
	PathPrefix        string
	AppID             string
	TenantID          string
	RuntimeID         string
	RouteID           string
	RouteKind         string
	ClientIP          string
	ClientCountry     string
	ClientRegion      string
	ClientASN         string
	StartedAt         time.Time
	LastReadAt        time.Time
	ContentLength     int64
	BytesRead         int64
	ReadCalls         int64
	AvgBPS            int64
	MinWindowBPS      int64
	MinWindowObserved bool
	BodyReadBlock     time.Duration
	FileWrite         time.Duration
	FirstBodyByte     time.Duration
	LastBodyByte      time.Duration
	MaxReadGap        time.Duration
	Elapsed           time.Duration
	LastReadAge       time.Duration
}

type edgeRequestBodyBufferDebugEntry struct {
	EdgeRequestID   string `json:"edge_request_id"`
	TraceID         string `json:"trace_id,omitempty"`
	RequestID       string `json:"request_id,omitempty"`
	Host            string `json:"host,omitempty"`
	Method          string `json:"method,omitempty"`
	Path            string `json:"path,omitempty"`
	PathPrefix      string `json:"path_prefix,omitempty"`
	AppID           string `json:"app_id,omitempty"`
	TenantID        string `json:"tenant_id,omitempty"`
	RuntimeID       string `json:"runtime_id,omitempty"`
	RouteID         string `json:"route_id,omitempty"`
	RouteKind       string `json:"route_kind,omitempty"`
	ClientIP        string `json:"client_ip,omitempty"`
	ClientCountry   string `json:"client_country,omitempty"`
	ClientRegion    string `json:"client_region,omitempty"`
	ClientASN       string `json:"client_asn,omitempty"`
	StartedAt       string `json:"started_at"`
	LastReadAt      string `json:"last_read_at"`
	ElapsedMS       int64  `json:"elapsed_ms"`
	LastReadAgeMS   int64  `json:"last_read_age_ms"`
	ContentLength   int64  `json:"content_length"`
	BytesRead       int64  `json:"bytes_read"`
	ReadCalls       int64  `json:"read_calls"`
	AvgBPS          int64  `json:"avg_bps"`
	MinWindowBPS    int64  `json:"min_window_bps"`
	BodyReadBlockMS int64  `json:"body_read_block_ms"`
	FileWriteMS     int64  `json:"file_write_ms"`
	FirstBodyByteMS int64  `json:"first_body_byte_ms"`
	LastBodyByteMS  int64  `json:"last_body_byte_ms"`
	MaxReadGapMS    int64  `json:"max_read_gap_ms"`
}

type edgeRequestBodyBufferDebugResponse struct {
	Count  int                               `json:"count"`
	Active []edgeRequestBodyBufferDebugEntry `json:"active"`
}

func (s *Service) handleRequestBodyBuffers(w http.ResponseWriter, r *http.Request) {
	snapshots := s.activeRequestBodyBufferReadSnapshots(time.Now().UTC())
	entries := make([]edgeRequestBodyBufferDebugEntry, 0, len(snapshots))
	for _, snapshot := range snapshots {
		entries = append(entries, edgeRequestBodyBufferDebugEntry{
			EdgeRequestID:   strings.TrimSpace(snapshot.EdgeRequestID),
			TraceID:         strings.TrimSpace(snapshot.TraceID),
			RequestID:       strings.TrimSpace(snapshot.RequestID),
			Host:            strings.TrimSpace(snapshot.Host),
			Method:          strings.TrimSpace(snapshot.Method),
			Path:            strings.TrimSpace(snapshot.Path),
			PathPrefix:      strings.TrimSpace(snapshot.PathPrefix),
			AppID:           strings.TrimSpace(snapshot.AppID),
			TenantID:        strings.TrimSpace(snapshot.TenantID),
			RuntimeID:       strings.TrimSpace(snapshot.RuntimeID),
			RouteID:         strings.TrimSpace(snapshot.RouteID),
			RouteKind:       strings.TrimSpace(snapshot.RouteKind),
			ClientIP:        strings.TrimSpace(snapshot.ClientIP),
			ClientCountry:   strings.TrimSpace(snapshot.ClientCountry),
			ClientRegion:    strings.TrimSpace(snapshot.ClientRegion),
			ClientASN:       strings.TrimSpace(snapshot.ClientASN),
			StartedAt:       snapshot.StartedAt.UTC().Format(time.RFC3339Nano),
			LastReadAt:      snapshot.LastReadAt.UTC().Format(time.RFC3339Nano),
			ElapsedMS:       durationMilliseconds(snapshot.Elapsed),
			LastReadAgeMS:   durationMilliseconds(snapshot.LastReadAge),
			ContentLength:   nonNegativeInt64(snapshot.ContentLength),
			BytesRead:       nonNegativeInt64(snapshot.BytesRead),
			ReadCalls:       nonNegativeInt64(snapshot.ReadCalls),
			AvgBPS:          nonNegativeInt64(snapshot.AvgBPS),
			MinWindowBPS:    nonNegativeInt64(snapshot.MinWindowBPS),
			BodyReadBlockMS: durationMilliseconds(snapshot.BodyReadBlock),
			FileWriteMS:     durationMilliseconds(snapshot.FileWrite),
			FirstBodyByteMS: durationMilliseconds(snapshot.FirstBodyByte),
			LastBodyByteMS:  durationMilliseconds(snapshot.LastBodyByte),
			MaxReadGapMS:    durationMilliseconds(snapshot.MaxReadGap),
		})
	}
	httpx.WriteJSON(w, http.StatusOK, edgeRequestBodyBufferDebugResponse{
		Count:  len(entries),
		Active: entries,
	})
}

func (s *Service) startActiveRequestBodyBufferRead(observed edgeProxyObservation, contentLength int64, startedAt time.Time) string {
	if s == nil {
		return ""
	}
	edgeRequestID := strings.TrimSpace(observed.EdgeRequestID)
	if edgeRequestID == "" {
		return ""
	}
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	} else {
		startedAt = startedAt.UTC()
	}
	entry := edgeActiveRequestBodyBuffer{
		EdgeRequestID: strings.TrimSpace(observed.EdgeRequestID),
		TraceID:       strings.TrimSpace(observed.TraceID),
		RequestID:     strings.TrimSpace(observed.RequestID),
		Host:          firstNonEmpty(strings.TrimSpace(observed.Route.Hostname), strings.TrimSpace(observed.Host)),
		Method:        strings.TrimSpace(observed.Method),
		Path:          strings.TrimSpace(observed.Path),
		PathPrefix:    model.NormalizeAppRoutePathPrefix(observed.Route.PathPrefix),
		AppID:         strings.TrimSpace(observed.Route.AppID),
		TenantID:      strings.TrimSpace(observed.Route.TenantID),
		RuntimeID:     strings.TrimSpace(observed.Route.RuntimeID),
		RouteID:       strings.TrimSpace(observed.Route.RouteGeneration),
		RouteKind:     strings.TrimSpace(observed.Route.RouteKind),
		ClientIP:      strings.TrimSpace(observed.ClientIP),
		ClientCountry: strings.TrimSpace(observed.ClientCountry),
		ClientRegion:  strings.TrimSpace(observed.ClientRegion),
		ClientASN:     strings.TrimSpace(observed.ClientASN),
		StartedAt:     startedAt,
		LastReadAt:    startedAt,
		ContentLength: contentLength,
	}
	s.bodyBufferActiveMu.Lock()
	defer s.bodyBufferActiveMu.Unlock()
	if s.activeBodyBufferReads == nil {
		s.activeBodyBufferReads = make(map[string]edgeActiveRequestBodyBuffer)
	}
	s.activeBodyBufferReads[edgeRequestID] = entry
	return edgeRequestID
}

func (s *Service) updateActiveRequestBodyBufferRead(edgeRequestID string, result edgeRequestBodyBufferCopyResult, lastReadAt time.Time) {
	edgeRequestID = strings.TrimSpace(edgeRequestID)
	if s == nil || edgeRequestID == "" {
		return
	}
	s.bodyBufferActiveMu.Lock()
	defer s.bodyBufferActiveMu.Unlock()
	entry, ok := s.activeBodyBufferReads[edgeRequestID]
	if !ok {
		return
	}
	entry.BytesRead = result.BytesRead
	entry.ReadCalls = result.ReadCalls
	if !lastReadAt.IsZero() {
		entry.LastReadAt = lastReadAt.UTC()
	}
	entry.AvgBPS = result.AvgBPS
	if result.MinWindowBPS > 0 && (!entry.MinWindowObserved || result.MinWindowBPS < entry.MinWindowBPS) {
		entry.MinWindowBPS = result.MinWindowBPS
		entry.MinWindowObserved = true
	}
	entry.BodyReadBlock = result.BodyReadBlock
	entry.FileWrite = result.FileWrite
	entry.FirstBodyByte = result.FirstBodyByte
	entry.LastBodyByte = result.LastBodyByte
	entry.MaxReadGap = result.MaxReadGap
	s.activeBodyBufferReads[edgeRequestID] = entry
}

func (s *Service) recordActiveRequestBodyBufferWindowBPS(edgeRequestID string, bps int64) {
	edgeRequestID = strings.TrimSpace(edgeRequestID)
	if s == nil || edgeRequestID == "" {
		return
	}
	if bps < 0 {
		bps = 0
	}
	s.bodyBufferActiveMu.Lock()
	defer s.bodyBufferActiveMu.Unlock()
	entry, ok := s.activeBodyBufferReads[edgeRequestID]
	if !ok {
		return
	}
	if !entry.MinWindowObserved || bps < entry.MinWindowBPS {
		entry.MinWindowBPS = bps
		entry.MinWindowObserved = true
		s.activeBodyBufferReads[edgeRequestID] = entry
	}
}

func (s *Service) finishActiveRequestBodyBufferRead(edgeRequestID string) {
	edgeRequestID = strings.TrimSpace(edgeRequestID)
	if s == nil || edgeRequestID == "" {
		return
	}
	s.bodyBufferActiveMu.Lock()
	defer s.bodyBufferActiveMu.Unlock()
	delete(s.activeBodyBufferReads, edgeRequestID)
}

func (s *Service) activeRequestBodyBufferReadSnapshots(now time.Time) []edgeActiveRequestBodyBufferSnapshot {
	if s == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	s.bodyBufferActiveMu.Lock()
	defer s.bodyBufferActiveMu.Unlock()
	out := make([]edgeActiveRequestBodyBufferSnapshot, 0, len(s.activeBodyBufferReads))
	for _, entry := range s.activeBodyBufferReads {
		out = append(out, activeRequestBodyBufferSnapshot(entry, now))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].EdgeRequestID < out[j].EdgeRequestID
		}
		return out[i].StartedAt.Before(out[j].StartedAt)
	})
	return out
}

func (s *Service) activeRequestBodyBufferReadSnapshot(edgeRequestID string, now time.Time) (edgeActiveRequestBodyBufferSnapshot, bool) {
	edgeRequestID = strings.TrimSpace(edgeRequestID)
	if s == nil || edgeRequestID == "" {
		return edgeActiveRequestBodyBufferSnapshot{}, false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	s.bodyBufferActiveMu.Lock()
	defer s.bodyBufferActiveMu.Unlock()
	entry, ok := s.activeBodyBufferReads[edgeRequestID]
	if !ok {
		return edgeActiveRequestBodyBufferSnapshot{}, false
	}
	return activeRequestBodyBufferSnapshot(entry, now), true
}

func activeRequestBodyBufferSnapshot(entry edgeActiveRequestBodyBuffer, now time.Time) edgeActiveRequestBodyBufferSnapshot {
	if entry.LastReadAt.IsZero() {
		entry.LastReadAt = entry.StartedAt
	}
	elapsed := now.Sub(entry.StartedAt)
	if elapsed < 0 {
		elapsed = 0
	}
	lastReadAge := now.Sub(entry.LastReadAt)
	if lastReadAge < 0 {
		lastReadAge = 0
	}
	if entry.AvgBPS <= 0 && entry.BytesRead > 0 && elapsed > 0 {
		entry.AvgBPS = int64(float64(entry.BytesRead) / elapsed.Seconds())
	}
	return edgeActiveRequestBodyBufferSnapshot{
		EdgeRequestID:     entry.EdgeRequestID,
		TraceID:           entry.TraceID,
		RequestID:         entry.RequestID,
		Host:              entry.Host,
		Method:            entry.Method,
		Path:              entry.Path,
		PathPrefix:        entry.PathPrefix,
		AppID:             entry.AppID,
		TenantID:          entry.TenantID,
		RuntimeID:         entry.RuntimeID,
		RouteID:           entry.RouteID,
		RouteKind:         entry.RouteKind,
		ClientIP:          entry.ClientIP,
		ClientCountry:     entry.ClientCountry,
		ClientRegion:      entry.ClientRegion,
		ClientASN:         entry.ClientASN,
		StartedAt:         entry.StartedAt,
		LastReadAt:        entry.LastReadAt,
		ContentLength:     entry.ContentLength,
		BytesRead:         entry.BytesRead,
		ReadCalls:         entry.ReadCalls,
		AvgBPS:            entry.AvgBPS,
		MinWindowBPS:      entry.MinWindowBPS,
		MinWindowObserved: entry.MinWindowObserved,
		BodyReadBlock:     entry.BodyReadBlock,
		FileWrite:         entry.FileWrite,
		FirstBodyByte:     entry.FirstBodyByte,
		LastBodyByte:      entry.LastBodyByte,
		MaxReadGap:        entry.MaxReadGap,
		Elapsed:           elapsed,
		LastReadAge:       lastReadAge,
	}
}

func (s *Service) startRequestBodyBufferProgressLogger(ctx context.Context, observed edgeProxyObservation, edgeRequestID string) func() {
	if s == nil || strings.TrimSpace(edgeRequestID) == "" {
		return nil
	}
	interval := s.requestBodyBufferProgressEvery()
	if interval <= 0 {
		return nil
	}
	done := make(chan struct{})
	var once sync.Once
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		lastTick := time.Now().UTC()
		var lastBytes int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case now := <-ticker.C:
				snapshot, ok := s.activeRequestBodyBufferReadSnapshot(edgeRequestID, now.UTC())
				if !ok {
					return
				}
				windowElapsed := now.Sub(lastTick)
				if windowElapsed > 0 {
					windowBytes := snapshot.BytesRead - lastBytes
					if windowBytes < 0 {
						windowBytes = 0
					}
					s.recordActiveRequestBodyBufferWindowBPS(edgeRequestID, int64(float64(windowBytes)/windowElapsed.Seconds()))
					lastTick = now.UTC()
					lastBytes = snapshot.BytesRead
				}
				snapshot, ok = s.activeRequestBodyBufferReadSnapshot(edgeRequestID, now.UTC())
				if !ok {
					return
				}
				s.logRequestBodyBufferProgressEvent(observed, snapshot, "edge_request_body_buffer_progress")
				if snapshot.Elapsed >= s.requestBodyBufferSlowThreshold() && s.markActiveRequestBodyBufferSlowLogged(edgeRequestID) {
					s.logRequestBodyBufferProgressEvent(observed, snapshot, "edge_request_body_buffer_slow")
				}
			}
		}
	}()
	return func() {
		once.Do(func() {
			close(done)
		})
	}
}

func (s *Service) markActiveRequestBodyBufferSlowLogged(edgeRequestID string) bool {
	edgeRequestID = strings.TrimSpace(edgeRequestID)
	if s == nil || edgeRequestID == "" {
		return false
	}
	s.bodyBufferActiveMu.Lock()
	defer s.bodyBufferActiveMu.Unlock()
	entry, ok := s.activeBodyBufferReads[edgeRequestID]
	if !ok || entry.SlowLogged {
		return false
	}
	entry.SlowLogged = true
	s.activeBodyBufferReads[edgeRequestID] = entry
	return true
}

func (s *Service) logRequestBodyBufferSlowIfNeeded(observed edgeProxyObservation, edgeRequestID string) {
	if s == nil || strings.TrimSpace(edgeRequestID) == "" {
		return
	}
	snapshot, ok := s.activeRequestBodyBufferReadSnapshot(edgeRequestID, time.Now().UTC())
	if !ok || snapshot.Elapsed < s.requestBodyBufferSlowThreshold() {
		return
	}
	if s.markActiveRequestBodyBufferSlowLogged(edgeRequestID) {
		s.logRequestBodyBufferProgressEvent(observed, snapshot, "edge_request_body_buffer_slow")
	}
}

func (s *Service) logRequestBodyBufferProgressEvent(observed edgeProxyObservation, snapshot edgeActiveRequestBodyBufferSnapshot, eventType string) {
	if s == nil || s.Logger == nil {
		return
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		eventType = "edge_request_body_buffer_progress"
	}
	severity := "info"
	message := "edge request body buffer progress"
	if eventType == "edge_request_body_buffer_slow" {
		severity = "warning"
		message = "edge request body buffer slow"
	}
	writeEdgeStructuredLog(s.Logger.Writer(), map[string]any{
		"event_type":         eventType,
		"severity":           severity,
		"message":            message,
		"tenant_id":          strings.TrimSpace(observed.Route.TenantID),
		"app_id":             strings.TrimSpace(observed.Route.AppID),
		"runtime_id":         strings.TrimSpace(observed.Route.RuntimeID),
		"edge_id":            strings.TrimSpace(s.Config.EdgeID),
		"trace_id":           strings.TrimSpace(firstNonEmpty(observed.TraceID, snapshot.TraceID)),
		"request_id":         strings.TrimSpace(firstNonEmpty(observed.RequestID, snapshot.RequestID)),
		"route_id":           strings.TrimSpace(observed.Route.RouteGeneration),
		"hostname":           firstNonEmpty(observed.Route.Hostname, observed.Host),
		"path_template":      model.NormalizeAppRoutePathPrefix(observed.Route.PathPrefix),
		"method":             strings.TrimSpace(observed.Method),
		"edge_request_id":    strings.TrimSpace(snapshot.EdgeRequestID),
		"bytes_read":         nonNegativeInt64(snapshot.BytesRead),
		"content_length":     nonNegativeInt64(snapshot.ContentLength),
		"elapsed_ms":         durationMilliseconds(snapshot.Elapsed),
		"last_read_age_ms":   durationMilliseconds(snapshot.LastReadAge),
		"avg_bps":            nonNegativeInt64(snapshot.AvgBPS),
		"min_window_bps":     nonNegativeInt64(snapshot.MinWindowBPS),
		"read_calls":         nonNegativeInt64(snapshot.ReadCalls),
		"body_read_block_ms": durationMilliseconds(snapshot.BodyReadBlock),
		"file_write_ms":      durationMilliseconds(snapshot.FileWrite),
		"first_body_byte_ms": durationMilliseconds(snapshot.FirstBodyByte),
		"last_body_byte_ms":  durationMilliseconds(snapshot.LastBodyByte),
		"max_read_gap_ms":    durationMilliseconds(snapshot.MaxReadGap),
		"client_country":     strings.ToLower(strings.TrimSpace(snapshot.ClientCountry)),
		"client_region":      strings.TrimSpace(snapshot.ClientRegion),
		"client_asn":         strings.TrimSpace(snapshot.ClientASN),
	})
}
