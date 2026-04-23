package api

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/httpstream"
	"fugue/internal/httpx"
	"fugue/internal/model"
)

const (
	appRequestStreamDefaultMaxChunks     = 5
	appRequestStreamDefaultMaxChunkBytes = 2048
)

func (s *Server) handleRequestAppInternalHTTPStream(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	app, allowed := s.loadAuthorizedApp(w, r, principal)
	if !allowed {
		return
	}

	var req struct {
		Method         string              `json:"method"`
		Path           string              `json:"path"`
		Query          map[string][]string `json:"query"`
		Headers        map[string][]string `json:"headers"`
		HeadersFromEnv map[string]string   `json:"headers_from_env"`
		Body           string              `json:"body"`
		BodyEncoding   string              `json:"body_encoding"`
		TimeoutMS      int                 `json:"timeout_ms"`
		MaxChunks      int                 `json:"max_chunks"`
		MaxChunkBytes  int                 `json:"max_chunk_bytes"`
		Accepts        []string            `json:"accepts"`
	}
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if strings.TrimSpace(req.Method) == "" {
		req.Method = http.MethodGet
	}
	if strings.TrimSpace(req.Path) == "" {
		httpx.WriteError(w, http.StatusBadRequest, "path is required")
		return
	}
	accepts, err := normalizeStreamAcceptHeaders(req.Accepts)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.MaxChunks <= 0 {
		req.MaxChunks = appRequestStreamDefaultMaxChunks
	}
	if req.MaxChunkBytes <= 0 {
		req.MaxChunkBytes = appRequestStreamDefaultMaxChunkBytes
	}

	spec := cloneAppSpec(app.Spec)
	if appDeployBaselineNeedsRecovery(spec, app.Source) {
		recoveredSpec, _, err := s.recoverAppDeployBaseline(app)
		if err != nil {
			s.writeStoreError(w, err)
			return
		}
		spec = recoveredSpec
	}
	app.Spec = spec
	envDetails := mergedAppEnvDetails(app, spec)

	targetURL, err := s.resolveAppInternalRequestURL(r.Context(), app, req.Path, req.Query)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	bodyBytes, err := decodeAppRequestBody(req.Body, req.BodyEncoding)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	baseHeaders := http.Header{}
	if err := applyAppRequestHeaders(baseHeaders, req.Headers, req.HeadersFromEnv, envDetails.Env); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	timeout := appRequestDefaultTimeout
	if req.TimeoutMS > 0 {
		timeout = time.Duration(req.TimeoutMS) * time.Millisecond
	}
	httpClient := s.appRequestHTTPClient
	if httpClient == nil {
		httpClient = &http.Client{}
	}

	probes := make([]model.HTTPStreamProbe, 0, len(accepts))
	for _, accept := range accepts {
		probeCtx, cancel := context.WithTimeout(r.Context(), timeout)
		httpReq, buildErr := http.NewRequestWithContext(probeCtx, strings.ToUpper(strings.TrimSpace(req.Method)), targetURL, bytes.NewReader(bodyBytes))
		if buildErr != nil {
			cancel()
			httpx.WriteError(w, http.StatusBadRequest, "build app request: "+buildErr.Error())
			return
		}
		if len(bodyBytes) == 0 {
			httpReq.Body = nil
		}
		httpReq.Header = baseHeaders.Clone()
		httpReq.Header.Del("Accept")
		httpReq.Header.Set("Accept", accept)
		probes = append(probes, httpstream.Probe(httpClient, httpReq, httpstream.ProbeOptions{
			Target:        "internal_service",
			Accept:        accept,
			MaxChunks:     req.MaxChunks,
			MaxChunkBytes: req.MaxChunkBytes,
		}))
		cancel()
	}

	s.appendAudit(principal, "app.request_stream", "app", app.ID, app.TenantID, map[string]string{
		"method": strings.ToUpper(strings.TrimSpace(req.Method)),
		"path":   req.Path,
	})
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"probes": probes})
}

func normalizeStreamAcceptHeaders(values []string) ([]string, error) {
	if len(values) == 0 {
		return []string{"*/*", "text/event-stream"}, nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("at least one non-empty accept header is required")
	}
	return out, nil
}
