package cli

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
)

type appObservabilitySourceStatus struct {
	Available       bool     `json:"available"`
	Status          string   `json:"status"`
	Mode            string   `json:"mode"`
	Retention       string   `json:"retention"`
	ActiveExporters []string `json:"active_exporters"`
	Reason          string   `json:"reason"`
	Freshness       string   `json:"freshness,omitempty"`
}

type appObservabilityWindow struct {
	Since string `json:"since"`
	Until string `json:"until"`
}

type appObservabilityDiagnosis struct {
	Bottleneck  string   `json:"bottleneck"`
	Confidence  float64  `json:"confidence"`
	Evidence    []string `json:"evidence"`
	NextActions []string `json:"next_actions"`
}

type appObservabilityWindowOptions struct {
	Since string `json:"since,omitempty"`
	Until string `json:"until,omitempty"`
}

type appObservabilityMetricsOptions struct {
	appObservabilityWindowOptions
	Query string
}

type appObservabilityLogsOptions struct {
	appObservabilityWindowOptions
	Limit   int
	TraceID string
	Grep    string
	Level   string
}

type appObservabilityRequestsOptions struct {
	appObservabilityWindowOptions
	Limit       int
	TraceID     string
	StatusClass string
	Slow        bool
	Errors      bool
	Follow      bool
	Fields      string
}

type appObservabilityDiagnosisOptions struct {
	appObservabilityWindowOptions
}

type appObservabilityMetricsSummaryResponse struct {
	Source  appObservabilitySourceStatus `json:"source"`
	Window  appObservabilityWindow       `json:"window"`
	Metrics []map[string]any             `json:"metrics"`
}

type appObservabilityMetricsQueryResponse struct {
	Source  appObservabilitySourceStatus `json:"source"`
	Window  appObservabilityWindow       `json:"window"`
	Query   string                       `json:"query"`
	Metrics []map[string]any             `json:"metrics"`
}

type appObservabilityLogsQueryResponse struct {
	Source appObservabilitySourceStatus `json:"source"`
	Window appObservabilityWindow       `json:"window"`
	Logs   []map[string]any             `json:"logs"`
}

type appObservabilityRequestsResponse struct {
	Source   appObservabilitySourceStatus `json:"source"`
	Window   appObservabilityWindow       `json:"window"`
	Requests []map[string]any             `json:"requests"`
}

type appObservabilityTraceResponse struct {
	Source  appObservabilitySourceStatus `json:"source"`
	TraceID string                       `json:"trace_id"`
	Spans   []map[string]any             `json:"spans"`
}

type appObservabilityDiagnosisResponse struct {
	Source    appObservabilitySourceStatus `json:"source"`
	Window    appObservabilityWindow       `json:"window"`
	Diagnosis appObservabilityDiagnosis    `json:"diagnosis"`
}

func (c *Client) GetAppObservabilityMetricsSummary(id string, opts appObservabilityMetricsOptions) (appObservabilityMetricsSummaryResponse, error) {
	values := url.Values{}
	appendAppObservabilityWindowValues(values, opts.appObservabilityWindowOptions)
	relative := appObservabilityPath(id, "metrics", "summary")
	if encoded := values.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response appObservabilityMetricsSummaryResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appObservabilityMetricsSummaryResponse{}, err
	}
	return response, nil
}

func (c *Client) QueryAppObservabilityMetrics(id string, opts appObservabilityMetricsOptions) (appObservabilityMetricsQueryResponse, error) {
	values := url.Values{}
	appendAppObservabilityWindowValues(values, opts.appObservabilityWindowOptions)
	if query := strings.TrimSpace(opts.Query); query != "" {
		values.Set("query", query)
	}
	relative := appObservabilityPath(id, "metrics", "query")
	if encoded := values.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response appObservabilityMetricsQueryResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appObservabilityMetricsQueryResponse{}, err
	}
	return response, nil
}

func (c *Client) QueryAppObservabilityLogs(id string, opts appObservabilityLogsOptions) (appObservabilityLogsQueryResponse, error) {
	values := url.Values{}
	appendAppObservabilityWindowValues(values, opts.appObservabilityWindowOptions)
	appendPositiveIntQueryValue(values, "limit", opts.Limit)
	if value := strings.TrimSpace(opts.TraceID); value != "" {
		values.Set("trace_id", value)
	}
	if value := strings.TrimSpace(opts.Grep); value != "" {
		values.Set("grep", value)
	}
	if value := strings.TrimSpace(opts.Level); value != "" {
		values.Set("level", value)
	}
	relative := appObservabilityPath(id, "logs", "query")
	if encoded := values.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response appObservabilityLogsQueryResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appObservabilityLogsQueryResponse{}, err
	}
	return response, nil
}

func (c *Client) ListAppObservabilityRequests(id string, opts appObservabilityRequestsOptions) (appObservabilityRequestsResponse, error) {
	values := url.Values{}
	appendAppObservabilityWindowValues(values, opts.appObservabilityWindowOptions)
	appendAppObservabilityRequestFilterValues(values, opts)
	relative := appObservabilityPath(id, "requests")
	if encoded := values.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response appObservabilityRequestsResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appObservabilityRequestsResponse{}, err
	}
	return response, nil
}

func (c *Client) StreamAppObservabilityRequests(id string, opts appObservabilityRequestsOptions, handler func(sseEvent) error) error {
	values := url.Values{}
	appendAppObservabilityWindowValues(values, opts.appObservabilityWindowOptions)
	appendAppObservabilityRequestFilterValues(values, opts)
	values.Set("follow", "true")
	relative := appObservabilityPath(id, "requests", "stream") + "?" + values.Encode()
	return c.streamSSEWithOptions(relative, streamSSEOptions{Follow: true}, handler)
}

func (c *Client) GetAppObservabilityTrace(id, traceID string) (appObservabilityTraceResponse, error) {
	traceID = strings.TrimSpace(traceID)
	if traceID == "" {
		return appObservabilityTraceResponse{}, fmt.Errorf("trace_id is required")
	}
	relative := appObservabilityPath(id, "traces") + "/" + url.PathEscape(traceID)
	var response appObservabilityTraceResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appObservabilityTraceResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAppObservabilityDiagnosis(id string, opts appObservabilityDiagnosisOptions) (appObservabilityDiagnosisResponse, error) {
	values := url.Values{}
	appendAppObservabilityWindowValues(values, opts.appObservabilityWindowOptions)
	relative := appObservabilityPath(id, "diagnosis")
	if encoded := values.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	var response appObservabilityDiagnosisResponse
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appObservabilityDiagnosisResponse{}, err
	}
	return response, nil
}

func appObservabilityPath(id string, parts ...string) string {
	elements := []string{"/v1/apps", strings.TrimSpace(id), "observability"}
	elements = append(elements, parts...)
	return path.Join(elements...)
}

func appendAppObservabilityWindowValues(values url.Values, opts appObservabilityWindowOptions) {
	if value := strings.TrimSpace(opts.Since); value != "" {
		values.Set("since", value)
	}
	if value := strings.TrimSpace(opts.Until); value != "" {
		values.Set("until", value)
	}
}

func appendPositiveIntQueryValue(values url.Values, key string, value int) {
	if value > 0 {
		values.Set(key, fmt.Sprintf("%d", value))
	}
}

func appendAppObservabilityRequestFilterValues(values url.Values, opts appObservabilityRequestsOptions) {
	appendPositiveIntQueryValue(values, "limit", opts.Limit)
	if value := strings.TrimSpace(opts.TraceID); value != "" {
		values.Set("trace_id", value)
	}
	if value := strings.TrimSpace(opts.StatusClass); value != "" {
		values.Set("status_class", value)
	}
	if opts.Slow {
		values.Set("slow", "true")
	}
	if opts.Errors {
		values.Set("errors", "true")
	}
}
