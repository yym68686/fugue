package cli

import (
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"fugue/internal/livediagnostics"
)

const platformDiagnosticsPath = "/v1/admin/diagnostics/sessions"

type platformDiagnosticTargetRequest struct {
	Type        livediagnostics.TargetType `json:"type"`
	Component   string                     `json:"component,omitempty"`
	Namespace   string                     `json:"namespace,omitempty"`
	Pod         string                     `json:"pod,omitempty"`
	Container   string                     `json:"container,omitempty"`
	Node        string                     `json:"node,omitempty"`
	ProcessName string                     `json:"process_name,omitempty"`
}

type platformDiagnosticStartRequest struct {
	Target                     platformDiagnosticTargetRequest `json:"target"`
	Kind                       livediagnostics.ProbeKind       `json:"kind"`
	DurationSeconds            int                             `json:"duration_seconds"`
	FrequencyHz                int                             `json:"frequency_hz"`
	SampleIntervalMilliseconds int                             `json:"sample_interval_milliseconds,omitempty"`
}

type platformDiagnosticSession struct {
	ID                         string                    `json:"id"`
	Kind                       livediagnostics.ProbeKind `json:"kind"`
	Status                     string                    `json:"status"`
	Target                     livediagnostics.Target    `json:"target"`
	ControlPath                string                    `json:"control_path"`
	DurationSeconds            int                       `json:"duration_seconds"`
	FrequencyHz                int                       `json:"frequency_hz"`
	SampleIntervalMilliseconds int                       `json:"sample_interval_milliseconds"`
	CreatedAt                  time.Time                 `json:"created_at"`
	StartedAt                  *time.Time                `json:"started_at,omitempty"`
	FinishedAt                 *time.Time                `json:"finished_at,omitempty"`
	ExpiresAt                  *time.Time                `json:"expires_at,omitempty"`
	FailureReason              string                    `json:"failure_reason,omitempty"`
}

type platformDiagnosticSessionResponse struct {
	Session platformDiagnosticSession `json:"session"`
}

type platformDiagnosticSessionListResponse struct {
	Sessions []platformDiagnosticSession `json:"sessions"`
}

type platformDiagnosticSessionCancelResponse struct {
	Session  platformDiagnosticSession `json:"session"`
	Canceled bool                      `json:"canceled"`
}

type platformDiagnosticReportResponse struct {
	Session platformDiagnosticSession `json:"session"`
	Report  map[string]any            `json:"report"`
}

func (c *Client) StartPlatformDiagnosticSession(request platformDiagnosticStartRequest) (platformDiagnosticSessionResponse, error) {
	var response platformDiagnosticSessionResponse
	if err := c.doJSON(http.MethodPost, platformDiagnosticsPath, request, &response); err != nil {
		return platformDiagnosticSessionResponse{}, err
	}
	return response, nil
}

func (c *Client) ListPlatformDiagnosticSessions() (platformDiagnosticSessionListResponse, error) {
	var response platformDiagnosticSessionListResponse
	if err := c.doJSON(http.MethodGet, platformDiagnosticsPath, nil, &response); err != nil {
		return platformDiagnosticSessionListResponse{}, err
	}
	return response, nil
}

func (c *Client) GetPlatformDiagnosticSession(sessionID string) (platformDiagnosticSessionResponse, error) {
	var response platformDiagnosticSessionResponse
	if err := c.doJSON(http.MethodGet, platformDiagnosticSessionPath(sessionID), nil, &response); err != nil {
		return platformDiagnosticSessionResponse{}, err
	}
	return response, nil
}

func (c *Client) GetPlatformDiagnosticReport(sessionID string) (platformDiagnosticReportResponse, error) {
	var response platformDiagnosticReportResponse
	if err := c.doJSON(http.MethodGet, path.Join(platformDiagnosticSessionPath(sessionID), "report"), nil, &response); err != nil {
		return platformDiagnosticReportResponse{}, err
	}
	return response, nil
}

func (c *Client) CancelPlatformDiagnosticSession(sessionID string) (platformDiagnosticSessionCancelResponse, error) {
	var response platformDiagnosticSessionCancelResponse
	if err := c.doJSON(http.MethodDelete, platformDiagnosticSessionPath(sessionID), nil, &response); err != nil {
		return platformDiagnosticSessionCancelResponse{}, err
	}
	return response, nil
}

func platformDiagnosticSessionPath(sessionID string) string {
	return path.Join(platformDiagnosticsPath, url.PathEscape(strings.TrimSpace(sessionID)))
}
