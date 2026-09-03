package cli

import (
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

type appDiagnosticSession struct {
	ID              string     `json:"id"`
	AppID           string     `json:"app_id"`
	Kind            string     `json:"kind"`
	Status          string     `json:"status"`
	TargetPod       string     `json:"target_pod"`
	TargetContainer string     `json:"target_container"`
	TargetNode      string     `json:"target_node"`
	DurationSeconds int        `json:"duration_seconds"`
	FrequencyHz     int        `json:"frequency_hz"`
	CreatedAt       time.Time  `json:"created_at"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
	ExpiresAt       *time.Time `json:"expires_at,omitempty"`
	FailureReason   string     `json:"failure_reason,omitempty"`
}

type appDiagnosticSessionStartRequest struct {
	Kind            string `json:"kind"`
	DurationSeconds int    `json:"duration_seconds"`
	FrequencyHz     int    `json:"frequency_hz"`
	Pod             string `json:"pod,omitempty"`
	Container       string `json:"container,omitempty"`
}

type appDiagnosticSessionResponse struct {
	Session appDiagnosticSession `json:"session"`
}

type appDiagnosticSessionListResponse struct {
	Sessions []appDiagnosticSession `json:"sessions"`
}

type appDiagnosticSessionCancelResponse struct {
	Session  appDiagnosticSession `json:"session"`
	Canceled bool                 `json:"canceled"`
}

type appDiagnosticReportResponse struct {
	Session appDiagnosticSession `json:"session"`
	Report  map[string]any       `json:"report"`
}

func (c *Client) StartAppDiagnosticSession(appID string, request appDiagnosticSessionStartRequest) (appDiagnosticSessionResponse, error) {
	var response appDiagnosticSessionResponse
	if err := c.doJSON(http.MethodPost, appDiagnosticsPath(appID), request, &response); err != nil {
		return appDiagnosticSessionResponse{}, err
	}
	return response, nil
}

func (c *Client) ListAppDiagnosticSessions(appID string) (appDiagnosticSessionListResponse, error) {
	var response appDiagnosticSessionListResponse
	if err := c.doJSON(http.MethodGet, appDiagnosticsPath(appID), nil, &response); err != nil {
		return appDiagnosticSessionListResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAppDiagnosticSession(appID, sessionID string) (appDiagnosticSessionResponse, error) {
	var response appDiagnosticSessionResponse
	if err := c.doJSON(http.MethodGet, path.Join(appDiagnosticsPath(appID), url.PathEscape(strings.TrimSpace(sessionID))), nil, &response); err != nil {
		return appDiagnosticSessionResponse{}, err
	}
	return response, nil
}

func (c *Client) GetAppDiagnosticReport(appID, sessionID string) (appDiagnosticReportResponse, error) {
	var response appDiagnosticReportResponse
	relative := path.Join(appDiagnosticsPath(appID), url.PathEscape(strings.TrimSpace(sessionID)), "report")
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return appDiagnosticReportResponse{}, err
	}
	return response, nil
}

func (c *Client) CancelAppDiagnosticSession(appID, sessionID string) (appDiagnosticSessionCancelResponse, error) {
	var response appDiagnosticSessionCancelResponse
	if err := c.doJSON(http.MethodDelete, path.Join(appDiagnosticsPath(appID), url.PathEscape(strings.TrimSpace(sessionID))), nil, &response); err != nil {
		return appDiagnosticSessionCancelResponse{}, err
	}
	return response, nil
}

func appDiagnosticsPath(appID string) string {
	return path.Join("/v1/apps", strings.TrimSpace(appID), "diagnostics", "sessions")
}
