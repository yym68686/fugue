package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"fugue/internal/model"
)

type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

type importUploadProjectRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type importUploadRequest struct {
	AppID           string                      `json:"app_id,omitempty"`
	TenantID        string                      `json:"tenant_id,omitempty"`
	ProjectID       string                      `json:"project_id,omitempty"`
	Project         *importUploadProjectRequest `json:"project,omitempty"`
	SourceDir       string                      `json:"source_dir,omitempty"`
	Name            string                      `json:"name,omitempty"`
	Description     string                      `json:"description,omitempty"`
	BuildStrategy   string                      `json:"build_strategy,omitempty"`
	RuntimeID       string                      `json:"runtime_id,omitempty"`
	Replicas        int                         `json:"replicas,omitempty"`
	ServicePort     int                         `json:"service_port,omitempty"`
	DockerfilePath  string                      `json:"dockerfile_path,omitempty"`
	BuildContextDir string                      `json:"build_context_dir,omitempty"`
}

type apiError struct {
	Error string `json:"error"`
}

func NewClient(baseURL, token string) (*Client, error) {
	baseURL = strings.TrimSpace(baseURL)
	token = strings.TrimSpace(token)
	if baseURL == "" {
		return nil, fmt.Errorf("base url is required")
	}
	if token == "" {
		return nil, fmt.Errorf("token is required")
	}
	if _, err := url.Parse(baseURL); err != nil {
		return nil, fmt.Errorf("parse base url: %w", err)
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}, nil
}

func (c *Client) ListTenants() ([]model.Tenant, error) {
	var response struct {
		Tenants []model.Tenant `json:"tenants"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/tenants", nil, &response); err != nil {
		return nil, err
	}
	return response.Tenants, nil
}

func (c *Client) ListProjects(tenantID string) ([]model.Project, error) {
	var response struct {
		Projects []model.Project `json:"projects"`
	}
	relative := "/v1/projects"
	if trimmed := strings.TrimSpace(tenantID); trimmed != "" {
		relative += "?tenant_id=" + url.QueryEscape(trimmed)
	}
	if err := c.doJSON(http.MethodGet, relative, nil, &response); err != nil {
		return nil, err
	}
	return response.Projects, nil
}

func (c *Client) ListApps() ([]model.App, error) {
	var response struct {
		Apps []model.App `json:"apps"`
	}
	if err := c.doJSON(http.MethodGet, "/v1/apps", nil, &response); err != nil {
		return nil, err
	}
	return response.Apps, nil
}

func (c *Client) ImportUpload(req importUploadRequest, archiveName string, archiveBytes []byte) (model.App, model.Operation, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	requestJSON, err := json.Marshal(req)
	if err != nil {
		return model.App{}, model.Operation{}, fmt.Errorf("marshal request: %w", err)
	}
	if err := writer.WriteField("request", string(requestJSON)); err != nil {
		return model.App{}, model.Operation{}, fmt.Errorf("write request field: %w", err)
	}
	part, err := writer.CreateFormFile("archive", archiveName)
	if err != nil {
		return model.App{}, model.Operation{}, fmt.Errorf("create archive field: %w", err)
	}
	if _, err := part.Write(archiveBytes); err != nil {
		return model.App{}, model.Operation{}, fmt.Errorf("write archive field: %w", err)
	}
	if err := writer.Close(); err != nil {
		return model.App{}, model.Operation{}, fmt.Errorf("close multipart writer: %w", err)
	}

	httpReq, err := http.NewRequest(http.MethodPost, c.resolveURL("/v1/apps/import-upload"), &body)
	if err != nil {
		return model.App{}, model.Operation{}, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	httpReq.Header.Set("Content-Type", writer.FormDataContentType())

	payload, err := c.do(httpReq)
	if err != nil {
		return model.App{}, model.Operation{}, err
	}
	var response struct {
		App       model.App       `json:"app"`
		Operation model.Operation `json:"operation"`
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		return model.App{}, model.Operation{}, fmt.Errorf("decode import upload response: %w", err)
	}
	return response.App, response.Operation, nil
}

func (c *Client) GetOperation(id string) (model.Operation, error) {
	var response struct {
		Operation model.Operation `json:"operation"`
	}
	if err := c.doJSON(http.MethodGet, path.Join("/v1/operations", id), nil, &response); err != nil {
		return model.Operation{}, err
	}
	return response.Operation, nil
}

func (c *Client) GetApp(id string) (model.App, error) {
	var response struct {
		App model.App `json:"app"`
	}
	if err := c.doJSON(http.MethodGet, path.Join("/v1/apps", id), nil, &response); err != nil {
		return model.App{}, err
	}
	return response.App, nil
}

func (c *Client) GetBuildLogs(appID, operationID string) (string, error) {
	query := url.Values{}
	if strings.TrimSpace(operationID) != "" {
		query.Set("operation_id", strings.TrimSpace(operationID))
	}
	query.Set("tail_lines", "200")
	relative := path.Join("/v1/apps", appID, "build-logs")
	if encoded := query.Encode(); encoded != "" {
		relative += "?" + encoded
	}
	payload, err := c.doJSONRaw(http.MethodGet, relative, nil)
	if err != nil {
		return "", err
	}
	var response struct {
		Logs    string `json:"logs"`
		Summary string `json:"summary"`
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		return "", fmt.Errorf("decode build logs response: %w", err)
	}
	if strings.TrimSpace(response.Logs) != "" {
		return response.Logs, nil
	}
	return response.Summary, nil
}

func (c *Client) doJSON(method, relativePath string, requestBody any, responseBody any) error {
	payload, err := c.doJSONRaw(method, relativePath, requestBody)
	if err != nil {
		return err
	}
	if responseBody == nil {
		return nil
	}
	if err := json.Unmarshal(payload, responseBody); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func (c *Client) doJSONRaw(method, relativePath string, requestBody any) ([]byte, error) {
	var body io.Reader
	if requestBody != nil {
		raw, err := json.Marshal(requestBody)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(raw)
	}
	httpReq, err := http.NewRequest(method, c.resolveURL(relativePath), body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.token)
	if requestBody != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	return c.do(httpReq)
}

func (c *Client) do(httpReq *http.Request) ([]byte, error) {
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr apiError
		if err := json.Unmarshal(payload, &apiErr); err == nil && strings.TrimSpace(apiErr.Error) != "" {
			return nil, fmt.Errorf("%s", apiErr.Error)
		}
		if trimmed := strings.TrimSpace(string(payload)); trimmed != "" {
			return nil, fmt.Errorf("request failed: status=%d body=%s", resp.StatusCode, trimmed)
		}
		return nil, fmt.Errorf("request failed: status=%d", resp.StatusCode)
	}
	return payload, nil
}

func (c *Client) resolveURL(relativePath string) string {
	return c.baseURL + relativePath
}
