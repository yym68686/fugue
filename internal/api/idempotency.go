package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"fugue/internal/model"
)

const maxIdempotencyKeyLength = 200

type importProjectRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type importGitHubRequest struct {
	TenantID        string                 `json:"tenant_id"`
	ProjectID       string                 `json:"project_id"`
	Project         *importProjectRequest  `json:"project,omitempty"`
	RepoURL         string                 `json:"repo_url"`
	Branch          string                 `json:"branch"`
	SourceDir       string                 `json:"source_dir"`
	Name            string                 `json:"name"`
	Description     string                 `json:"description"`
	BuildStrategy   string                 `json:"build_strategy"`
	RuntimeID       string                 `json:"runtime_id"`
	Replicas        int                    `json:"replicas"`
	ServicePort     int                    `json:"service_port"`
	DockerfilePath  string                 `json:"dockerfile_path"`
	BuildContextDir string                 `json:"build_context_dir"`
	Env             map[string]string      `json:"env"`
	ConfigContent   string                 `json:"config_content"`
	Files           []model.AppFile        `json:"files"`
	Postgres        *model.AppPostgresSpec `json:"postgres"`
	IdempotencyKey  string                 `json:"idempotency_key"`
}

func resolveIdempotencyKey(r *http.Request, bodyKey string) (string, error) {
	headerKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	bodyKey = strings.TrimSpace(bodyKey)
	if headerKey != "" && bodyKey != "" && headerKey != bodyKey {
		return "", fmt.Errorf("Idempotency-Key header does not match body idempotency_key")
	}
	key := headerKey
	if key == "" {
		key = bodyKey
	}
	if len(key) > maxIdempotencyKeyLength {
		return "", fmt.Errorf("idempotency key exceeds %d characters", maxIdempotencyKeyLength)
	}
	return key, nil
}

func hashImportGitHubRequest(tenantID string, req importGitHubRequest, runtimeID string, replicas int) (string, error) {
	payload := struct {
		TenantID        string                 `json:"tenant_id"`
		ProjectID       string                 `json:"project_id"`
		Project         *importProjectRequest  `json:"project,omitempty"`
		RepoURL         string                 `json:"repo_url"`
		Branch          string                 `json:"branch"`
		SourceDir       string                 `json:"source_dir"`
		Name            string                 `json:"name"`
		Description     string                 `json:"description"`
		BuildStrategy   string                 `json:"build_strategy"`
		RuntimeID       string                 `json:"runtime_id"`
		Replicas        int                    `json:"replicas"`
		ServicePort     int                    `json:"service_port"`
		DockerfilePath  string                 `json:"dockerfile_path"`
		BuildContextDir string                 `json:"build_context_dir"`
		Env             map[string]string      `json:"env"`
		ConfigContent   string                 `json:"config_content"`
		Files           []model.AppFile        `json:"files"`
		Postgres        *model.AppPostgresSpec `json:"postgres"`
	}{
		TenantID:        strings.TrimSpace(tenantID),
		ProjectID:       strings.TrimSpace(req.ProjectID),
		Project:         normalizedImportProjectRequest(req.Project),
		RepoURL:         strings.TrimSpace(req.RepoURL),
		Branch:          strings.TrimSpace(req.Branch),
		SourceDir:       strings.TrimSpace(req.SourceDir),
		Name:            strings.TrimSpace(req.Name),
		Description:     strings.TrimSpace(req.Description),
		BuildStrategy:   normalizeBuildStrategy(req.BuildStrategy),
		RuntimeID:       strings.TrimSpace(runtimeID),
		Replicas:        replicas,
		ServicePort:     req.ServicePort,
		DockerfilePath:  strings.TrimSpace(req.DockerfilePath),
		BuildContextDir: strings.TrimSpace(req.BuildContextDir),
		Env:             req.Env,
		ConfigContent:   strings.TrimSpace(req.ConfigContent),
		Files:           req.Files,
		Postgres:        req.Postgres,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func normalizedImportProjectRequest(project *importProjectRequest) *importProjectRequest {
	if project == nil {
		return nil
	}
	return &importProjectRequest{
		Name:        strings.TrimSpace(project.Name),
		Description: strings.TrimSpace(project.Description),
	}
}
