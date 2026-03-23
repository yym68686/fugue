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

type importGitHubRequest struct {
	TenantID        string                 `json:"tenant_id"`
	ProjectID       string                 `json:"project_id"`
	RepoURL         string                 `json:"repo_url"`
	Branch          string                 `json:"branch"`
	SourceDir       string                 `json:"source_dir"`
	Name            string                 `json:"name"`
	Description     string                 `json:"description"`
	RuntimeID       string                 `json:"runtime_id"`
	Replicas        int                    `json:"replicas"`
	Profile         string                 `json:"profile"`
	DockerfilePath  string                 `json:"dockerfile_path"`
	BuildContextDir string                 `json:"build_context_dir"`
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

func hashImportGitHubRequest(tenantID string, req importGitHubRequest, runtimeID string, replicas int, profile string) (string, error) {
	payload := struct {
		TenantID        string                 `json:"tenant_id"`
		ProjectID       string                 `json:"project_id"`
		RepoURL         string                 `json:"repo_url"`
		Branch          string                 `json:"branch"`
		SourceDir       string                 `json:"source_dir"`
		Name            string                 `json:"name"`
		Description     string                 `json:"description"`
		RuntimeID       string                 `json:"runtime_id"`
		Replicas        int                    `json:"replicas"`
		Profile         string                 `json:"profile"`
		DockerfilePath  string                 `json:"dockerfile_path"`
		BuildContextDir string                 `json:"build_context_dir"`
		ConfigContent   string                 `json:"config_content"`
		Files           []model.AppFile        `json:"files"`
		Postgres        *model.AppPostgresSpec `json:"postgres"`
	}{
		TenantID:        strings.TrimSpace(tenantID),
		ProjectID:       strings.TrimSpace(req.ProjectID),
		RepoURL:         strings.TrimSpace(req.RepoURL),
		Branch:          strings.TrimSpace(req.Branch),
		SourceDir:       strings.TrimSpace(req.SourceDir),
		Name:            strings.TrimSpace(req.Name),
		Description:     strings.TrimSpace(req.Description),
		RuntimeID:       strings.TrimSpace(runtimeID),
		Replicas:        replicas,
		Profile:         strings.TrimSpace(profile),
		DockerfilePath:  strings.TrimSpace(req.DockerfilePath),
		BuildContextDir: strings.TrimSpace(req.BuildContextDir),
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
