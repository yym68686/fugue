package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"fugue/internal/model"
)

const maxIdempotencyKeyLength = 200

type importProjectRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

type importGitHubPersistentStorageSeedFile struct {
	Service     string `json:"service"`
	Path        string `json:"path"`
	SeedContent string `json:"seed_content"`
}

type importGitHubRequest struct {
	TenantID                   string                                  `json:"tenant_id"`
	ProjectID                  string                                  `json:"project_id"`
	Project                    *importProjectRequest                   `json:"project,omitempty"`
	RepoURL                    string                                  `json:"repo_url"`
	RepoVisibility             string                                  `json:"repo_visibility"`
	RepoAuthToken              string                                  `json:"repo_auth_token"`
	Branch                     string                                  `json:"branch"`
	SourceDir                  string                                  `json:"source_dir"`
	Name                       string                                  `json:"name"`
	Description                string                                  `json:"description"`
	BuildStrategy              string                                  `json:"build_strategy"`
	RuntimeID                  string                                  `json:"runtime_id"`
	Replicas                   int                                     `json:"replicas"`
	ServicePort                int                                     `json:"service_port"`
	DockerfilePath             string                                  `json:"dockerfile_path"`
	BuildContextDir            string                                  `json:"build_context_dir"`
	Env                        map[string]string                       `json:"env"`
	ConfigContent              string                                  `json:"config_content"`
	Files                      []model.AppFile                         `json:"files"`
	StartupCommand             *string                                 `json:"startup_command,omitempty"`
	PersistentStorage          *model.AppPersistentStorageSpec         `json:"persistent_storage,omitempty"`
	PersistentStorageSeedFiles []importGitHubPersistentStorageSeedFile `json:"persistent_storage_seed_files"`
	Postgres                   *model.AppPostgresSpec                  `json:"postgres"`
	IdempotencyKey             string                                  `json:"idempotency_key"`
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
		TenantID                   string                                  `json:"tenant_id"`
		ProjectID                  string                                  `json:"project_id"`
		Project                    *importProjectRequest                   `json:"project,omitempty"`
		RepoURL                    string                                  `json:"repo_url"`
		RepoVisibility             string                                  `json:"repo_visibility"`
		RepoAuthToken              string                                  `json:"repo_auth_token"`
		Branch                     string                                  `json:"branch"`
		SourceDir                  string                                  `json:"source_dir"`
		Name                       string                                  `json:"name"`
		Description                string                                  `json:"description"`
		BuildStrategy              string                                  `json:"build_strategy"`
		RuntimeID                  string                                  `json:"runtime_id"`
		Replicas                   int                                     `json:"replicas"`
		ServicePort                int                                     `json:"service_port"`
		DockerfilePath             string                                  `json:"dockerfile_path"`
		BuildContextDir            string                                  `json:"build_context_dir"`
		Env                        map[string]string                       `json:"env"`
		ConfigContent              string                                  `json:"config_content"`
		Files                      []model.AppFile                         `json:"files"`
		StartupCommand             string                                  `json:"startup_command"`
		PersistentStorage          *model.AppPersistentStorageSpec         `json:"persistent_storage,omitempty"`
		PersistentStorageSeedFiles []importGitHubPersistentStorageSeedFile `json:"persistent_storage_seed_files"`
		Postgres                   *model.AppPostgresSpec                  `json:"postgres"`
	}{
		TenantID:                   strings.TrimSpace(tenantID),
		ProjectID:                  strings.TrimSpace(req.ProjectID),
		Project:                    normalizedImportProjectRequest(req.Project),
		RepoURL:                    strings.TrimSpace(req.RepoURL),
		RepoVisibility:             strings.TrimSpace(req.RepoVisibility),
		RepoAuthToken:              strings.TrimSpace(req.RepoAuthToken),
		Branch:                     strings.TrimSpace(req.Branch),
		SourceDir:                  strings.TrimSpace(req.SourceDir),
		Name:                       strings.TrimSpace(req.Name),
		Description:                strings.TrimSpace(req.Description),
		BuildStrategy:              normalizeBuildStrategy(req.BuildStrategy),
		RuntimeID:                  strings.TrimSpace(runtimeID),
		Replicas:                   replicas,
		ServicePort:                req.ServicePort,
		DockerfilePath:             strings.TrimSpace(req.DockerfilePath),
		BuildContextDir:            strings.TrimSpace(req.BuildContextDir),
		Env:                        req.Env,
		ConfigContent:              strings.TrimSpace(req.ConfigContent),
		Files:                      req.Files,
		StartupCommand:             strings.Join(normalizeStartupCommand(req.StartupCommand), "\x00"),
		PersistentStorage:          req.PersistentStorage,
		PersistentStorageSeedFiles: normalizedImportGitHubPersistentStorageSeedFiles(req.PersistentStorageSeedFiles),
		Postgres:                   req.Postgres,
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

func normalizedImportGitHubPersistentStorageSeedFiles(files []importGitHubPersistentStorageSeedFile) []importGitHubPersistentStorageSeedFile {
	if len(files) == 0 {
		return nil
	}

	normalized := make([]importGitHubPersistentStorageSeedFile, 0, len(files))
	for _, file := range files {
		normalized = append(normalized, importGitHubPersistentStorageSeedFile{
			Service:     strings.TrimSpace(file.Service),
			Path:        strings.TrimSpace(file.Path),
			SeedContent: file.SeedContent,
		})
	}

	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].Service == normalized[j].Service {
			return normalized[i].Path < normalized[j].Path
		}
		return normalized[i].Service < normalized[j].Service
	})

	return normalized
}
