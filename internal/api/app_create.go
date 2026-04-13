package api

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

func (s *Server) normalizeCreateAppSource(raw *model.AppSource) (*model.AppSource, error) {
	if raw == nil {
		return nil, nil
	}

	source := cloneAppSource(raw)
	if source == nil {
		return nil, nil
	}

	sourceType := strings.TrimSpace(source.Type)
	switch {
	case sourceType == model.AppSourceTypeDockerImage || (sourceType == "" && strings.TrimSpace(source.ImageRef) != ""):
		queued, err := buildQueuedImageSource(
			strings.TrimSpace(source.ImageRef),
			strings.TrimSpace(source.ImageNameSuffix),
			strings.TrimSpace(source.ComposeService),
		)
		if err != nil {
			return nil, err
		}
		return &queued, nil
	case model.IsGitHubAppSourceType(sourceType) || (sourceType == "" && strings.TrimSpace(source.RepoURL) != ""):
		queued, err := buildQueuedGitHubSource(
			strings.TrimSpace(source.RepoURL),
			model.ResolveGitHubAppSourceType(sourceType, strings.TrimSpace(source.RepoAuthToken) != ""),
			strings.TrimSpace(source.RepoAuthToken),
			strings.TrimSpace(source.RepoBranch),
			strings.TrimSpace(source.SourceDir),
			strings.TrimSpace(source.DockerfilePath),
			strings.TrimSpace(source.BuildContextDir),
			normalizeBuildStrategy(source.BuildStrategy),
			strings.TrimSpace(source.ImageNameSuffix),
			strings.TrimSpace(source.ComposeService),
		)
		if err != nil {
			return nil, err
		}
		return &queued, nil
	case sourceType == model.AppSourceTypeUpload || (sourceType == "" && strings.TrimSpace(source.UploadID) != ""):
		uploadID := strings.TrimSpace(source.UploadID)
		if uploadID == "" {
			return nil, fmt.Errorf("source upload_id is required")
		}
		upload, err := s.store.GetSourceUpload(uploadID)
		if err != nil {
			return nil, err
		}
		queued, err := buildQueuedUploadSource(
			upload,
			strings.TrimSpace(source.SourceDir),
			strings.TrimSpace(source.DockerfilePath),
			strings.TrimSpace(source.BuildContextDir),
			normalizeBuildStrategy(source.BuildStrategy),
			strings.TrimSpace(source.ImageNameSuffix),
			strings.TrimSpace(source.ComposeService),
		)
		if err != nil {
			return nil, err
		}
		return &queued, nil
	default:
		return nil, fmt.Errorf("unsupported source type %q", strings.TrimSpace(source.Type))
	}
}
