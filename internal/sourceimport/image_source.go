package sourceimport

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"fugue/internal/model"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

type DockerImageSourceImportRequest struct {
	AppName          string
	ImageNameSuffix  string
	ImageRef         string
	ImageRepository  string
	RegistryPushBase string
}

func (i *Importer) ImportDockerImageSource(ctx context.Context, req DockerImageSourceImportRequest) (GitHubSourceImportOutput, error) {
	if strings.TrimSpace(req.RegistryPushBase) == "" {
		return GitHubSourceImportOutput{}, fmt.Errorf("registry push base is empty")
	}

	sourceImageRef, err := normalizeContainerImageRef(req.ImageRef)
	if err != nil {
		return GitHubSourceImportOutput{}, err
	}

	sourceOptions := craneOptionsForImageRef(ctx, sourceImageRef)
	digest, err := crane.Digest(sourceImageRef, sourceOptions...)
	if err != nil {
		return GitHubSourceImportOutput{}, fmt.Errorf("resolve image digest: %w", err)
	}

	destImageRef := defaultMirroredImageRef(
		req.RegistryPushBase,
		req.ImageRepository,
		req.AppName,
		sourceImageRef,
		digest,
		req.ImageNameSuffix,
	)
	copyOptions := append([]crane.Option{}, sourceOptions...)
	if isInsecureRegistryHost(registryHostFromImageRef(destImageRef)) {
		copyOptions = append(copyOptions, crane.Insecure)
	}
	if err := crane.Copy(sourceImageRef, destImageRef, copyOptions...); err != nil {
		return GitHubSourceImportOutput{}, fmt.Errorf("mirror image into internal registry: %w", err)
	}

	detectedPort := 80
	configFile, configErr := readRemoteImageConfig(sourceImageRef, sourceOptions...)
	if configErr == nil {
		if port := detectExposedPortFromImageConfig(configFile); port > 0 {
			detectedPort = port
		}
	} else if i != nil && i.Logger != nil {
		i.Logger.Printf("skip image config inspection for %s: %v", sourceImageRef, configErr)
	}

	return GitHubSourceImportOutput{
		ImportResult: GitHubImportResult{
			DefaultAppName:   defaultImportedImageAppName(req.AppName, sourceImageRef),
			DetectedPort:     detectedPort,
			DetectedProvider: model.AppSourceTypeDockerImage,
			ImageRef:         destImageRef,
		},
		Source: model.AppSource{
			Type:             model.AppSourceTypeDockerImage,
			ImageRef:         strings.TrimSpace(req.ImageRef),
			ResolvedImageRef: destImageRef,
			ImageNameSuffix:  strings.TrimSpace(req.ImageNameSuffix),
			DetectedProvider: model.AppSourceTypeDockerImage,
		},
	}, nil
}

func normalizeContainerImageRef(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("image_ref is required")
	}

	ref, err := name.ParseReference(trimmed, imageReferenceNameOptions(trimmed)...)
	if err != nil {
		return "", fmt.Errorf("parse image_ref: %w", err)
	}
	return ref.Name(), nil
}

func imageReferenceNameOptions(imageRef string) []name.Option {
	if isInsecureRegistryHost(registryHostFromImageRef(imageRef)) {
		return []name.Option{name.Insecure}
	}
	return nil
}

func craneOptionsForImageRef(ctx context.Context, imageRef string) []crane.Option {
	options := []crane.Option{
		crane.WithContext(ctx),
		crane.WithAuthFromKeychain(authn.DefaultKeychain),
	}
	if isInsecureRegistryHost(registryHostFromImageRef(imageRef)) {
		options = append(options, crane.Insecure)
	}
	return options
}

func InspectRemoteImageConfig(ctx context.Context, imageRef string) (*v1.ConfigFile, error) {
	normalized, err := normalizeContainerImageRef(imageRef)
	if err != nil {
		return nil, err
	}
	return readRemoteImageConfig(normalized, craneOptionsForImageRef(ctx, normalized)...)
}

func readRemoteImageConfig(imageRef string, options ...crane.Option) (*v1.ConfigFile, error) {
	data, err := crane.Config(imageRef, options...)
	if err != nil {
		return nil, err
	}

	var configFile v1.ConfigFile
	if err := json.Unmarshal(data, &configFile); err != nil {
		return nil, fmt.Errorf("decode image config: %w", err)
	}
	return &configFile, nil
}

func detectExposedPortFromImageConfig(configFile *v1.ConfigFile) int {
	if configFile == nil || len(configFile.Config.ExposedPorts) == 0 {
		return 0
	}

	detected := 0
	for rawPort := range configFile.Config.ExposedPorts {
		value := strings.TrimSpace(rawPort)
		if idx := strings.Index(value, "/"); idx >= 0 {
			value = value[:idx]
		}
		port, err := strconv.Atoi(value)
		if err != nil || port <= 0 {
			continue
		}
		if detected == 0 || port < detected {
			detected = port
		}
	}
	return detected
}

func defaultMirroredImageRef(registryPushBase, imageRepository, appName, sourceImageRef, digest, imageNameSuffix string) string {
	imageRepository = strings.Trim(strings.TrimSpace(imageRepository), "/")
	if imageRepository == "" {
		imageRepository = "fugue-apps"
	}

	repoPath := defaultImportedImageAppName(appName, sourceImageRef)
	if trimmedSuffix := strings.TrimSpace(imageNameSuffix); trimmedSuffix != "" {
		if suffix := model.Slugify(trimmedSuffix); suffix != "" {
			repoPath += "-" + suffix
		}
	}
	if repoPath == "" {
		repoPath = "image"
	}

	tagSeed := strings.TrimSpace(strings.TrimPrefix(digest, "sha256:"))
	if tagSeed == "" {
		tagSeed = model.Slugify(sourceImageRef)
	}
	if tagSeed == "" {
		tagSeed = "latest"
	}

	return fmt.Sprintf(
		"%s/%s/%s:image-%s",
		strings.TrimSpace(registryPushBase),
		imageRepository,
		repoPath,
		shortCommit(tagSeed),
	)
}

func defaultImportedImageAppName(appName, sourceImageRef string) string {
	appName = strings.TrimSpace(appName)
	if appName != "" {
		if slug := model.Slugify(appName); slug != "" {
			return slug
		}
	}
	if baseName := imageSourceBaseName(sourceImageRef); baseName != "" {
		if slug := model.Slugify(baseName); slug != "" {
			return slug
		}
	}
	return "app"
}

func imageSourceBaseName(imageRef string) string {
	imageRef = strings.TrimSpace(imageRef)
	if imageRef == "" {
		return ""
	}
	if idx := strings.Index(imageRef, "@"); idx >= 0 {
		imageRef = imageRef[:idx]
	}

	lastSegment := imageRef
	if idx := strings.LastIndex(lastSegment, "/"); idx >= 0 {
		lastSegment = lastSegment[idx+1:]
	}
	if idx := strings.Index(lastSegment, ":"); idx >= 0 {
		lastSegment = lastSegment[:idx]
	}
	return lastSegment
}
