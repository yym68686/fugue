package sourceimport

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"fugue/internal/model"
)

type UploadSourceImportRequest struct {
	UploadID              string
	ArchiveFilename       string
	ArchiveSHA256         string
	ArchiveSizeBytes      int64
	ArchiveData           []byte
	ArchiveDownloadURL    string
	AppName               string
	SourceDir             string
	DockerfilePath        string
	BuildContextDir       string
	BuildStrategy         string
	RegistryPushBase      string
	ImageRepository       string
	ImageNameSuffix       string
	ComposeService        string
	JobLabels             map[string]string
	PlacementNodeSelector map[string]string
	Stateful              bool
}

type extractedUploadSource struct {
	UploadID       string
	ArchiveName    string
	ArchiveSHA256  string
	ArchiveSize    int64
	RootDir        string
	DefaultAppName string
}

func (i *Importer) ImportUploadedArchiveSource(ctx context.Context, req UploadSourceImportRequest) (GitHubSourceImportOutput, error) {
	if strings.TrimSpace(req.UploadID) == "" || len(req.ArchiveData) == 0 {
		return GitHubSourceImportOutput{}, fmt.Errorf("upload archive is required")
	}
	if strings.TrimSpace(req.RegistryPushBase) == "" {
		return GitHubSourceImportOutput{}, fmt.Errorf("registry push base is empty")
	}

	src, err := i.extractUploadedArchive(req)
	if err != nil {
		return GitHubSourceImportOutput{}, err
	}
	defer releaseExtractedUploadSource(src)

	buildStrategy := normalizeGitHubBuildStrategy(req.BuildStrategy)
	if buildStrategy == "" {
		buildStrategy = model.AppBuildStrategyAuto
	}

	switch buildStrategy {
	case model.AppBuildStrategyAuto:
		detectedStrategy, sourceDir, dockerfilePath, buildContextDir, err := detectAutoImportInputs(src.RootDir, req.SourceDir, req.DockerfilePath, req.BuildContextDir)
		if err != nil {
			return GitHubSourceImportOutput{}, err
		}
		req.BuildStrategy = detectedStrategy
		req.SourceDir = sourceDir
		req.DockerfilePath = dockerfilePath
		req.BuildContextDir = buildContextDir
		return i.ImportUploadedArchiveSource(ctx, req)
	case model.AppBuildStrategyStaticSite:
		result, err := importStaticSiteFromExtractedUpload(src, req.SourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix)
		if err != nil {
			return GitHubSourceImportOutput{}, err
		}
		return GitHubSourceImportOutput{
			ImportResult: result,
			Source: model.AppSource{
				Type:             model.AppSourceTypeUpload,
				UploadID:         src.UploadID,
				UploadFilename:   strings.TrimSpace(req.ArchiveFilename),
				ArchiveSHA256:    src.ArchiveSHA256,
				ArchiveSizeBytes: src.ArchiveSize,
				ResolvedImageRef: result.ImageRef,
				SourceDir:        result.SourceDir,
				BuildStrategy:    result.BuildStrategy,
				CommitSHA:        src.ArchiveSHA256,
				ImageNameSuffix:  strings.TrimSpace(req.ImageNameSuffix),
				ComposeService:   strings.TrimSpace(req.ComposeService),
				DetectedProvider: strings.TrimSpace(result.DetectedProvider),
				DetectedStack:    strings.TrimSpace(result.DetectedStack),
			},
		}, nil
	case model.AppBuildStrategyDockerfile:
		result, err := importDockerfileFromExtractedUpload(ctx, src, req.ArchiveDownloadURL, req.DockerfilePath, req.BuildContextDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful)
		if err != nil {
			return GitHubSourceImportOutput{}, err
		}
		return GitHubSourceImportOutput{
			ImportResult: result,
			Source: model.AppSource{
				Type:             model.AppSourceTypeUpload,
				UploadID:         src.UploadID,
				UploadFilename:   strings.TrimSpace(req.ArchiveFilename),
				ArchiveSHA256:    src.ArchiveSHA256,
				ArchiveSizeBytes: src.ArchiveSize,
				ResolvedImageRef: result.ImageRef,
				BuildStrategy:    result.BuildStrategy,
				CommitSHA:        src.ArchiveSHA256,
				DockerfilePath:   result.DockerfilePath,
				BuildContextDir:  result.BuildContextDir,
				ImageNameSuffix:  strings.TrimSpace(req.ImageNameSuffix),
				ComposeService:   strings.TrimSpace(req.ComposeService),
				DetectedProvider: strings.TrimSpace(result.DetectedProvider),
				DetectedStack:    strings.TrimSpace(result.DetectedStack),
			},
		}, nil
	case model.AppBuildStrategyBuildpacks:
		result, err := importBuildpacksFromExtractedUpload(ctx, src, req.ArchiveDownloadURL, req.SourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful)
		if err != nil {
			return GitHubSourceImportOutput{}, err
		}
		return GitHubSourceImportOutput{
			ImportResult: result,
			Source: model.AppSource{
				Type:             model.AppSourceTypeUpload,
				UploadID:         src.UploadID,
				UploadFilename:   strings.TrimSpace(req.ArchiveFilename),
				ArchiveSHA256:    src.ArchiveSHA256,
				ArchiveSizeBytes: src.ArchiveSize,
				ResolvedImageRef: result.ImageRef,
				SourceDir:        result.SourceDir,
				BuildStrategy:    result.BuildStrategy,
				CommitSHA:        src.ArchiveSHA256,
				ImageNameSuffix:  strings.TrimSpace(req.ImageNameSuffix),
				ComposeService:   strings.TrimSpace(req.ComposeService),
				DetectedProvider: strings.TrimSpace(result.DetectedProvider),
				DetectedStack:    strings.TrimSpace(result.DetectedStack),
			},
		}, nil
	case model.AppBuildStrategyNixpacks:
		result, err := importNixpacksFromExtractedUpload(ctx, src, req.ArchiveDownloadURL, req.SourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful)
		if err != nil {
			return GitHubSourceImportOutput{}, err
		}
		return GitHubSourceImportOutput{
			ImportResult: result,
			Source: model.AppSource{
				Type:             model.AppSourceTypeUpload,
				UploadID:         src.UploadID,
				UploadFilename:   strings.TrimSpace(req.ArchiveFilename),
				ArchiveSHA256:    src.ArchiveSHA256,
				ArchiveSizeBytes: src.ArchiveSize,
				ResolvedImageRef: result.ImageRef,
				SourceDir:        result.SourceDir,
				BuildStrategy:    result.BuildStrategy,
				CommitSHA:        src.ArchiveSHA256,
				ImageNameSuffix:  strings.TrimSpace(req.ImageNameSuffix),
				ComposeService:   strings.TrimSpace(req.ComposeService),
				DetectedProvider: strings.TrimSpace(result.DetectedProvider),
				DetectedStack:    strings.TrimSpace(result.DetectedStack),
			},
		}, nil
	default:
		return GitHubSourceImportOutput{}, fmt.Errorf("unsupported build strategy %q", buildStrategy)
	}
}

func (i *Importer) extractUploadedArchive(req UploadSourceImportRequest) (extractedUploadSource, error) {
	if err := os.MkdirAll(i.WorkDir, 0o755); err != nil {
		return extractedUploadSource{}, fmt.Errorf("create import work dir: %w", err)
	}
	rootDir, err := os.MkdirTemp(i.WorkDir, "upload-import-*")
	if err != nil {
		return extractedUploadSource{}, fmt.Errorf("create upload import temp dir: %w", err)
	}
	if err := extractTarGzArchive(rootDir, req.ArchiveData); err != nil {
		_ = os.RemoveAll(rootDir)
		return extractedUploadSource{}, err
	}

	defaultAppName := model.Slugify(strings.TrimSpace(req.AppName))
	if defaultAppName == "" {
		defaultAppName = model.Slugify(strings.TrimSuffix(strings.TrimSpace(req.ArchiveFilename), filepath.Ext(strings.TrimSpace(req.ArchiveFilename))))
	}
	if defaultAppName == "" {
		defaultAppName = "app"
	}

	return extractedUploadSource{
		UploadID:       strings.TrimSpace(req.UploadID),
		ArchiveName:    strings.TrimSpace(req.ArchiveFilename),
		ArchiveSHA256:  strings.TrimSpace(req.ArchiveSHA256),
		ArchiveSize:    req.ArchiveSizeBytes,
		RootDir:        rootDir,
		DefaultAppName: defaultAppName,
	}, nil
}

func releaseExtractedUploadSource(src extractedUploadSource) {
	if strings.TrimSpace(src.RootDir) == "" {
		return
	}
	_ = os.RemoveAll(src.RootDir)
}

func extractTarGzArchive(dstDir string, archiveData []byte) error {
	gzipReader, err := gzip.NewReader(bytes.NewReader(archiveData))
	if err != nil {
		return fmt.Errorf("open uploaded archive: %w", err)
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		switch {
		case err == io.EOF:
			return nil
		case err != nil:
			return fmt.Errorf("read uploaded archive: %w", err)
		}

		name := filepath.Clean(strings.TrimPrefix(header.Name, "./"))
		if name == "." || name == "" {
			continue
		}
		if strings.HasPrefix(name, ".."+string(filepath.Separator)) || name == ".." || filepath.IsAbs(name) {
			return fmt.Errorf("archive entry %q escapes destination", header.Name)
		}
		targetPath := filepath.Join(dstDir, name)
		if !strings.HasPrefix(filepath.Clean(targetPath), filepath.Clean(dstDir)+string(filepath.Separator)) && filepath.Clean(targetPath) != filepath.Clean(dstDir) {
			return fmt.Errorf("archive entry %q escapes destination", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, 0o755); err != nil {
				return fmt.Errorf("mkdir %q: %w", name, err)
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
				return fmt.Errorf("mkdir parent for %q: %w", name, err)
			}
			file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(header.Mode)&0o777)
			if err != nil {
				return fmt.Errorf("create %q: %w", name, err)
			}
			if _, err := io.Copy(file, tarReader); err != nil {
				file.Close()
				return fmt.Errorf("write %q: %w", name, err)
			}
			if err := file.Close(); err != nil {
				return fmt.Errorf("close %q: %w", name, err)
			}
		default:
			// Skip special files and links during local inspection extraction.
		}
	}
}

func importStaticSiteFromExtractedUpload(src extractedUploadSource, requestedSourceDir, registryPushBase, imageRepository, imageNameSuffix string) (GitHubImportResult, error) {
	sourceDir, err := detectStaticSiteDir(src.RootDir, requestedSourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	detectedStack := detectPrimaryTechStack(src.RootDir, relativeImportedSourceDir(src.RootDir, sourceDir))

	imageRef := defaultUploadedImageRef(registryPushBase, imageRepository, src.DefaultAppName, src.ArchiveSHA256, imageNameSuffix)
	if err := buildAndPushStaticSiteImage(sourceDir, imageRef); err != nil {
		return GitHubImportResult{}, err
	}

	return GitHubImportResult{
		CommitSHA:      src.ArchiveSHA256,
		SourceDir:      relativeImportedSourceDir(src.RootDir, sourceDir),
		BuildStrategy:  model.AppBuildStrategyStaticSite,
		ImageRef:       imageRef,
		DefaultAppName: src.DefaultAppName,
		DetectedPort:   80,
		DetectedStack:  detectedStack,
	}, nil
}

func importDockerfileFromExtractedUpload(ctx context.Context, src extractedUploadSource, archiveDownloadURL, dockerfilePath, buildContextDir, registryPushBase, imageRepository, imageNameSuffix string, jobLabels, placementNodeSelector map[string]string, builderPolicy BuilderPodPolicy, stateful bool) (GitHubImportResult, error) {
	if strings.TrimSpace(archiveDownloadURL) == "" {
		return GitHubImportResult{}, fmt.Errorf("archive download url is required for dockerfile builds")
	}
	dockerfilePath, buildContextDir, err := detectDockerBuildInputs(src.RootDir, dockerfilePath, buildContextDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	detectedPort, err := detectDockerfilePort(src.RootDir, dockerfilePath)
	if err != nil {
		return GitHubImportResult{}, err
	}
	detectedStack := detectPrimaryTechStack(src.RootDir, buildContextDir)
	imageRef := defaultUploadedImageRef(registryPushBase, imageRepository, src.DefaultAppName, src.ArchiveSHA256, imageNameSuffix)
	if err := buildAndPushDockerfileImage(ctx, dockerfileBuildRequest{
		CommitSHA:             src.ArchiveSHA256,
		SourceLabel:           src.DefaultAppName,
		ArchiveDownloadURL:    strings.TrimSpace(archiveDownloadURL),
		DockerfilePath:        dockerfilePath,
		BuildContextDir:       buildContextDir,
		ImageRef:              imageRef,
		JobLabels:             jobLabels,
		PlacementNodeSelector: placementNodeSelector,
		PodPolicy:             builderPolicy,
		WorkloadProfile:       builderWorkloadProfileFor(model.AppBuildStrategyDockerfile, stateful),
	}); err != nil {
		return GitHubImportResult{}, err
	}
	return GitHubImportResult{
		CommitSHA:        src.ArchiveSHA256,
		BuildStrategy:    model.AppBuildStrategyDockerfile,
		DockerfilePath:   dockerfilePath,
		BuildContextDir:  buildContextDir,
		ImageRef:         imageRef,
		DefaultAppName:   src.DefaultAppName,
		DetectedPort:     detectedPort,
		DetectedProvider: model.AppBuildStrategyDockerfile,
		DetectedStack:    detectedStack,
	}, nil
}

func importBuildpacksFromExtractedUpload(ctx context.Context, src extractedUploadSource, archiveDownloadURL, sourceDir, registryPushBase, imageRepository, imageNameSuffix string, jobLabels, placementNodeSelector map[string]string, builderPolicy BuilderPodPolicy, stateful bool) (GitHubImportResult, error) {
	if strings.TrimSpace(archiveDownloadURL) == "" {
		return GitHubImportResult{}, fmt.Errorf("archive download url is required for buildpacks builds")
	}
	normalizedSourceDir, err := normalizeRepoSourceDir(src.RootDir, sourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	provider, port := detectBuildpacksProviderAndPort(src.RootDir, normalizedSourceDir)
	detectedStack := detectPrimaryTechStack(src.RootDir, normalizedSourceDir)
	imageRef := defaultUploadedImageRef(registryPushBase, imageRepository, src.DefaultAppName, src.ArchiveSHA256, imageNameSuffix)
	if err := buildAndPushBuildpacksImage(ctx, buildpacksBuildRequest{
		CommitSHA:             src.ArchiveSHA256,
		SourceLabel:           src.DefaultAppName,
		ArchiveDownloadURL:    strings.TrimSpace(archiveDownloadURL),
		SourceDir:             normalizedSourceDir,
		ImageRef:              imageRef,
		JobLabels:             jobLabels,
		PlacementNodeSelector: placementNodeSelector,
		PodPolicy:             builderPolicy,
		WorkloadProfile:       builderWorkloadProfileFor(model.AppBuildStrategyBuildpacks, stateful),
	}); err != nil {
		return GitHubImportResult{}, err
	}
	return GitHubImportResult{
		CommitSHA:        src.ArchiveSHA256,
		SourceDir:        normalizeImportedSourceDirValue(normalizedSourceDir),
		BuildStrategy:    model.AppBuildStrategyBuildpacks,
		ImageRef:         imageRef,
		DefaultAppName:   src.DefaultAppName,
		DetectedPort:     port,
		DetectedProvider: provider,
		DetectedStack:    detectedStack,
		SuggestedEnv:     suggestedBuildpacksEnv(port),
	}, nil
}

func importNixpacksFromExtractedUpload(ctx context.Context, src extractedUploadSource, archiveDownloadURL, sourceDir, registryPushBase, imageRepository, imageNameSuffix string, jobLabels, placementNodeSelector map[string]string, builderPolicy BuilderPodPolicy, stateful bool) (GitHubImportResult, error) {
	if strings.TrimSpace(archiveDownloadURL) == "" {
		return GitHubImportResult{}, fmt.Errorf("archive download url is required for nixpacks builds")
	}
	normalizedSourceDir, err := normalizeRepoSourceDir(src.RootDir, sourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	provider, port := detectNixpacksProviderAndPort(src.RootDir, normalizedSourceDir)
	detectedStack := detectPrimaryTechStack(src.RootDir, normalizedSourceDir)
	imageRef := defaultUploadedImageRef(registryPushBase, imageRepository, src.DefaultAppName, src.ArchiveSHA256, imageNameSuffix)
	if err := buildAndPushNixpacksImage(ctx, nixpacksBuildRequest{
		CommitSHA:             src.ArchiveSHA256,
		SourceLabel:           src.DefaultAppName,
		ArchiveDownloadURL:    strings.TrimSpace(archiveDownloadURL),
		SourceDir:             normalizedSourceDir,
		ImageRef:              imageRef,
		JobLabels:             jobLabels,
		PlacementNodeSelector: placementNodeSelector,
		PodPolicy:             builderPolicy,
		WorkloadProfile:       builderWorkloadProfileFor(model.AppBuildStrategyNixpacks, stateful),
	}); err != nil {
		return GitHubImportResult{}, err
	}
	return GitHubImportResult{
		CommitSHA:        src.ArchiveSHA256,
		SourceDir:        normalizeImportedSourceDirValue(normalizedSourceDir),
		BuildStrategy:    model.AppBuildStrategyNixpacks,
		ImageRef:         imageRef,
		DefaultAppName:   src.DefaultAppName,
		DetectedPort:     port,
		DetectedProvider: provider,
		DetectedStack:    detectedStack,
		SuggestedEnv:     suggestedNixpacksEnv(port),
	}, nil
}

func defaultUploadedImageRef(registryPushBase, imageRepository, appName, archiveSHA256, imageNameSuffix string) string {
	imageRepository = strings.Trim(strings.TrimSpace(imageRepository), "/")
	if imageRepository == "" {
		imageRepository = "fugue-apps"
	}
	repoPath := model.Slugify(appName)
	if repoPath == "" {
		repoPath = "app"
	}
	if suffix := model.Slugify(imageNameSuffix); suffix != "" {
		repoPath += "-" + suffix
	}
	tagSeed := strings.TrimSpace(archiveSHA256)
	if tagSeed == "" {
		tagSeed = repoPath
	}
	return fmt.Sprintf("%s/%s/%s:upload-%s", strings.TrimSpace(registryPushBase), imageRepository, repoPath, shortCommit(tagSeed))
}

func buildArchiveDownloadInitContainers(archiveURL string) []map[string]any {
	script := fmt.Sprintf(`set -euo pipefail
mkdir -p /workspace/repo
wget -O /workspace/source.tgz %q
tar -xzf /workspace/source.tgz -C /workspace/repo
`, strings.TrimSpace(archiveURL))
	return []map[string]any{
		{
			"name":         "source-download",
			"image":        defaultGitCloneImage,
			"command":      []string{"sh", "-lc", script},
			"volumeMounts": []map[string]any{{"name": "workspace", "mountPath": "/workspace"}},
		},
	}
}
