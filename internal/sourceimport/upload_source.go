package sourceimport

import (
	"context"
	"fmt"
	"log"
	"os"
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
	CleanupDir     string
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
		result, err := importStaticSiteFromExtractedUpload(ctx, src, req.ArchiveDownloadURL, req.SourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful, i.Logger)
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
		result, err := importDockerfileFromExtractedUpload(ctx, src, req.ArchiveDownloadURL, req.DockerfilePath, req.BuildContextDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful, i.Logger)
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
		result, err := importBuildpacksFromExtractedUpload(ctx, src, req.ArchiveDownloadURL, req.SourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful, i.Logger)
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
		result, err := importNixpacksFromExtractedUpload(ctx, src, req.ArchiveDownloadURL, req.SourceDir, req.RegistryPushBase, req.ImageRepository, req.ImageNameSuffix, req.JobLabels, req.PlacementNodeSelector, i.BuilderPolicy, req.Stateful, i.Logger)
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
	effectiveRootDir, err := extractUploadArchive(rootDir, req.ArchiveFilename, req.ArchiveData)
	if err != nil {
		_ = os.RemoveAll(rootDir)
		return extractedUploadSource{}, err
	}

	defaultAppName := resolveUploadedArchiveDefaultAppName(req.AppName, req.ArchiveFilename)

	return extractedUploadSource{
		UploadID:       strings.TrimSpace(req.UploadID),
		ArchiveName:    strings.TrimSpace(req.ArchiveFilename),
		ArchiveSHA256:  strings.TrimSpace(req.ArchiveSHA256),
		ArchiveSize:    req.ArchiveSizeBytes,
		CleanupDir:     rootDir,
		RootDir:        effectiveRootDir,
		DefaultAppName: defaultAppName,
	}, nil
}

func releaseExtractedUploadSource(src extractedUploadSource) {
	targetDir := strings.TrimSpace(src.CleanupDir)
	if targetDir == "" {
		targetDir = strings.TrimSpace(src.RootDir)
	}
	if targetDir == "" {
		return
	}
	_ = os.RemoveAll(targetDir)
}

func resolveUploadedArchiveDefaultAppName(requestedName, archiveFilename string) string {
	if slug := model.SlugifyOptional(strings.TrimSpace(requestedName)); slug != "" {
		return slug
	}
	if slug := model.SlugifyOptional(uploadArchiveBaseName(archiveFilename)); slug != "" {
		return slug
	}
	return "app"
}

func importStaticSiteFromExtractedUpload(ctx context.Context, src extractedUploadSource, archiveDownloadURL, requestedSourceDir, registryPushBase, imageRepository, imageNameSuffix string, jobLabels, placementNodeSelector map[string]string, builderPolicy BuilderPodPolicy, stateful bool, logger *log.Logger) (GitHubImportResult, error) {
	sourceDir, err := detectStaticSiteDir(src.RootDir, requestedSourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	plan, err := planStaticSiteImport(src.RootDir, relativeImportedSourceDir(src.RootDir, sourceDir))
	if err != nil {
		return GitHubImportResult{}, err
	}

	imageRef := defaultUploadedImageRef(registryPushBase, imageRepository, src.DefaultAppName, src.ArchiveSHA256, imageNameSuffix)
	buildJobNameValue := ""
	if len(plan.SourceOverlay) > 0 {
		if strings.TrimSpace(archiveDownloadURL) == "" {
			return GitHubImportResult{}, fmt.Errorf("archive download url is required for static-site source builds")
		}
		buildReq := dockerfileBuildRequest{
			CommitSHA:             src.ArchiveSHA256,
			SourceLabel:           src.DefaultAppName,
			ArchiveDownloadURL:    strings.TrimSpace(archiveDownloadURL),
			DockerfilePath:        plan.DockerfilePath,
			BuildContextDir:       plan.BuildContextDir,
			ImageRef:              imageRef,
			SourceOverlayFiles:    plan.SourceOverlay,
			JobLabels:             jobLabels,
			PlacementNodeSelector: placementNodeSelector,
			PodPolicy:             builderPolicy,
			WorkloadProfile:       builderWorkloadProfileFor(model.AppBuildStrategyDockerfile, stateful),
			Logger:                logger,
		}
		buildJobNameValue = buildJobName(buildReq)
		if err := buildAndPushDockerfileImage(ctx, buildReq); err != nil {
			return GitHubImportResult{}, err
		}
	} else {
		if err := buildAndPushStaticSiteImage(sourceDir, imageRef); err != nil {
			return GitHubImportResult{}, err
		}
	}

	return GitHubImportResult{
		CommitSHA:            src.ArchiveSHA256,
		SourceDir:            plan.SourceDir,
		BuildStrategy:        model.AppBuildStrategyStaticSite,
		ImageRef:             imageRef,
		BuildJobName:         buildJobNameValue,
		DefaultAppName:       src.DefaultAppName,
		DetectedPort:         80,
		ExposesPublicService: true,
		DetectedProvider:     plan.DetectedProvider,
		DetectedStack:        plan.DetectedStack,
	}, nil
}

func importDockerfileFromExtractedUpload(ctx context.Context, src extractedUploadSource, archiveDownloadURL, dockerfilePath, buildContextDir, registryPushBase, imageRepository, imageNameSuffix string, jobLabels, placementNodeSelector map[string]string, builderPolicy BuilderPodPolicy, stateful bool, logger *log.Logger) (GitHubImportResult, error) {
	if strings.TrimSpace(archiveDownloadURL) == "" {
		return GitHubImportResult{}, fmt.Errorf("archive download url is required for dockerfile builds")
	}
	dockerfilePath, buildContextDir, err := detectDockerBuildInputs(src.RootDir, dockerfilePath, buildContextDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	detectedPort, exposesPublicService, err := detectDockerfilePortSignal(src.RootDir, dockerfilePath)
	if err != nil {
		return GitHubImportResult{}, err
	}
	detectedStack := detectPrimaryTechStack(src.RootDir, buildContextDir)
	if exposesPublicService && shouldSuppressDetectedPublicServiceForProject(src.RootDir, buildContextDir, detectedStack) {
		exposesPublicService = false
	}
	imageRef := defaultUploadedImageRef(registryPushBase, imageRepository, src.DefaultAppName, src.ArchiveSHA256, imageNameSuffix)
	buildReq := dockerfileBuildRequest{
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
		Logger:                logger,
	}
	buildJobNameValue := buildJobName(buildReq)
	if err := buildAndPushDockerfileImage(ctx, buildReq); err != nil {
		return GitHubImportResult{}, err
	}
	return GitHubImportResult{
		CommitSHA:            src.ArchiveSHA256,
		BuildStrategy:        model.AppBuildStrategyDockerfile,
		DockerfilePath:       dockerfilePath,
		BuildContextDir:      buildContextDir,
		ImageRef:             imageRef,
		BuildJobName:         buildJobNameValue,
		DefaultAppName:       src.DefaultAppName,
		DetectedPort:         detectedPort,
		ExposesPublicService: exposesPublicService,
		DetectedProvider:     model.AppBuildStrategyDockerfile,
		DetectedStack:        detectedStack,
	}, nil
}

func importBuildpacksFromExtractedUpload(ctx context.Context, src extractedUploadSource, archiveDownloadURL, sourceDir, registryPushBase, imageRepository, imageNameSuffix string, jobLabels, placementNodeSelector map[string]string, builderPolicy BuilderPodPolicy, stateful bool, logger *log.Logger) (GitHubImportResult, error) {
	if strings.TrimSpace(archiveDownloadURL) == "" {
		return GitHubImportResult{}, fmt.Errorf("archive download url is required for buildpacks builds")
	}
	normalizedSourceDir, err := normalizeRepoSourceDir(src.RootDir, sourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	provider, port, exposesPublicService := detectZeroConfigProviderAndPortSignal(src.RootDir, normalizedSourceDir)
	detectedStack := detectPrimaryTechStack(src.RootDir, normalizedSourceDir)
	sourceOverlayFiles, pythonAnalysis, err := buildPythonOverlayFiles(src.RootDir, normalizedSourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	systemOverlayFiles, systemPackages, err := buildBuildpacksSystemPackageOverlayFiles(src.RootDir, normalizedSourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	sourceOverlayFiles = append(sourceOverlayFiles, systemOverlayFiles...)
	imageRef := defaultUploadedImageRef(registryPushBase, imageRepository, src.DefaultAppName, src.ArchiveSHA256, imageNameSuffix)
	buildReq := buildpacksBuildRequest{
		CommitSHA:             src.ArchiveSHA256,
		SourceLabel:           src.DefaultAppName,
		ArchiveDownloadURL:    strings.TrimSpace(archiveDownloadURL),
		SourceDir:             normalizedSourceDir,
		ImageRef:              imageRef,
		SourceOverlayFiles:    sourceOverlayFiles,
		JobLabels:             jobLabels,
		PlacementNodeSelector: placementNodeSelector,
		DetectedProvider:      provider,
		IncludeAptBuildpack:   len(systemPackages.Packages) > 0 || systemPackages.HasExplicitBuildpackApt,
		PodPolicy:             builderPolicy,
		WorkloadProfile:       builderWorkloadProfileFor(model.AppBuildStrategyBuildpacks, stateful),
		Logger:                logger,
	}
	buildJobNameValue := buildJobName(dockerfileBuildRequest{
		CommitSHA:          buildReq.CommitSHA,
		SourceLabel:        buildReq.SourceLabel,
		ArchiveDownloadURL: buildReq.ArchiveDownloadURL,
		BuildContextDir:    buildReq.SourceDir,
		ImageRef:           buildReq.ImageRef,
		JobLabels:          buildReq.JobLabels,
	})
	if err := buildAndPushBuildpacksImage(ctx, buildReq); err != nil {
		return GitHubImportResult{}, err
	}
	return GitHubImportResult{
		CommitSHA:               src.ArchiveSHA256,
		SourceDir:               normalizeImportedSourceDirValue(normalizedSourceDir),
		BuildStrategy:           model.AppBuildStrategyBuildpacks,
		ImageRef:                imageRef,
		BuildJobName:            buildJobNameValue,
		DefaultAppName:          src.DefaultAppName,
		DetectedPort:            port,
		ExposesPublicService:    exposesPublicService,
		DetectedProvider:        provider,
		DetectedStack:           detectedStack,
		SuggestedEnv:            suggestedBuildpacksEnv(port),
		SuggestedStartupCommand: pythonAnalysis.SuggestedStartCommand,
	}, nil
}

func importNixpacksFromExtractedUpload(ctx context.Context, src extractedUploadSource, archiveDownloadURL, sourceDir, registryPushBase, imageRepository, imageNameSuffix string, jobLabels, placementNodeSelector map[string]string, builderPolicy BuilderPodPolicy, stateful bool, logger *log.Logger) (GitHubImportResult, error) {
	if strings.TrimSpace(archiveDownloadURL) == "" {
		return GitHubImportResult{}, fmt.Errorf("archive download url is required for nixpacks builds")
	}
	normalizedSourceDir, err := normalizeRepoSourceDir(src.RootDir, sourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	provider, port, exposesPublicService := detectZeroConfigProviderAndPortSignal(src.RootDir, normalizedSourceDir)
	detectedStack := detectPrimaryTechStack(src.RootDir, normalizedSourceDir)
	sourceOverlayFiles, pythonAnalysis, err := buildPythonOverlayFiles(src.RootDir, normalizedSourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	systemPackages, err := analyzeSystemPackages(src.RootDir, normalizedSourceDir)
	if err != nil {
		return GitHubImportResult{}, err
	}
	imageRef := defaultUploadedImageRef(registryPushBase, imageRepository, src.DefaultAppName, src.ArchiveSHA256, imageNameSuffix)
	buildReq := nixpacksBuildRequest{
		CommitSHA:             src.ArchiveSHA256,
		SourceLabel:           src.DefaultAppName,
		ArchiveDownloadURL:    strings.TrimSpace(archiveDownloadURL),
		SourceDir:             normalizedSourceDir,
		ImageRef:              imageRef,
		SourceOverlayFiles:    sourceOverlayFiles,
		SystemPackages:        systemPackages.Packages,
		JobLabels:             jobLabels,
		PlacementNodeSelector: placementNodeSelector,
		PodPolicy:             builderPolicy,
		WorkloadProfile:       builderWorkloadProfileFor(model.AppBuildStrategyNixpacks, stateful),
		Logger:                logger,
	}
	buildJobNameValue := buildJobName(dockerfileBuildRequest{
		CommitSHA:          buildReq.CommitSHA,
		SourceLabel:        buildReq.SourceLabel,
		ArchiveDownloadURL: buildReq.ArchiveDownloadURL,
		BuildContextDir:    buildReq.SourceDir,
		ImageRef:           buildReq.ImageRef,
		JobLabels:          buildReq.JobLabels,
	})
	if err := buildAndPushNixpacksImage(ctx, buildReq); err != nil {
		return GitHubImportResult{}, err
	}
	return GitHubImportResult{
		CommitSHA:               src.ArchiveSHA256,
		SourceDir:               normalizeImportedSourceDirValue(normalizedSourceDir),
		BuildStrategy:           model.AppBuildStrategyNixpacks,
		ImageRef:                imageRef,
		BuildJobName:            buildJobNameValue,
		DefaultAppName:          src.DefaultAppName,
		DetectedPort:            port,
		ExposesPublicService:    exposesPublicService,
		DetectedProvider:        provider,
		DetectedStack:           detectedStack,
		SuggestedEnv:            suggestedNixpacksEnv(port),
		SuggestedStartupCommand: pythonAnalysis.SuggestedStartCommand,
	}, nil
}

// UploadImageRepositoryName returns the managed registry repository suffix for an
// uploaded source build. When the caller already folded the logical service name
// into appName (for example "argus-runtime"), avoid appending the same suffix a
// second time.
func UploadImageRepositoryName(appName, imageNameSuffix string) string {
	repoPath := model.Slugify(appName)
	if repoPath == "" {
		repoPath = "app"
	}
	suffix := model.SlugifyOptional(imageNameSuffix)
	if suffix == "" {
		return repoPath
	}
	if repoPath == suffix || strings.HasSuffix(repoPath, "-"+suffix) {
		return repoPath
	}
	return repoPath + "-" + suffix
}

func defaultUploadedImageRef(registryPushBase, imageRepository, appName, archiveSHA256, imageNameSuffix string) string {
	imageRepository = strings.Trim(strings.TrimSpace(imageRepository), "/")
	if imageRepository == "" {
		imageRepository = "fugue-apps"
	}
	repoPath := UploadImageRepositoryName(appName, imageNameSuffix)
	tagSeed := strings.TrimSpace(archiveSHA256)
	if tagSeed == "" {
		tagSeed = repoPath
	}
	return fmt.Sprintf("%s/%s/%s:upload-%s", strings.TrimSpace(registryPushBase), imageRepository, repoPath, shortCommit(tagSeed))
}

func buildArchiveDownloadInitContainers(archiveURL string) []map[string]any {
	script := fmt.Sprintf(`set -euo pipefail
python3 - <<'PY'
import os
import shutil
import sys
import tarfile
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath

archive_url = %q
workspace = Path("/workspace")
archive_path = workspace / "source-archive"
repo_root = workspace / "repo"

repo_root.mkdir(parents=True, exist_ok=True)

with urllib.request.urlopen(archive_url) as response, archive_path.open("wb") as output:
    shutil.copyfileobj(response, output)

with archive_path.open("rb") as handle:
    magic = handle.read(4)

def should_skip(rel_path: str) -> bool:
    parts = [part for part in PurePosixPath(rel_path).parts if part not in ("", ".")]
    if "__MACOSX" in parts:
        return True
    base = parts[-1] if parts else ""
    return base == ".DS_Store" or base.startswith("._")

def safe_target(rel_path: str) -> Path:
    target = (repo_root / Path(rel_path)).resolve()
    base = repo_root.resolve()
    if target != base and base not in target.parents:
        raise ValueError(f"archive entry {rel_path!r} escapes destination")
    return target

def normalized_parts(raw_name: str):
    normalized_name = raw_name.replace("\\", "/")
    if normalized_name.startswith("./"):
        normalized_name = normalized_name[2:]
    rel = PurePosixPath(normalized_name)
    rel_text = rel.as_posix()
    if rel_text in ("", "."):
        return None
    if rel.is_absolute() or rel.parts and rel.parts[0] == "..":
        raise ValueError(f"archive entry {raw_name!r} escapes destination")
    if should_skip(rel_text):
        return None
    return rel_text

def extract_tar():
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in archive.getmembers():
            rel_text = normalized_parts(member.name)
            if not rel_text:
                continue
            target = safe_target(rel_text)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
            elif member.isreg():
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as source, target.open("wb") as output:
                    shutil.copyfileobj(source, output)
                os.chmod(target, member.mode & 0o777 or 0o644)

def extract_zip():
    with zipfile.ZipFile(archive_path) as archive:
        for member in archive.infolist():
            rel_text = normalized_parts(member.filename)
            if not rel_text:
                continue
            target = safe_target(rel_text)
            is_dir = member.is_dir() if hasattr(member, "is_dir") else member.filename.endswith("/")
            if is_dir:
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member) as source, target.open("wb") as output:
                    shutil.copyfileobj(source, output)
                mode = (member.external_attr >> 16) & 0o777
                os.chmod(target, mode or 0o644)

if magic.startswith(b"\x1f\x8b"):
    extract_tar()
elif magic in (b"PK\x03\x04", b"PK\x05\x06", b"PK\x07\x08"):
    extract_zip()
else:
    raise SystemExit("unsupported source upload archive format")

entries = list(repo_root.iterdir())
if not entries:
    raise SystemExit("uploaded archive does not contain any files")
if len(entries) == 1 and entries[0].is_dir():
    lifted_root = workspace / "repo-lifted"
    if lifted_root.exists():
        shutil.rmtree(lifted_root)
    entries[0].replace(lifted_root)
    repo_root.rmdir()
    lifted_root.replace(repo_root)
PY
`, strings.TrimSpace(archiveURL))
	return []map[string]any{
		{
			"name":         "source-download",
			"image":        "python:3.12-alpine",
			"command":      []string{"sh", "-lc", script},
			"volumeMounts": []map[string]any{{"name": "workspace", "mountPath": "/workspace"}},
		},
	}
}
