package sourceimport

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"fugue/internal/model"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
)

type DockerImageSourceImportRequest struct {
	AppName          string
	ImageNameSuffix  string
	ImageRef         string
	ImageRepository  string
	RegistryPushBase string
}

type remoteImageDigestFunc func(string, ...crane.Option) (string, error)
type remoteImageManifestFunc func(string, ...crane.Option) ([]byte, error)

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

	destOptions := craneOptionsForImageRef(ctx, destImageRef)
	if err := validateMirroredImageReference(destImageRef, digest, destOptions...); err != nil {
		return GitHubSourceImportOutput{}, fmt.Errorf("validate mirrored image in internal registry: %w", err)
	}
	detectedPort := 80
	exposesPublicService := false
	detectedStack := ""
	configFile, configErr := readRemoteImageConfig(destImageRef, destOptions...)
	if configErr == nil {
		if port := detectExposedPortFromImageConfig(configFile); port > 0 {
			detectedPort = port
			exposesPublicService = true
		}
		stack, suppressPublicService, suppressErr := detectImageBackgroundOverride(destImageRef, configFile, destOptions...)
		if suppressErr == nil {
			detectedStack = stack
			if suppressPublicService {
				exposesPublicService = false
			}
		} else if i != nil && i.Logger != nil {
			i.Logger.Printf("skip image background inspection for %s: %v", destImageRef, suppressErr)
		}
	} else if i != nil && i.Logger != nil {
		i.Logger.Printf("skip image config inspection for %s: %v", destImageRef, configErr)
	}

	return GitHubSourceImportOutput{
		ImportResult: GitHubImportResult{
			DefaultAppName:       defaultImportedImageAppName(req.AppName, sourceImageRef),
			DetectedPort:         detectedPort,
			ExposesPublicService: exposesPublicService,
			DetectedProvider:     model.AppSourceTypeDockerImage,
			DetectedStack:        detectedStack,
			ImageRef:             destImageRef,
		},
		Source: model.AppSource{
			Type:             model.AppSourceTypeDockerImage,
			ImageRef:         strings.TrimSpace(req.ImageRef),
			ResolvedImageRef: destImageRef,
			ImageNameSuffix:  strings.TrimSpace(req.ImageNameSuffix),
			DetectedProvider: model.AppSourceTypeDockerImage,
			DetectedStack:    detectedStack,
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

func validateMirroredImageReference(imageRef, expectedDigest string, options ...crane.Option) error {
	return validateMirroredImageReferenceWithClients(imageRef, expectedDigest, crane.Digest, crane.Manifest, options...)
}

func validateMirroredImageReferenceWithClients(imageRef, expectedDigest string, digestFn remoteImageDigestFunc, manifestFn remoteImageManifestFunc, options ...crane.Option) error {
	imageRef = strings.TrimSpace(imageRef)
	expectedDigest = strings.TrimSpace(expectedDigest)
	if imageRef == "" {
		return fmt.Errorf("destination image ref is empty")
	}
	if expectedDigest == "" {
		return fmt.Errorf("expected digest is empty")
	}

	if _, err := manifestFn(imageRef, options...); err != nil {
		return fmt.Errorf("fetch manifest by tag: %w", err)
	}
	actualDigest, err := digestFn(imageRef, options...)
	if err != nil {
		return fmt.Errorf("resolve mirrored image digest: %w", err)
	}
	if actualDigest != expectedDigest {
		return fmt.Errorf("digest mismatch: expected %s, got %s", expectedDigest, actualDigest)
	}
	digestRef, err := digestReferenceFromImageRef(imageRef, actualDigest)
	if err != nil {
		return err
	}
	if _, err := manifestFn(digestRef, options...); err != nil {
		return fmt.Errorf("fetch manifest by digest: %w", err)
	}
	return nil
}

func digestReferenceFromImageRef(imageRef, digest string) (string, error) {
	imageRef = strings.TrimSpace(imageRef)
	digest = strings.TrimSpace(digest)
	if imageRef == "" {
		return "", fmt.Errorf("image ref is empty")
	}
	if digest == "" {
		return "", fmt.Errorf("digest is empty")
	}
	ref, err := name.ParseReference(imageRef, imageReferenceNameOptions(imageRef)...)
	if err != nil {
		return "", fmt.Errorf("parse image ref for digest lookup: %w", err)
	}
	digestRef, err := name.NewDigest(ref.Context().Name()+"@"+digest, imageReferenceNameOptions(imageRef)...)
	if err != nil {
		return "", fmt.Errorf("parse digest reference: %w", err)
	}
	return digestRef.Name(), nil
}

func detectImageBackgroundOverride(imageRef string, configFile *v1.ConfigFile, options ...crane.Option) (string, bool, error) {
	img, err := crane.Pull(imageRef, options...)
	if err != nil {
		return "", false, fmt.Errorf("pull image for background inspection: %w", err)
	}
	return detectImageBackgroundOverrideFromImage(img, configFile)
}

func detectImageBackgroundOverrideFromImage(img v1.Image, configFile *v1.ConfigFile) (string, bool, error) {
	analysis, ok, err := analyzePythonProjectInImage(img, configFile)
	if err != nil {
		return "", false, err
	}
	if !ok || !analysis.IsPythonProject {
		return "", false, nil
	}
	return "python", pythonProjectPrefersBackgroundNetwork(analysis), nil
}

func analyzePythonProjectInRemoteImage(imageRef string, configFile *v1.ConfigFile, options ...crane.Option) (pythonProjectAnalysis, bool, error) {
	img, err := crane.Pull(imageRef, options...)
	if err != nil {
		return pythonProjectAnalysis{}, false, fmt.Errorf("pull image for python inspection: %w", err)
	}
	return analyzePythonProjectInImage(img, configFile)
}

func analyzePythonProjectInImage(img v1.Image, configFile *v1.ConfigFile) (pythonProjectAnalysis, bool, error) {
	appDir, ok := pythonImageAnalysisDir(configFile)
	if !ok {
		return pythonProjectAnalysis{}, false, nil
	}

	extractRoot, err := os.MkdirTemp("", "fugue-image-python-*")
	if err != nil {
		return pythonProjectAnalysis{}, false, fmt.Errorf("create image inspection dir: %w", err)
	}
	defer os.RemoveAll(extractRoot)

	extractedDir, ok, err := extractImageSubtreeToDir(img, extractRoot, appDir)
	if err != nil {
		return pythonProjectAnalysis{}, false, err
	}
	if !ok {
		return pythonProjectAnalysis{}, false, nil
	}

	analysis, err := analyzePythonProjectInDir(extractedDir)
	if err != nil {
		return pythonProjectAnalysis{}, false, err
	}
	return analysis, true, nil
}

func pythonImageAnalysisDir(configFile *v1.ConfigFile) (string, bool) {
	args, ok := pythonInvocationArgs(imageConfigCommandTokens(configFile))
	if !ok {
		return "", false
	}

	workingDir := normalizeImageConfigPath(configFile.Config.WorkingDir)
	for index := 0; index < len(args); index++ {
		token := strings.TrimSpace(args[index])
		if token == "" {
			continue
		}
		switch token {
		case "-m":
			if workingDir == "" || workingDir == "/" {
				return "", false
			}
			return workingDir, true
		case "-c":
			return "", false
		}
		if strings.HasPrefix(token, "-") {
			continue
		}
		if !strings.HasSuffix(strings.ToLower(token), ".py") {
			return "", false
		}

		scriptPath := resolveImageConfigPath(token, workingDir)
		if scriptPath == "" {
			return "", false
		}
		scriptDir := path.Dir(scriptPath)
		if scriptDir == "." || scriptDir == "/" {
			return "", false
		}
		return scriptDir, true
	}

	return "", false
}

func imageConfigCommandTokens(configFile *v1.ConfigFile) []string {
	if configFile == nil {
		return nil
	}
	tokens := make([]string, 0, len(configFile.Config.Entrypoint)+len(configFile.Config.Cmd))
	for _, token := range configFile.Config.Entrypoint {
		if trimmed := strings.TrimSpace(token); trimmed != "" {
			tokens = append(tokens, trimmed)
		}
	}
	for _, token := range configFile.Config.Cmd {
		if trimmed := strings.TrimSpace(token); trimmed != "" {
			tokens = append(tokens, trimmed)
		}
	}
	if len(tokens) >= 3 && isPOSIXShellExecutable(tokens[0]) && strings.TrimSpace(tokens[1]) == "-c" {
		return strings.Fields(strings.TrimSpace(tokens[2]))
	}
	return tokens
}

func pythonInvocationArgs(tokens []string) ([]string, bool) {
	if len(tokens) == 0 {
		return nil, false
	}
	for len(tokens) > 0 && strings.TrimSpace(tokens[0]) == "exec" {
		tokens = tokens[1:]
	}
	if len(tokens) == 0 {
		return nil, false
	}

	if looksLikePythonExecutable(tokens[0]) {
		return tokens[1:], true
	}
	if path.Base(strings.TrimSpace(tokens[0])) == "env" && len(tokens) > 1 && looksLikePythonExecutable(tokens[1]) {
		return tokens[2:], true
	}
	return nil, false
}

func looksLikePythonExecutable(token string) bool {
	base := strings.ToLower(path.Base(strings.TrimSpace(token)))
	switch {
	case base == "python":
		return true
	case strings.HasPrefix(base, "python2"):
		return true
	case strings.HasPrefix(base, "python3"):
		return true
	case strings.HasPrefix(base, "pypy"):
		return true
	default:
		return false
	}
}

func isPOSIXShellExecutable(token string) bool {
	switch strings.ToLower(path.Base(strings.TrimSpace(token))) {
	case "sh", "ash", "bash", "dash":
		return true
	default:
		return false
	}
}

func normalizeImageConfigPath(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	clean := path.Clean(raw)
	if clean == "." {
		return ""
	}
	if !strings.HasPrefix(clean, "/") {
		clean = "/" + clean
	}
	return path.Clean(clean)
}

func resolveImageConfigPath(rawPath, workingDir string) string {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return ""
	}
	if strings.HasPrefix(rawPath, "/") {
		return normalizeImageConfigPath(rawPath)
	}
	workingDir = normalizeImageConfigPath(workingDir)
	if workingDir == "" {
		return ""
	}
	return path.Clean(path.Join(workingDir, rawPath))
}

func extractImageSubtreeToDir(img v1.Image, rootDir, imageDir string) (string, bool, error) {
	imageDir = strings.TrimPrefix(normalizeImageConfigPath(imageDir), "/")
	if imageDir == "" {
		return "", false, nil
	}

	reader := mutate.Extract(img)
	defer reader.Close()

	tarReader := tar.NewReader(reader)
	wrote := false
	for {
		header, err := tarReader.Next()
		switch {
		case err == io.EOF:
			if !wrote {
				return "", false, nil
			}
			return filepath.Join(rootDir, filepath.FromSlash(imageDir)), true, nil
		case err != nil:
			return "", false, fmt.Errorf("extract image filesystem: %w", err)
		}

		entryName := strings.TrimPrefix(path.Clean(strings.TrimSpace(header.Name)), "/")
		if entryName == "." || entryName == "" {
			continue
		}
		if entryName != imageDir && !strings.HasPrefix(entryName, imageDir+"/") {
			continue
		}

		targetPath := filepath.Join(rootDir, filepath.FromSlash(entryName))
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, 0o755); err != nil {
				return "", false, fmt.Errorf("create extracted dir %s: %w", targetPath, err)
			}
			wrote = true
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
				return "", false, fmt.Errorf("create extracted file parent %s: %w", targetPath, err)
			}
			mode := os.FileMode(header.Mode).Perm()
			if mode == 0 {
				mode = 0o644
			}
			file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
			if err != nil {
				return "", false, fmt.Errorf("create extracted file %s: %w", targetPath, err)
			}
			if _, err := io.Copy(file, tarReader); err != nil {
				file.Close()
				return "", false, fmt.Errorf("write extracted file %s: %w", targetPath, err)
			}
			if err := file.Close(); err != nil {
				return "", false, fmt.Errorf("close extracted file %s: %w", targetPath, err)
			}
			wrote = true
		}
	}
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
