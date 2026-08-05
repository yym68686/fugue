package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"fugue/internal/declarativerelease"
)

var registryVerifierPath = "scripts/verify_registry_image.py"

func runBuild(args []string, output io.Writer) error {
	if len(args) != 4 {
		return errors.New("usage: fugue-declarative-release build PLAN COMPONENT RECEIPT_FILE")
	}
	plan, err := readPlan(args[1])
	if err != nil {
		return err
	}
	release, err := selectedRelease(plan, args[2])
	if err != nil {
		return err
	}
	if err := verifyReleaseCheckout(plan.HeadSHA); err != nil {
		return err
	}
	for _, filename := range []string{release.Artifact.Dockerfile, registryVerifierPath} {
		info, err := os.Stat(filename)
		if err != nil || !info.Mode().IsRegular() {
			return fmt.Errorf("required build input %q is unavailable", filename)
		}
	}
	if _, err := boundedExternal(context.Background(), nil, 2<<20, "go", "list", release.Artifact.BuildPackage); err != nil {
		return fmt.Errorf("build package is unavailable: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	preflightRaw, err := boundedExternal(ctx, nil, 1<<20, "python3", registryVerifierPath,
		"--image", release.Artifact.Repository+":"+plan.HeadSHA,
		"--expected-revision", plan.HeadSHA, "--allow-missing-tag",
		"--timeout-seconds", "18", "--request-timeout-seconds", "5",
		"--max-attempts", "2", "--retry-delay-seconds", "0.1")
	if err != nil {
		return fmt.Errorf("immutable tag preflight failed: %w", err)
	}
	existing, err := decodeTagPreflight(preflightRaw, release.Artifact.Repository+":"+plan.HeadSHA)
	if err != nil {
		return err
	}
	if existing != nil {
		return emitArtifactReceipt(output, args[3], plan, release.ComponentID, *existing)
	}
	metadataFile, err := os.CreateTemp(os.Getenv("RUNNER_TEMP"), "fugue-declarative-build-*.json")
	if err != nil {
		return err
	}
	metadataPath := metadataFile.Name()
	if err := metadataFile.Chmod(0o600); err != nil {
		_ = metadataFile.Close()
		return err
	}
	if err := metadataFile.Close(); err != nil {
		return err
	}
	defer os.Remove(metadataPath)
	if _, err := boundedExternal(ctx, nil, 4<<20, "docker", buildArguments(release, plan.HeadSHA, metadataPath)...); err != nil {
		return fmt.Errorf("component image build failed: %w", err)
	}
	metadataRaw, err := os.ReadFile(metadataPath)
	if err != nil || len(metadataRaw) == 0 || len(metadataRaw) > 128<<10 {
		return errors.New("build metadata is unavailable or oversized")
	}
	topDigest, err := topDigestFromBuildMetadata(metadataRaw)
	if err != nil {
		return err
	}
	verificationRaw, err := boundedExternal(ctx, nil, 1<<20, "python3", registryVerifierPath,
		"--image", release.Artifact.Repository+"@"+topDigest,
		"--platform", "linux/amd64", "--expected-revision", plan.HeadSHA,
		"--timeout-seconds", "45", "--request-timeout-seconds", "8",
		"--max-attempts", "3", "--retry-delay-seconds", "0.2")
	if err != nil {
		return fmt.Errorf("immutable artifact verification failed; artifact must be quarantined: %w", err)
	}
	verification, err := declarativerelease.DecodeRegistryVerification(bytes.NewReader(verificationRaw))
	if err != nil {
		return err
	}
	return emitArtifactReceipt(output, args[3], plan, release.ComponentID, verification)
}

func verifyReleaseCheckout(expectedSHA string) error {
	headRaw, err := boundedExternal(context.Background(), nil, 32<<10, "git", "rev-parse", "HEAD")
	if err != nil || strings.TrimSpace(string(headRaw)) != expectedSHA {
		return errors.New("release checkout does not match plan head")
	}
	statusRaw, err := boundedExternal(context.Background(), nil, 128<<10, "git", "status", "--porcelain", "--untracked-files=no")
	if err != nil || len(bytes.TrimSpace(statusRaw)) != 0 {
		return errors.New("release checkout tracked files are not clean")
	}
	return nil
}

func emitArtifactReceipt(output io.Writer, filename string, plan declarativerelease.Plan, componentID string, verification declarativerelease.RegistryVerification) error {
	receipt, err := declarativerelease.MaterializeArtifactReceipt(plan, componentID, verification)
	if err != nil {
		return err
	}
	encoded, err := declarativerelease.CanonicalJSON(receipt)
	if err != nil {
		return err
	}
	if err := writeExclusive(filename, append(encoded, '\n')); err != nil {
		return err
	}
	_, err = output.Write(append(encoded, '\n'))
	return err
}

func selectedRelease(plan declarativerelease.Plan, componentID string) (declarativerelease.PlanRelease, error) {
	for _, release := range plan.Releases {
		if release.ComponentID == componentID {
			return release, nil
		}
	}
	return declarativerelease.PlanRelease{}, fmt.Errorf("component %q is not in release plan", componentID)
}

func buildArguments(release declarativerelease.PlanRelease, headSHA, metadataFile string) []string {
	return []string{
		"buildx", "build", "--platform", "linux/amd64",
		"--file", release.Artifact.Dockerfile,
		"--label", "org.opencontainers.image.revision=" + headSHA,
		"--tag", release.Artifact.Repository + ":" + headSHA,
		"--metadata-file", metadataFile,
		"--push", release.Artifact.Context,
	}
}

func decodeTagPreflight(raw []byte, image string) (*declarativerelease.RegistryVerification, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, errors.New("immutable source tag preflight receipt is invalid")
	}
	if _, exists := fields["exists"]; exists {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		var value struct {
			Exists bool   `json:"exists"`
			Image  string `json:"image"`
		}
		if err := decoder.Decode(&value); err != nil || value.Exists || value.Image != image {
			return nil, errors.New("immutable source tag preflight receipt is invalid")
		}
		return nil, nil
	}
	verification, err := declarativerelease.DecodeRegistryVerification(bytes.NewReader(raw))
	if err != nil || verification.Image == "" || verification.Verification != "registry_manifest_config_and_layer_get" ||
		verification.LayerProbeCount < 1 || verification.TotalLayerBytes < 1 {
		return nil, errors.New("existing immutable source tag lacks complete registry verification")
	}
	return &verification, nil
}

func topDigestFromBuildMetadata(raw []byte) (string, error) {
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	digest, _ := value["containerimage.digest"].(string)
	if !strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+64 {
		return "", errors.New("build metadata top digest is invalid")
	}
	for _, character := range digest[len("sha256:"):] {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return "", errors.New("build metadata top digest is invalid")
		}
	}
	return digest, nil
}

func boundedExternal(ctx context.Context, input []byte, limit int, binary string, arguments ...string) ([]byte, error) {
	return boundedExternalInDirectory(ctx, "", input, limit, binary, arguments...)
}

func boundedExternalInDirectory(ctx context.Context, directory string, input []byte, limit int, binary string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, binary, arguments...)
	command.Dir = directory
	command.Stdin = bytes.NewReader(input)
	var stdout, stderr limitedBuffer
	stdout.limit = limit
	stderr.limit = 128 << 10
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("%s failed: %w: %s", filepath.Base(binary), err, strings.TrimSpace(stderr.String()))
	}
	if stdout.exceeded || stderr.exceeded {
		return nil, errors.New("external command output exceeded limit")
	}
	return stdout.Bytes(), nil
}
