package sourceimport

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/crane"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
)

func TestNormalizeContainerImageRefAppliesDefaultRegistryAndTag(t *testing.T) {
	t.Parallel()

	got, err := normalizeContainerImageRef("nginx")
	if err != nil {
		t.Fatalf("normalize image ref: %v", err)
	}
	if got != "index.docker.io/library/nginx:latest" {
		t.Fatalf("unexpected normalized image ref: %q", got)
	}
}

func TestDetectExposedPortFromImageConfigPrefersSmallestNumericPort(t *testing.T) {
	t.Parallel()

	got := detectExposedPortFromImageConfig(&v1.ConfigFile{
		Config: v1.Config{
			ExposedPorts: map[string]struct{}{
				"3000/tcp": {},
				"8080/tcp": {},
				"80/tcp":   {},
			},
		},
	})
	if got != 80 {
		t.Fatalf("expected smallest exposed port 80, got %d", got)
	}
}

func TestDefaultMirroredImageRefUsesDigestTagAndSluggedRepoName(t *testing.T) {
	t.Parallel()

	got := defaultMirroredImageRef(
		"registry.internal.example",
		"fugue-apps",
		"",
		"ghcr.io/example/demo:1.2.3",
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"",
	)
	if !strings.HasPrefix(got, "registry.internal.example/fugue-apps/ghcr-io-example-demo:image-0123456789ab") {
		t.Fatalf("unexpected mirrored image ref: %q", got)
	}
}

func TestDefaultMirroredImageRefIgnoresAppNameForSameDockerSource(t *testing.T) {
	t.Parallel()

	left := defaultMirroredImageRef(
		"registry.internal.example",
		"fugue-apps",
		"agent-session-a",
		"ghcr.io/example/runtime:latest",
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"",
	)
	right := defaultMirroredImageRef(
		"registry.internal.example",
		"fugue-apps",
		"agent-session-b",
		"ghcr.io/example/runtime:latest",
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"",
	)
	if left != right {
		t.Fatalf("expected mirrored ref to stay stable across app names, got %q vs %q", left, right)
	}
}

func TestValidateMirroredImageReferenceWithClients(t *testing.T) {
	t.Parallel()

	imageRef := "registry.internal.example/fugue-apps/demo:image-0123456789ab"
	expectedDigest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	wantDigestRef := "registry.internal.example/fugue-apps/demo@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	manifestRefs := make([]string, 0, 2)

	err := validateMirroredImageReferenceWithClients(
		imageRef,
		expectedDigest,
		func(ref string, _ ...crane.Option) (string, error) {
			if ref != imageRef {
				t.Fatalf("unexpected digest lookup ref %q", ref)
			}
			return expectedDigest, nil
		},
		func(ref string, _ ...crane.Option) ([]byte, error) {
			manifestRefs = append(manifestRefs, ref)
			return []byte("{}"), nil
		},
	)
	if err != nil {
		t.Fatalf("validate mirrored image ref: %v", err)
	}
	if len(manifestRefs) != 2 || manifestRefs[0] != imageRef || manifestRefs[1] != wantDigestRef {
		t.Fatalf("unexpected manifest lookup refs: %v", manifestRefs)
	}
}

func TestValidateMirroredImageReferenceWithClientsDetectsDigestMismatch(t *testing.T) {
	t.Parallel()

	imageRef := "registry.internal.example/fugue-apps/demo:image-0123456789ab"
	expectedDigest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	err := validateMirroredImageReferenceWithClients(
		imageRef,
		expectedDigest,
		func(string, ...crane.Option) (string, error) {
			return "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil
		},
		func(string, ...crane.Option) ([]byte, error) {
			return []byte("{}"), nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("expected digest mismatch error, got %v", err)
	}
}

func TestValidateMirroredImageReferenceWithClientsDetectsMissingDigestManifest(t *testing.T) {
	t.Parallel()

	imageRef := "registry.internal.example/fugue-apps/demo:image-0123456789ab"
	expectedDigest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	err := validateMirroredImageReferenceWithClients(
		imageRef,
		expectedDigest,
		func(string, ...crane.Option) (string, error) {
			return expectedDigest, nil
		},
		func(ref string, _ ...crane.Option) ([]byte, error) {
			if strings.Contains(ref, "@sha256:") {
				return nil, errors.New("MANIFEST_UNKNOWN")
			}
			return []byte("{}"), nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "fetch manifest by digest") {
		t.Fatalf("expected digest manifest error, got %v", err)
	}
}

func TestMirroredImageReferenceMatchesDigestWithClients(t *testing.T) {
	t.Parallel()

	imageRef := "registry.internal.example/fugue-apps/demo:image-0123456789ab"
	expectedDigest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	matches, err := mirroredImageReferenceMatchesDigestWithClients(
		imageRef,
		expectedDigest,
		func(string, ...crane.Option) (string, error) {
			return expectedDigest, nil
		},
		func(string, ...crane.Option) ([]byte, error) {
			return []byte("{}"), nil
		},
	)
	if err != nil {
		t.Fatalf("match mirrored image ref: %v", err)
	}
	if !matches {
		t.Fatal("expected mirrored image ref to match digest")
	}
}

func TestMirroredImageReferenceMatchesDigestWithClientsDetectsMismatch(t *testing.T) {
	t.Parallel()

	imageRef := "registry.internal.example/fugue-apps/demo:image-0123456789ab"
	expectedDigest := "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	matches, err := mirroredImageReferenceMatchesDigestWithClients(
		imageRef,
		expectedDigest,
		func(string, ...crane.Option) (string, error) {
			return "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil
		},
		func(string, ...crane.Option) ([]byte, error) {
			return []byte("{}"), nil
		},
	)
	if err != nil {
		t.Fatalf("match mirrored image ref: %v", err)
	}
	if matches {
		t.Fatal("expected mirrored image ref mismatch to return false")
	}
}

func TestDigestReferenceFromImageRef(t *testing.T) {
	t.Parallel()

	got, err := DigestReferenceFromImageRef(
		"registry.internal.example/fugue-apps/demo:image-0123456789ab",
		"sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	)
	if err != nil {
		t.Fatalf("digest reference from image ref: %v", err)
	}
	want := "registry.internal.example/fugue-apps/demo@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if got != want {
		t.Fatalf("unexpected digest reference %q want %q", got, want)
	}
}

func TestDetectImageBackgroundOverrideFromImageDetectsDualModePythonBot(t *testing.T) {
	t.Parallel()

	img, configFile := newTestImageWithConfig(t, map[string]string{
		"/home/bot.py": `from telegram.ext import ApplicationBuilder

application = ApplicationBuilder().token("demo").build()

if WEB_HOOK:
    application.run_webhook("0.0.0.0", 8000, webhook_url=WEB_HOOK)
else:
    application.run_polling()
`,
		"/home/pyproject.toml": "[project]\nname='demo-bot'\n",
	}, v1.Config{
		Entrypoint: []string{"python", "-u", "/home/bot.py"},
		WorkingDir: "/home",
		ExposedPorts: map[string]struct{}{
			"8000/tcp": {},
		},
	})

	stack, suppress, err := detectImageBackgroundOverrideFromImage(img, configFile)
	if err != nil {
		t.Fatalf("detect image background override: %v", err)
	}
	if stack != "python" {
		t.Fatalf("expected detected stack python, got %q", stack)
	}
	if !suppress {
		t.Fatal("expected dual-mode python bot image to suppress public-service readiness")
	}
}

func TestDetectImageBackgroundOverrideFromImageKeepsPythonWebServicePublic(t *testing.T) {
	t.Parallel()

	img, configFile := newTestImageWithConfig(t, map[string]string{
		"/app/service.py": `from flask import Flask

application = Flask(__name__)

if __name__ == "__main__":
    application.run(host="0.0.0.0", port=8000)
`,
		"/app/pyproject.toml": "[project]\nname='demo-web'\n",
	}, v1.Config{
		Entrypoint: []string{"python", "-u", "/app/service.py"},
		WorkingDir: "/app",
		ExposedPorts: map[string]struct{}{
			"8000/tcp": {},
		},
	})

	stack, suppress, err := detectImageBackgroundOverrideFromImage(img, configFile)
	if err != nil {
		t.Fatalf("detect image background override: %v", err)
	}
	if stack != "python" {
		t.Fatalf("expected detected stack python, got %q", stack)
	}
	if suppress {
		t.Fatal("expected python web service image to keep public-service readiness")
	}
}

func TestPythonImageAnalysisDirSupportsShellWrappedPythonEntrypoint(t *testing.T) {
	t.Parallel()

	got, ok := pythonImageAnalysisDir(&v1.ConfigFile{
		Config: v1.Config{
			Entrypoint: []string{"/bin/sh", "-c", "exec python -u /home/bot.py"},
			WorkingDir: "/home",
		},
	})
	if !ok {
		t.Fatal("expected shell-wrapped python entrypoint to be recognized")
	}
	if got != "/home" {
		t.Fatalf("expected /home analysis dir, got %q", got)
	}
}

func newTestImageWithConfig(t *testing.T, files map[string]string, config v1.Config) (v1.Image, *v1.ConfigFile) {
	t.Helper()

	fileMap := make(map[string][]byte, len(files))
	for path, content := range files {
		fileMap[path] = []byte(content)
	}

	img, err := crane.Image(fileMap)
	if err != nil {
		t.Fatalf("create test image: %v", err)
	}

	configFile, err := img.ConfigFile()
	if err != nil {
		t.Fatalf("read base config file: %v", err)
	}
	configFile.Config = config

	img, err = mutate.ConfigFile(img, configFile)
	if err != nil {
		t.Fatalf("apply config file: %v", err)
	}

	configFile, err = img.ConfigFile()
	if err != nil {
		t.Fatalf("read updated config file: %v", err)
	}
	return img, configFile
}
