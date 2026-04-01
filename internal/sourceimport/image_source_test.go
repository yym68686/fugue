package sourceimport

import (
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
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
	if !strings.HasPrefix(got, "registry.internal.example/fugue-apps/demo:image-0123456789ab") {
		t.Fatalf("unexpected mirrored image ref: %q", got)
	}
}
