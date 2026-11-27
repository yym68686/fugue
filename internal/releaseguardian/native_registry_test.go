package releaseguardian

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"fugue/internal/declarativerelease"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

const testNativeRegistry = "registry.fixture.invalid"

type registryTestContent struct {
	body      []byte
	mediaType types.MediaType
}

func registryImageFixture(t *testing.T, objects map[string]registryTestContent, repo, revision string, index bool) (declarativerelease.ArtifactReceipt, string) {
	t.Helper()
	encode := func(value any) []byte {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	add := func(kind string, data []byte, mediaType types.MediaType) v1.Descriptor {
		hash, err := v1.NewHash(digest(data))
		if err != nil {
			t.Fatal(err)
		}
		objects["/v2/"+repo+"/"+kind+"/"+hash.String()] = registryTestContent{data, mediaType}
		return v1.Descriptor{Digest: hash, Size: int64(len(data)), MediaType: mediaType}
	}
	config := add("blobs", encode(v1.ConfigFile{Architecture: "amd64", OS: "linux", Config: v1.Config{
		Labels: map[string]string{"org.opencontainers.image.revision": revision},
	}}), types.OCIConfigJSON)
	layer := add("blobs", []byte("deterministic compressed-layer fixture "+revision), types.OCILayer)
	manifest := add("manifests", encode(v1.Manifest{SchemaVersion: 2, MediaType: types.OCIManifestSchema1, Config: config,
		Layers: []v1.Descriptor{layer}}), types.OCIManifestSchema1)
	top := manifest
	if index {
		manifest.Platform = &v1.Platform{OS: "linux", Architecture: "amd64"}
		top = add("manifests", encode(v1.IndexManifest{SchemaVersion: 2, MediaType: types.OCIImageIndex,
			Manifests: []v1.Descriptor{manifest}}), types.OCIImageIndex)
	}
	repository := testNativeRegistry + "/" + repo
	return declarativerelease.ArtifactReceipt{Repository: repository, ImmutableRef: repository + "@" + top.Digest.String(),
		TopDigest: top.Digest.String(), PlatformManifestDigest: manifest.Digest.String(), ConfigDigest: config.Digest.String(),
		OCIRevision: revision, Platform: "linux/amd64", ReceiptDigest: testDigest}, "/v2/" + repo + "/blobs/" + layer.Digest.String()
}

type registryRoundTrip func(*http.Request) (*http.Response, error)

func (f registryRoundTrip) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestIndependentRegistryReadbacksForwardAndRollback(t *testing.T) {
	for _, mode := range []string{"verified", "missing rollback layer", "corrupt rollback layer", "off-origin redirect", "mutable alias", "receipt revision mismatch"} {
		t.Run(mode, func(t *testing.T) {
			objects := map[string]registryTestContent{}
			forward, forwardLayer := registryImageFixture(t, objects, "fixture/component", strings.Repeat("1", 40), true)
			rollback, rollbackLayer := registryImageFixture(t, objects, "fixture/component", strings.Repeat("2", 40), false)
			var forwardReads, rollbackReads, crossOrigin atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/v2/" {
					w.WriteHeader(http.StatusOK)
					return
				}
				if req.Method != http.MethodGet {
					http.Error(w, "readback needs GET", http.StatusMethodNotAllowed)
					return
				}
				item, ok := objects[req.URL.Path]
				if !ok {
					http.NotFound(w, req)
					return
				}
				switch req.URL.Path {
				case forwardLayer:
					forwardReads.Add(1)
				case rollbackLayer:
					rollbackReads.Add(1)
					switch mode {
					case "missing rollback layer":
						http.NotFound(w, req)
						return
					case "corrupt rollback layer":
						item.body = []byte(strings.Repeat("x", len(item.body)))
					case "off-origin redirect":
						http.Redirect(w, req, "https://elsewhere.fixture.invalid/blob", http.StatusTemporaryRedirect)
						return
					}
				}
				w.Header().Set("Content-Type", string(item.mediaType))
				_, _ = w.Write(item.body)
			}))
			defer server.Close()
			loopback, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			registry, err := NewIndependentRegistry(testNativeRegistry, nil, registryRoundTrip(func(req *http.Request) (*http.Response, error) {
				if req.URL.Host != testNativeRegistry || req.URL.Scheme != "https" {
					crossOrigin.Add(1)
					return &http.Response{StatusCode: http.StatusBadRequest, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
				}
				mapped := req.Clone(req.Context())
				mapped.URL.Host = loopback.Host
				return server.Client().Transport.RoundTrip(mapped)
			}))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "mutable alias" {
				forward.ImmutableRef = forward.Repository + ":latest"
			}
			if mode == "receipt revision mismatch" {
				rollback.OCIRevision = strings.Repeat("3", 40)
			}
			err = registry.VerifyReleaseArtifacts(context.Background(), NativePublicationBoundary{
				ForwardOCI: forward.ImmutableRef, RollbackOCI: rollback.ImmutableRef, ForwardReceipt: forward, RollbackReceipt: rollback,
				ArtifactReceiptDigest: forward.ReceiptDigest,
			})
			if (err == nil) != (mode == "verified") || crossOrigin.Load() != 0 {
				t.Fatalf("mode=%s err=%v unexpected external requests=%d", mode, err, crossOrigin.Load())
			}
			if mode == "verified" && (forwardReads.Load() != 1 || rollbackReads.Load() != 1) {
				t.Fatalf("expected both layer bodies to be read once, got forward=%d rollback=%d", forwardReads.Load(), rollbackReads.Load())
			}
			if mode == "verified" {
				evidence, err := registry.VerifyRuntimeArtifact(context.Background(), forward)
				if err != nil || evidence.Image != forward.ImmutableRef || evidence.IndexDigest != forward.TopDigest ||
					evidence.ManifestDigest != forward.PlatformManifestDigest || evidence.ConfigDigest != forward.ConfigDigest ||
					evidence.OCIRevision != forward.OCIRevision || forwardReads.Load() != 2 {
					t.Fatalf("Guardian runtime evidence=%+v err=%v layerReads=%d", evidence, err, forwardReads.Load())
				}
			}
		})
	}
}

func TestIndependentRegistryRejectsGitHubOrigins(t *testing.T) {
	for _, host := range []string{"ghcr.io", "ghcr.io:443", "GHCR.IO.", "uploads.github.com", "objects.githubusercontent.com:443", "registry", "https://registry.fixture.invalid"} {
		if _, err := NewIndependentRegistry(host, nil, nil); err == nil {
			t.Fatalf("independent registry accepted forbidden host %q", host)
		}
	}
}
