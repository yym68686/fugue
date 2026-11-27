package releaseguardian

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/declarativerelease"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

const (
	maxNativeManifestBytes = 8 << 20
	maxNativeConfigBytes   = 4 << 20
	maxNativeImageBytes    = 8 << 30
	nativeRegistryDeadline = 3 * time.Minute
)

// IndependentRegistry verifies actual OCI content in the configured runtime
// registry. Authentication is explicitly supplied; no Docker credential
// helper, external registry, or off-origin redirect is used. The operator
// must provision the registry and its credentials independently of GitHub.
type IndependentRegistry struct {
	host      string
	transport http.RoundTripper
	auth      authn.Authenticator
}

func NewIndependentRegistry(host string, auth authn.Authenticator, transport http.RoundTripper) (*IndependentRegistry, error) {
	registry, err := name.NewRegistry(host, name.StrictValidation)
	parsed, parseErr := url.Parse("https://" + host)
	if err != nil || host == "" || registry.RegistryStr() != host || registry.Scheme() != "https" ||
		parseErr != nil || parsed.Host != host || parsed.User != nil || !strings.Contains(parsed.Hostname(), ".") ||
		githubRegistryHost(parsed.Hostname()) {
		return nil, errors.New("native release requires an explicit, independent HTTPS registry")
	}
	if transport == nil {
		transport = remote.DefaultTransport
	}
	if auth == nil {
		auth = authn.Anonymous
	}
	return &IndependentRegistry{host, registryOriginTransport{host, transport}, auth}, nil
}

func githubRegistryHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	for _, domain := range []string{"ghcr.io", "github.com", "githubusercontent.com"} {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}

type registryOriginTransport struct {
	host string
	next http.RoundTripper
}

func (t registryOriginTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "https" || r.URL.Host != t.host || r.URL.User != nil ||
		(r.Method != http.MethodGet && r.Method != http.MethodHead) {
		return nil, errors.New("native registry request crossed its configured read-only HTTPS origin")
	}
	return t.next.RoundTrip(r)
}

// VerifyReleaseArtifacts reads the exact runtime references rather than a
// downloadable CI artifact, a tag, or an equivalent digest in another repo.
// It is a point-in-time check, not a registry retention guarantee.
func (r *IndependentRegistry) VerifyReleaseArtifacts(ctx context.Context, boundary NativePublicationBoundary) error {
	if r == nil || r.transport == nil {
		return errors.New("native artifact verifier is unavailable")
	}
	if boundary.ForwardReceipt.ImmutableRef != boundary.ForwardOCI || boundary.RollbackReceipt.ImmutableRef != boundary.RollbackOCI ||
		boundary.ForwardReceipt.ReceiptDigest != boundary.ArtifactReceiptDigest {
		return errors.New("native artifact receipts differ from the runtime image references")
	}
	ctx, cancel := context.WithTimeout(ctx, nativeRegistryDeadline)
	defer cancel()
	for _, artifact := range []struct {
		role    string
		receipt declarativerelease.ArtifactReceipt
	}{{"forward", boundary.ForwardReceipt}, {"rollback", boundary.RollbackReceipt}} {
		if err := r.verifyReceipt(ctx, artifact.receipt); err != nil {
			return fmt.Errorf("verify independent %s artifact: %w", artifact.role, err)
		}
	}
	return nil
}

// VerifyRuntimeArtifact reads the complete image named by the bound receipt.
// Its result can be compared with kubelet imageIDs during this execution;
// the historic receipt alone is not proof the image can still be pulled.
func (r *IndependentRegistry) VerifyRuntimeArtifact(ctx context.Context, receipt declarativerelease.ArtifactReceipt) (declarativerelease.RegistryVerification, error) {
	if r == nil || r.transport == nil {
		return declarativerelease.RegistryVerification{}, errors.New("native artifact verifier is unavailable")
	}
	ctx, cancel := context.WithTimeout(ctx, nativeRegistryDeadline)
	defer cancel()
	if err := r.verifyReceipt(ctx, receipt); err != nil {
		return declarativerelease.RegistryVerification{}, err
	}
	return declarativerelease.RegistryVerification{
		Image: receipt.ImmutableRef, IndexDigest: receipt.TopDigest, ManifestDigest: receipt.PlatformManifestDigest,
		ConfigDigest: receipt.ConfigDigest, OCIRevision: receipt.OCIRevision, Platform: receipt.Platform,
		Verification: "independent_registry_full_readback",
	}, nil
}

func (r *IndependentRegistry) verifyReceipt(ctx context.Context, receipt declarativerelease.ArtifactReceipt) error {
	ref, err := name.NewDigest(receipt.ImmutableRef, name.StrictValidation)
	if err != nil || ref.Context().RegistryStr() != r.host || ref.Name() != receipt.ImmutableRef || ref.DigestStr() != receipt.TopDigest ||
		receipt.Platform != "linux/amd64" || !shaPattern.MatchString(receipt.OCIRevision) ||
		!digestPattern.MatchString(receipt.PlatformManifestDigest) || !digestPattern.MatchString(receipt.ConfigDigest) {
		return errors.New("artifact is not an exact digest in the configured independent registry")
	}
	puller, err := remote.NewPuller(remote.WithAuth(r.auth), remote.WithTransport(r.transport))
	if err != nil {
		return err
	}
	get := func(ref name.Digest, expected string) (*remote.Descriptor, error) {
		d, err := puller.Get(ctx, ref)
		if err != nil {
			return nil, err
		}
		if len(d.Manifest) == 0 || len(d.Manifest) > maxNativeManifestBytes || digest(d.Manifest) != expected {
			return nil, errors.New("registry response has the wrong manifest digest or size")
		}
		return d, nil
	}
	manifestDescriptor, err := get(ref, receipt.TopDigest)
	if err != nil {
		return err
	}
	if manifestDescriptor.MediaType == types.OCIImageIndex || manifestDescriptor.MediaType == types.DockerManifestList {
		var index v1.IndexManifest
		if err := json.Unmarshal(manifestDescriptor.Manifest, &index); err != nil || index.SchemaVersion != 2 || len(index.Manifests) > 64 {
			return errors.New("invalid OCI index")
		}
		var member *v1.Descriptor
		for i := range index.Manifests {
			item := &index.Manifests[i]
			if item.Platform != nil && item.Platform.OS == "linux" && item.Platform.Architecture == "amd64" &&
				item.Platform.Variant == "" && item.Platform.OSVersion == "" {
				if member != nil {
					return errors.New("OCI index has ambiguous runtime platform")
				}
				member = item
			}
		}
		if member == nil || member.Digest.String() != receipt.PlatformManifestDigest || len(member.URLs) != 0 ||
			member.Size <= 0 || member.Size > maxNativeManifestBytes || !nativeImageMediaType(member.MediaType) {
			return errors.New("OCI index lacks the reviewed platform manifest")
		}
		manifestDescriptor, err = get(ref.Context().Digest(receipt.PlatformManifestDigest), receipt.PlatformManifestDigest)
		if err != nil {
			return err
		}
		if int64(len(manifestDescriptor.Manifest)) != member.Size || manifestDescriptor.MediaType != member.MediaType {
			return errors.New("registry platform manifest differs from its index descriptor")
		}
	} else if receipt.PlatformManifestDigest != receipt.TopDigest {
		return errors.New("single platform manifest does not match the artifact receipt")
	}
	var manifest v1.Manifest
	if !nativeImageMediaType(manifestDescriptor.MediaType) || json.Unmarshal(manifestDescriptor.Manifest, &manifest) != nil ||
		manifest.SchemaVersion != 2 || manifest.Config.Digest.String() != receipt.ConfigDigest ||
		(manifest.Config.MediaType != types.OCIConfigJSON && manifest.Config.MediaType != types.DockerConfigJSON) ||
		manifest.Config.Size > maxNativeConfigBytes || len(manifest.Layers) == 0 || len(manifest.Layers) > 512 {
		return errors.New("OCI manifest does not match the reviewed image configuration")
	}
	var configBody strings.Builder
	if err := verifyNativeBlob(ctx, puller, ref.Context(), manifest.Config, &configBody); err != nil {
		return fmt.Errorf("read OCI configuration: %w", err)
	}
	var config v1.ConfigFile
	if err := json.Unmarshal([]byte(configBody.String()), &config); err != nil || config.OS != "linux" ||
		config.Architecture != "amd64" || config.Variant != "" ||
		config.Config.Labels["org.opencontainers.image.revision"] != receipt.OCIRevision {
		return errors.New("OCI configuration platform or source revision differs from the artifact receipt")
	}
	var total int64
	sizes := make(map[v1.Hash]int64, len(manifest.Layers))
	for _, layer := range manifest.Layers {
		if !layer.MediaType.IsLayer() || !layer.MediaType.IsDistributable() || layer.Size <= 0 {
			return errors.New("OCI layers exceed native release bounds or require external content")
		}
		if size, verified := sizes[layer.Digest]; verified {
			if size != layer.Size || len(layer.URLs) != 0 {
				return errors.New("OCI layer has conflicting descriptors")
			}
			continue
		}
		if layer.Size > maxNativeImageBytes-total {
			return errors.New("OCI layers exceed native release bounds")
		}
		total += layer.Size
		if err := verifyNativeBlob(ctx, puller, ref.Context(), layer, io.Discard); err != nil {
			return fmt.Errorf("read OCI layer: %w", err)
		}
		sizes[layer.Digest] = layer.Size
	}
	return nil
}

func nativeImageMediaType(mediaType types.MediaType) bool {
	return mediaType == types.OCIManifestSchema1 || mediaType == types.DockerManifestSchema2
}

func verifyNativeBlob(ctx context.Context, puller *remote.Puller, repo name.Repository, d v1.Descriptor, dest io.Writer) error {
	if d.Size <= 0 || !digestPattern.MatchString(d.Digest.String()) || len(d.URLs) != 0 {
		return errors.New("OCI blob is not bound to an exact registry-local descriptor")
	}
	blob, err := puller.Layer(ctx, repo.Digest(d.Digest.String()))
	if err != nil {
		return err
	}
	reader, err := blob.Compressed()
	if err != nil {
		return err
	}
	defer reader.Close()
	hash := sha256.New()
	size, err := io.Copy(io.MultiWriter(hash, dest), io.LimitReader(reader, d.Size+1))
	if err != nil {
		return err
	}
	if size != d.Size || fmt.Sprintf("sha256:%x", hash.Sum(nil)) != d.Digest.String() {
		return errors.New("downloaded blob does not match its exact size and digest")
	}
	return nil
}
