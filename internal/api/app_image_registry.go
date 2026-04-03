package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

const appImageRegistryCacheTTL = 30 * time.Second

type appImageRegistry interface {
	InspectImage(ctx context.Context, imageRef string) (appImageRegistryInspectResult, error)
	DeleteImage(ctx context.Context, imageRef string) (appImageRegistryDeleteResult, error)
}

type appImageRegistryInspectResult struct {
	ImageRef  string
	Digest    string
	Exists    bool
	SizeBytes int64
	BlobSizes map[string]int64
}

type appImageRegistryDeleteResult struct {
	ImageRef       string
	Digest         string
	Deleted        bool
	AlreadyMissing bool
}

type appImageRegistryCacheEntry struct {
	CheckedAt time.Time
	Result    appImageRegistryInspectResult
}

type remoteAppImageRegistry struct {
	ttl   time.Duration
	mu    sync.Mutex
	cache map[string]appImageRegistryCacheEntry
}

func newRemoteAppImageRegistry() appImageRegistry {
	return &remoteAppImageRegistry{
		ttl:   appImageRegistryCacheTTL,
		cache: make(map[string]appImageRegistryCacheEntry),
	}
}

func (r *remoteAppImageRegistry) InspectImage(ctx context.Context, imageRef string) (appImageRegistryInspectResult, error) {
	normalized := strings.TrimSpace(imageRef)
	if normalized == "" {
		return appImageRegistryInspectResult{}, fmt.Errorf("image_ref is required")
	}
	if cached, ok := r.readCache(normalized); ok {
		return cached, nil
	}

	ref, err := parseAppImageRegistryReference(normalized)
	if err != nil {
		return appImageRegistryInspectResult{}, err
	}
	descriptor, err := remote.Get(ref, appImageRegistryRemoteOptions(ctx)...)
	if err != nil {
		if appImageRegistryIsNotFound(err) {
			result := appImageRegistryInspectResult{
				ImageRef: normalized,
				Exists:   false,
			}
			r.writeCache(normalized, result)
			return result, nil
		}
		return appImageRegistryInspectResult{}, fmt.Errorf("inspect image %s: %w", normalized, err)
	}

	blobSizes, err := r.collectDescriptorBlobSizes(ctx, ref, descriptor, make(map[string]struct{}))
	if err != nil {
		return appImageRegistryInspectResult{}, fmt.Errorf("inspect image %s: %w", normalized, err)
	}

	result := appImageRegistryInspectResult{
		ImageRef:  normalized,
		Digest:    descriptor.Digest.String(),
		Exists:    true,
		SizeBytes: sumAppImageBlobSizes(blobSizes),
		BlobSizes: blobSizes,
	}
	r.writeCache(normalized, result)
	return cloneAppImageRegistryInspectResult(result), nil
}

func (r *remoteAppImageRegistry) DeleteImage(ctx context.Context, imageRef string) (appImageRegistryDeleteResult, error) {
	normalized := strings.TrimSpace(imageRef)
	if normalized == "" {
		return appImageRegistryDeleteResult{}, fmt.Errorf("image_ref is required")
	}

	inspect, err := r.InspectImage(ctx, normalized)
	if err != nil {
		return appImageRegistryDeleteResult{}, err
	}
	if !inspect.Exists || strings.TrimSpace(inspect.Digest) == "" {
		r.clearCache(normalized)
		return appImageRegistryDeleteResult{
			ImageRef:       normalized,
			Digest:         inspect.Digest,
			AlreadyMissing: true,
		}, nil
	}

	ref, err := parseAppImageRegistryReference(normalized)
	if err != nil {
		return appImageRegistryDeleteResult{}, err
	}
	digestRef, err := parseAppImageRegistryReference(fmt.Sprintf("%s@%s", ref.Context().Name(), inspect.Digest))
	if err != nil {
		return appImageRegistryDeleteResult{}, err
	}
	if err := remote.Delete(digestRef, appImageRegistryRemoteOptions(ctx)...); err != nil {
		if appImageRegistryIsNotFound(err) {
			r.clearCache(normalized)
			return appImageRegistryDeleteResult{
				ImageRef:       normalized,
				Digest:         inspect.Digest,
				AlreadyMissing: true,
			}, nil
		}
		return appImageRegistryDeleteResult{}, fmt.Errorf("delete image %s: %w", normalized, err)
	}

	r.clearCache(normalized)
	return appImageRegistryDeleteResult{
		ImageRef: normalized,
		Digest:   inspect.Digest,
		Deleted:  true,
	}, nil
}

func (r *remoteAppImageRegistry) collectDescriptorBlobSizes(
	ctx context.Context,
	ref name.Reference,
	descriptor *remote.Descriptor,
	seenChildManifests map[string]struct{},
) (map[string]int64, error) {
	blobSizes := make(map[string]int64)
	addAppImageBlobSize(blobSizes, descriptor.Digest.String(), descriptorSizeBytes(descriptor))

	switch descriptor.MediaType {
	case types.OCIManifestSchema1, types.DockerManifestSchema2:
		var manifest v1.Manifest
		if err := json.Unmarshal(descriptor.Manifest, &manifest); err != nil {
			return nil, fmt.Errorf("decode image manifest: %w", err)
		}
		addAppImageBlobDescriptor(blobSizes, manifest.Config)
		for _, layer := range manifest.Layers {
			addAppImageBlobDescriptor(blobSizes, layer)
		}
		return blobSizes, nil
	case types.OCIImageIndex, types.DockerManifestList:
		var index v1.IndexManifest
		if err := json.Unmarshal(descriptor.Manifest, &index); err != nil {
			return nil, fmt.Errorf("decode image index: %w", err)
		}
		for _, child := range index.Manifests {
			childDigest := child.Digest.String()
			if childDigest == "" {
				continue
			}
			if _, exists := seenChildManifests[childDigest]; exists {
				continue
			}
			seenChildManifests[childDigest] = struct{}{}

			childRef, err := parseAppImageRegistryReference(fmt.Sprintf("%s@%s", ref.Context().Name(), childDigest))
			if err != nil {
				return nil, err
			}
			childDescriptor, err := remote.Get(childRef, appImageRegistryRemoteOptions(ctx)...)
			if err != nil {
				if appImageRegistryIsNotFound(err) {
					continue
				}
				return nil, fmt.Errorf("fetch child manifest %s: %w", childDigest, err)
			}
			childBlobSizes, err := r.collectDescriptorBlobSizes(ctx, childRef, childDescriptor, seenChildManifests)
			if err != nil {
				return nil, err
			}
			for digest, sizeBytes := range childBlobSizes {
				addAppImageBlobSize(blobSizes, digest, sizeBytes)
			}
		}
		return blobSizes, nil
	default:
		return nil, fmt.Errorf("unsupported media type %q", descriptor.MediaType)
	}
}

func (r *remoteAppImageRegistry) readCache(imageRef string) (appImageRegistryInspectResult, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.cache[imageRef]
	if !ok || time.Since(entry.CheckedAt) > r.ttl {
		return appImageRegistryInspectResult{}, false
	}
	return cloneAppImageRegistryInspectResult(entry.Result), true
}

func (r *remoteAppImageRegistry) writeCache(imageRef string, result appImageRegistryInspectResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache[imageRef] = appImageRegistryCacheEntry{
		CheckedAt: time.Now().UTC(),
		Result:    cloneAppImageRegistryInspectResult(result),
	}
}

func (r *remoteAppImageRegistry) clearCache(imageRef string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.cache, imageRef)
}

func cloneAppImageRegistryInspectResult(result appImageRegistryInspectResult) appImageRegistryInspectResult {
	out := result
	if len(result.BlobSizes) == 0 {
		return out
	}
	out.BlobSizes = make(map[string]int64, len(result.BlobSizes))
	for digest, sizeBytes := range result.BlobSizes {
		out.BlobSizes[digest] = sizeBytes
	}
	return out
}

func sumAppImageBlobSizes(blobSizes map[string]int64) int64 {
	var total int64
	for _, sizeBytes := range blobSizes {
		if sizeBytes > 0 {
			total += sizeBytes
		}
	}
	return total
}

func descriptorSizeBytes(descriptor *remote.Descriptor) int64 {
	if descriptor == nil {
		return 0
	}
	if descriptor.Size > 0 {
		return descriptor.Size
	}
	return int64(len(descriptor.Manifest))
}

func addAppImageBlobDescriptor(blobSizes map[string]int64, descriptor v1.Descriptor) {
	addAppImageBlobSize(blobSizes, descriptor.Digest.String(), descriptor.Size)
}

func addAppImageBlobSize(blobSizes map[string]int64, digest string, sizeBytes int64) {
	digest = strings.TrimSpace(digest)
	if digest == "" || sizeBytes <= 0 {
		return
	}
	if existing, ok := blobSizes[digest]; ok && existing >= sizeBytes {
		return
	}
	blobSizes[digest] = sizeBytes
}

func parseAppImageRegistryReference(imageRef string) (name.Reference, error) {
	ref, err := name.ParseReference(strings.TrimSpace(imageRef), appImageRegistryNameOptions(imageRef)...)
	if err != nil {
		return nil, fmt.Errorf("parse image_ref: %w", err)
	}
	return ref, nil
}

func appImageRegistryNameOptions(imageRef string) []name.Option {
	if appImageRegistryIsInsecureHost(appImageRegistryHostFromImageRef(imageRef)) {
		return []name.Option{name.Insecure}
	}
	return nil
}

func appImageRegistryHostFromImageRef(imageRef string) string {
	host := strings.TrimSpace(imageRef)
	if idx := strings.Index(host, "/"); idx >= 0 {
		host = host[:idx]
	}
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	}
	return strings.Trim(strings.TrimSpace(host), "[]")
}

func appImageRegistryIsInsecureHost(host string) bool {
	host = strings.TrimSpace(strings.ToLower(host))
	if host == "" {
		return false
	}
	if host == "localhost" || net.ParseIP(host) != nil {
		return true
	}
	return strings.HasSuffix(host, ".svc") ||
		strings.HasSuffix(host, ".svc.cluster.local") ||
		strings.HasSuffix(host, ".cluster.local")
}

func appImageRegistryRemoteOptions(ctx context.Context) []remote.Option {
	return []remote.Option{
		remote.WithContext(ctx),
		remote.WithAuthFromKeychain(authn.DefaultKeychain),
	}
}

func appImageRegistryIsNotFound(err error) bool {
	var transportErr *transport.Error
	if !errors.As(err, &transportErr) {
		return false
	}
	if transportErr.StatusCode == http.StatusNotFound {
		return true
	}
	for _, diagnostic := range transportErr.Errors {
		switch diagnostic.Code {
		case transport.ManifestUnknownErrorCode,
			transport.NameUnknownErrorCode,
			transport.BlobUnknownErrorCode:
			return true
		}
	}
	return false
}

func sortedAppImageBlobDigests(blobSizes map[string]int64) []string {
	digests := make([]string, 0, len(blobSizes))
	for digest := range blobSizes {
		digests = append(digests, digest)
	}
	sort.Strings(digests)
	return digests
}
