package appimages

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
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

const remoteInspectorCacheTTL = 30 * time.Second

type remoteInspectorCacheEntry struct {
	CheckedAt time.Time
	Result    Inspection
}

// Inspection is a detached observation of one registry reference. The
// digest and blob sizes come from the same manifest read.
type Inspection struct {
	ImageRef  string
	Digest    string
	Exists    bool
	SizeBytes int64
	BlobSizes map[string]int64
}

func (result Inspection) Clone() Inspection {
	result.BlobSizes = maps.Clone(result.BlobSizes)
	return result
}

type RemoteInspector struct {
	ttl   time.Duration
	mu    sync.Mutex
	cache map[string]remoteInspectorCacheEntry
}

func NewRemoteInspector() *RemoteInspector {
	return &RemoteInspector{
		ttl:   remoteInspectorCacheTTL,
		cache: make(map[string]remoteInspectorCacheEntry),
	}
}

func (r *RemoteInspector) InspectImage(ctx context.Context, imageRef string) (bool, map[string]int64, error) {
	result, err := r.inspect(ctx, imageRef, false)
	if len(result.BlobSizes) == 0 {
		return result.Exists, nil, err
	}
	return result.Exists, result.BlobSizes, err
}

// inspect shares the manifest observation and cache while preserving the
// richer HTTP adapter's existing reference-qualified errors.
func (r *RemoteInspector) inspect(ctx context.Context, imageRef string, contextualErrors bool) (Inspection, error) {
	normalized := SanitizeImageRef(imageRef)
	if normalized == "" {
		return Inspection{}, fmt.Errorf("image_ref is required")
	}
	if result, ok := r.readCache(normalized); ok {
		return result, nil
	}

	ref, err := parseReference(normalized)
	if err != nil {
		return Inspection{}, err
	}
	descriptor, err := remote.Get(ref, remoteOptions(ctx)...)
	if err != nil {
		if isNotFound(err) {
			result := Inspection{ImageRef: normalized}
			r.writeCache(normalized, result)
			return result, nil
		}
		if contextualErrors {
			err = fmt.Errorf("inspect image %s: %w", normalized, err)
		}
		return Inspection{}, err
	}

	blobSizes, err := r.collectDescriptorBlobSizes(ctx, ref, descriptor, make(map[string]struct{}))
	if err != nil {
		if contextualErrors {
			err = fmt.Errorf("inspect image %s: %w", normalized, err)
		}
		return Inspection{}, err
	}
	result := Inspection{
		ImageRef:  normalized,
		Digest:    descriptor.Digest.String(),
		Exists:    true,
		SizeBytes: SumBlobSizes(blobSizes),
		BlobSizes: blobSizes,
	}
	r.writeCache(normalized, result)
	return result.Clone(), nil
}

func (r *RemoteInspector) collectDescriptorBlobSizes(
	ctx context.Context,
	ref name.Reference,
	descriptor *remote.Descriptor,
	seenChildManifests map[string]struct{},
) (map[string]int64, error) {
	blobSizes := make(map[string]int64)
	addBlobSize(blobSizes, descriptor.Digest.String(), descriptorSizeBytes(descriptor))

	switch descriptor.MediaType {
	case types.OCIManifestSchema1, types.DockerManifestSchema2:
		var manifest v1.Manifest
		if err := json.Unmarshal(descriptor.Manifest, &manifest); err != nil {
			return nil, fmt.Errorf("decode image manifest: %w", err)
		}
		addBlobDescriptor(blobSizes, manifest.Config)
		for _, layer := range manifest.Layers {
			addBlobDescriptor(blobSizes, layer)
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

			childRef, err := parseReference(fmt.Sprintf("%s@%s", ref.Context().Name(), childDigest))
			if err != nil {
				return nil, err
			}
			childDescriptor, err := remote.Get(childRef, remoteOptions(ctx)...)
			if err != nil {
				if isNotFound(err) {
					continue
				}
				return nil, fmt.Errorf("fetch child manifest %s: %w", childDigest, err)
			}
			childBlobSizes, err := r.collectDescriptorBlobSizes(ctx, childRef, childDescriptor, seenChildManifests)
			if err != nil {
				return nil, err
			}
			for digest, sizeBytes := range childBlobSizes {
				addBlobSize(blobSizes, digest, sizeBytes)
			}
		}
		return blobSizes, nil
	default:
		return nil, fmt.Errorf("unsupported media type %q", descriptor.MediaType)
	}
}

func (r *RemoteInspector) readCache(imageRef string) (Inspection, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.cache[imageRef]
	if !ok || time.Since(entry.CheckedAt) > r.ttl {
		return Inspection{}, false
	}
	return entry.Result.Clone(), true
}

func (r *RemoteInspector) writeCache(imageRef string, result Inspection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache[imageRef] = remoteInspectorCacheEntry{
		CheckedAt: time.Now().UTC(),
		Result:    result.Clone(),
	}
}

func (r *RemoteInspector) clearCache(imageRef string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.cache, imageRef)
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

func addBlobDescriptor(blobSizes map[string]int64, descriptor v1.Descriptor) {
	addBlobSize(blobSizes, descriptor.Digest.String(), descriptor.Size)
}

func addBlobSize(blobSizes map[string]int64, digest string, sizeBytes int64) {
	digest = strings.TrimSpace(digest)
	if digest == "" || sizeBytes <= 0 {
		return
	}
	if existing, ok := blobSizes[digest]; ok && existing >= sizeBytes {
		return
	}
	blobSizes[digest] = sizeBytes
}

func parseReference(imageRef string) (name.Reference, error) {
	imageRef = SanitizeImageRef(imageRef)
	ref, err := name.ParseReference(imageRef, nameOptions(imageRef)...)
	if err != nil {
		return nil, fmt.Errorf("parse image_ref: %w", err)
	}
	return ref, nil
}

func nameOptions(imageRef string) []name.Option {
	if isInsecureHost(hostFromImageRef(imageRef)) {
		return []name.Option{name.Insecure}
	}
	return nil
}

func hostFromImageRef(imageRef string) string {
	host := strings.TrimSpace(imageRef)
	if idx := strings.Index(host, "/"); idx >= 0 {
		host = host[:idx]
	}
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	}
	return strings.Trim(strings.TrimSpace(host), "[]")
}

func isInsecureHost(host string) bool {
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

func remoteOptions(ctx context.Context) []remote.Option {
	return []remote.Option{
		remote.WithContext(ctx),
		remote.WithAuthFromKeychain(authn.DefaultKeychain),
	}
}

func isNotFound(err error) bool {
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
