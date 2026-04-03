package appimages

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	Exists    bool
	BlobSizes map[string]int64
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
	normalized := strings.TrimSpace(imageRef)
	if normalized == "" {
		return false, nil, fmt.Errorf("image_ref is required")
	}
	if exists, blobSizes, ok := r.readCache(normalized); ok {
		return exists, blobSizes, nil
	}

	ref, err := parseReference(normalized)
	if err != nil {
		return false, nil, err
	}
	descriptor, err := remote.Get(ref, remoteOptions(ctx)...)
	if err != nil {
		if isNotFound(err) {
			r.writeCache(normalized, false, nil)
			return false, nil, nil
		}
		return false, nil, err
	}

	blobSizes, err := r.collectDescriptorBlobSizes(ctx, ref, descriptor, make(map[string]struct{}))
	if err != nil {
		return false, nil, err
	}
	r.writeCache(normalized, true, blobSizes)
	return true, cloneBlobSizes(blobSizes), nil
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

func (r *RemoteInspector) readCache(imageRef string) (bool, map[string]int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, ok := r.cache[imageRef]
	if !ok || time.Since(entry.CheckedAt) > r.ttl {
		return false, nil, false
	}
	return entry.Exists, cloneBlobSizes(entry.BlobSizes), true
}

func (r *RemoteInspector) writeCache(imageRef string, exists bool, blobSizes map[string]int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cache[imageRef] = remoteInspectorCacheEntry{
		CheckedAt: time.Now().UTC(),
		Exists:    exists,
		BlobSizes: cloneBlobSizes(blobSizes),
	}
}

func cloneBlobSizes(blobSizes map[string]int64) map[string]int64 {
	if len(blobSizes) == 0 {
		return nil
	}
	cloned := make(map[string]int64, len(blobSizes))
	for digest, sizeBytes := range blobSizes {
		cloned[digest] = sizeBytes
	}
	return cloned
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
	ref, err := name.ParseReference(strings.TrimSpace(imageRef), nameOptions(imageRef)...)
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
