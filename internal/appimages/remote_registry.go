package appimages

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// RemoteRegistry is the inventory facade. Its cache and manifest inspection
// are owned by the image domain, not the HTTP server. The Controller's
// maintenance deletion remains an uncached, fresh observation.
type RemoteRegistry struct {
	inspector *RemoteInspector
}

func NewRemoteRegistry() *RemoteRegistry {
	return &RemoteRegistry{inspector: NewRemoteInspector()}
}

func (r *RemoteRegistry) InspectImage(ctx context.Context, imageRef string) (Inspection, error) {
	return r.inspector.inspect(ctx, imageRef, true)
}

func (r *RemoteRegistry) DeleteImage(ctx context.Context, imageRef string) (DeleteResult, error) {
	normalized := SanitizeImageRef(imageRef)
	if normalized == "" {
		return DeleteResult{}, fmt.Errorf("image_ref is required")
	}
	inspect, err := r.InspectImage(ctx, normalized)
	if err != nil {
		return DeleteResult{}, err
	}
	if !inspect.Exists || strings.TrimSpace(inspect.Digest) == "" {
		r.inspector.clearCache(normalized)
		return DeleteResult{ImageRef: normalized, Digest: inspect.Digest, AlreadyMissing: true}, nil
	}
	result, err := deleteInspectedImage(ctx, normalized, inspect.Digest)
	if err == nil {
		r.inspector.clearCache(normalized)
	}
	return result, err
}

func SortedBlobDigests(blobSizes map[string]int64) []string {
	digests := make([]string, 0, len(blobSizes))
	for digest := range blobSizes {
		digests = append(digests, digest)
	}
	sort.Strings(digests)
	return digests
}
