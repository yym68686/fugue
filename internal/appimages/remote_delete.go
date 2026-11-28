package appimages

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/v1/remote"
)

type DeleteResult struct {
	ImageRef       string
	Digest         string
	Deleted        bool
	AlreadyMissing bool
}

func DeleteRemoteImage(ctx context.Context, imageRef string) (DeleteResult, error) {
	normalized := SanitizeImageRef(imageRef)
	if normalized == "" {
		return DeleteResult{}, fmt.Errorf("image_ref is required")
	}

	ref, err := parseReference(normalized)
	if err != nil {
		return DeleteResult{}, err
	}
	descriptor, err := remote.Get(ref, remoteOptions(ctx)...)
	if err != nil {
		if isNotFound(err) {
			return DeleteResult{
				ImageRef:       normalized,
				AlreadyMissing: true,
			}, nil
		}
		return DeleteResult{}, fmt.Errorf("inspect image %s: %w", normalized, err)
	}

	digest := strings.TrimSpace(descriptor.Digest.String())
	if digest == "" {
		return DeleteResult{
			ImageRef:       normalized,
			AlreadyMissing: true,
		}, nil
	}

	return deleteInspectedReference(ctx, ref.Context().Name(), normalized, digest)
}

func deleteInspectedImage(ctx context.Context, normalized, digest string) (DeleteResult, error) {
	ref, err := parseReference(normalized)
	if err != nil {
		return DeleteResult{}, err
	}
	return deleteInspectedReference(ctx, ref.Context().Name(), normalized, digest)
}

func deleteInspectedReference(ctx context.Context, repository, normalized, digest string) (DeleteResult, error) {
	digestRef, err := parseReference(fmt.Sprintf("%s@%s", repository, digest))
	if err != nil {
		return DeleteResult{}, err
	}
	if err := remote.Delete(digestRef, remoteOptions(ctx)...); err != nil {
		if isNotFound(err) {
			return DeleteResult{
				ImageRef:       normalized,
				Digest:         digest,
				AlreadyMissing: true,
			}, nil
		}
		return DeleteResult{}, fmt.Errorf("delete image %s: %w", normalized, err)
	}

	return DeleteResult{
		ImageRef: normalized,
		Digest:   digest,
		Deleted:  true,
	}, nil
}
