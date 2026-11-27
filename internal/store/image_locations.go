package store

import (
	"context"
	"strings"
	"time"

	"fugue/internal/model"
)

func (s *Store) UpsertImageLocation(location model.ImageLocation) (model.ImageLocation, error) {
	rawStatus := strings.TrimSpace(location.Status)
	location = normalizeImageLocation(location)
	if location.Digest == "" && location.Status == model.ImageLocationStatusPresent {
		resolved, err := s.resolveImageLocationDigest(location)
		if err != nil {
			return model.ImageLocation{}, err
		}
		location.Digest = resolved
	}
	if location.ImageRef == "" && location.Digest == "" {
		return model.ImageLocation{}, ErrInvalidInput
	}
	if rawStatus != "" && location.Status == "" {
		return model.ImageLocation{}, ErrInvalidInput
	}
	if location.Status == "" {
		location.Status = model.ImageLocationStatusPresent
	}
	return s.pgUpsertImageLocation(location)
}

// DemoteLegacyImageLocationIfUnchanged marks a historical distributed
// Present row Missing without re-normalizing its identity. Legacy rows may
// have an empty digest even when ImageRef is digest-qualified; sending them
// through UpsertImageLocation would derive that digest and can collide with
// the canonical row that superseded them. The UpdatedAt predicate is the
// compare-and-swap fence against a concurrent reporter refreshing the row.
func (s *Store) DemoteLegacyImageLocationIfUnchanged(
	id string,
	expectedUpdatedAt time.Time,
	observedAt time.Time,
	lastError string,
) (model.ImageLocation, error) {
	id = strings.TrimSpace(id)
	lastError = strings.TrimSpace(lastError)
	if id == "" || expectedUpdatedAt.IsZero() || observedAt.IsZero() || lastError == "" {
		return model.ImageLocation{}, ErrInvalidInput
	}
	expectedUpdatedAt = expectedUpdatedAt.UTC()
	observedAt = observedAt.UTC()
	updatedAt := time.Now().UTC()
	if minimum := expectedUpdatedAt.Add(time.Microsecond); updatedAt.Before(minimum) {
		updatedAt = minimum
	}
	return s.pgDemoteLegacyImageLocationIfUnchanged(
		id,
		expectedUpdatedAt,
		observedAt,
		updatedAt,
		lastError,
	)
}

// resolveImageLocationDigest bridges compatibility reports that identify an
// image by tag while still keeping distributed Present records
// content-addressed.  The lookup is intentionally conservative: a digest is
// adopted only when exactly one canonical image identity matches the report.
func (s *Store) resolveImageLocationDigest(location model.ImageLocation) (string, error) {
	if digest := CanonicalImageDigest(location.Digest); digest != "" {
		return digest, nil
	}
	if digest := imageDigestFromReference(location.ImageRef); digest != "" {
		return digest, nil
	}
	filter := model.ImageFilter{PlatformAdmin: true}
	if strings.TrimSpace(location.AppID) != "" {
		filter.AppID = strings.TrimSpace(location.AppID)
	}
	if strings.TrimSpace(location.TenantID) != "" {
		filter.TenantID = strings.TrimSpace(location.TenantID)
	}
	if strings.TrimSpace(location.ImageRef) != "" {
		filter.ImageRef = strings.TrimSpace(location.ImageRef)
	}
	images, err := s.ListImages(filter)
	if err != nil {
		return "", err
	}
	digests := map[string]struct{}{}
	for _, image := range images {
		if digest := CanonicalImageDigest(image.CanonicalDigest); digest != "" {
			digests[digest] = struct{}{}
		}
	}
	if len(digests) == 1 {
		for digest := range digests {
			return digest, nil
		}
	}
	return "", nil
}

// ResolveImageLocationDigest returns the sole canonical image identity that
// can be proven for a tag-only location report.  An empty result is
// intentionally ambiguous/missing evidence and must not be promoted to a
// distributed Present row.
func (s *Store) ResolveImageLocationDigest(location model.ImageLocation) (string, error) {
	location = normalizeImageLocation(location)
	return s.resolveImageLocationDigest(location)
}

func (s *Store) ListImageLocations(filter model.ImageLocationFilter) ([]model.ImageLocation, error) {
	filter = normalizeImageLocationFilter(filter)
	return s.pgListImageLocations(filter)
}

func normalizeImageLocation(location model.ImageLocation) model.ImageLocation {
	location.ID = stripImageNUL(location.ID)
	location.TenantID = stripImageNUL(location.TenantID)
	location.AppID = stripImageNUL(location.AppID)
	location.ImageRef = stripImageNUL(location.ImageRef)
	location.Digest = stripImageNUL(location.Digest)
	location.SourceOperationID = stripImageNUL(location.SourceOperationID)
	location.NodeID = stripImageNUL(location.NodeID)
	location.RuntimeID = stripImageNUL(location.RuntimeID)
	location.ClusterNodeName = stripImageNUL(location.ClusterNodeName)
	location.CacheEndpoint = stripImageNUL(location.CacheEndpoint)
	location.Status = stripImageNUL(location.Status)
	location.LastError = stripImageNUL(location.LastError)
	location.TenantID = strings.TrimSpace(location.TenantID)
	location.AppID = strings.TrimSpace(location.AppID)
	location.ImageRef = strings.TrimSpace(location.ImageRef)
	location.Digest = normalizeImageDigest(location.Digest)
	if location.Digest == "" {
		location.Digest = imageDigestFromReference(location.ImageRef)
	}
	location.SourceOperationID = strings.TrimSpace(location.SourceOperationID)
	location.NodeID = strings.TrimSpace(location.NodeID)
	location.RuntimeID = strings.TrimSpace(location.RuntimeID)
	location.ClusterNodeName = strings.TrimSpace(location.ClusterNodeName)
	location.CacheEndpoint = strings.TrimRight(strings.TrimSpace(location.CacheEndpoint), "/")
	location.Status = normalizeImageLocationStatus(location.Status)
	location.LastError = strings.TrimSpace(location.LastError)
	if location.SizeBytes < 0 {
		location.SizeBytes = 0
	}
	return location
}

func normalizeImageLocationFilter(filter model.ImageLocationFilter) model.ImageLocationFilter {
	filter.TenantID = strings.TrimSpace(filter.TenantID)
	filter.AppID = strings.TrimSpace(filter.AppID)
	filter.ImageRef = strings.TrimSpace(filter.ImageRef)
	filter.Digest = normalizeImageDigest(filter.Digest)
	filter.Status = normalizeImageLocationStatus(filter.Status)
	filter.NodeID = strings.TrimSpace(filter.NodeID)
	filter.RuntimeID = strings.TrimSpace(filter.RuntimeID)
	filter.ClusterNodeName = strings.TrimSpace(filter.ClusterNodeName)
	filter.AppIDs = sortedTrimmedStringKeys(trimmedStringSet(filter.AppIDs))
	return filter
}

func normalizeImageLocationStatus(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", model.ImageLocationStatusPresent:
		return model.ImageLocationStatusPresent
	case model.ImageLocationStatusMissing:
		return model.ImageLocationStatusMissing
	case model.ImageLocationStatusPulling:
		return model.ImageLocationStatusPulling
	case model.ImageLocationStatusFailed:
		return model.ImageLocationStatusFailed
	default:
		return ""
	}
}

func normalizeImageDigest(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "sha256:") {
		return raw
	}
	if strings.HasPrefix(raw, "@sha256:") {
		return strings.TrimPrefix(raw, "@")
	}
	return raw
}

func (s *Store) ListImageLocationsContext(ctx context.Context, filter model.ImageLocationFilter) ([]model.ImageLocation, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.pgListImageLocationsContext(ctx, normalizeImageLocationFilter(filter))
}
