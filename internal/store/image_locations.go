package store

import (
	"sort"
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
	if s.usingDatabase() {
		return s.pgUpsertImageLocation(location)
	}

	var out model.ImageLocation
	err := s.withLockedState(true, func(state *model.State) error {
		now := time.Now().UTC()
		index := findImageLocation(state, location)
		if index >= 0 {
			current := state.ImageLocations[index]
			location.ID = current.ID
			location.CreatedAt = current.CreatedAt
			if location.TenantID == "" {
				location.TenantID = current.TenantID
			}
			if location.AppID == "" {
				location.AppID = current.AppID
			}
			if location.SourceOperationID == "" {
				location.SourceOperationID = current.SourceOperationID
			}
			if location.NodeID == "" {
				location.NodeID = current.NodeID
			}
			if location.RuntimeID == "" {
				location.RuntimeID = current.RuntimeID
			}
			if location.ClusterNodeName == "" {
				location.ClusterNodeName = current.ClusterNodeName
			}
			if location.CacheEndpoint == "" {
				location.CacheEndpoint = current.CacheEndpoint
			}
			if location.LastSeenAt == nil {
				location.LastSeenAt = current.LastSeenAt
			}
			location.UpdatedAt = now
			state.ImageLocations[index] = location
			out = location
			return nil
		}
		location.ID = model.NewID("imgloc")
		location.CreatedAt = now
		location.UpdatedAt = now
		if location.LastSeenAt == nil {
			location.LastSeenAt = &now
		}
		state.ImageLocations = append(state.ImageLocations, location)
		out = location
		return nil
	})
	return out, err
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
	if s.usingDatabase() {
		return s.pgListImageLocations(filter)
	}
	out := []model.ImageLocation{}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, location := range state.ImageLocations {
			if imageLocationMatchesFilter(location, filter) {
				out = append(out, location)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		leftSeen, rightSeen := imageLocationSeenAt(out[i]), imageLocationSeenAt(out[j])
		if !leftSeen.Equal(rightSeen) {
			return leftSeen.After(rightSeen)
		}
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out, nil
}

func normalizeImageLocation(location model.ImageLocation) model.ImageLocation {
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

func findImageLocation(state *model.State, location model.ImageLocation) int {
	if strings.TrimSpace(location.ID) != "" {
		for idx, existing := range state.ImageLocations {
			if strings.TrimSpace(existing.ID) == strings.TrimSpace(location.ID) {
				return idx
			}
		}
	}
	for idx, existing := range state.ImageLocations {
		if strings.TrimSpace(existing.ImageRef) != location.ImageRef {
			continue
		}
		if normalizeImageDigest(existing.Digest) != location.Digest {
			continue
		}
		if strings.TrimSpace(existing.NodeID) != location.NodeID {
			continue
		}
		if strings.TrimSpace(existing.RuntimeID) != location.RuntimeID {
			continue
		}
		if strings.TrimSpace(existing.ClusterNodeName) != location.ClusterNodeName {
			continue
		}
		return idx
	}
	return -1
}

func imageLocationMatchesFilter(location model.ImageLocation, filter model.ImageLocationFilter) bool {
	if !filter.PlatformAdmin && strings.TrimSpace(location.TenantID) != filter.TenantID {
		return false
	}
	if filter.AppID != "" && strings.TrimSpace(location.AppID) != filter.AppID {
		return false
	}
	if filter.ImageRef != "" && strings.TrimSpace(location.ImageRef) != filter.ImageRef {
		return false
	}
	if filter.Digest != "" && normalizeImageDigest(location.Digest) != filter.Digest {
		return false
	}
	if filter.Status != "" && strings.TrimSpace(location.Status) != filter.Status {
		return false
	}
	if filter.NodeID != "" && strings.TrimSpace(location.NodeID) != filter.NodeID {
		return false
	}
	if filter.RuntimeID != "" && strings.TrimSpace(location.RuntimeID) != filter.RuntimeID {
		return false
	}
	if filter.ClusterNodeName != "" && strings.TrimSpace(location.ClusterNodeName) != filter.ClusterNodeName {
		return false
	}
	return true
}

func imageLocationSeenAt(location model.ImageLocation) time.Time {
	if location.LastSeenAt != nil {
		return *location.LastSeenAt
	}
	return location.UpdatedAt
}
