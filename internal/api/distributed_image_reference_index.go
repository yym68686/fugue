package api

import (
	"sort"
	"strings"

	"fugue/internal/model"
)

// Positions, rather than row IDs, preserve duplicate evidence and its original
// order. Each reference is parsed once per snapshot instead of per candidate.
type distributedImageReferenceIndex map[string]map[string][]int

func (index distributedImageReferenceIndex) add(appID string, position int, ref, digest string) {
	if index[appID] == nil {
		index[appID] = make(map[string][]int)
	}
	for _, key := range distributedImageCandidateKeys(appImageCandidate{ImageRef: ref}, digest) {
		index[appID][key] = append(index[appID][key], position)
	}
}

func (index distributedImageReferenceIndex) matches(appID string, keys []string) []int {
	seen := make(map[int]struct{})
	for _, key := range keys {
		for _, position := range index[appID][key] {
			seen[position] = struct{}{}
		}
	}
	positions := make([]int, 0, len(seen))
	for position := range seen {
		positions = append(positions, position)
	}
	sort.Ints(positions)
	return positions
}

func (e *distributedImageUsageEvidence) buildReferenceIndexes() {
	e.imageReferenceIndex = make(distributedImageReferenceIndex)
	e.locationReferenceIndex = make(distributedImageReferenceIndex)
	e.staleLocationReferenceIndex = make(distributedImageReferenceIndex)
	for appID, images := range e.imagesByAppID {
		for position, image := range images {
			e.imageReferenceIndex.add(appID, position, image.ImageRef, image.CanonicalDigest)
		}
	}
	for appID, locations := range e.locationsByAppID {
		for position, location := range locations {
			e.locationReferenceIndex.add(appID, position, location.ImageRef, location.Digest)
		}
	}
	for appID, locations := range e.staleLocationsByAppID {
		for position, location := range locations {
			e.staleLocationReferenceIndex.add(appID, position, location.ImageRef, location.Digest)
		}
	}
}

func distributedImageLocationsForCandidateWithReferences(appID string, candidate appImageCandidate, byAppID map[string][]model.ImageLocation, index distributedImageReferenceIndex) []model.ImageLocation {
	if index == nil {
		return distributedImageLocationsForCandidateFromIndex(appID, candidate, byAppID)
	}
	locations := make([]model.ImageLocation, 0)
	for _, position := range index.matches(appID, distributedImageCandidateKeys(candidate, "")) {
		location := byAppID[appID][position]
		if strings.TrimSpace(location.Status) == model.ImageLocationStatusPresent {
			locations = append(locations, location)
		}
	}
	return locations
}
