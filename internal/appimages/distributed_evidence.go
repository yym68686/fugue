package appimages

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/imagecacheevidence"
	"fugue/internal/imagecachekeys"
	"fugue/internal/model"
)

const (
	ProjectImageUsageMeasurementComplete           = "complete"
	ProjectImageUsageMeasurementPartial            = "partial"
	ProjectImageUsageMeasurementUnavailable        = "unavailable"
	ProjectImageUsageReasonDigestConflict          = "digest_conflict"
	ProjectImageUsageReasonSizeConflict            = "size_conflict"
	ProjectImageUsageReasonStaleInventory          = "stale_inventory"
	ProjectImageUsageReasonMissingManifestEvidence = "missing_manifest_evidence"
	ProjectImageUsageReasonMissingSizeEvidence     = "missing_size_evidence"
	ProjectImageUsageReasonMissingManifestSize     = "missing_manifest_size_evidence"
	ProjectImageUsageReasonMissingBlobSize         = "missing_blob_size_evidence"
	ProjectImageUsageReasonMissingChildManifest    = "missing_child_manifest"
	ProjectImageUsageReasonMissingBlob             = "missing_blob"
	ProjectImageUsageReasonNoStorageEvidence       = "no_storage_evidence"
	ProjectImageUsageReasonRegistryNotConfigured   = "registry_not_configured"
)

type Candidate struct {
	ImageRef        string
	RuntimeImageRef string
	Source          model.AppSource
	Current         bool
	LastDeployedAt  *time.Time
}

type DistributedEvidence struct {
	imagesByAppID               map[string][]model.Image
	locationsByAppID            map[string][]model.ImageLocation
	staleLocationsByAppID       map[string][]model.ImageLocation
	manifestsByKey              map[string][]model.ImageCacheManifest
	imageReferenceIndex         distributedImageReferenceIndex
	locationReferenceIndex      distributedImageReferenceIndex
	staleLocationReferenceIndex distributedImageReferenceIndex
	staleManifestsByKey         map[string][]model.ImageCacheManifest
	observedAt                  time.Time
	manifestObservedAt          time.Time
}

type distributedImageCandidateMeasurement struct {
	locations           []model.ImageLocation
	manifests           []model.ImageCacheManifest
	manifest            model.ImageCacheManifest
	hasManifest         bool
	hadFreshEvidence    bool
	staleEvidence       bool
	digest              string
	sizeBytes           int64
	hasSize             bool
	complete            bool
	digestConflict      bool
	sizeConflict        bool
	graphFailureReasons []string
	reasons             []string
}

type distributedImageManifestConflict struct {
	digest bool
	size   bool
}

// BuildDistributedEvidence indexes one read-only observation of authorized
// app IDs. Manifest evidence is shared; image and location rows are restricted
// to the supplied app set before any measurement is calculated.
func BuildDistributedEvidence(appIDs map[string]struct{}, images []model.Image, locations []model.ImageLocation, manifests []model.ImageCacheManifest, cutoff time.Time) DistributedEvidence {
	evidence := DistributedEvidence{
		imagesByAppID:         make(map[string][]model.Image),
		locationsByAppID:      make(map[string][]model.ImageLocation),
		staleLocationsByAppID: make(map[string][]model.ImageLocation),
		manifestsByKey:        make(map[string][]model.ImageCacheManifest),
		staleManifestsByKey:   make(map[string][]model.ImageCacheManifest),
	}
	for _, image := range images {
		if _, ok := appIDs[strings.TrimSpace(image.AppID)]; !ok {
			continue
		}
		evidence.imagesByAppID[image.AppID] = append(evidence.imagesByAppID[image.AppID], image)
	}

	for _, location := range locations {
		if _, ok := appIDs[strings.TrimSpace(location.AppID)]; !ok {
			continue
		}
		if distributedImageLocationIsFresh(location, cutoff) {
			evidence.locationsByAppID[location.AppID] = append(evidence.locationsByAppID[location.AppID], location)
			if observed := distributedImageLocationObservedAt(location); observed.After(evidence.observedAt) {
				evidence.observedAt = observed
			}
		} else {
			evidence.staleLocationsByAppID[location.AppID] = append(evidence.staleLocationsByAppID[location.AppID], location)
		}
	}

	for _, manifest := range manifests {
		index := evidence.staleManifestsByKey
		if distributedImageManifestIsFresh(manifest, cutoff) {
			index = evidence.manifestsByKey
			if manifest.LastSeenAt.After(evidence.manifestObservedAt) {
				evidence.manifestObservedAt = manifest.LastSeenAt.UTC()
			}
			if manifest.LastSeenAt.After(evidence.observedAt) {
				evidence.observedAt = manifest.LastSeenAt.UTC()
			}
		}
		for _, key := range distributedImageManifestKeys(manifest) {
			index[key] = append(index[key], manifest)
		}
	}
	evidence.buildReferenceIndexes()

	return evidence
}

func (e DistributedEvidence) ObservedAt() time.Time { return e.observedAt }

// ObservedAtForApp includes globally observed manifest evidence and only
// fresh location observations belonging to this app.
func (e DistributedEvidence) ObservedAtForApp(appID string) time.Time {
	observed := e.manifestObservedAt
	for _, location := range e.locationsByAppID[appID] {
		if at := distributedImageLocationObservedAt(location); at.After(observed) {
			observed = at
		}
	}
	return observed
}

// DistributedMeasurement is the response-facing result. The snapshot's
// indexes, evidence rows and conflict bookkeeping stay private to this domain.
type DistributedMeasurement struct {
	Digest           string
	SizeBytes        int64
	HasSize          bool
	Available        bool
	HadFreshEvidence bool
	Status           string
	Reasons          []string
}

func (e DistributedEvidence) Measure(appID string, candidate Candidate) DistributedMeasurement {
	result := distributedImageCandidateMeasurementFor(model.App{ID: appID}, candidate, e)
	return DistributedMeasurement{
		Digest:           result.digest,
		SizeBytes:        result.sizeBytes,
		HasSize:          result.hasSize,
		Available:        result.hasEvidence(),
		HadFreshEvidence: result.hadFreshEvidence,
		Status:           distributedMeasurementStatus(result),
		Reasons:          result.reasons,
	}
}

func (m DistributedMeasurement) SizeKey(candidate Candidate) string {
	return distributedImageUsageSizeKey(candidate, m.Digest)
}

func ManagedImageDigest(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	index := strings.LastIndex(value, "sha256:")
	if index < 0 {
		return ""
	}
	digest := value[index:]
	if len(digest) != len("sha256:")+64 {
		return ""
	}
	for _, r := range digest[len("sha256:"):] {
		if r < '0' || r > '9' && r < 'a' || r > 'f' {
			return ""
		}
	}
	return digest
}

func (m distributedImageCandidateMeasurement) hasEvidence() bool {
	return m.hasManifest || len(m.locations) > 0
}

func distributedImageCandidateMeasurementFor(
	app model.App,
	candidate Candidate,
	evidence DistributedEvidence,
) distributedImageCandidateMeasurement {
	locations := distributedImageLocationsForCandidate(app.ID, candidate, evidence)
	manifests := distributedImageManifestsForCandidate(candidate, evidence)
	completeManifests := make([]model.ImageCacheManifest, 0, len(manifests))
	graphFailureReasons := []string(nil)
	for _, manifest := range manifests {
		if imagecacheevidence.GraphIsIncomplete(manifest.GraphStatus) {
			if reason := distributedImageGraphFailureMeasurementReason(manifest.GraphFailureReason); reason != "" {
				graphFailureReasons = append(graphFailureReasons, reason)
			}
			continue
		}
		completeManifests = append(completeManifests, manifest)
	}
	measurement := distributedImageCandidateMeasurement{
		locations:           locations,
		manifests:           completeManifests,
		hadFreshEvidence:    len(locations) > 0 || len(manifests) > 0,
		graphFailureReasons: MergeMeasurementReasons(graphFailureReasons),
		staleEvidence: len(distributedImageLocationsForCandidateWithReferences(
			app.ID,
			candidate,
			evidence.staleLocationsByAppID,
			evidence.staleLocationReferenceIndex,
		)) > 0 || len(distributedImageManifestsForCandidateFromIndex(
			candidate,
			evidence.staleManifestsByKey,
		)) > 0,
	}
	manifests = completeManifests
	if image, ok := distributedImageForCandidate(app.ID, candidate, evidence); ok {
		measurement.digest = strings.TrimSpace(image.CanonicalDigest)
	}

	expectedDigest := firstNonEmptyDistributedDigest(measurement.digest, ManagedImageDigest(candidate.ImageRef))
	if expectedDigest != "" {
		matchingLocations := make([]model.ImageLocation, 0, len(measurement.locations))
		for _, location := range measurement.locations {
			locationDigest := firstNonEmptyDistributedDigest(location.Digest, ManagedImageDigest(location.ImageRef))
			switch {
			case locationDigest == "":
				// A location without digest evidence cannot corroborate or
				// contradict the canonical digest.
			case strings.EqualFold(locationDigest, expectedDigest):
				matchingLocations = append(matchingLocations, location)
			default:
				measurement.digestConflict = true
			}
		}
		measurement.locations = matchingLocations

		matched := make([]model.ImageCacheManifest, 0, len(manifests))
		for _, manifest := range manifests {
			manifestDigest := strings.TrimSpace(manifest.Digest)
			switch {
			case manifestDigest == "":
				// A manifest without digest evidence cannot corroborate or
				// contradict the canonical digest.
			case strings.EqualFold(manifestDigest, expectedDigest):
				matched = append(matched, manifest)
			default:
				measurement.digestConflict = true
			}
		}
		if len(matched) > 0 {
			manifests = matched
		} else if len(manifests) > 0 {
			measurement.digestConflict = true
			manifests = nil
		}
	} else if len(measurement.locations) > 0 {
		filtered := make([]model.ImageCacheManifest, 0, len(manifests))
		hadComparableLocation := false
		for _, location := range measurement.locations {
			for _, manifest := range manifests {
				if distributedImageLocationMatchesManifest(location, manifest) {
					filtered = appendUniqueDistributedManifest(filtered, manifest)
				}
				if distributedLocationHasComparableIdentity(location, manifest) {
					hadComparableLocation = true
				}
			}
		}
		if len(filtered) > 0 || hadComparableLocation {
			manifests = filtered
		}
	}
	measurement.manifests = manifests
	if selected, ok, conflict := selectDistributedManifest(manifests); ok {
		measurement.manifest = selected
		measurement.hasManifest = true
		if digest := strings.TrimSpace(selected.Digest); digest != "" {
			measurement.digest = digest
		}
		if size, complete := distributedManifestSize(selected); size > 0 {
			measurement.sizeBytes = size
			measurement.hasSize = true
			measurement.complete = complete
		}
		measurement.digestConflict = measurement.digestConflict || conflict.digest
		measurement.sizeConflict = measurement.sizeConflict || conflict.size
	}
	if measurement.digestConflict || measurement.sizeConflict {
		measurement.complete = false
		measurement.hasSize = false
		measurement.sizeBytes = 0
		if expectedDigest == "" {
			// Different cache nodes reported different content for an unpinned tag.
			// Do not expose one arbitrary node's size as an exact measurement.
			measurement.digest = ""
		}
	}
	if len(measurement.graphFailureReasons) > 0 {
		measurement.complete = false
	}
	measurement.reasons = distributedImageMeasurementReasons(measurement)
	return measurement
}

func distributedImageGraphFailureMeasurementReason(reason string) string {
	switch imagecacheevidence.NormalizeGraphFailureReason(reason) {
	case imagecacheevidence.ReasonMissingChildManifest:
		return ProjectImageUsageReasonMissingChildManifest
	case imagecacheevidence.ReasonMissingBlob:
		return ProjectImageUsageReasonMissingBlob
	default:
		return ""
	}
}

func distributedImageForCandidate(appID string, candidate Candidate, evidence DistributedEvidence) (model.Image, bool) {
	keys := distributedImageCandidateKeys(candidate, "")
	var best model.Image
	found := false
	images := evidence.imagesByAppID[appID]
	if evidence.imageReferenceIndex != nil {
		matches := make([]model.Image, 0)
		for _, position := range evidence.imageReferenceIndex.matches(appID, keys) {
			matches = append(matches, images[position])
		}
		images = matches
	}
	for _, image := range images {
		if image.LifecycleState != model.ImageLifecycleAvailable {
			continue
		}
		if evidence.imageReferenceIndex == nil && !distributedReferenceSetsIntersect(keys, distributedImageCandidateKeys(Candidate{ImageRef: image.ImageRef}, image.CanonicalDigest)) {
			continue
		}
		if !found || image.UpdatedAt.After(best.UpdatedAt) {
			best = image
			found = true
		}
	}
	return best, found
}

func distributedImageLocationsForCandidate(appID string, candidate Candidate, evidence DistributedEvidence) []model.ImageLocation {
	return distributedImageLocationsForCandidateWithReferences(appID, candidate, evidence.locationsByAppID, evidence.locationReferenceIndex)
}

func distributedImageLocationsForCandidateFromIndex(
	appID string,
	candidate Candidate,
	locationsByAppID map[string][]model.ImageLocation,
) []model.ImageLocation {
	keys := distributedImageCandidateKeys(candidate, "")
	locations := make([]model.ImageLocation, 0)
	for _, location := range locationsByAppID[appID] {
		if strings.TrimSpace(location.Status) != model.ImageLocationStatusPresent {
			continue
		}
		locationKeys := distributedImageCandidateKeys(Candidate{ImageRef: location.ImageRef}, location.Digest)
		if distributedReferenceSetsIntersect(keys, locationKeys) {
			locations = append(locations, location)
		}
	}
	return locations
}

func distributedImageManifestsForCandidate(candidate Candidate, evidence DistributedEvidence) []model.ImageCacheManifest {
	return distributedImageManifestsForCandidateFromIndex(candidate, evidence.manifestsByKey)
}

func distributedImageManifestsForCandidateFromIndex(
	candidate Candidate,
	manifestsByKey map[string][]model.ImageCacheManifest,
) []model.ImageCacheManifest {
	keys := distributedImageCandidateKeys(candidate, "")
	seen := map[string]struct{}{}
	out := make([]model.ImageCacheManifest, 0)
	for _, key := range keys {
		for _, manifest := range manifestsByKey[key] {
			identity := distributedImageManifestIdentity(manifest)
			if _, ok := seen[identity]; ok {
				continue
			}
			seen[identity] = struct{}{}
			out = append(out, manifest)
		}
	}
	return out
}

func selectDistributedManifest(manifests []model.ImageCacheManifest) (model.ImageCacheManifest, bool, distributedImageManifestConflict) {
	if len(manifests) == 0 {
		return model.ImageCacheManifest{}, false, distributedImageManifestConflict{}
	}
	digests := map[string]struct{}{}
	completeSizes := map[int64]struct{}{}
	for _, manifest := range manifests {
		if digest := strings.ToLower(strings.TrimSpace(manifest.Digest)); digest != "" {
			digests[digest] = struct{}{}
		}
		if size, complete := distributedManifestSize(manifest); complete {
			completeSizes[size] = struct{}{}
		}
	}
	conflict := distributedImageManifestConflict{
		digest: len(digests) > 1,
		size:   len(digests) <= 1 && len(completeSizes) > 1,
	}
	sort.SliceStable(manifests, func(i, j int) bool {
		leftSize, _ := distributedManifestSize(manifests[i])
		rightSize, _ := distributedManifestSize(manifests[j])
		if leftSize != rightSize {
			return leftSize > rightSize
		}
		return manifests[i].LastSeenAt.After(manifests[j].LastSeenAt)
	})
	return manifests[0], true, conflict
}

func distributedManifestSize(manifest model.ImageCacheManifest) (int64, bool) {
	manifestBytes := manifest.ManifestSizeBytes
	blobBytes := manifest.TotalBlobBytes
	if manifestBytes < 0 {
		manifestBytes = 0
	}
	if blobBytes < 0 {
		blobBytes = 0
	}
	if manifestBytes == 0 && blobBytes == 0 {
		return 0, false
	}
	return manifestBytes + blobBytes, manifestBytes > 0 && blobBytes > 0
}

func distributedMeasurementStatus(measurement distributedImageCandidateMeasurement) string {
	if !measurement.hasSize {
		return ProjectImageUsageMeasurementUnavailable
	}
	if measurement.complete && !measurement.digestConflict && !measurement.sizeConflict {
		return ProjectImageUsageMeasurementComplete
	}
	return ProjectImageUsageMeasurementPartial
}

func distributedImageMeasurementReasons(measurement distributedImageCandidateMeasurement) []string {
	reasons := append([]string(nil), measurement.graphFailureReasons...)
	if measurement.digestConflict {
		reasons = append(reasons, ProjectImageUsageReasonDigestConflict)
	}
	if measurement.sizeConflict {
		reasons = append(reasons, ProjectImageUsageReasonSizeConflict)
	}
	if measurement.digestConflict || measurement.sizeConflict {
		return MergeMeasurementReasons(reasons)
	}
	if !measurement.hasManifest {
		if len(reasons) == 0 {
			switch {
			case len(measurement.locations) > 0 || measurement.hadFreshEvidence:
				reasons = append(reasons, ProjectImageUsageReasonMissingManifestEvidence)
			case measurement.staleEvidence:
				reasons = append(reasons, ProjectImageUsageReasonStaleInventory)
			default:
				reasons = append(reasons, ProjectImageUsageReasonNoStorageEvidence)
			}
		}
		return MergeMeasurementReasons(reasons)
	}
	if measurement.manifest.ManifestSizeBytes <= 0 {
		reasons = append(reasons, ProjectImageUsageReasonMissingManifestSize)
	}
	if measurement.manifest.TotalBlobBytes <= 0 {
		reasons = append(reasons, ProjectImageUsageReasonMissingBlobSize)
	}
	if !measurement.hasSize && len(reasons) == 0 {
		reasons = append(reasons, ProjectImageUsageReasonMissingSizeEvidence)
	}
	return MergeMeasurementReasons(reasons)
}

func distributedImageUsageSizeKey(candidate Candidate, digest string) string {
	if digest = strings.ToLower(strings.TrimSpace(digest)); digest != "" {
		return "digest:" + digest
	}
	if ref := strings.ToLower(strings.TrimSpace(candidate.ImageRef)); ref != "" {
		return "ref:" + ref
	}
	return ""
}

func distributedImageCandidateKeys(candidate Candidate, digest string) []string {
	keys := append([]string{}, distributedImageUsageKeys(candidate.ImageRef, digest)...)
	if distributedRuntimeImageRefMatchesCandidate(candidate.ImageRef, candidate.RuntimeImageRef) {
		keys = append(keys, distributedImageUsageKeys(candidate.RuntimeImageRef, digest)...)
	}
	return uniqueDistributedImageKeys(keys)
}

type distributedImageReferenceIdentity struct {
	repository string
	tag        string
	digest     string
}

// Historical operations can contain a DesiredSpec image from a different
// generation than DesiredSource. Treat the runtime ref as an alias only when
// both references themselves prove the same repository and tag or digest.
func distributedRuntimeImageRefMatchesCandidate(imageRef, runtimeImageRef string) bool {
	imageIdentity, imageOK := parseDistributedImageReferenceIdentity(imageRef)
	runtimeIdentity, runtimeOK := parseDistributedImageReferenceIdentity(runtimeImageRef)
	if !imageOK || !runtimeOK || imageIdentity.repository != runtimeIdentity.repository {
		return false
	}
	if imageIdentity.digest != "" && runtimeIdentity.digest != "" {
		return imageIdentity.digest == runtimeIdentity.digest
	}
	if imageIdentity.tag != "" && runtimeIdentity.tag != "" {
		return imageIdentity.tag == runtimeIdentity.tag
	}
	return false
}

func parseDistributedImageReferenceIdentity(ref string) (distributedImageReferenceIdentity, bool) {
	ref = strings.ToLower(strings.TrimSpace(ref))
	ref = strings.TrimPrefix(ref, "http://")
	ref = strings.TrimPrefix(ref, "https://")
	if ref == "" || strings.ContainsAny(ref, " ,\t\r\n") {
		return distributedImageReferenceIdentity{}, false
	}
	ref = imagecachekeys.StripRegistry(ref)
	if ref == "" {
		return distributedImageReferenceIdentity{}, false
	}

	identity := distributedImageReferenceIdentity{}
	name := ref
	if before, after, found := strings.Cut(name, "@"); found {
		if strings.Contains(after, "@") {
			return distributedImageReferenceIdentity{}, false
		}
		identity.digest = ManagedImageDigest(after)
		if identity.digest == "" || identity.digest != strings.TrimSpace(after) {
			return distributedImageReferenceIdentity{}, false
		}
		name = before
	} else if index := strings.LastIndex(name, ":sha256:"); index > strings.LastIndex(name, "/") {
		digest := name[index+1:]
		identity.digest = ManagedImageDigest(digest)
		if identity.digest == "" || identity.digest != digest {
			return distributedImageReferenceIdentity{}, false
		}
		name = name[:index]
	}

	lastSlash := strings.LastIndex(name, "/")
	if lastColon := strings.LastIndex(name, ":"); lastColon > lastSlash {
		if lastColon+1 == len(name) {
			return distributedImageReferenceIdentity{}, false
		}
		identity.tag = strings.TrimSpace(name[lastColon+1:])
		name = name[:lastColon]
	}
	identity.repository = strings.Trim(name, "/")
	if identity.repository == "" || strings.ContainsAny(identity.repository, "@:") || strings.ContainsAny(identity.tag, "@:/") {
		return distributedImageReferenceIdentity{}, false
	}
	if identity.tag == "" && identity.digest == "" {
		identity.tag = "latest"
	}
	return identity, true
}

func distributedImageUsageKeys(ref, digest string) []string {
	out := []string{}
	for _, key := range imagecachekeys.ExactImageReferenceKeys(ref, digest) {
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" || (!strings.Contains(key, "/") && !strings.HasPrefix(key, "sha256:")) {
			continue
		}
		out = append(out, key)
	}
	return uniqueDistributedImageKeys(out)
}

func distributedImageManifestKeys(manifest model.ImageCacheManifest) []string {
	out := []string{}
	for _, key := range imagecachekeys.ExactManifestReferenceKeys(manifest.Repo, manifest.Target, manifest.Digest, manifest.ImageRef) {
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" || (!strings.Contains(key, "/") && !strings.HasPrefix(key, "sha256:")) {
			continue
		}
		out = append(out, key)
	}
	return uniqueDistributedImageKeys(out)
}

func uniqueDistributedImageKeys(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func distributedReferenceSetsIntersect(left, right []string) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(left))
	for _, value := range left {
		set[value] = struct{}{}
	}
	for _, value := range right {
		if _, ok := set[value]; ok {
			return true
		}
	}
	return false
}

func distributedImageManifestIdentity(manifest model.ImageCacheManifest) string {
	return strings.Join([]string{
		strings.TrimSpace(manifest.NodeID),
		strings.TrimSpace(manifest.ClusterNodeName),
		strings.TrimSpace(manifest.RuntimeID),
		strings.TrimSpace(manifest.Repo),
		strings.TrimSpace(manifest.Target),
		strings.TrimSpace(manifest.Digest),
	}, "\x00")
}

func appendUniqueDistributedManifest(out []model.ImageCacheManifest, manifest model.ImageCacheManifest) []model.ImageCacheManifest {
	identity := distributedImageManifestIdentity(manifest)
	for _, existing := range out {
		if distributedImageManifestIdentity(existing) == identity {
			return out
		}
	}
	return append(out, manifest)
}

func distributedImageManifestIsFresh(manifest model.ImageCacheManifest, cutoff time.Time) bool {
	return !manifest.LastSeenAt.IsZero() && !manifest.LastSeenAt.Before(cutoff)
}

func distributedImageLocationObservedAt(location model.ImageLocation) time.Time {
	if location.LastSeenAt != nil && !location.LastSeenAt.IsZero() {
		return location.LastSeenAt.UTC()
	}
	return location.UpdatedAt.UTC()
}

func distributedImageLocationIsFresh(location model.ImageLocation, cutoff time.Time) bool {
	observed := distributedImageLocationObservedAt(location)
	return !observed.IsZero() && !observed.Before(cutoff)
}

func distributedLocationHasComparableIdentity(location model.ImageLocation, manifest model.ImageCacheManifest) bool {
	return (strings.TrimSpace(location.NodeID) != "" && strings.TrimSpace(manifest.NodeID) != "") ||
		(strings.TrimSpace(location.ClusterNodeName) != "" && strings.TrimSpace(manifest.ClusterNodeName) != "") ||
		(strings.TrimSpace(location.RuntimeID) != "" && strings.TrimSpace(manifest.RuntimeID) != "") ||
		(firstNonEmptyDistributedDigest(location.Digest, ManagedImageDigest(location.ImageRef)) != "" && strings.TrimSpace(manifest.Digest) != "")
}

func distributedImageLocationMatchesManifest(location model.ImageLocation, manifest model.ImageCacheManifest) bool {
	locationDigest := firstNonEmptyDistributedDigest(location.Digest, ManagedImageDigest(location.ImageRef))
	manifestDigest := firstNonEmptyDistributedDigest(manifest.Digest, ManagedImageDigest(manifest.ImageRef))
	if locationDigest != "" && manifestDigest != "" && !strings.EqualFold(locationDigest, manifestDigest) {
		return false
	}
	if location.NodeID != "" && manifest.NodeID != "" && strings.TrimSpace(location.NodeID) != strings.TrimSpace(manifest.NodeID) {
		return false
	}
	if location.ClusterNodeName != "" && manifest.ClusterNodeName != "" && strings.TrimSpace(location.ClusterNodeName) != strings.TrimSpace(manifest.ClusterNodeName) {
		return false
	}
	if location.RuntimeID != "" && manifest.RuntimeID != "" && strings.TrimSpace(location.RuntimeID) != strings.TrimSpace(manifest.RuntimeID) {
		return false
	}
	return true
}

func CombineMeasurementStatus(left, right string) string {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" {
		return right
	}
	if right == "" || left == right {
		return left
	}
	if left == ProjectImageUsageMeasurementUnavailable && right == ProjectImageUsageMeasurementUnavailable {
		return ProjectImageUsageMeasurementUnavailable
	}
	if left == ProjectImageUsageMeasurementComplete && right == ProjectImageUsageMeasurementComplete {
		return ProjectImageUsageMeasurementComplete
	}
	return ProjectImageUsageMeasurementPartial
}

func MergeMeasurementReasons(groups ...[]string) []string {
	seen := map[string]struct{}{}
	for _, group := range groups {
		for _, reason := range group {
			reason = strings.TrimSpace(reason)
			if reason != "" {
				seen[reason] = struct{}{}
			}
		}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]string, 0, len(seen))
	for reason := range seen {
		out = append(out, reason)
	}
	sort.Strings(out)
	return out
}

func firstNonEmptyDistributedDigest(values ...string) string {
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if strings.HasPrefix(value, "sha256:") {
			return value
		}
	}
	return ""
}
