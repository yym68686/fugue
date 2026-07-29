package api

import (
	"context"
	"sort"
	"strings"
	"time"

	"fugue/internal/imagecachekeys"
	"fugue/internal/model"

	"golang.org/x/sync/errgroup"
)

const distributedImageUsageMeasurementNote = "distributed image sizes are derived from fresh image-cache manifests; totals are logical image payloads and generations without complete blob evidence are marked partial"

const distributedImageReclaimNote = "distributed cache retention is automatic; per-version deletion and exact reclaimable-byte reporting are not supported"

type projectImageUsageInventoryResult struct {
	App       model.App
	Inventory builtAppImageInventory
}

type distributedImageUsageEvidence struct {
	imagesByAppID         map[string][]model.Image
	locationsByAppID      map[string][]model.ImageLocation
	staleLocationsByAppID map[string][]model.ImageLocation
	manifestsByKey        map[string][]model.ImageCacheManifest
	staleManifestsByKey   map[string][]model.ImageCacheManifest
	observedAt            time.Time
}

type distributedImageCandidateMeasurement struct {
	locations        []model.ImageLocation
	manifests        []model.ImageCacheManifest
	manifest         model.ImageCacheManifest
	hasManifest      bool
	hadFreshEvidence bool
	staleEvidence    bool
	digest           string
	sizeBytes        int64
	hasSize          bool
	complete         bool
	digestConflict   bool
	sizeConflict     bool
	reasons          []string
}

type distributedImageManifestConflict struct {
	digest bool
	size   bool
}

func projectImageUsageStoreMode(s *Server) string {
	if s != nil && strings.EqualFold(strings.TrimSpace(s.imageStoreMode), "distributed") {
		return projectImageUsageModeDistributed
	}
	if s != nil && s.appImageInventoryConfigured() {
		return projectImageUsageModeRegistry
	}
	return ""
}

func (s *Server) loadDistributedImageUsageEvidence(ctx context.Context, apps []model.App) (distributedImageUsageEvidence, error) {
	_ = ctx
	evidence := distributedImageUsageEvidence{
		imagesByAppID:         make(map[string][]model.Image),
		locationsByAppID:      make(map[string][]model.ImageLocation),
		staleLocationsByAppID: make(map[string][]model.ImageLocation),
		manifestsByKey:        make(map[string][]model.ImageCacheManifest),
		staleManifestsByKey:   make(map[string][]model.ImageCacheManifest),
	}
	appIDs := make(map[string]struct{}, len(apps))
	for _, app := range apps {
		if id := strings.TrimSpace(app.ID); id != "" {
			appIDs[id] = struct{}{}
		}
	}
	if len(appIDs) == 0 || s == nil || s.store == nil {
		return evidence, nil
	}

	images, err := s.store.ListImages(model.ImageFilter{PlatformAdmin: true})
	if err != nil {
		return distributedImageUsageEvidence{}, err
	}
	for _, image := range images {
		if _, ok := appIDs[strings.TrimSpace(image.AppID)]; !ok {
			continue
		}
		evidence.imagesByAppID[image.AppID] = append(evidence.imagesByAppID[image.AppID], image)
	}

	cutoff := time.Now().UTC().Add(-defaultImageCacheInventoryTTL)
	locations, err := s.store.ListImageLocations(model.ImageLocationFilter{
		Status:        model.ImageLocationStatusPresent,
		PlatformAdmin: true,
	})
	if err != nil {
		return distributedImageUsageEvidence{}, err
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

	manifests, err := s.store.ListImageCacheManifests(model.ImageCacheManifestFilter{
		PresentOnly: true,
	})
	if err != nil {
		return distributedImageUsageEvidence{}, err
	}
	for _, manifest := range manifests {
		index := evidence.staleManifestsByKey
		if distributedImageManifestIsFresh(manifest, cutoff) {
			index = evidence.manifestsByKey
			if manifest.LastSeenAt.After(evidence.observedAt) {
				evidence.observedAt = manifest.LastSeenAt.UTC()
			}
		}
		for _, key := range distributedImageManifestKeys(manifest) {
			index[key] = append(index[key], manifest)
		}
	}
	return evidence, nil
}

func aggregateProjectImageUsageInventories(
	response projectImageUsageResponse,
	inventoryResults []projectImageUsageInventoryResult,
) projectImageUsageResponse {
	projectSummaries := make(map[string]*projectImageUsageAccumulator)
	for _, result := range inventoryResults {
		app := result.App
		inventory := result.Inventory
		if inventory.Response.Summary.VersionCount == 0 {
			continue
		}

		projectID := normalizedProjectIDForImageInventory(app.ProjectID)
		accumulator := projectSummaries[projectID]
		if accumulator == nil {
			accumulator = &projectImageUsageAccumulator{
				Summary: projectImageUsageSummary{
					ProjectID:          projectID,
					MeasurementStatus:  inventory.Response.MeasurementStatus,
					MeasurementReasons: append([]string(nil), inventory.Response.MeasurementReasons...),
				},
				TotalBlobSizes:       make(map[string]int64),
				CurrentBlobSizes:     make(map[string]int64),
				StaleBlobSizes:       make(map[string]int64),
				ReclaimableBlobSizes: make(map[string]int64),
			}
			projectSummaries[projectID] = accumulator
		}

		accumulator.Summary.VersionCount += inventory.Response.Summary.VersionCount
		accumulator.Summary.CurrentVersionCount += inventory.Response.Summary.CurrentVersionCount
		accumulator.Summary.StaleVersionCount += inventory.Response.Summary.StaleVersionCount
		accumulator.Summary.MeasurementStatus = combineProjectImageMeasurementStatus(
			accumulator.Summary.MeasurementStatus,
			inventory.Response.MeasurementStatus,
		)
		accumulator.Summary.MeasurementReasons = mergeProjectImageMeasurementReasons(
			accumulator.Summary.MeasurementReasons,
			inventory.Response.MeasurementReasons,
		)
		accumulator.Summary.Apps = append(accumulator.Summary.Apps, projectImageUsageAppSummary{
			AppID:                app.ID,
			AppName:              app.Name,
			VersionCount:         inventory.Response.Summary.VersionCount,
			CurrentVersionCount:  inventory.Response.Summary.CurrentVersionCount,
			StaleVersionCount:    inventory.Response.Summary.StaleVersionCount,
			TotalSizeBytes:       inventory.Response.Summary.TotalSizeBytes,
			CurrentSizeBytes:     inventory.Response.Summary.CurrentSizeBytes,
			StaleSizeBytes:       inventory.Response.Summary.StaleSizeBytes,
			ReclaimableSizeBytes: inventory.Response.Summary.ReclaimableSizeBytes,
			MeasurementStatus:    inventory.Response.MeasurementStatus,
			MeasurementReasons:   append([]string(nil), inventory.Response.MeasurementReasons...),
		})
		unionAppImageBlobSizes(accumulator.TotalBlobSizes, inventory.TotalBlobSizes)
		unionAppImageBlobSizes(accumulator.CurrentBlobSizes, inventory.CurrentBlobSizes)
		unionAppImageBlobSizes(accumulator.StaleBlobSizes, inventory.StaleBlobSizes)
		unionAppImageBlobSizes(accumulator.ReclaimableBlobSizes, inventory.ReclaimableBlobSizes)
	}

	projectIDs := make([]string, 0, len(projectSummaries))
	for projectID := range projectSummaries {
		projectIDs = append(projectIDs, projectID)
	}
	sort.Strings(projectIDs)
	response.Projects = make([]projectImageUsageSummary, 0, len(projectIDs))
	for _, projectID := range projectIDs {
		accumulator := projectSummaries[projectID]
		sort.Slice(accumulator.Summary.Apps, func(i, j int) bool {
			if accumulator.Summary.Apps[i].AppName == accumulator.Summary.Apps[j].AppName {
				return accumulator.Summary.Apps[i].AppID < accumulator.Summary.Apps[j].AppID
			}
			return accumulator.Summary.Apps[i].AppName < accumulator.Summary.Apps[j].AppName
		})
		accumulator.Summary.TotalSizeBytes = sumAppImageBlobSizes(accumulator.TotalBlobSizes)
		accumulator.Summary.CurrentSizeBytes = sumAppImageBlobSizes(accumulator.CurrentBlobSizes)
		accumulator.Summary.StaleSizeBytes = sumAppImageBlobSizes(accumulator.StaleBlobSizes)
		accumulator.Summary.ReclaimableSizeBytes = sumAppImageBlobSizes(accumulator.ReclaimableBlobSizes)
		response.Projects = append(response.Projects, accumulator.Summary)
	}

	if len(response.Projects) == 0 {
		if response.MeasurementStatus == "" {
			response.MeasurementStatus = projectImageUsageMeasurementUnavailable
		}
	} else {
		status := ""
		reasons := []string(nil)
		for _, project := range response.Projects {
			status = combineProjectImageMeasurementStatus(status, project.MeasurementStatus)
			reasons = mergeProjectImageMeasurementReasons(reasons, project.MeasurementReasons)
		}
		response.MeasurementStatus = status
		response.MeasurementReasons = reasons
	}
	return response
}

func (s *Server) buildDistributedProjectImageUsageResponse(
	ctx context.Context,
	apps []model.App,
	opsByAppID map[string][]model.Operation,
) (projectImageUsageResponse, error) {
	response := projectImageUsageResponse{
		RegistryConfigured: false,
		ReclaimNote:        distributedImageReclaimNote,
		ImageStoreMode:     projectImageUsageModeDistributed,
		MeasurementStatus:  projectImageUsageMeasurementComplete,
		MeasurementNote:    distributedImageUsageMeasurementNote,
		Projects:           []projectImageUsageSummary{},
	}
	evidence, err := s.loadDistributedImageUsageEvidence(ctx, apps)
	if err != nil {
		return projectImageUsageResponse{}, err
	}
	if !evidence.observedAt.IsZero() {
		observed := evidence.observedAt.UTC()
		response.ObservedAt = &observed
	}

	inventoryResults := make([]projectImageUsageInventoryResult, len(apps))
	inventoryGroup, _ := errgroup.WithContext(ctx)
	inventoryGroup.SetLimit(projectImageUsageAppBuildLimit)
	for index, app := range apps {
		index, app := index, app
		inventoryGroup.Go(func() error {
			inventoryResults[index] = projectImageUsageInventoryResult{
				App:       app,
				Inventory: s.buildDistributedAppImageInventory(app, opsByAppID[app.ID], evidence),
			}
			return nil
		})
	}
	if err := inventoryGroup.Wait(); err != nil {
		return projectImageUsageResponse{}, err
	}
	return aggregateProjectImageUsageInventories(response, inventoryResults), nil
}

func (s *Server) buildDistributedAppImageInventory(
	app model.App,
	ops []model.Operation,
	evidence distributedImageUsageEvidence,
) builtAppImageInventory {
	inventory := builtAppImageInventory{
		Response: appImageInventoryResponse{
			AppID:              app.ID,
			RegistryConfigured: false,
			ReclaimNote:        distributedImageReclaimNote,
			MeasurementStatus:  projectImageUsageMeasurementUnavailable,
			MeasurementNote:    distributedImageUsageMeasurementNote,
			Versions:           []appImageVersion{},
		},
		VersionByImageRef:    make(map[string]builtAppImageVersion),
		TotalBlobSizes:       make(map[string]int64),
		CurrentBlobSizes:     make(map[string]int64),
		StaleBlobSizes:       make(map[string]int64),
		ReclaimableBlobSizes: make(map[string]int64),
	}
	candidatesByImageRef := s.collectAppImageCandidates(app, ops)
	if len(candidatesByImageRef) == 0 {
		inventory.Response.MeasurementStatus = projectImageUsageMeasurementComplete
		return inventory
	}

	imageRefs := make([]string, 0, len(candidatesByImageRef))
	for imageRef := range candidatesByImageRef {
		imageRefs = append(imageRefs, imageRef)
	}
	sort.Strings(imageRefs)
	keptCandidates := make([]appImageCandidate, 0, len(imageRefs))
	measurements := make(map[string]distributedImageCandidateMeasurement, len(imageRefs))
	for _, imageRef := range imageRefs {
		candidate := candidatesByImageRef[imageRef]
		measurement := distributedImageCandidateMeasurementFor(app, candidate, evidence)
		measurements[imageRef] = measurement
		hasEvidence := measurement.hasEvidence()
		if !hasEvidence && !candidate.Current {
			continue
		}
		keptCandidates = append(keptCandidates, candidate)
		if measurement.hasSize {
			key := distributedImageUsageSizeKey(candidate, measurement.digest)
			if key != "" {
				inventory.TotalBlobSizes[key] = maxInt64(inventory.TotalBlobSizes[key], measurement.sizeBytes)
				if candidate.Current {
					inventory.CurrentBlobSizes[key] = maxInt64(inventory.CurrentBlobSizes[key], measurement.sizeBytes)
				} else {
					inventory.StaleBlobSizes[key] = maxInt64(inventory.StaleBlobSizes[key], measurement.sizeBytes)
				}
			}
		}
	}

	sort.Slice(keptCandidates, func(i, j int) bool {
		if keptCandidates[i].Current != keptCandidates[j].Current {
			return keptCandidates[i].Current
		}
		leftTimestamp := timestampFromPointer(keptCandidates[i].LastDeployedAt)
		rightTimestamp := timestampFromPointer(keptCandidates[j].LastDeployedAt)
		if leftTimestamp != rightTimestamp {
			return leftTimestamp > rightTimestamp
		}
		return keptCandidates[i].ImageRef < keptCandidates[j].ImageRef
	})

	currentCount := 0
	for _, candidate := range keptCandidates {
		if candidate.Current {
			currentCount++
		}
		measurement := measurements[candidate.ImageRef]
		version := appImageVersion{
			ImageRef:               candidate.ImageRef,
			RuntimeImageRef:        candidate.RuntimeImageRef,
			Digest:                 measurement.digest,
			Status:                 appImageStatusMissing,
			Current:                candidate.Current,
			SizeBytes:              measurement.sizeBytes,
			SizeMeasurementStatus:  distributedMeasurementStatus(measurement),
			SizeMeasurementReasons: append([]string(nil), measurement.reasons...),
			DeleteSupported:        false,
			RedeploySupported:      false,
			LastDeployedAt:         cloneTimePointer(candidate.LastDeployedAt),
			Source:                 sanitizeAppSourceForAPI(&candidate.Source),
		}
		if measurement.hasEvidence() {
			version.Status = appImageStatusAvailable
		}
		inventory.Response.Versions = append(inventory.Response.Versions, version)
		inventory.VersionByImageRef[candidate.ImageRef] = builtAppImageVersion{
			Candidate: candidate,
			Response:  version,
		}
	}

	inventory.Response.Summary = appImageSummary{
		VersionCount:         len(keptCandidates),
		CurrentVersionCount:  currentCount,
		StaleVersionCount:    maxInt(len(keptCandidates)-currentCount, 0),
		TotalSizeBytes:       sumAppImageBlobSizes(inventory.TotalBlobSizes),
		CurrentSizeBytes:     sumAppImageBlobSizes(inventory.CurrentBlobSizes),
		StaleSizeBytes:       sumAppImageBlobSizes(inventory.StaleBlobSizes),
		ReclaimableSizeBytes: sumAppImageBlobSizes(inventory.ReclaimableBlobSizes),
	}
	inventory.Response.MeasurementStatus = summarizeAppImageVersionMeasurementStatus(inventory.Response.Versions)
	inventory.Response.MeasurementReasons = summarizeAppImageVersionMeasurementReasons(inventory.Response.Versions)
	return inventory
}

func (m distributedImageCandidateMeasurement) hasEvidence() bool {
	return m.hasManifest || len(m.locations) > 0
}

func distributedImageCandidateMeasurementFor(
	app model.App,
	candidate appImageCandidate,
	evidence distributedImageUsageEvidence,
) distributedImageCandidateMeasurement {
	locations := distributedImageLocationsForCandidate(app.ID, candidate, evidence)
	manifests := distributedImageManifestsForCandidate(candidate, evidence)
	measurement := distributedImageCandidateMeasurement{
		locations:        locations,
		manifests:        manifests,
		hadFreshEvidence: len(locations) > 0 || len(manifests) > 0,
		staleEvidence: len(distributedImageLocationsForCandidateFromIndex(
			app.ID,
			candidate,
			evidence.staleLocationsByAppID,
		)) > 0 || len(distributedImageManifestsForCandidateFromIndex(
			candidate,
			evidence.staleManifestsByKey,
		)) > 0,
	}
	if image, ok := distributedImageForCandidate(app.ID, candidate, evidence); ok {
		measurement.digest = strings.TrimSpace(image.CanonicalDigest)
	}

	expectedDigest := firstNonEmptyDistributedDigest(measurement.digest, managedImageDigest(candidate.ImageRef))
	if expectedDigest != "" {
		matchingLocations := make([]model.ImageLocation, 0, len(measurement.locations))
		for _, location := range measurement.locations {
			locationDigest := firstNonEmptyDistributedDigest(location.Digest, managedImageDigest(location.ImageRef))
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
	measurement.reasons = distributedImageMeasurementReasons(measurement)
	return measurement
}

func distributedImageForCandidate(appID string, candidate appImageCandidate, evidence distributedImageUsageEvidence) (model.Image, bool) {
	keys := distributedImageCandidateKeys(candidate, "")
	var best model.Image
	found := false
	for _, image := range evidence.imagesByAppID[appID] {
		if image.LifecycleState != model.ImageLifecycleAvailable {
			continue
		}
		if !distributedReferenceSetsIntersect(keys, distributedImageCandidateKeys(appImageCandidate{ImageRef: image.ImageRef}, image.CanonicalDigest)) {
			continue
		}
		if !found || image.UpdatedAt.After(best.UpdatedAt) {
			best = image
			found = true
		}
	}
	return best, found
}

func distributedImageLocationsForCandidate(appID string, candidate appImageCandidate, evidence distributedImageUsageEvidence) []model.ImageLocation {
	return distributedImageLocationsForCandidateFromIndex(appID, candidate, evidence.locationsByAppID)
}

func distributedImageLocationsForCandidateFromIndex(
	appID string,
	candidate appImageCandidate,
	locationsByAppID map[string][]model.ImageLocation,
) []model.ImageLocation {
	keys := distributedImageCandidateKeys(candidate, "")
	locations := make([]model.ImageLocation, 0)
	for _, location := range locationsByAppID[appID] {
		if strings.TrimSpace(location.Status) != model.ImageLocationStatusPresent {
			continue
		}
		locationKeys := distributedImageCandidateKeys(appImageCandidate{ImageRef: location.ImageRef}, location.Digest)
		if distributedReferenceSetsIntersect(keys, locationKeys) {
			locations = append(locations, location)
		}
	}
	return locations
}

func distributedImageManifestsForCandidate(candidate appImageCandidate, evidence distributedImageUsageEvidence) []model.ImageCacheManifest {
	return distributedImageManifestsForCandidateFromIndex(candidate, evidence.manifestsByKey)
}

func distributedImageManifestsForCandidateFromIndex(
	candidate appImageCandidate,
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
		return projectImageUsageMeasurementUnavailable
	}
	if measurement.complete && !measurement.digestConflict && !measurement.sizeConflict {
		return projectImageUsageMeasurementComplete
	}
	return projectImageUsageMeasurementPartial
}

func distributedImageMeasurementReasons(measurement distributedImageCandidateMeasurement) []string {
	reasons := []string{}
	if measurement.digestConflict {
		reasons = append(reasons, projectImageUsageReasonDigestConflict)
	}
	if measurement.sizeConflict {
		reasons = append(reasons, projectImageUsageReasonSizeConflict)
	}
	if len(reasons) > 0 {
		return mergeProjectImageMeasurementReasons(reasons)
	}
	if !measurement.hasManifest {
		switch {
		case len(measurement.locations) > 0 || measurement.hadFreshEvidence:
			reasons = append(reasons, projectImageUsageReasonMissingManifestEvidence)
		case measurement.staleEvidence:
			reasons = append(reasons, projectImageUsageReasonStaleInventory)
		default:
			reasons = append(reasons, projectImageUsageReasonNoStorageEvidence)
		}
		return reasons
	}
	if measurement.manifest.ManifestSizeBytes <= 0 {
		reasons = append(reasons, projectImageUsageReasonMissingManifestSize)
	}
	if measurement.manifest.TotalBlobBytes <= 0 {
		reasons = append(reasons, projectImageUsageReasonMissingBlobSize)
	}
	if !measurement.hasSize && len(reasons) == 0 {
		reasons = append(reasons, projectImageUsageReasonMissingSizeEvidence)
	}
	return mergeProjectImageMeasurementReasons(reasons)
}

func distributedImageUsageSizeKey(candidate appImageCandidate, digest string) string {
	if digest = strings.ToLower(strings.TrimSpace(digest)); digest != "" {
		return "digest:" + digest
	}
	if ref := strings.ToLower(strings.TrimSpace(candidate.ImageRef)); ref != "" {
		return "ref:" + ref
	}
	return ""
}

func distributedImageCandidateKeys(candidate appImageCandidate, digest string) []string {
	keys := append([]string{}, distributedImageUsageKeys(candidate.ImageRef, digest)...)
	keys = append(keys, distributedImageUsageKeys(candidate.RuntimeImageRef, digest)...)
	return uniqueDistributedImageKeys(keys)
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
		(firstNonEmptyDistributedDigest(location.Digest, managedImageDigest(location.ImageRef)) != "" && strings.TrimSpace(manifest.Digest) != "")
}

func distributedImageLocationMatchesManifest(location model.ImageLocation, manifest model.ImageCacheManifest) bool {
	locationDigest := firstNonEmptyDistributedDigest(location.Digest, managedImageDigest(location.ImageRef))
	manifestDigest := firstNonEmptyDistributedDigest(manifest.Digest, managedImageDigest(manifest.ImageRef))
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

func combineProjectImageMeasurementStatus(left, right string) string {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	if left == "" {
		return right
	}
	if right == "" || left == right {
		return left
	}
	if left == projectImageUsageMeasurementUnavailable && right == projectImageUsageMeasurementUnavailable {
		return projectImageUsageMeasurementUnavailable
	}
	if left == projectImageUsageMeasurementComplete && right == projectImageUsageMeasurementComplete {
		return projectImageUsageMeasurementComplete
	}
	return projectImageUsageMeasurementPartial
}

func summarizeAppImageVersionMeasurementStatus(versions []appImageVersion) string {
	if len(versions) == 0 {
		return projectImageUsageMeasurementComplete
	}
	status := ""
	for _, version := range versions {
		versionStatus := strings.TrimSpace(version.SizeMeasurementStatus)
		if versionStatus == "" {
			versionStatus = projectImageUsageMeasurementUnavailable
		}
		status = combineProjectImageMeasurementStatus(status, versionStatus)
	}
	return status
}

func summarizeAppImageVersionMeasurementReasons(versions []appImageVersion) []string {
	reasons := []string(nil)
	for _, version := range versions {
		reasons = mergeProjectImageMeasurementReasons(reasons, version.SizeMeasurementReasons)
	}
	return reasons
}

func mergeProjectImageMeasurementReasons(groups ...[]string) []string {
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
