package api

import (
	"context"
	"sort"
	"strings"
	"time"

	"fugue/internal/appimages"
	"fugue/internal/model"

	"golang.org/x/sync/errgroup"
)

const distributedImageUsageMeasurementNote = "distributed image sizes are derived from fresh image-cache manifests; totals are logical image payloads and generations without complete blob evidence are marked partial"

const distributedImageReclaimNote = "distributed cache retention is automatic; per-version deletion and exact reclaimable-byte reporting are not supported"

type projectImageUsageInventoryResult struct {
	App       model.App
	Inventory builtAppImageInventory
}

type distributedImageUsageEvidence = appimages.DistributedEvidence

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
	timings := serverTimingFromContext(ctx)
	appIDs := make(map[string]struct{}, len(apps))
	for _, app := range apps {
		if id := strings.TrimSpace(app.ID); id != "" {
			appIDs[id] = struct{}{}
		}
	}
	if len(appIDs) == 0 || s == nil || s.store == nil {
		return distributedImageUsageEvidence{}, nil
	}
	filterAppIDs := make([]string, 0, len(appIDs))
	for appID := range appIDs {
		filterAppIDs = append(filterAppIDs, appID)
	}
	sort.Strings(filterAppIDs)

	var images []model.Image
	var locations []model.ImageLocation
	var manifests []model.ImageCacheManifest
	group := new(errgroup.Group)
	group.Go(func() error {
		started := time.Now()
		var err error
		images, err = s.store.ListImages(model.ImageFilter{PlatformAdmin: true, AppIDs: filterAppIDs})
		timings.Add("image_evidence_images", time.Since(started))
		return err
	})
	group.Go(func() error {
		started := time.Now()
		var err error
		locations, err = s.store.ListImageLocations(model.ImageLocationFilter{
			Status: model.ImageLocationStatusPresent, PlatformAdmin: true, AppIDs: filterAppIDs,
		})
		timings.Add("image_evidence_locations", time.Since(started))
		return err
	})
	group.Go(func() error {
		started := time.Now()
		var err error
		manifests, err = s.store.ListImageCacheManifests(model.ImageCacheManifestFilter{PresentOnly: true, IncludeIncomplete: true})
		timings.Add("image_evidence_manifests", time.Since(started))
		return err
	})
	if err := group.Wait(); err != nil {
		return distributedImageUsageEvidence{}, err
	}
	started := time.Now()
	evidence := appimages.BuildDistributedEvidence(appIDs, images, locations, manifests, time.Now().UTC().Add(-defaultImageCacheInventoryTTL))
	timings.Add("image_evidence_index", time.Since(started))
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
		accumulator.Summary.MeasurementStatus = appimages.CombineMeasurementStatus(
			accumulator.Summary.MeasurementStatus,
			inventory.Response.MeasurementStatus,
		)
		accumulator.Summary.MeasurementReasons = appimages.MergeMeasurementReasons(
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
		accumulator.Summary.TotalSizeBytes = appimages.SumBlobSizes(accumulator.TotalBlobSizes)
		accumulator.Summary.CurrentSizeBytes = appimages.SumBlobSizes(accumulator.CurrentBlobSizes)
		accumulator.Summary.StaleSizeBytes = appimages.SumBlobSizes(accumulator.StaleBlobSizes)
		accumulator.Summary.ReclaimableSizeBytes = appimages.SumBlobSizes(accumulator.ReclaimableBlobSizes)
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
			status = appimages.CombineMeasurementStatus(status, project.MeasurementStatus)
			reasons = appimages.MergeMeasurementReasons(reasons, project.MeasurementReasons)
		}
		response.MeasurementStatus = status
		response.MeasurementReasons = reasons
	}
	return response
}

func newDistributedProjectImageUsageResponse(observedAt time.Time) projectImageUsageResponse {
	response := projectImageUsageResponse{
		RegistryConfigured: false,
		ReclaimNote:        distributedImageReclaimNote,
		ImageStoreMode:     projectImageUsageModeDistributed,
		MeasurementStatus:  projectImageUsageMeasurementComplete,
		MeasurementNote:    distributedImageUsageMeasurementNote,
		Projects:           []projectImageUsageSummary{},
	}
	if !observedAt.IsZero() {
		observed := observedAt.UTC()
		response.ObservedAt = &observed
	}
	return response
}

func (s *Server) buildDistributedProjectImageUsageResponse(
	ctx context.Context,
	apps []model.App,
	opsByAppID map[string][]model.Operation,
) (projectImageUsageResponse, error) {
	evidence, err := s.loadDistributedImageUsageEvidence(ctx, apps)
	if err != nil {
		return projectImageUsageResponse{}, err
	}
	inventoryResults, err := s.buildDistributedImageUsageInventories(ctx, apps, opsByAppID, evidence)
	if err != nil {
		return projectImageUsageResponse{}, err
	}
	started := time.Now()
	response := aggregateProjectImageUsageInventories(newDistributedProjectImageUsageResponse(evidence.ObservedAt()), inventoryResults)
	serverTimingFromContext(ctx).Add("image_evidence_aggregate", time.Since(started))
	return response, nil
}

func (s *Server) buildDistributedImageUsageInventories(ctx context.Context, apps []model.App, opsByAppID map[string][]model.Operation, evidence distributedImageUsageEvidence) ([]projectImageUsageInventoryResult, error) {
	inventoryResults := make([]projectImageUsageInventoryResult, len(apps))
	started := time.Now()
	inventoryGroup, buildCtx := errgroup.WithContext(ctx)
	inventoryGroup.SetLimit(projectImageUsageAppBuildLimit)
	for index, app := range apps {
		index, app := index, app
		inventoryGroup.Go(func() error {
			if err := buildCtx.Err(); err != nil {
				return err
			}
			inventoryResults[index] = projectImageUsageInventoryResult{
				App:       app,
				Inventory: s.buildDistributedAppImageInventory(app, opsByAppID[app.ID], evidence),
			}
			return nil
		})
	}
	if err := inventoryGroup.Wait(); err != nil {
		return nil, err
	}
	serverTimingFromContext(ctx).Add("image_evidence_match", time.Since(started))
	return inventoryResults, nil
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
	measurements := make(map[string]appimages.DistributedMeasurement, len(imageRefs))
	for _, imageRef := range imageRefs {
		candidate := candidatesByImageRef[imageRef]
		measurement := evidence.Measure(app.ID, candidate)
		measurements[imageRef] = measurement
		hasEvidence := measurement.Available || measurement.HadFreshEvidence
		if !hasEvidence && !candidate.Current {
			continue
		}
		keptCandidates = append(keptCandidates, candidate)
		if measurement.HasSize {
			key := measurement.SizeKey(candidate)
			if key != "" {
				inventory.TotalBlobSizes[key] = maxInt64(inventory.TotalBlobSizes[key], measurement.SizeBytes)
				if candidate.Current {
					inventory.CurrentBlobSizes[key] = maxInt64(inventory.CurrentBlobSizes[key], measurement.SizeBytes)
				} else {
					inventory.StaleBlobSizes[key] = maxInt64(inventory.StaleBlobSizes[key], measurement.SizeBytes)
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
			Digest:                 measurement.Digest,
			Status:                 appImageStatusMissing,
			Current:                candidate.Current,
			SizeBytes:              measurement.SizeBytes,
			SizeMeasurementStatus:  measurement.Status,
			SizeMeasurementReasons: append([]string(nil), measurement.Reasons...),
			DeleteSupported:        false,
			RedeploySupported:      false,
			LastDeployedAt:         cloneTimePointer(candidate.LastDeployedAt),
			Source:                 sanitizeAppSourceForAPI(&candidate.Source),
		}
		if measurement.Available {
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
		TotalSizeBytes:       appimages.SumBlobSizes(inventory.TotalBlobSizes),
		CurrentSizeBytes:     appimages.SumBlobSizes(inventory.CurrentBlobSizes),
		StaleSizeBytes:       appimages.SumBlobSizes(inventory.StaleBlobSizes),
		ReclaimableSizeBytes: appimages.SumBlobSizes(inventory.ReclaimableBlobSizes),
	}
	inventory.Response.MeasurementStatus = summarizeAppImageVersionMeasurementStatus(inventory.Response.Versions)
	inventory.Response.MeasurementReasons = summarizeAppImageVersionMeasurementReasons(inventory.Response.Versions)
	return inventory
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
		status = appimages.CombineMeasurementStatus(status, versionStatus)
	}
	return status
}

func summarizeAppImageVersionMeasurementReasons(versions []appImageVersion) []string {
	reasons := []string(nil)
	for _, version := range versions {
		reasons = appimages.MergeMeasurementReasons(reasons, version.SizeMeasurementReasons)
	}
	return reasons
}
