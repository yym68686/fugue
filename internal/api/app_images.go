package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"
	"fugue/internal/sourceimport"

	"golang.org/x/sync/errgroup"
)

const (
	appImageStatusAvailable          = "available"
	appImageStatusMissing            = "missing"
	defaultProjectImageUsageCacheTTL = 5 * time.Minute
	projectImageUsageAppBuildLimit   = 8
	projectImageUsageSoftWait        = 250 * time.Millisecond
)

type appImageSummary struct {
	VersionCount         int   `json:"version_count"`
	CurrentVersionCount  int   `json:"current_version_count"`
	StaleVersionCount    int   `json:"stale_version_count"`
	TotalSizeBytes       int64 `json:"total_size_bytes"`
	CurrentSizeBytes     int64 `json:"current_size_bytes"`
	StaleSizeBytes       int64 `json:"stale_size_bytes"`
	ReclaimableSizeBytes int64 `json:"reclaimable_size_bytes"`
}

type appImageVersion struct {
	ImageRef             string           `json:"image_ref"`
	RuntimeImageRef      string           `json:"runtime_image_ref,omitempty"`
	Digest               string           `json:"digest,omitempty"`
	Status               string           `json:"status"`
	Current              bool             `json:"current"`
	SizeBytes            int64            `json:"size_bytes,omitempty"`
	ReclaimableSizeBytes int64            `json:"reclaimable_size_bytes,omitempty"`
	DeleteSupported      bool             `json:"delete_supported"`
	RedeploySupported    bool             `json:"redeploy_supported"`
	LastDeployedAt       *time.Time       `json:"last_deployed_at,omitempty"`
	Source               *model.AppSource `json:"source,omitempty"`
}

type appImageInventoryResponse struct {
	AppID              string            `json:"app_id"`
	RegistryConfigured bool              `json:"registry_configured"`
	ReclaimRequiresGC  bool              `json:"reclaim_requires_gc"`
	ReclaimNote        string            `json:"reclaim_note,omitempty"`
	Summary            appImageSummary   `json:"summary"`
	Versions           []appImageVersion `json:"versions"`
}

type appImageActionRequest struct {
	ImageRef string `json:"image_ref"`
}

type appImageDeleteResponse struct {
	Image              *appImageVersion `json:"image,omitempty"`
	Deleted            bool             `json:"deleted"`
	AlreadyMissing     bool             `json:"already_missing"`
	RegistryConfigured bool             `json:"registry_configured"`
	ReclaimRequiresGC  bool             `json:"reclaim_requires_gc"`
	ReclaimNote        string           `json:"reclaim_note,omitempty"`
	ReclaimedSizeBytes int64            `json:"reclaimed_size_bytes,omitempty"`
}

type appImageRedeployResponse struct {
	Image     *appImageVersion `json:"image,omitempty"`
	Operation model.Operation  `json:"operation"`
}

type projectImageUsageAppSummary struct {
	AppID                string `json:"app_id"`
	AppName              string `json:"app_name"`
	VersionCount         int    `json:"version_count"`
	CurrentVersionCount  int    `json:"current_version_count"`
	StaleVersionCount    int    `json:"stale_version_count"`
	TotalSizeBytes       int64  `json:"total_size_bytes"`
	CurrentSizeBytes     int64  `json:"current_size_bytes"`
	StaleSizeBytes       int64  `json:"stale_size_bytes"`
	ReclaimableSizeBytes int64  `json:"reclaimable_size_bytes"`
}

type projectImageUsageSummary struct {
	ProjectID            string                        `json:"project_id"`
	VersionCount         int                           `json:"version_count"`
	CurrentVersionCount  int                           `json:"current_version_count"`
	StaleVersionCount    int                           `json:"stale_version_count"`
	TotalSizeBytes       int64                         `json:"total_size_bytes"`
	CurrentSizeBytes     int64                         `json:"current_size_bytes"`
	StaleSizeBytes       int64                         `json:"stale_size_bytes"`
	ReclaimableSizeBytes int64                         `json:"reclaimable_size_bytes"`
	Apps                 []projectImageUsageAppSummary `json:"apps"`
}

type projectImageUsageResponse struct {
	RegistryConfigured bool                       `json:"registry_configured"`
	ReclaimRequiresGC  bool                       `json:"reclaim_requires_gc"`
	ReclaimNote        string                     `json:"reclaim_note,omitempty"`
	Projects           []projectImageUsageSummary `json:"projects"`
}

type projectImageUsageLoadResult struct {
	err      error
	response projectImageUsageResponse
}

type appImageCandidate struct {
	ImageRef        string
	RuntimeImageRef string
	Source          model.AppSource
	Current         bool
	LastDeployedAt  *time.Time
}

type builtAppImageVersion struct {
	Candidate appImageCandidate
	Response  appImageVersion
	BlobSizes map[string]int64
}

type builtAppImageInventory struct {
	Response             appImageInventoryResponse
	VersionByImageRef    map[string]builtAppImageVersion
	TotalBlobSizes       map[string]int64
	CurrentBlobSizes     map[string]int64
	StaleBlobSizes       map[string]int64
	ReclaimableBlobSizes map[string]int64
}

type projectImageUsageAccumulator struct {
	Summary              projectImageUsageSummary
	TotalBlobSizes       map[string]int64
	CurrentBlobSizes     map[string]int64
	StaleBlobSizes       map[string]int64
	ReclaimableBlobSizes map[string]int64
}

func (s *Server) handleListProjectImageUsage(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	response, err := s.cachedProjectImageUsageResponse(r.Context(), principal)
	if err != nil {
		var httpErr consoleHTTPError
		if errors.As(err, &httpErr) {
			httpx.WriteError(w, httpErr.status, httpErr.message)
			return
		}
		s.writeStoreError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) cachedProjectImageUsageResponse(
	ctx context.Context,
	principal model.Principal,
) (projectImageUsageResponse, error) {
	key := projectImageUsageCacheKey(principal)
	if response, ok := s.projectImageUsageCache.get(key); ok {
		return response, nil
	}

	resultCh := make(chan projectImageUsageLoadResult, 1)
	refreshCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	go func() {
		defer cancel()
		response, err := s.projectImageUsageCache.do(key, func() (projectImageUsageResponse, error) {
			return s.loadProjectImageUsageResponse(refreshCtx, principal)
		})
		resultCh <- projectImageUsageLoadResult{err: err, response: response}
	}()

	timer := time.NewTimer(projectImageUsageSoftWait)
	defer timer.Stop()

	select {
	case result := <-resultCh:
		if result.err != nil {
			var httpErr consoleHTTPError
			if errors.As(result.err, &httpErr) {
				return projectImageUsageResponse{}, result.err
			}
			return projectImageUsageResponse{}, result.err
		}
		return result.response, nil
	case <-ctx.Done():
		return projectImageUsageResponse{}, ctx.Err()
	case <-timer.C:
		if entry, ok := s.projectImageUsageCache.getEntry(key); ok {
			go s.logProjectImageUsageRefreshResult(resultCh)
			return entry.value, nil
		}
		go s.logProjectImageUsageRefreshResult(resultCh)
		return projectImageUsageResponse{
			RegistryConfigured: s.appImageInventoryConfigured(),
		}, nil
	}
}

func projectImageUsageCacheKey(principal model.Principal) string {
	return principalVisibilityCacheKey(principal)
}

func (s *Server) logProjectImageUsageRefreshResult(resultCh <-chan projectImageUsageLoadResult) {
	result := <-resultCh
	if result.err != nil && s.log != nil {
		s.log.Printf("project image usage background refresh failed: %v", result.err)
	}
}

func (s *Server) loadProjectImageUsageResponse(
	ctx context.Context,
	principal model.Principal,
) (projectImageUsageResponse, error) {
	timings := serverTimingFromContext(ctx)

	appsStartedAt := time.Now()
	apps, err := s.store.ListAppsMetadata(principal.TenantID, principal.IsPlatformAdmin())
	timings.Add("store_apps", time.Since(appsStartedAt))
	if err != nil {
		return projectImageUsageResponse{}, err
	}

	opsStartedAt := time.Now()
	opsByAppID, err := s.loadProjectImageUsageOperations(ctx, principal, apps)
	timings.Add("store_image_usage_operations", time.Since(opsStartedAt))
	if err != nil {
		return projectImageUsageResponse{}, err
	}

	buildStartedAt := time.Now()
	response, err := s.buildProjectImageUsageResponse(
		ctx,
		visibleAppsForImageInventory(apps),
		opsByAppID,
	)
	timings.Add("project_image_usage", time.Since(buildStartedAt))
	if err != nil {
		return projectImageUsageResponse{}, consoleHTTPError{
			message: err.Error(),
			status:  http.StatusBadGateway,
		}
	}

	return response, nil
}

func (s *Server) loadProjectImageUsageOperations(
	ctx context.Context,
	principal model.Principal,
	apps []model.App,
) (map[string][]model.Operation, error) {
	appIDs := make([]string, 0, len(apps))
	seen := make(map[string]struct{}, len(apps))
	for _, app := range visibleAppsForImageInventory(apps) {
		appID := strings.TrimSpace(app.ID)
		if appID == "" {
			continue
		}
		if _, ok := seen[appID]; ok {
			continue
		}
		seen[appID] = struct{}{}
		appIDs = append(appIDs, appID)
	}
	sort.Strings(appIDs)
	return s.store.ListOperationsWithDesiredSourceByApps(
		principal.TenantID,
		principal.IsPlatformAdmin(),
		appIDs,
	)
}

func (s *Server) handleGetAppImages(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	timings := serverTimingFromContext(r.Context())
	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}

	opsStartedAt := time.Now()
	ops, err := s.store.ListOperationsByApp(principal.TenantID, principal.IsPlatformAdmin(), app.ID)
	timings.Add("store_operations_app", time.Since(opsStartedAt))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	inventoryStartedAt := time.Now()
	inventory, err := s.buildAppImageInventory(r.Context(), app, ops)
	timings.Add("app_image_inventory", time.Since(inventoryStartedAt))
	if err != nil {
		httpx.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, inventory.Response)
}

func (s *Server) handleRedeployAppImage(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	timings := serverTimingFromContext(r.Context())
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.deploy") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.deploy scope")
		return
	}
	if !s.appImageInventoryConfigured() {
		httpx.WriteError(w, http.StatusBadRequest, "internal registry image inventory is not configured")
		return
	}

	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	var req appImageActionRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	opsStartedAt := time.Now()
	ops, err := s.store.ListOperationsByApp(principal.TenantID, principal.IsPlatformAdmin(), app.ID)
	timings.Add("store_operations_app", time.Since(opsStartedAt))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	inventoryStartedAt := time.Now()
	inventory, err := s.buildAppImageInventory(r.Context(), app, ops)
	timings.Add("app_image_inventory", time.Since(inventoryStartedAt))
	if err != nil {
		httpx.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	version, ok := inventory.VersionByImageRef[strings.TrimSpace(req.ImageRef)]
	if !ok {
		httpx.WriteError(w, http.StatusNotFound, "image version not found")
		return
	}
	if !version.Response.RedeploySupported {
		httpx.WriteError(w, http.StatusConflict, "image version is not available in the registry")
		return
	}

	source := version.Candidate.Source
	source.ResolvedImageRef = strings.TrimSpace(version.Candidate.ImageRef)

	spec := cloneAppSpec(app.Spec)
	if runtimeImageRef := s.runtimeImageRefFromManagedRefWithDigest(version.Candidate.ImageRef, version.Response.Digest); runtimeImageRef != "" {
		spec.Image = runtimeImageRef
	} else if strings.TrimSpace(version.Candidate.RuntimeImageRef) != "" {
		spec.Image = strings.TrimSpace(version.Candidate.RuntimeImageRef)
	} else if runtimeImageRef := s.runtimeImageRefFromManagedRef(version.Candidate.ImageRef); runtimeImageRef != "" {
		spec.Image = runtimeImageRef
	} else {
		spec.Image = strings.TrimSpace(version.Candidate.ImageRef)
	}
	if spec.Replicas < 1 {
		spec.Replicas = 1
	}

	op, err := s.store.CreateOperation(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     principal.ActorType,
		RequestedByID:       principal.ActorID,
		AppID:               app.ID,
		DesiredSpec:         &spec,
		DesiredSource:       &source,
		DesiredOriginSource: model.AppOriginSource(app),
	})
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	s.appendAudit(principal, "app.image.redeploy", "operation", op.ID, app.TenantID, map[string]string{
		"app_id":    app.ID,
		"image_ref": version.Candidate.ImageRef,
		"digest":    version.Response.Digest,
	})
	httpx.WriteJSON(w, http.StatusAccepted, appImageRedeployResponse{
		Image:     &version.Response,
		Operation: sanitizeOperationForAPI(op),
	})
}

func (s *Server) handleDeleteAppImage(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	timings := serverTimingFromContext(r.Context())
	if !principal.IsPlatformAdmin() && !principal.HasScope("app.write") && !principal.HasScope("app.delete") {
		httpx.WriteError(w, http.StatusForbidden, "missing app.write or app.delete scope")
		return
	}
	if !s.appImageInventoryConfigured() {
		httpx.WriteError(w, http.StatusBadRequest, "internal registry image inventory is not configured")
		return
	}

	app, allowed := s.loadAuthorizedAppMetadata(w, r, principal)
	if !allowed {
		return
	}
	var req appImageActionRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	opsStartedAt := time.Now()
	ops, err := s.store.ListOperationsByApp(principal.TenantID, principal.IsPlatformAdmin(), app.ID)
	timings.Add("store_operations_app", time.Since(opsStartedAt))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	inventoryStartedAt := time.Now()
	inventory, err := s.buildAppImageInventory(r.Context(), app, ops)
	timings.Add("app_image_inventory", time.Since(inventoryStartedAt))
	if err != nil {
		httpx.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}
	version, ok := inventory.VersionByImageRef[strings.TrimSpace(req.ImageRef)]
	if !ok {
		httpx.WriteError(w, http.StatusNotFound, "image version not found")
		return
	}
	if version.Candidate.Current {
		httpx.WriteError(w, http.StatusConflict, "cannot delete the current image version")
		return
	}
	if !version.Response.DeleteSupported {
		httpx.WriteError(w, http.StatusConflict, "image version is not available in the registry")
		return
	}

	deleteResult, err := s.appImageRegistry.DeleteImage(r.Context(), version.Candidate.ImageRef)
	if err != nil {
		httpx.WriteError(w, http.StatusBadGateway, err.Error())
		return
	}

	if err := s.runAppImageRegistryGarbageCollect(r.Context()); err != nil {
		s.appendAudit(principal, "app.image.delete", "app", app.ID, app.TenantID, map[string]string{
			"app_id":       app.ID,
			"image_ref":    version.Candidate.ImageRef,
			"digest":       deleteResult.Digest,
			"registry_gc":  "failed",
			"delete_state": appImageDeleteAuditState(deleteResult),
		})
		httpx.WriteError(w, http.StatusBadGateway, fmt.Sprintf(
			"saved image reference was removed, but immediate registry cleanup failed: %v",
			err,
		))
		return
	}

	s.appendAudit(principal, "app.image.delete", "app", app.ID, app.TenantID, map[string]string{
		"app_id":       app.ID,
		"image_ref":    version.Candidate.ImageRef,
		"digest":       deleteResult.Digest,
		"registry_gc":  "completed",
		"delete_state": appImageDeleteAuditState(deleteResult),
	})
	s.scheduleTenantBillingImageStorageRefresh(app.TenantID)
	httpx.WriteJSON(w, http.StatusOK, appImageDeleteResponse{
		Image:              &version.Response,
		Deleted:            deleteResult.Deleted,
		AlreadyMissing:     deleteResult.AlreadyMissing,
		RegistryConfigured: true,
		ReclaimedSizeBytes: version.Response.ReclaimableSizeBytes,
	})
}

func (s *Server) buildProjectImageUsageResponse(
	ctx context.Context,
	apps []model.App,
	opsByAppID map[string][]model.Operation,
) (projectImageUsageResponse, error) {
	response := projectImageUsageResponse{
		RegistryConfigured: s.appImageInventoryConfigured(),
	}
	if !response.RegistryConfigured {
		return response, nil
	}

	type appInventoryResult struct {
		App       model.App
		Inventory builtAppImageInventory
	}

	inventoryResults := make([]appInventoryResult, len(apps))
	inventoryGroup, inventoryCtx := errgroup.WithContext(ctx)
	inventoryGroup.SetLimit(projectImageUsageAppBuildLimit)
	for index, app := range apps {
		index, app := index, app
		inventoryGroup.Go(func() error {
			inventory, err := s.buildAppImageInventory(inventoryCtx, app, opsByAppID[app.ID])
			if err != nil {
				return err
			}
			inventoryResults[index] = appInventoryResult{
				App:       app,
				Inventory: inventory,
			}
			return nil
		})
	}
	if err := inventoryGroup.Wait(); err != nil {
		return projectImageUsageResponse{}, err
	}

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
					ProjectID: projectID,
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
	return response, nil
}

func (s *Server) buildAppImageInventory(
	ctx context.Context,
	app model.App,
	ops []model.Operation,
) (builtAppImageInventory, error) {
	inventory := builtAppImageInventory{
		Response: appImageInventoryResponse{
			AppID:              app.ID,
			RegistryConfigured: s.appImageInventoryConfigured(),
			Versions:           []appImageVersion{},
		},
		VersionByImageRef:    make(map[string]builtAppImageVersion),
		TotalBlobSizes:       make(map[string]int64),
		CurrentBlobSizes:     make(map[string]int64),
		StaleBlobSizes:       make(map[string]int64),
		ReclaimableBlobSizes: make(map[string]int64),
	}
	if !inventory.Response.RegistryConfigured {
		return inventory, nil
	}

	candidatesByImageRef := s.collectAppImageCandidates(app, ops)
	if len(candidatesByImageRef) == 0 {
		return inventory, nil
	}

	imageRefs := make([]string, 0, len(candidatesByImageRef))
	for imageRef := range candidatesByImageRef {
		imageRefs = append(imageRefs, imageRef)
	}
	sort.Strings(imageRefs)

	inspectResults := make(map[string]appImageRegistryInspectResult, len(imageRefs))
	var inspectResultsMu sync.Mutex

	inspectGroup, inspectCtx := errgroup.WithContext(ctx)
	inspectGroup.SetLimit(6)
	for _, imageRef := range imageRefs {
		imageRef := imageRef
		inspectGroup.Go(func() error {
			result, err := s.appImageRegistry.InspectImage(inspectCtx, imageRef)
			if err != nil {
				return err
			}

			inspectResultsMu.Lock()
			inspectResults[imageRef] = result
			inspectResultsMu.Unlock()
			return nil
		})
	}
	if err := inspectGroup.Wait(); err != nil {
		return builtAppImageInventory{}, err
	}

	blobUsageCount := make(map[string]int)
	keptCandidates := make([]appImageCandidate, 0, len(imageRefs))
	for _, imageRef := range imageRefs {
		candidate := candidatesByImageRef[imageRef]
		inspectResult := inspectResults[imageRef]
		if !inspectResult.Exists && !candidate.Current {
			continue
		}
		keptCandidates = append(keptCandidates, candidate)
		if !inspectResult.Exists {
			continue
		}
		unionAppImageBlobSizes(inventory.TotalBlobSizes, inspectResult.BlobSizes)
		if candidate.Current {
			unionAppImageBlobSizes(inventory.CurrentBlobSizes, inspectResult.BlobSizes)
		} else {
			unionAppImageBlobSizes(inventory.StaleBlobSizes, inspectResult.BlobSizes)
		}
		for _, digest := range sortedAppImageBlobDigests(inspectResult.BlobSizes) {
			blobUsageCount[digest]++
		}
	}

	for digest, sizeBytes := range inventory.StaleBlobSizes {
		if _, usedByCurrent := inventory.CurrentBlobSizes[digest]; usedByCurrent {
			continue
		}
		inventory.ReclaimableBlobSizes[digest] = sizeBytes
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

	for _, candidate := range keptCandidates {
		inspectResult := inspectResults[candidate.ImageRef]
		reclaimableSizeBytes := int64(0)
		if inspectResult.Exists && !candidate.Current {
			for digest, sizeBytes := range inspectResult.BlobSizes {
				if blobUsageCount[digest] == 1 {
					reclaimableSizeBytes += sizeBytes
				}
			}
		}
		version := appImageVersion{
			ImageRef:             candidate.ImageRef,
			RuntimeImageRef:      candidate.RuntimeImageRef,
			Digest:               inspectResult.Digest,
			Status:               appImageStatusMissing,
			Current:              candidate.Current,
			DeleteSupported:      false,
			RedeploySupported:    false,
			LastDeployedAt:       cloneTimePointer(candidate.LastDeployedAt),
			ReclaimableSizeBytes: reclaimableSizeBytes,
			Source:               sanitizeAppSourceForAPI(&candidate.Source),
		}
		if inspectResult.Exists {
			version.Status = appImageStatusAvailable
			version.SizeBytes = inspectResult.SizeBytes
			version.DeleteSupported = !candidate.Current
			version.RedeploySupported = true
		}
		inventory.Response.Versions = append(inventory.Response.Versions, version)
		inventory.VersionByImageRef[candidate.ImageRef] = builtAppImageVersion{
			Candidate: candidate,
			Response:  version,
			BlobSizes: inspectResult.BlobSizes,
		}
	}

	currentCount := 0
	for _, candidate := range keptCandidates {
		if candidate.Current {
			currentCount++
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
	return inventory, nil
}

func (s *Server) collectAppImageCandidates(app model.App, ops []model.Operation) map[string]appImageCandidate {
	candidates := make(map[string]appImageCandidate)
	if candidate, ok := s.buildAppImageCandidate(app, app.Source, app.Spec.Image, true, appCurrentImageTimestamp(app)); ok {
		candidates[candidate.ImageRef] = candidate
	}
	for _, op := range ops {
		if op.DesiredSource == nil {
			continue
		}
		runtimeImageRef := ""
		if op.DesiredSpec != nil {
			runtimeImageRef = strings.TrimSpace(op.DesiredSpec.Image)
		}
		candidate, ok := s.buildAppImageCandidate(app, op.DesiredSource, runtimeImageRef, false, appImageOperationTimestamp(op))
		if !ok {
			continue
		}
		if existing, exists := candidates[candidate.ImageRef]; exists {
			candidates[candidate.ImageRef] = mergeAppImageCandidate(existing, candidate)
			continue
		}
		candidates[candidate.ImageRef] = candidate
	}
	return candidates
}

func (s *Server) buildAppImageCandidate(
	app model.App,
	source *model.AppSource,
	runtimeImageRef string,
	current bool,
	lastDeployedAt *time.Time,
) (appImageCandidate, bool) {
	if source == nil {
		return appImageCandidate{}, false
	}
	imageRef := s.managedImageRefForSource(app, source, runtimeImageRef)
	if imageRef == "" {
		return appImageCandidate{}, false
	}

	sourceCopy := *source
	sourceCopy.ResolvedImageRef = imageRef
	if runtimeImageRef == "" {
		runtimeImageRef = s.runtimeImageRefFromManagedRef(imageRef)
	}

	return appImageCandidate{
		ImageRef:        imageRef,
		RuntimeImageRef: strings.TrimSpace(runtimeImageRef),
		Source:          sourceCopy,
		Current:         current,
		LastDeployedAt:  cloneTimePointer(lastDeployedAt),
	}, true
}

func (s *Server) managedImageRefForSource(app model.App, source *model.AppSource, runtimeImageRef string) string {
	if source == nil {
		return ""
	}
	if resolved := strings.TrimSpace(source.ResolvedImageRef); resolved != "" {
		return resolved
	}
	if managedRuntimeImageRef := s.registryRefFromRuntimeImageRef(runtimeImageRef); managedRuntimeImageRef != "" {
		return managedRuntimeImageRef
	}

	switch strings.TrimSpace(source.Type) {
	case model.AppSourceTypeGitHubPublic, model.AppSourceTypeGitHubPrivate:
		return inferManagedGitHubImageRef(s.registryPushBase, source)
	case model.AppSourceTypeUpload:
		return inferManagedUploadImageRef(s.registryPushBase, app.Name, source)
	case model.AppSourceTypeDockerImage:
		if imageRef := strings.TrimSpace(source.ImageRef); s.isManagedRegistryRef(imageRef) {
			return imageRef
		}
	default:
		if imageRef := strings.TrimSpace(source.ImageRef); s.isManagedRegistryRef(imageRef) {
			return imageRef
		}
	}
	return ""
}

func inferManagedGitHubImageRef(registryPushBase string, source *model.AppSource) string {
	if source == nil {
		return ""
	}
	repoOwner, repoName, err := sourceimport.ParseGitHubRepoURL(strings.TrimSpace(source.RepoURL))
	if err != nil {
		return ""
	}
	commitSHA := shortAppImageCommit(strings.TrimSpace(source.CommitSHA))
	if commitSHA == "" {
		return ""
	}

	repoPath := repoOwner + "-" + repoName
	if suffix := model.SlugifyOptional(source.ImageNameSuffix); suffix != "" {
		repoPath += "-" + suffix
	}
	return fmt.Sprintf("%s/fugue-apps/%s:git-%s", strings.Trim(strings.TrimSpace(registryPushBase), "/"), repoPath, commitSHA)
}

func inferManagedUploadImageRef(registryPushBase, appName string, source *model.AppSource) string {
	if source == nil {
		return ""
	}
	tagSeed := strings.TrimSpace(source.ArchiveSHA256)
	if tagSeed == "" {
		tagSeed = strings.TrimSpace(source.CommitSHA)
	}
	shortTag := shortAppImageCommit(tagSeed)
	if shortTag == "" {
		return ""
	}

	repoPath := sourceimport.UploadImageRepositoryName(appName, source.ImageNameSuffix)
	return fmt.Sprintf("%s/fugue-apps/%s:upload-%s", strings.Trim(strings.TrimSpace(registryPushBase), "/"), repoPath, shortTag)
}

func shortAppImageCommit(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 12 {
		return value[:12]
	}
	return value
}

func (s *Server) registryRefFromRuntimeImageRef(runtimeImageRef string) string {
	runtimeImageRef = strings.TrimSpace(runtimeImageRef)
	if runtimeImageRef == "" {
		return ""
	}
	pushBase := strings.Trim(strings.TrimSpace(s.registryPushBase), "/")
	pullBase := strings.Trim(strings.TrimSpace(s.registryPullBase), "/")
	if pushBase == "" {
		return ""
	}
	if strings.HasPrefix(runtimeImageRef, pushBase+"/") {
		return runtimeImageRef
	}
	if pullBase == "" || pullBase == pushBase {
		return ""
	}
	prefix := pullBase + "/"
	if !strings.HasPrefix(runtimeImageRef, prefix) {
		return ""
	}
	return pushBase + "/" + strings.TrimPrefix(runtimeImageRef, prefix)
}

func (s *Server) runtimeImageRefFromManagedRef(imageRef string) string {
	imageRef = strings.TrimSpace(imageRef)
	pushBase := strings.Trim(strings.TrimSpace(s.registryPushBase), "/")
	pullBase := strings.Trim(strings.TrimSpace(s.registryPullBase), "/")
	if imageRef == "" {
		return ""
	}
	if pullBase == "" || pullBase == pushBase || pushBase == "" {
		return imageRef
	}
	prefix := pushBase + "/"
	if !strings.HasPrefix(imageRef, prefix) {
		return imageRef
	}
	return pullBase + "/" + strings.TrimPrefix(imageRef, prefix)
}

func (s *Server) runtimeImageRefFromManagedRefWithDigest(imageRef, digest string) string {
	imageRef = strings.TrimSpace(imageRef)
	digest = strings.TrimSpace(digest)
	if imageRef == "" || digest == "" {
		return ""
	}
	digestRef, err := sourceimport.DigestReferenceFromImageRef(imageRef, digest)
	if err != nil {
		return ""
	}
	return s.runtimeImageRefFromManagedRef(digestRef)
}

func (s *Server) isManagedRegistryRef(imageRef string) bool {
	imageRef = strings.TrimSpace(imageRef)
	pushBase := strings.Trim(strings.TrimSpace(s.registryPushBase), "/")
	if imageRef == "" || pushBase == "" {
		return false
	}
	return strings.HasPrefix(imageRef, pushBase+"/")
}

func (s *Server) appImageInventoryConfigured() bool {
	return strings.TrimSpace(s.registryPushBase) != "" && s.appImageRegistry != nil
}

func appImageDeleteAuditState(result appImageRegistryDeleteResult) string {
	switch {
	case result.Deleted:
		return "deleted"
	case result.AlreadyMissing:
		return "already_missing"
	default:
		return "noop"
	}
}

func visibleAppsForImageInventory(apps []model.App) []model.App {
	visible := make([]model.App, 0, len(apps))
	for _, app := range apps {
		if strings.EqualFold(strings.TrimSpace(app.Status.Phase), "deleting") {
			continue
		}
		visible = append(visible, app)
	}
	return visible
}

func appCurrentImageTimestamp(app model.App) *time.Time {
	if app.Status.CurrentReleaseReadyAt != nil {
		return cloneTimePointer(app.Status.CurrentReleaseReadyAt)
	}
	if !app.Status.UpdatedAt.IsZero() {
		value := app.Status.UpdatedAt.UTC()
		return &value
	}
	if !app.UpdatedAt.IsZero() {
		value := app.UpdatedAt.UTC()
		return &value
	}
	return nil
}

func appImageOperationTimestamp(op model.Operation) *time.Time {
	if op.CompletedAt != nil {
		return cloneTimePointer(op.CompletedAt)
	}
	if op.StartedAt != nil {
		return cloneTimePointer(op.StartedAt)
	}
	if !op.UpdatedAt.IsZero() {
		value := op.UpdatedAt.UTC()
		return &value
	}
	if !op.CreatedAt.IsZero() {
		value := op.CreatedAt.UTC()
		return &value
	}
	return nil
}

func mergeAppImageCandidate(existing, next appImageCandidate) appImageCandidate {
	out := existing
	if next.Current {
		out.Current = true
	}
	if out.RuntimeImageRef == "" || (next.Current && next.RuntimeImageRef != "") {
		out.RuntimeImageRef = next.RuntimeImageRef
	}
	if appImageSourceFieldCount(next.Source) > appImageSourceFieldCount(out.Source) || next.Current {
		out.Source = next.Source
	}
	if timestampFromPointer(next.LastDeployedAt) > timestampFromPointer(out.LastDeployedAt) {
		out.LastDeployedAt = cloneTimePointer(next.LastDeployedAt)
		if next.RuntimeImageRef != "" {
			out.RuntimeImageRef = next.RuntimeImageRef
		}
	}
	return out
}

func appImageSourceFieldCount(source model.AppSource) int {
	count := 0
	values := []string{
		source.Type,
		source.RepoURL,
		source.RepoBranch,
		source.ImageRef,
		source.ResolvedImageRef,
		source.UploadID,
		source.UploadFilename,
		source.ArchiveSHA256,
		source.SourceDir,
		source.BuildStrategy,
		source.CommitSHA,
		source.CommitCommittedAt,
		source.DockerfilePath,
		source.BuildContextDir,
		source.ImageNameSuffix,
		source.ComposeService,
		source.DetectedProvider,
		source.DetectedStack,
	}
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			count++
		}
	}
	if source.ArchiveSizeBytes > 0 {
		count++
	}
	return count
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := value.UTC()
	return &copied
}

func timestampFromPointer(value *time.Time) int64 {
	if value == nil {
		return 0
	}
	return value.UTC().UnixNano()
}

func unionAppImageBlobSizes(target, source map[string]int64) {
	for digest, sizeBytes := range source {
		if sizeBytes <= 0 {
			continue
		}
		if existing, ok := target[digest]; ok && existing >= sizeBytes {
			continue
		}
		target[digest] = sizeBytes
	}
}

func normalizedProjectIDForImageInventory(projectID string) string {
	if strings.TrimSpace(projectID) == "" {
		return "unassigned"
	}
	return strings.TrimSpace(projectID)
}

func maxInt(value, minimum int) int {
	if value < minimum {
		return minimum
	}
	return value
}
