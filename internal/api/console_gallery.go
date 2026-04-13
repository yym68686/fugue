package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/httpx"
	"fugue/internal/model"

	"golang.org/x/sync/errgroup"
)

const (
	consoleGalleryStreamPollInterval      = 5 * time.Second
	consoleGalleryStreamHeartbeatInterval = 15 * time.Second
	consoleGalleryStreamRetryMS           = 5000
	defaultConsoleGalleryCacheTTL         = 5 * time.Second
)

type consoleHTTPError struct {
	message string
	status  int
}

func (e consoleHTTPError) Error() string {
	return e.message
}

type consoleProjectBadge struct {
	Kind  string `json:"kind"`
	Label string `json:"label"`
	Meta  string `json:"meta"`
}

type consoleProjectLifecycle struct {
	Label    string `json:"label"`
	Live     bool   `json:"live"`
	SyncMode string `json:"sync_mode"`
	Tone     string `json:"tone"`
}

type consoleProjectSummary struct {
	AppCount              int                     `json:"app_count"`
	ID                    string                  `json:"id"`
	Lifecycle             consoleProjectLifecycle `json:"lifecycle"`
	Name                  string                  `json:"name"`
	ResourceUsageSnapshot model.ResourceUsage     `json:"resource_usage_snapshot"`
	ServiceBadges         []consoleProjectBadge   `json:"service_badges"`
	ServiceCount          int                     `json:"service_count"`
}

type consoleGalleryResponse struct {
	Projects []consoleProjectSummary `json:"projects"`
}

type consoleProjectDetailResponse struct {
	Apps         []model.App         `json:"apps"`
	ClusterNodes []model.ClusterNode `json:"cluster_nodes"`
	Operations   []model.Operation   `json:"operations"`
	Project      *model.Project      `json:"project,omitempty"`
	ProjectID    string              `json:"project_id"`
	ProjectName  string              `json:"project_name"`
}

type consoleGalleryStreamEvent struct {
	Hash string `json:"hash"`
}

type consoleProjectSummaryRecord struct {
	project consoleProjectSummary
	sortAt  time.Time
}

func normalizeConsoleText(value string) string {
	return strings.TrimSpace(strings.ToLower(value))
}

func humanizeConsole(value string) string {
	normalized := strings.TrimSpace(value)
	if normalized == "" {
		return "Unknown"
	}

	normalized = strings.NewReplacer(".", " ", "_", " ", "-", " ").Replace(normalized)
	parts := strings.Fields(normalized)
	for index, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(strings.ToLower(part))
		if len(runes) == 0 {
			continue
		}
		runes[0] = []rune(strings.ToUpper(string(runes[0])))[0]
		parts[index] = string(runes)
	}

	return strings.Join(parts, " ")
}

func consoleTechnologyLabel(value string) string {
	switch normalizeConsoleText(value) {
	case "next", "nextjs":
		return "Next.js"
	case "react":
		return "React"
	case "node", "nodejs":
		return "Node.js"
	case "python":
		return "Python"
	case "go":
		return "Go"
	case "java":
		return "Java"
	case "ruby":
		return "Ruby"
	case "php":
		return "PHP"
	case "dotnet":
		return ".NET"
	case "rust":
		return "Rust"
	default:
		return ""
	}
}

func consoleLanguageBadgeKind(value string) string {
	switch normalizeConsoleText(value) {
	case "next", "nextjs":
		return "nextjs"
	case "react":
		return "react"
	case "node", "nodejs":
		return "node"
	case "python":
		return "python"
	case "go":
		return "go"
	case "java":
		return "java"
	case "ruby":
		return "ruby"
	case "php":
		return "php"
	case "dotnet":
		return "dotnet"
	case "rust":
		return "rust"
	default:
		return ""
	}
}

func consoleBuildBadgeKind(value string) string {
	switch normalizeConsoleText(value) {
	case model.AppBuildStrategyDockerfile:
		return "docker"
	case model.AppBuildStrategyBuildpacks:
		return "buildpacks"
	case model.AppBuildStrategyNixpacks:
		return "nixpacks"
	case model.AppBuildStrategyStaticSite:
		return "static"
	default:
		return ""
	}
}

func readConsoleAppSourceType(app model.App) string {
	if app.Source == nil {
		return ""
	}
	return app.Source.Type
}

func readConsoleAppBuildStrategy(app model.App) string {
	if app.Source == nil {
		return ""
	}
	return app.Source.BuildStrategy
}

func readConsoleAppCommitSHA(app model.App) string {
	if app.Source == nil {
		return ""
	}
	return app.Source.CommitSHA
}

func readConsoleOperationCommitSHA(operation *model.Operation) string {
	if operation == nil || operation.DesiredSource == nil {
		return ""
	}
	return operation.DesiredSource.CommitSHA
}

func consoleBadgeFromTech(item model.AppTechnology) (consoleProjectBadge, bool) {
	normalizedKind := normalizeConsoleText(item.Kind)
	normalizedSlug := normalizeConsoleText(item.Slug)
	label := strings.TrimSpace(item.Name)
	if label == "" {
		label = consoleTechnologyLabel(item.Slug)
	}
	if label == "" {
		label = humanizeConsole(item.Slug)
	}

	switch normalizedKind {
	case "language", "stack":
		kind := consoleLanguageBadgeKind(normalizedSlug)
		if kind == "" {
			kind = "runtime"
		}
		meta := "Language"
		if normalizedKind == "stack" {
			meta = "Stack"
		}
		return consoleProjectBadge{
			Kind:  kind,
			Label: label,
			Meta:  meta,
		}, true
	case "service":
		kind := "runtime"
		serviceLabel := label
		if normalizedSlug == model.BackingServiceTypePostgres {
			kind = "postgres"
			serviceLabel = "PostgreSQL"
		}
		return consoleProjectBadge{
			Kind:  kind,
			Label: serviceLabel,
			Meta:  "Service",
		}, true
	case "build":
		kind := consoleBuildBadgeKind(normalizedSlug)
		if kind == "" {
			kind = "runtime"
		}
		return consoleProjectBadge{
			Kind:  kind,
			Label: label,
			Meta:  "Build",
		}, true
	default:
		if normalizedSlug == "" {
			return consoleProjectBadge{}, false
		}
		return consoleProjectBadge{
			Kind:  "runtime",
			Label: label,
			Meta:  humanizeConsole(item.Kind),
		}, true
	}
}

func buildConsolePrimaryBadge(app model.App) consoleProjectBadge {
	for _, item := range app.TechStack {
		if badge, ok := consoleBadgeFromTech(item); ok && badge.Kind != "postgres" {
			return badge
		}
	}

	buildStrategy := readConsoleAppBuildStrategy(app)
	if kind := consoleBuildBadgeKind(buildStrategy); kind != "" {
		return consoleProjectBadge{
			Kind:  kind,
			Label: humanizeConsole(buildStrategy),
			Meta:  "Build",
		}
	}

	sourceType := readConsoleAppSourceType(app)
	switch normalizeConsoleText(sourceType) {
	case model.AppSourceTypeGitHubPublic, model.AppSourceTypeGitHubPrivate:
		return consoleProjectBadge{
			Kind:  "github",
			Label: "GitHub",
			Meta:  "Source",
		}
	case model.AppSourceTypeDockerImage:
		return consoleProjectBadge{
			Kind:  "docker",
			Label: "Docker",
			Meta:  "Source",
		}
	default:
		return consoleProjectBadge{
			Kind:  "runtime",
			Label: humanizeConsole(sourceType),
			Meta:  "Service",
		}
	}
}

func consoleBadgeKey(badge consoleProjectBadge) string {
	return normalizeConsoleText(badge.Kind + ":" + badge.Label + ":" + badge.Meta)
}

func buildConsoleProjectServiceBadges(apps []model.App) []consoleProjectBadge {
	seen := make(map[string]struct{})
	badges := make([]consoleProjectBadge, 0, len(apps)+2)

	addBadge := func(badge consoleProjectBadge) {
		key := consoleBadgeKey(badge)
		if key == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		badges = append(badges, badge)
	}

	for _, app := range apps {
		addBadge(buildConsolePrimaryBadge(app))
		if appHasBackingServiceType(app, model.BackingServiceTypePostgres) {
			addBadge(consoleProjectBadge{
				Kind:  "postgres",
				Label: "PostgreSQL",
				Meta:  "Service",
			})
		}
	}

	return badges
}

func appHasBackingServiceType(app model.App, serviceType string) bool {
	normalizedType := normalizeConsoleText(serviceType)
	for _, service := range app.BackingServices {
		if normalizeConsoleText(service.Type) == normalizedType {
			return true
		}
	}
	return false
}

func includesConsoleKeyword(value string, keywords ...string) bool {
	for _, keyword := range keywords {
		if strings.Contains(value, keyword) {
			return true
		}
	}
	return false
}

func isConsolePausedLifecycleValue(value string) bool {
	normalized := normalizeConsoleText(value)
	return normalized != "" && includesConsoleKeyword(normalized, "disabled", "paused")
}

func isConsoleTerminalAppFailurePhase(app model.App) bool {
	normalized := normalizeConsoleText(app.Status.Phase)
	return normalized != "" && includesConsoleKeyword(normalized, "error", "fail", "stopped")
}

func buildConsoleProjectLifecycle(statuses []string, appCount, serviceCount int, tracksGitHub bool, hasLiveApp, hasPendingApp bool) consoleProjectLifecycle {
	normalized := make([]string, 0, len(statuses))
	for _, status := range statuses {
		value := normalizeConsoleText(status)
		if value != "" {
			normalized = append(normalized, value)
		}
	}

	switch {
	case containsConsoleStatus(normalized, "deleting"):
		return consoleProjectLifecycle{Label: "Deleting", Live: true, SyncMode: "active", Tone: "danger"}
	case containsAnyConsoleStatus(normalized, []string{"error", "fail", "stopped"}):
		return consoleProjectLifecycle{Label: "Error", Live: false, SyncMode: "passive", Tone: "danger"}
	case hasLiveApp && hasPendingApp:
		return consoleProjectLifecycle{Label: "Updating", Live: true, SyncMode: "active", Tone: "info"}
	case containsConsoleStatus(normalized, "importing"):
		return consoleProjectLifecycle{Label: "Importing", Live: true, SyncMode: "active", Tone: "positive"}
	case containsConsoleStatus(normalized, "building"):
		return consoleProjectLifecycle{Label: "Building", Live: true, SyncMode: "active", Tone: "positive"}
	case containsConsoleStatus(normalized, "deploying"):
		return consoleProjectLifecycle{Label: "Deploying", Live: true, SyncMode: "active", Tone: "positive"}
	case containsAnyConsoleStatus(normalized, []string{"queued", "pending", "migrating"}):
		return consoleProjectLifecycle{Label: "Queued", Live: true, SyncMode: "active", Tone: "positive"}
	case len(normalized) > 0 && everyConsoleStatusPaused(normalized):
		return consoleProjectLifecycle{Label: "Paused", Live: false, SyncMode: "idle", Tone: "warning"}
	case appCount > 0:
		syncMode := "idle"
		if tracksGitHub {
			syncMode = "passive"
		}
		return consoleProjectLifecycle{Label: "Running", Live: false, SyncMode: syncMode, Tone: "positive"}
	case serviceCount > 0:
		return consoleProjectLifecycle{Label: "Ready", Live: false, SyncMode: "idle", Tone: "positive"}
	default:
		return consoleProjectLifecycle{Label: "Idle", Live: false, SyncMode: "idle", Tone: "neutral"}
	}
}

func containsConsoleStatus(statuses []string, keyword string) bool {
	for _, status := range statuses {
		if strings.Contains(status, keyword) {
			return true
		}
	}
	return false
}

func containsAnyConsoleStatus(statuses []string, keywords []string) bool {
	for _, status := range statuses {
		if includesConsoleKeyword(status, keywords...) {
			return true
		}
	}
	return false
}

func everyConsoleStatusPaused(statuses []string) bool {
	if len(statuses) == 0 {
		return false
	}
	for _, status := range statuses {
		if !isConsolePausedLifecycleValue(status) {
			return false
		}
	}
	return true
}

func isConsoleOperationActive(status string) bool {
	switch normalizeConsoleText(status) {
	case "canceled", "cancelled", "completed", "failed":
		return false
	default:
		return true
	}
}

func readConsoleOperationTimestamp(operation model.Operation) time.Time {
	switch {
	case operation.CompletedAt != nil:
		return operation.CompletedAt.UTC()
	case !operation.UpdatedAt.IsZero():
		return operation.UpdatedAt.UTC()
	case operation.StartedAt != nil:
		return operation.StartedAt.UTC()
	default:
		return operation.CreatedAt.UTC()
	}
}

func isConsoleReleaseOperationCandidate(operation model.Operation) bool {
	normalizedType := normalizeConsoleText(operation.Type)
	normalizedStatus := normalizeConsoleText(operation.Status)
	return normalizedType == model.OperationTypeImport ||
		normalizedType == "build" ||
		normalizedType == model.OperationTypeDeploy ||
		strings.Contains(normalizedStatus, "import") ||
		strings.Contains(normalizedStatus, "build") ||
		strings.Contains(normalizedStatus, "deploy") ||
		operation.DesiredSource != nil
}

func hasConsoleLiveRelease(app model.App) bool {
	normalizedPhase := normalizeConsoleText(app.Status.Phase)
	if app.Status.CurrentReplicas > 0 {
		return true
	}

	return normalizedPhase != "" &&
		(includesConsoleKeyword(normalizedPhase, "running", "healthy", "active", "deployed") ||
			isConsolePausedLifecycleValue(normalizedPhase))
}

func readConsoleActiveReleaseOperation(operation *model.Operation, app model.App) *model.Operation {
	if operation == nil {
		return nil
	}
	if isConsoleTerminalAppFailurePhase(app) {
		return nil
	}

	normalizedType := normalizeConsoleText(operation.Type)
	normalizedStatus := normalizeConsoleText(operation.Status)
	desiredCommit := strings.TrimSpace(readConsoleOperationCommitSHA(operation))
	runningCommit := strings.TrimSpace(readConsoleAppCommitSHA(app))

	if normalizedType == model.OperationTypeImport || normalizedType == "build" || normalizedType == model.OperationTypeDeploy {
		return operation
	}
	if includesConsoleKeyword(normalizedStatus, "import", "build", "deploy") {
		return operation
	}
	if desiredCommit != "" && desiredCommit != runningCommit {
		return operation
	}
	if includesConsoleKeyword(normalizedStatus, "queued", "pending", "migrating", "running") && operation.DesiredSource != nil {
		return operation
	}

	return nil
}

func readConsolePendingServiceLabel(operation *model.Operation) string {
	if operation == nil {
		return "Pending"
	}

	normalizedStatus := normalizeConsoleText(operation.Status)
	normalizedType := normalizeConsoleText(operation.Type)

	switch {
	case includesConsoleKeyword(normalizedStatus, "queued", "pending"):
		return "Queued"
	case normalizedType == model.OperationTypeDeploy || strings.Contains(normalizedStatus, "deploy"):
		return "Deploying"
	case normalizedType == model.OperationTypeImport || normalizedType == "build" || includesConsoleKeyword(normalizedStatus, "build", "import"):
		return "Building"
	case strings.Contains(normalizedStatus, "running"):
		return "Updating"
	default:
		return humanizeConsole(operation.Status)
	}
}

func collectConsoleActiveOperations(operations []model.Operation) map[string]*model.Operation {
	sorted := append([]model.Operation(nil), operations...)
	sort.Slice(sorted, func(i, j int) bool {
		return readConsoleOperationTimestamp(sorted[i]).After(readConsoleOperationTimestamp(sorted[j]))
	})

	activeByAppID := make(map[string]*model.Operation)
	for index := range sorted {
		operation := &sorted[index]
		if strings.TrimSpace(operation.AppID) == "" || !isConsoleOperationActive(operation.Status) {
			continue
		}
		if _, ok := activeByAppID[operation.AppID]; ok {
			continue
		}
		activeByAppID[operation.AppID] = operation
	}

	return activeByAppID
}

func sumConsoleResourceUsage(items []*model.ResourceUsage) model.ResourceUsage {
	var (
		cpuMillicores         *int64
		memoryBytes           *int64
		ephemeralStorageBytes *int64
	)

	add := func(current **int64, next *int64) {
		if next == nil {
			return
		}
		if *current == nil {
			value := *next
			*current = &value
			return
		}
		value := **current + *next
		*current = &value
	}

	for _, item := range items {
		if item == nil {
			continue
		}
		add(&cpuMillicores, item.CPUMilliCores)
		add(&memoryBytes, item.MemoryBytes)
		add(&ephemeralStorageBytes, item.EphemeralStorageBytes)
	}

	return model.ResourceUsage{
		CPUMilliCores:         cpuMillicores,
		EphemeralStorageBytes: ephemeralStorageBytes,
		MemoryBytes:           memoryBytes,
	}
}

func readConsoleProjectSortTime(project *model.Project, apps []model.App, services []model.BackingService) time.Time {
	if project != nil && !project.CreatedAt.IsZero() {
		return project.CreatedAt.UTC()
	}

	var earliest time.Time
	update := func(candidate time.Time) {
		if candidate.IsZero() {
			return
		}
		if earliest.IsZero() || candidate.Before(earliest) {
			earliest = candidate
		}
	}

	for _, app := range apps {
		update(app.CreatedAt.UTC())
	}
	for _, service := range services {
		update(service.CreatedAt.UTC())
	}

	return earliest
}

func readConsoleProjectName(project *model.Project, projectID string) string {
	if project != nil && strings.TrimSpace(project.Name) != "" {
		return project.Name
	}
	if projectID == "unassigned" {
		return "Unassigned"
	}
	return humanizeConsole(projectID)
}

func visibleConsoleApps(apps []model.App) []model.App {
	visible := make([]model.App, 0, len(apps))
	for _, app := range apps {
		if normalizeConsoleText(app.Status.Phase) == "deleting" {
			continue
		}
		visible = append(visible, app)
	}
	return visible
}

func (s *Server) loadConsoleApps(ctx context.Context, principal model.Principal, includeLiveStatus bool, includeResourceUsage bool) ([]model.App, error) {
	apps, err := s.store.ListApps(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		return nil, err
	}
	visible := visibleConsoleApps(apps)
	if includeLiveStatus {
		visible = s.overlayManagedAppStatuses(ctx, visible)
	}
	if includeResourceUsage {
		visible = s.overlayCurrentResourceUsageOnApps(ctx, visible)
	}
	return sanitizeAppsForAPI(visible), nil
}

func (s *Server) buildConsoleGalleryResponse(ctx context.Context, principal model.Principal, includeLiveStatus bool) (consoleGalleryResponse, error) {
	timings := serverTimingFromContext(ctx)

	var (
		projects   []model.Project
		apps       []model.App
		operations []model.Operation
	)

	loadGroup, loadCtx := errgroup.WithContext(ctx)
	loadGroup.Go(func() error {
		startedAt := time.Now()
		result, err := s.store.ListProjects(principal.TenantID)
		timings.Add("store_projects", time.Since(startedAt))
		if err != nil {
			return err
		}
		projects = result
		return nil
	})
	loadGroup.Go(func() error {
		startedAt := time.Now()
		// The landing gallery only needs rollout / lifecycle state to render the
		// project list. Live resource usage overlays trigger a full cluster
		// inventory walk and turn the summary path into the slowest request in the
		// console. Keep the summary lean and let detailed views refresh usage on
		// demand.
		result, err := s.loadConsoleApps(loadCtx, principal, includeLiveStatus, false)
		timings.Add("console_apps", time.Since(startedAt))
		if err != nil {
			return err
		}
		apps = result
		return nil
	})
	loadGroup.Go(func() error {
		startedAt := time.Now()
		result, err := s.store.ListOperations(principal.TenantID, principal.IsPlatformAdmin())
		timings.Add("store_operations", time.Since(startedAt))
		if err != nil {
			return err
		}
		operations = result
		return nil
	})
	if err := loadGroup.Wait(); err != nil {
		return consoleGalleryResponse{}, err
	}

	activeOperationsByAppID := collectConsoleActiveOperations(operations)
	projectsByID := make(map[string]*model.Project, len(projects))
	projectApps := make(map[string][]model.App)

	for index := range projects {
		projectsByID[projects[index].ID] = &projects[index]
	}
	for _, app := range apps {
		projectID := strings.TrimSpace(app.ProjectID)
		if projectID == "" {
			projectID = "unassigned"
		}
		projectApps[projectID] = append(projectApps[projectID], app)
	}

	projectIDs := make([]string, 0, len(projectsByID)+len(projectApps))
	seenProjectIDs := make(map[string]struct{}, len(projectsByID)+len(projectApps))
	for _, project := range projects {
		if _, ok := seenProjectIDs[project.ID]; ok {
			continue
		}
		seenProjectIDs[project.ID] = struct{}{}
		projectIDs = append(projectIDs, project.ID)
	}
	for projectID := range projectApps {
		if _, ok := seenProjectIDs[projectID]; ok {
			continue
		}
		seenProjectIDs[projectID] = struct{}{}
		projectIDs = append(projectIDs, projectID)
	}

	records := make([]consoleProjectSummaryRecord, 0, len(projectIDs))
	for _, projectID := range projectIDs {
		appItems := projectApps[projectID]
		sort.Slice(appItems, func(i, j int) bool {
			left := appItems[i].Status.UpdatedAt
			if left.IsZero() {
				left = appItems[i].UpdatedAt
			}
			right := appItems[j].Status.UpdatedAt
			if right.IsZero() {
				right = appItems[j].UpdatedAt
			}
			if !left.Equal(right) {
				return left.After(right)
			}
			return appItems[i].ID < appItems[j].ID
		})

		backingServicesByID := make(map[string]model.BackingService)
		resourceUsageItems := make([]*model.ResourceUsage, 0, len(appItems)*2)
		statuses := make([]string, 0, len(appItems)*2)
		hasLiveApp := false
		hasPendingApp := false
		tracksGitHub := false
		serviceCount := 0

		for _, app := range appItems {
			if model.IsGitHubAppSourceType(readConsoleAppSourceType(app)) {
				tracksGitHub = true
			}

			activeOperation := readConsoleActiveReleaseOperation(activeOperationsByAppID[app.ID], app)
			liveRelease := hasConsoleLiveRelease(app)
			if liveRelease {
				hasLiveApp = true
			}
			if activeOperation != nil {
				hasPendingApp = true
			}

			if liveRelease || activeOperation == nil {
				serviceCount++
				statuses = append(statuses, app.Status.Phase)
			}

			if activeOperation != nil {
				serviceCount++
				statuses = append(statuses, readConsolePendingServiceLabel(activeOperation))
			}

			resourceUsageItems = append(resourceUsageItems, app.CurrentResourceUsage)
			for _, service := range app.BackingServices {
				backingServicesByID[service.ID] = service
			}
		}

		backingServices := make([]model.BackingService, 0, len(backingServicesByID))
		for _, service := range backingServicesByID {
			backingServices = append(backingServices, service)
			resourceUsageItems = append(resourceUsageItems, service.CurrentResourceUsage)
			statuses = append(statuses, service.Status)
		}

		serviceCount += len(backingServices)
		project := projectsByID[projectID]
		lifecycle := buildConsoleProjectLifecycle(
			statuses,
			len(appItems),
			serviceCount,
			tracksGitHub,
			hasLiveApp,
			hasPendingApp,
		)
		records = append(records, consoleProjectSummaryRecord{
			project: consoleProjectSummary{
				AppCount:              len(appItems),
				ID:                    projectID,
				Lifecycle:             lifecycle,
				Name:                  readConsoleProjectName(project, projectID),
				ResourceUsageSnapshot: sumConsoleResourceUsage(resourceUsageItems),
				ServiceBadges:         buildConsoleProjectServiceBadges(appItems),
				ServiceCount:          serviceCount,
			},
			sortAt: readConsoleProjectSortTime(project, appItems, backingServices),
		})
	}

	sort.Slice(records, func(i, j int) bool {
		if !records[i].sortAt.Equal(records[j].sortAt) {
			return records[i].sortAt.After(records[j].sortAt)
		}
		return records[i].project.ID < records[j].project.ID
	})

	response := consoleGalleryResponse{
		Projects: make([]consoleProjectSummary, 0, len(records)),
	}
	for _, record := range records {
		response.Projects = append(response.Projects, record.project)
	}

	return response, nil
}

func (s *Server) buildConsoleGalleryHash(ctx context.Context, principal model.Principal, includeLiveStatus bool) (string, error) {
	response, err := s.cachedConsoleGalleryResponse(ctx, principal, includeLiveStatus)
	if err != nil {
		return "", err
	}

	// The stream should only notify clients when the visible gallery summary
	// changes. Hashing raw apps / operations causes false positives from
	// metadata churn that never reaches the UI.
	data, err := json.Marshal(response)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:8]), nil
}

func (s *Server) loadConsoleProjectClusterNodes(
	ctx context.Context,
	principal model.Principal,
	projectID string,
	appIDs map[string]struct{},
	serviceIDs map[string]struct{},
) ([]model.ClusterNode, error) {
	timings := serverTimingFromContext(ctx)

	snapshots, err := s.loadClusterNodeInventory(ctx)
	if err != nil {
		return nil, consoleHTTPError{
			message: err.Error(),
			status:  http.StatusServiceUnavailable,
		}
	}

	syncStartedAt := time.Now()
	if err := s.syncManagedSharedLocationRuntimesFromSnapshots(snapshots); err != nil {
		return nil, err
	}
	timings.Add("runtime_sync", time.Since(syncStartedAt))

	managedSharedRuntime, err := s.store.GetRuntime(tenantSharedRuntimeID)
	if err != nil {
		return nil, err
	}
	_, defaultSharedDisplayRegion, _ := selectDefaultManagedSharedLocation(snapshots)

	storeNodesStartedAt := time.Now()
	runtimes, err := s.store.ListNodes(principal.TenantID, principal.IsPlatformAdmin())
	timings.Add("store_nodes", time.Since(storeNodesStartedAt))
	if err != nil {
		return nil, err
	}

	storeAppsStartedAt := time.Now()
	apps, err := s.store.ListApps(principal.TenantID, principal.IsPlatformAdmin())
	timings.Add("store_apps", time.Since(storeAppsStartedAt))
	if err != nil {
		return nil, err
	}

	storeServicesStartedAt := time.Now()
	services, err := s.store.ListBackingServices(principal.TenantID, principal.IsPlatformAdmin())
	timings.Add("store_services", time.Since(storeServicesStartedAt))
	if err != nil {
		return nil, err
	}

	runtimeByClusterNode := make(map[string]model.Runtime, len(runtimes))
	for _, runtimeObj := range runtimes {
		name := strings.TrimSpace(runtimeObj.ClusterNodeName)
		if name == "" {
			continue
		}
		if existing, ok := runtimeByClusterNode[name]; ok && existing.UpdatedAt.After(runtimeObj.UpdatedAt) {
			continue
		}
		runtimeByClusterNode[name] = runtimeObj
	}

	workloadResolver := newClusterWorkloadResolver(apps, services)
	resolvedSnapshots := make([]resolvedClusterNodeSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		resolvedSnapshots = append(resolvedSnapshots, resolvedClusterNodeSnapshot{
			snapshot:  snapshot,
			workloads: workloadResolver.resolve(snapshot.pods),
		})
	}
	resolvedSnapshots = collapseClusterNodeSnapshots(resolvedSnapshots, runtimeByClusterNode)

	filtered := make([]model.ClusterNode, 0, len(resolvedSnapshots))
	sharedSnapshots := make([]resolvedClusterNodeSnapshot, 0, len(resolvedSnapshots))
	for _, resolved := range resolvedSnapshots {
		snapshot := resolved.snapshot
		node := snapshot.node
		workloads := resolved.workloads
		runtimeObj, ok := runtimeByClusterNode[node.Name]
		var runtimeForNode *model.Runtime
		if ok {
			runtimeForNode = &runtimeObj
		}
		node.PublicIP = resolveClusterNodePublicIP(node, runtimeForNode)
		if ok {
			node.RuntimeID = runtimeObj.ID
			node.TenantID = runtimeObj.TenantID
		}
		node.Workloads = workloads
		if principal.IsPlatformAdmin() || ok {
			filtered = append(filtered, node)
			continue
		}
		if !snapshot.sharedPool && snapshot.runtimeID != "" && !strings.EqualFold(snapshot.runtimeID, tenantSharedRuntimeID) {
			continue
		}
		sharedSnapshots = append(sharedSnapshots, resolvedClusterNodeSnapshot{
			snapshot:  resolved.snapshot,
			workloads: workloads,
		})
	}

	if !principal.IsPlatformAdmin() {
		if sharedNode, ok := buildTenantSharedClusterNode(sharedSnapshots, managedSharedRuntime, defaultSharedDisplayRegion); ok {
			filtered = append(filtered, sharedNode)
		}
	}

	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].CreatedAt != nil && filtered[j].CreatedAt != nil && !filtered[i].CreatedAt.Equal(*filtered[j].CreatedAt) {
			return filtered[i].CreatedAt.Before(*filtered[j].CreatedAt)
		}
		return filtered[i].Name < filtered[j].Name
	})

	projectNodes := make([]model.ClusterNode, 0, len(filtered))
	for _, node := range filtered {
		workloads := make([]model.ClusterNodeWorkload, 0, len(node.Workloads))
		for _, workload := range node.Workloads {
			if workload.ProjectID != "" && workload.ProjectID == projectID {
				workloads = append(workloads, workload)
				continue
			}
			if _, ok := appIDs[workload.ID]; ok {
				workloads = append(workloads, workload)
				continue
			}
			if _, ok := serviceIDs[workload.ID]; ok {
				workloads = append(workloads, workload)
				continue
			}
		}
		if len(workloads) == 0 {
			continue
		}
		node.Workloads = workloads
		projectNodes = append(projectNodes, node)
	}

	return projectNodes, nil
}

func (s *Server) handleGetConsoleGallery(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	includeLiveStatus, err := readBoolQuery(r, "include_live_status", false)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	response, err := s.cachedConsoleGalleryResponse(
		r.Context(),
		principal,
		includeLiveStatus,
	)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) cachedConsoleGalleryResponse(
	ctx context.Context,
	principal model.Principal,
	includeLiveStatus bool,
) (consoleGalleryResponse, error) {
	return s.consoleGalleryCache.do(
		consoleGalleryCacheKey(principal, includeLiveStatus),
		func() (consoleGalleryResponse, error) {
			return s.buildConsoleGalleryResponse(ctx, principal, includeLiveStatus)
		},
	)
}

func consoleGalleryCacheKey(principal model.Principal, includeLiveStatus bool) string {
	return principalVisibilityCacheKey(principal) + "|live=" + strconv.FormatBool(includeLiveStatus)
}

func (s *Server) handleGetConsoleProject(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	projectID := strings.TrimSpace(r.PathValue("id"))
	if projectID == "" {
		httpx.WriteError(w, http.StatusBadRequest, "project id is required")
		return
	}
	includeLiveStatus, err := readBoolQuery(r, "include_live_status", false)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}

	projects, err := s.store.ListProjects(principal.TenantID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	projectByID := make(map[string]model.Project, len(projects))
	for _, project := range projects {
		projectByID[project.ID] = project
	}

	apps, err := s.loadConsoleApps(r.Context(), principal, includeLiveStatus, true)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	projectApps := make([]model.App, 0)
	appIDs := make(map[string]struct{})
	serviceIDs := make(map[string]struct{})
	for _, app := range apps {
		appProjectID := strings.TrimSpace(app.ProjectID)
		if appProjectID == "" {
			appProjectID = "unassigned"
		}
		if appProjectID != projectID {
			continue
		}
		projectApps = append(projectApps, app)
		appIDs[app.ID] = struct{}{}
		for _, service := range app.BackingServices {
			serviceIDs[service.ID] = struct{}{}
		}
	}

	project, hasProject := projectByID[projectID]
	if !hasProject && len(projectApps) == 0 {
		httpx.WriteError(w, http.StatusNotFound, "project not found")
		return
	}

	operations, err := s.store.ListOperations(principal.TenantID, principal.IsPlatformAdmin())
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	projectOperations := make([]model.Operation, 0)
	for _, operation := range sanitizeOperationsForAPI(operations) {
		if _, ok := appIDs[operation.AppID]; ok {
			projectOperations = append(projectOperations, operation)
		}
	}

	clusterNodes, err := s.loadConsoleProjectClusterNodes(r.Context(), principal, projectID, appIDs, serviceIDs)
	if err != nil {
		var httpErr consoleHTTPError
		if errors.As(err, &httpErr) {
			httpx.WriteError(w, httpErr.status, httpErr.message)
			return
		}
		s.writeStoreError(w, err)
		return
	}

	response := consoleProjectDetailResponse{
		Apps:         projectApps,
		ClusterNodes: clusterNodes,
		Operations:   projectOperations,
		ProjectID:    projectID,
		ProjectName:  readConsoleProjectName(valueOrNilProject(project, hasProject), projectID),
	}
	if hasProject {
		projectCopy := project
		response.Project = &projectCopy
	}

	httpx.WriteJSON(w, http.StatusOK, response)
}

func valueOrNilProject(project model.Project, ok bool) *model.Project {
	if !ok {
		return nil
	}
	projectCopy := project
	return &projectCopy
}

func (s *Server) handleStreamConsoleGallery(w http.ResponseWriter, r *http.Request) {
	principal := mustPrincipal(r)
	includeLiveStatus, err := readBoolQuery(r, "include_live_status", false)
	if err != nil {
		httpx.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	stream, err := newSSEWriter(w)
	if err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if err := stream.writeRetry(consoleGalleryStreamRetryMS); err != nil {
		return
	}

	hash, err := s.buildConsoleGalleryHash(r.Context(), principal, includeLiveStatus)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}

	cursorID := time.Now().UTC().Format(time.RFC3339Nano)
	if err := stream.writeEvent("ready", cursorID, consoleGalleryStreamEvent{Hash: hash}); err != nil {
		return
	}

	ticker := time.NewTicker(consoleGalleryStreamPollInterval)
	defer ticker.Stop()
	heartbeatTicker := time.NewTicker(consoleGalleryStreamHeartbeatInterval)
	defer heartbeatTicker.Stop()

	lastHash := hash
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeatTicker.C:
			cursorID = time.Now().UTC().Format(time.RFC3339Nano)
			if err := stream.writeEvent("heartbeat", cursorID, consoleGalleryStreamEvent{Hash: lastHash}); err != nil {
				return
			}
		case <-ticker.C:
			nextHash, err := s.buildConsoleGalleryHash(r.Context(), principal, includeLiveStatus)
			if err != nil {
				var httpErr consoleHTTPError
				if errors.As(err, &httpErr) {
					_ = stream.writeEvent("error", time.Now().UTC().Format(time.RFC3339Nano), map[string]string{"error": httpErr.message})
					return
				}
				_ = stream.writeEvent("error", time.Now().UTC().Format(time.RFC3339Nano), map[string]string{"error": err.Error()})
				return
			}
			if nextHash == lastHash {
				continue
			}
			lastHash = nextHash
			cursorID = time.Now().UTC().Format(time.RFC3339Nano)
			if err := stream.writeEvent("changed", cursorID, consoleGalleryStreamEvent{Hash: nextHash}); err != nil {
				return
			}
		}
	}
}
