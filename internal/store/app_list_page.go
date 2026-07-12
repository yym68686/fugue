package store

import (
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

type AppListSort string

const (
	AppListSortCreatedAtDesc AppListSort = "created_at_desc"
	AppListSortCreatedAtAsc  AppListSort = "created_at_asc"
	AppListSortUpdatedAtDesc AppListSort = "updated_at_desc"
	AppListSortNameAsc       AppListSort = "name_asc"
)

type AppListPageDirection string

const (
	AppListPageDirectionNext     AppListPageDirection = "next"
	AppListPageDirectionPrevious AppListPageDirection = "previous"
)

type AppListPageCursor struct {
	Direction AppListPageDirection
	ID        string
	Name      string
	Timestamp time.Time
}

type AppListPageOptions struct {
	Cursor             *AppListPageCursor
	Domain             string
	Limit              int
	Phase              string
	PlatformAdmin      bool
	PrincipalProjectID string
	ProjectID          string
	Query              string
	Sort               AppListSort
	SourceRef          string
	TenantID           string
}

type AppListPage struct {
	Apps            []model.App
	HasNextPage     bool
	HasPreviousPage bool
	TotalItems      int64
}

func (s *Store) ListAppsPage(options AppListPageOptions) (AppListPage, error) {
	if s.usingDatabase() {
		return s.pgListAppsPage(options)
	}
	return s.listAppsPageFromState(options)
}

func (s *Store) listAppsPageFromState(options AppListPageOptions) (AppListPage, error) {
	var page AppListPage
	err := s.withLockedState(false, func(state *model.State) error {
		domainHostsByAppID := make(map[string][]string)
		if strings.TrimSpace(options.Query) != "" || strings.TrimSpace(options.Domain) != "" {
			for _, domain := range state.AppDomains {
				if domain.Status != model.AppDomainStatusVerified {
					continue
				}
				domainHostsByAppID[strings.TrimSpace(domain.AppID)] = append(
					domainHostsByAppID[strings.TrimSpace(domain.AppID)],
					strings.TrimSpace(domain.Hostname),
				)
			}
		}

		filtered := make([]model.App, 0, len(state.Apps))
		for _, candidate := range state.Apps {
			app := candidate
			normalizeAppStatusForRead(&app)
			if isDeletedApp(app) || strings.EqualFold(strings.TrimSpace(app.Status.Phase), "deleting") {
				continue
			}
			if !appMatchesPageOptions(app, options, domainHostsByAppID[strings.TrimSpace(app.ID)]) {
				continue
			}
			app.Bindings = nil
			app.BackingServices = nil
			filtered = append(filtered, app)
		}

		sort.Slice(filtered, func(i, j int) bool {
			return compareAppPageOrder(filtered[i], filtered[j], options.Sort) < 0
		})
		page.TotalItems = int64(len(filtered))

		start, end := appPageWindow(filtered, options)
		page.HasPreviousPage = start > 0
		page.HasNextPage = end < len(filtered)
		if start == end {
			page.Apps = []model.App{}
			return nil
		}

		page.Apps = append([]model.App(nil), filtered[start:end]...)
		backingServiceIndex := newAppBackingServiceIndex(state)
		for index := range page.Apps {
			hydrateAppBackingServicesWithIndex(backingServiceIndex, &page.Apps[index])
		}
		return nil
	})
	return page, err
}

func appPageWindow(apps []model.App, options AppListPageOptions) (int, int) {
	limit := options.Limit
	if limit < 1 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if options.Cursor == nil {
		return 0, min(limit, len(apps))
	}

	boundary := sort.Search(len(apps), func(index int) bool {
		return compareAppToPageCursor(apps[index], *options.Cursor, options.Sort) >= 0
	})
	if options.Cursor.Direction == AppListPageDirectionNext {
		for boundary < len(apps) && compareAppToPageCursor(apps[boundary], *options.Cursor, options.Sort) == 0 {
			boundary++
		}
		return boundary, min(boundary+limit, len(apps))
	}

	end := boundary
	start := max(0, end-limit)
	return start, end
}

func appMatchesPageOptions(app model.App, options AppListPageOptions, domainHosts []string) bool {
	if tenantID := strings.TrimSpace(options.TenantID); tenantID != "" && strings.TrimSpace(app.TenantID) != tenantID {
		return false
	}
	if !options.PlatformAdmin && strings.TrimSpace(app.TenantID) != strings.TrimSpace(options.TenantID) {
		return false
	}
	if projectID := strings.TrimSpace(options.PrincipalProjectID); !options.PlatformAdmin && projectID != "" && strings.TrimSpace(app.ProjectID) != projectID {
		return false
	}
	if projectID := strings.TrimSpace(options.ProjectID); projectID != "" && strings.TrimSpace(app.ProjectID) != projectID {
		return false
	}
	if phase := strings.TrimSpace(options.Phase); phase != "" && !strings.EqualFold(strings.TrimSpace(app.Status.Phase), phase) {
		return false
	}
	if domain := strings.TrimSpace(options.Domain); domain != "" && !appMatchesPageDomain(app, domain, domainHosts) {
		return false
	}
	if sourceRef := strings.TrimSpace(options.SourceRef); sourceRef != "" && !containsFold(appPageSourceFields(app), sourceRef) {
		return false
	}
	if query := strings.TrimSpace(options.Query); query != "" && !containsFold(appPageSearchFields(app, domainHosts), query) {
		return false
	}
	return true
}

func appMatchesPageDomain(app model.App, query string, domainHosts []string) bool {
	fields := append([]string(nil), domainHosts...)
	if app.Route != nil {
		fields = append(fields, app.Route.Hostname, app.Route.PublicURL, app.Route.BaseDomain)
	}
	return containsFold(fields, query)
}

func appPageSearchFields(app model.App, domainHosts []string) []string {
	fields := []string{app.ID, app.Name, app.Description, app.ProjectID, app.TenantID}
	if app.Route != nil {
		fields = append(fields, app.Route.Hostname, app.Route.PublicURL, app.Route.BaseDomain, app.Route.DomainName, app.Route.EntrypointName)
	}
	fields = append(fields, domainHosts...)
	fields = append(fields, appPageSourceFields(app)...)
	return fields
}

func appPageSourceFields(app model.App) []string {
	fields := make([]string, 0, 36)
	for _, source := range []*model.AppSource{app.Source, app.OriginSource, app.BuildSource} {
		if source == nil {
			continue
		}
		fields = append(fields,
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
			source.DockerfilePath,
			source.BuildContextDir,
			source.ImageNameSuffix,
			source.ComposeService,
			source.DetectedProvider,
			source.DetectedStack,
		)
	}
	return fields
}

func containsFold(fields []string, query string) bool {
	query = strings.ToLower(strings.TrimSpace(query))
	for _, field := range fields {
		if strings.Contains(strings.ToLower(strings.TrimSpace(field)), query) {
			return true
		}
	}
	return false
}

func compareAppPageOrder(left, right model.App, sortOrder AppListSort) int {
	switch sortOrder {
	case AppListSortCreatedAtAsc:
		if compared := left.CreatedAt.Compare(right.CreatedAt); compared != 0 {
			return compared
		}
		return strings.Compare(left.ID, right.ID)
	case AppListSortUpdatedAtDesc:
		if compared := right.UpdatedAt.Compare(left.UpdatedAt); compared != 0 {
			return compared
		}
		return strings.Compare(right.ID, left.ID)
	case AppListSortNameAsc:
		if compared := strings.Compare(strings.ToLower(left.Name), strings.ToLower(right.Name)); compared != 0 {
			return compared
		}
		return strings.Compare(left.ID, right.ID)
	default:
		if compared := right.CreatedAt.Compare(left.CreatedAt); compared != 0 {
			return compared
		}
		return strings.Compare(right.ID, left.ID)
	}
}

func compareAppToPageCursor(app model.App, cursor AppListPageCursor, sortOrder AppListSort) int {
	switch sortOrder {
	case AppListSortCreatedAtAsc:
		if compared := app.CreatedAt.Compare(cursor.Timestamp); compared != 0 {
			return compared
		}
		return strings.Compare(app.ID, cursor.ID)
	case AppListSortUpdatedAtDesc:
		if compared := cursor.Timestamp.Compare(app.UpdatedAt); compared != 0 {
			return compared
		}
		return strings.Compare(cursor.ID, app.ID)
	case AppListSortNameAsc:
		if compared := strings.Compare(strings.ToLower(app.Name), strings.ToLower(cursor.Name)); compared != 0 {
			return compared
		}
		return strings.Compare(app.ID, cursor.ID)
	default:
		if compared := cursor.Timestamp.Compare(app.CreatedAt); compared != 0 {
			return compared
		}
		return strings.Compare(cursor.ID, app.ID)
	}
}
