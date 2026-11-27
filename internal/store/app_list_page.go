package store

import (
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
	return s.pgListAppsPage(options)
}
