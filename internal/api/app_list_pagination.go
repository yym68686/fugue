package api

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

const (
	defaultAppListPageLimit = 50
	maxAppListPageLimit     = 200
	maxAppListCursorLength  = 1024
)

var errInvalidAppListCursor = errors.New("invalid or expired cursor; restart pagination without cursor")

type appListPagination struct {
	Enabled bool
	Options store.AppListPageOptions
	Scope   string
}

type appListCursorPayload struct {
	Direction string `json:"d"`
	ID        string `json:"id"`
	Key       string `json:"key"`
	Scope     string `json:"scope"`
	Sort      string `json:"sort"`
	Version   int    `json:"v"`
}

type appListPageInfo struct {
	HasNextPage     bool   `json:"has_next_page"`
	HasPreviousPage bool   `json:"has_previous_page"`
	Limit           int    `json:"limit"`
	NextCursor      string `json:"next_cursor,omitempty"`
	PreviousCursor  string `json:"previous_cursor,omitempty"`
	Sort            string `json:"sort"`
	TotalItems      int64  `json:"total_items"`
}

func readAppListPagination(r *http.Request, principal model.Principal, filter appListFilter) (appListPagination, error) {
	query := r.URL.Query()
	enabled := query.Has("limit") || query.Has("cursor") || query.Has("sort")
	if !enabled {
		return appListPagination{}, nil
	}

	limit, err := readIntQuery(r, "limit", defaultAppListPageLimit)
	if err != nil {
		return appListPagination{}, err
	}
	if limit < 1 || limit > maxAppListPageLimit {
		return appListPagination{}, fmt.Errorf("limit must be between 1 and %d", maxAppListPageLimit)
	}

	sortOrder, err := normalizeAppListSort(query.Get("sort"))
	if err != nil {
		return appListPagination{}, err
	}
	if err := validateAppListFilterLengths(filter); err != nil {
		return appListPagination{}, err
	}

	scope := appListCursorScope(principal, filter, sortOrder)
	options := store.AppListPageOptions{
		Domain:             filter.Domain,
		Limit:              limit,
		Phase:              filter.Phase,
		PlatformAdmin:      principal.IsPlatformAdmin(),
		PrincipalProjectID: strings.TrimSpace(principal.ProjectID),
		ProjectID:          filter.ProjectID,
		Query:              filter.Query,
		Sort:               sortOrder,
		SourceRef:          filter.SourceRef,
		TenantID:           filter.TenantID,
	}

	rawCursor := strings.TrimSpace(query.Get("cursor"))
	if rawCursor != "" {
		cursor, decodeErr := decodeAppListCursor(rawCursor, sortOrder, scope)
		if decodeErr != nil {
			return appListPagination{}, decodeErr
		}
		options.Cursor = &cursor
	}

	return appListPagination{Enabled: true, Options: options, Scope: scope}, nil
}

func normalizeAppListSort(raw string) (store.AppListSort, error) {
	switch store.AppListSort(strings.TrimSpace(raw)) {
	case "", store.AppListSortCreatedAtDesc:
		return store.AppListSortCreatedAtDesc, nil
	case store.AppListSortCreatedAtAsc:
		return store.AppListSortCreatedAtAsc, nil
	case store.AppListSortUpdatedAtDesc:
		return store.AppListSortUpdatedAtDesc, nil
	case store.AppListSortNameAsc:
		return store.AppListSortNameAsc, nil
	default:
		return "", errors.New("sort must be one of created_at_desc, created_at_asc, updated_at_desc, or name_asc")
	}
}

func validateAppListFilterLengths(filter appListFilter) error {
	limits := []struct {
		label string
		limit int
		value string
	}{
		{label: "q", limit: 200, value: filter.Query},
		{label: "project_id", limit: 256, value: filter.ProjectID},
		{label: "domain", limit: 253, value: filter.Domain},
		{label: "source_ref", limit: 512, value: filter.SourceRef},
		{label: "phase", limit: 64, value: filter.Phase},
	}
	for _, item := range limits {
		if len(item.value) > item.limit {
			return fmt.Errorf("%s must be at most %d characters", item.label, item.limit)
		}
	}
	return nil
}

func appListCursorScope(principal model.Principal, filter appListFilter, sortOrder store.AppListSort) string {
	canonical := strings.Join([]string{
		fmt.Sprintf("admin=%t", principal.IsPlatformAdmin()),
		"principal_tenant=" + strings.TrimSpace(principal.TenantID),
		"principal_project=" + strings.TrimSpace(principal.ProjectID),
		"tenant=" + strings.TrimSpace(filter.TenantID),
		"project=" + strings.TrimSpace(filter.ProjectID),
		"q=" + strings.ToLower(strings.TrimSpace(filter.Query)),
		"domain=" + strings.ToLower(strings.TrimSpace(filter.Domain)),
		"source=" + strings.ToLower(strings.TrimSpace(filter.SourceRef)),
		"phase=" + strings.ToLower(strings.TrimSpace(filter.Phase)),
		"sort=" + string(sortOrder),
	}, "\n")
	digest := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(digest[:16])
}

func decodeAppListCursor(raw string, sortOrder store.AppListSort, scope string) (store.AppListPageCursor, error) {
	if len(raw) > maxAppListCursorLength {
		return store.AppListPageCursor{}, errInvalidAppListCursor
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || len(decoded) > maxAppListCursorLength {
		return store.AppListPageCursor{}, errInvalidAppListCursor
	}
	var payload appListCursorPayload
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return store.AppListPageCursor{}, errInvalidAppListCursor
	}
	if payload.Version != 1 || payload.Scope != scope || payload.Sort != string(sortOrder) || strings.TrimSpace(payload.ID) == "" {
		return store.AppListPageCursor{}, errInvalidAppListCursor
	}

	cursor := store.AppListPageCursor{
		Direction: store.AppListPageDirection(payload.Direction),
		ID:        strings.TrimSpace(payload.ID),
	}
	if cursor.Direction != store.AppListPageDirectionNext && cursor.Direction != store.AppListPageDirectionPrevious {
		return store.AppListPageCursor{}, errInvalidAppListCursor
	}
	if sortOrder == store.AppListSortNameAsc {
		cursor.Name = payload.Key
		if strings.TrimSpace(cursor.Name) == "" {
			return store.AppListPageCursor{}, errInvalidAppListCursor
		}
		return cursor, nil
	}

	timestamp, err := time.Parse(time.RFC3339Nano, payload.Key)
	if err != nil {
		return store.AppListPageCursor{}, errInvalidAppListCursor
	}
	cursor.Timestamp = timestamp.UTC()
	return cursor, nil
}

func encodeAppListCursor(app model.App, direction store.AppListPageDirection, sortOrder store.AppListSort, scope string) string {
	key := app.CreatedAt.UTC().Format(time.RFC3339Nano)
	switch sortOrder {
	case store.AppListSortUpdatedAtDesc:
		key = app.UpdatedAt.UTC().Format(time.RFC3339Nano)
	case store.AppListSortNameAsc:
		key = strings.ToLower(app.Name)
	}
	payload, _ := json.Marshal(appListCursorPayload{
		Direction: string(direction),
		ID:        app.ID,
		Key:       key,
		Scope:     scope,
		Sort:      string(sortOrder),
		Version:   1,
	})
	return base64.RawURLEncoding.EncodeToString(payload)
}

func buildAppListPageInfo(page store.AppListPage, pagination appListPagination) appListPageInfo {
	info := appListPageInfo{
		HasNextPage:     page.HasNextPage,
		HasPreviousPage: page.HasPreviousPage,
		Limit:           pagination.Options.Limit,
		Sort:            string(pagination.Options.Sort),
		TotalItems:      page.TotalItems,
	}
	if len(page.Apps) == 0 {
		return info
	}
	if page.HasNextPage {
		info.NextCursor = encodeAppListCursor(
			page.Apps[len(page.Apps)-1],
			store.AppListPageDirectionNext,
			pagination.Options.Sort,
			pagination.Scope,
		)
	}
	if page.HasPreviousPage {
		info.PreviousCursor = encodeAppListCursor(
			page.Apps[0],
			store.AppListPageDirectionPrevious,
			pagination.Options.Sort,
			pagination.Scope,
		)
	}
	return info
}
