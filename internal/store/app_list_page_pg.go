package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

const appListPageSelectColumns = `
SELECT a.id, a.tenant_id, a.project_id, a.name, a.description, a.source_json, a.route_json, a.spec_json, a.status_json, a.created_at, a.updated_at
FROM fugue_apps a
`

func (s *Store) pgListAppsPage(options AppListPageOptions) (AppListPage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	whereSQL, filterArgs := buildAppListPageWhere(options)
	var totalItems int64
	if err := s.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM fugue_apps a "+whereSQL,
		filterArgs...,
	).Scan(&totalItems); err != nil {
		return AppListPage{}, fmt.Errorf("count app page: %w", err)
	}

	query := appListPageSelectColumns + whereSQL
	args := append([]any(nil), filterArgs...)
	if options.Cursor != nil {
		predicate, values := appListCursorPredicate(options.Sort, *options.Cursor, len(args)+1)
		query += " AND " + predicate
		args = append(args, values...)
	}

	reverse := options.Cursor != nil && options.Cursor.Direction == AppListPageDirectionPrevious
	query += " ORDER BY " + appListPageOrderSQL(options.Sort, reverse)
	args = append(args, options.Limit+1)
	query += fmt.Sprintf(" LIMIT $%d", len(args))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return AppListPage{}, fmt.Errorf("list app page: %w", err)
	}
	defer rows.Close()

	apps := make([]model.App, 0, options.Limit+1)
	for rows.Next() {
		app, scanErr := scanApp(rows)
		if scanErr != nil {
			return AppListPage{}, scanErr
		}
		normalizeAppStatusForRead(&app)
		apps = append(apps, app)
	}
	if err := rows.Err(); err != nil {
		return AppListPage{}, fmt.Errorf("iterate app page: %w", err)
	}

	overflow := len(apps) > options.Limit
	if overflow {
		apps = apps[:options.Limit]
	}
	if reverse {
		for left, right := 0, len(apps)-1; left < right; left, right = left+1, right-1 {
			apps[left], apps[right] = apps[right], apps[left]
		}
	}
	if len(apps) > 0 {
		if err := s.pgHydrateAppsBackingServices(ctx, apps); err != nil {
			return AppListPage{}, err
		}
	}

	page := AppListPage{
		Apps:       apps,
		TotalItems: totalItems,
	}
	if reverse {
		page.HasPreviousPage = overflow
		page.HasNextPage = options.Cursor != nil
	} else {
		page.HasPreviousPage = options.Cursor != nil
		page.HasNextPage = overflow
	}
	return page, nil
}

func buildAppListPageWhere(options AppListPageOptions) (string, []any) {
	clauses := []string{
		`lower(trim(COALESCE(a.status_json->>'phase', ''))) NOT IN ('deleted', 'deleting')`,
		`NOT (
  (lower(a.name) LIKE '%-deleted' OR lower(a.name) LIKE '%-deleted-%')
  AND CASE WHEN COALESCE(a.spec_json->>'replicas', '') ~ '^[0-9]+$' THEN (a.spec_json->>'replicas')::integer ELSE 0 END <= 0
  AND CASE WHEN COALESCE(a.status_json->>'current_replicas', '') ~ '^[0-9]+$' THEN (a.status_json->>'current_replicas')::integer ELSE 0 END <= 0
  AND trim(COALESCE(a.status_json->>'current_runtime_id', '')) = ''
)`,
	}
	args := make([]any, 0, 8)
	addArg := func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}

	tenantID := strings.TrimSpace(options.TenantID)
	if tenantID != "" || !options.PlatformAdmin {
		clauses = append(clauses, "a.tenant_id = "+addArg(tenantID))
	}
	if principalProjectID := strings.TrimSpace(options.PrincipalProjectID); principalProjectID != "" && !options.PlatformAdmin {
		clauses = append(clauses, "a.project_id = "+addArg(principalProjectID))
	}
	if projectID := strings.TrimSpace(options.ProjectID); projectID != "" {
		clauses = append(clauses, "a.project_id = "+addArg(projectID))
	}
	if phase := strings.TrimSpace(options.Phase); phase != "" {
		clauses = append(clauses, "lower(trim(COALESCE(a.status_json->>'phase', ''))) = "+addArg(strings.ToLower(phase)))
	}
	if sourceRef := strings.TrimSpace(options.SourceRef); sourceRef != "" {
		clauses = append(clauses, "lower(COALESCE(a.source_json::text, '')) LIKE "+addArg("%"+strings.ToLower(sourceRef)+"%"))
	}
	if domain := strings.TrimSpace(options.Domain); domain != "" {
		pattern := addArg("%" + strings.ToLower(domain) + "%")
		clauses = append(clauses, `(
  lower(COALESCE(a.route_json->>'hostname', '')) LIKE `+pattern+`
  OR lower(COALESCE(a.route_json->>'public_url', '')) LIKE `+pattern+`
  OR lower(COALESCE(a.route_json->>'base_domain', '')) LIKE `+pattern+`
  OR EXISTS (
    SELECT 1
    FROM fugue_app_domains d
    WHERE d.app_id = a.id
      AND d.status = 'verified'
      AND lower(d.hostname) LIKE `+pattern+`
  )
)`)
	}
	if searchQuery := strings.TrimSpace(options.Query); searchQuery != "" {
		pattern := addArg("%" + strings.ToLower(searchQuery) + "%")
		clauses = append(clauses, `(
  lower(a.id) LIKE `+pattern+`
  OR lower(a.name) LIKE `+pattern+`
  OR lower(a.description) LIKE `+pattern+`
  OR lower(a.project_id) LIKE `+pattern+`
  OR lower(a.tenant_id) LIKE `+pattern+`
  OR lower(COALESCE(a.route_json::text, '')) LIKE `+pattern+`
  OR lower(COALESCE(a.source_json::text, '')) LIKE `+pattern+`
  OR EXISTS (
    SELECT 1
    FROM fugue_app_domains d
    WHERE d.app_id = a.id
      AND d.status = 'verified'
      AND lower(d.hostname) LIKE `+pattern+`
  )
)`)
	}

	return "WHERE " + strings.Join(clauses, " AND "), args
}

func appListCursorPredicate(sortOrder AppListSort, cursor AppListPageCursor, firstArg int) (string, []any) {
	direction := cursor.Direction
	switch sortOrder {
	case AppListSortCreatedAtAsc:
		operator := ">"
		if direction == AppListPageDirectionPrevious {
			operator = "<"
		}
		return fmt.Sprintf("(a.created_at, a.id) %s ($%d, $%d)", operator, firstArg, firstArg+1), []any{cursor.Timestamp, cursor.ID}
	case AppListSortUpdatedAtDesc:
		operator := "<"
		if direction == AppListPageDirectionPrevious {
			operator = ">"
		}
		return fmt.Sprintf("(a.updated_at, a.id) %s ($%d, $%d)", operator, firstArg, firstArg+1), []any{cursor.Timestamp, cursor.ID}
	case AppListSortNameAsc:
		operator := ">"
		if direction == AppListPageDirectionPrevious {
			operator = "<"
		}
		return fmt.Sprintf("(lower(a.name), a.id) %s ($%d, $%d)", operator, firstArg, firstArg+1), []any{strings.ToLower(cursor.Name), cursor.ID}
	default:
		operator := "<"
		if direction == AppListPageDirectionPrevious {
			operator = ">"
		}
		return fmt.Sprintf("(a.created_at, a.id) %s ($%d, $%d)", operator, firstArg, firstArg+1), []any{cursor.Timestamp, cursor.ID}
	}
}

func appListPageOrderSQL(sortOrder AppListSort, reverse bool) string {
	switch sortOrder {
	case AppListSortCreatedAtAsc:
		if reverse {
			return "a.created_at DESC, a.id DESC"
		}
		return "a.created_at ASC, a.id ASC"
	case AppListSortUpdatedAtDesc:
		if reverse {
			return "a.updated_at ASC, a.id ASC"
		}
		return "a.updated_at DESC, a.id DESC"
	case AppListSortNameAsc:
		if reverse {
			return "lower(a.name) DESC, a.id DESC"
		}
		return "lower(a.name) ASC, a.id ASC"
	default:
		if reverse {
			return "a.created_at ASC, a.id ASC"
		}
		return "a.created_at DESC, a.id DESC"
	}
}
