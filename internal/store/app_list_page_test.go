package store

import (
	"testing"
	"time"
)

func TestListAppsPagePreservesTenantProjectAndSoftDeleteFilters(t *testing.T) {
	t.Parallel()
	s := newStoreTestDatabase(t, "")
	seedStoreTestIdentity(t, s, "tenant-a", "project-a", "visible")
	seedStoreTestIdentity(t, s, "tenant-a", "project-b", "wrong-project")
	seedStoreTestIdentity(t, s, "tenant-b", "project-foreign", "wrong-tenant")
	seedStoreTestIdentity(t, s, "tenant-a", "project-a", "deleted")
	seedStoreTestIdentity(t, s, "tenant-a", "project-a", "deleting")
	if _, err := s.db.ExecContext(t.Context(), `UPDATE fugue_apps SET status_json=jsonb_build_object('phase',id)
WHERE id IN ('deleted','deleting')`); err != nil {
		t.Fatal(err)
	}
	page, err := s.ListAppsPage(AppListPageOptions{Limit: 50, PrincipalProjectID: "project-a", Sort: AppListSortCreatedAtDesc, TenantID: "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Apps) != 1 || page.Apps[0].ID != "visible" || page.TotalItems != 1 {
		t.Fatalf("permission or soft-delete filter drifted: %+v", page)
	}
}

func TestListAppsPageBoundsTenThousandStoredApps(t *testing.T) {
	t.Parallel()
	s := newStoreTestDatabase(t, "")
	seedStoreTestIdentity(t, s, "tenant-a", "project-a", "")
	const count, limit = 10_037, 137
	base := time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC)
	if _, err := s.db.ExecContext(t.Context(), `INSERT INTO fugue_projects
 (id,tenant_id,name,slug,created_at,updated_at)
 SELECT 'page-project-' || g,'tenant-a','page-project-' || g,'page-project-' || g,now(),now()
 FROM generate_series(0,$1) g`, count/97); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(t.Context(), `INSERT INTO fugue_apps
 (id,tenant_id,project_id,name,spec_json,status_json,created_at,updated_at)
 SELECT 'app-' || g, 'tenant-a','page-project-' || (g / 97),'service-' || (g % 97), '{"replicas":1}', '{"phase":"ready"}',
  $2::timestamptz + ((g % 113) * interval '1 second'), $2::timestamptz + ((g % 71) * interval '1 second')
 FROM generate_series(1,$1) g`, count, base); err != nil {
		t.Fatal(err)
	}
	for _, sort := range []AppListSort{AppListSortCreatedAtDesc, AppListSortCreatedAtAsc, AppListSortUpdatedAtDesc, AppListSortNameAsc} {
		t.Run(string(sort), func(t *testing.T) {
			t.Parallel()
			seen := make(map[string]bool, count)
			options := AppListPageOptions{Limit: limit, PlatformAdmin: true, Sort: sort}
			for {
				page, err := s.ListAppsPage(options)
				if err != nil {
					t.Fatal(err)
				}
				if len(page.Apps) > limit || page.TotalItems != count || (options.Cursor == nil && (len(page.Apps) != limit || !page.HasNextPage)) {
					t.Fatalf("unexpected page: length=%d total=%d next=%t", len(page.Apps), page.TotalItems, page.HasNextPage)
				}
				for _, app := range page.Apps {
					if seen[app.ID] {
						t.Fatalf("duplicate app across pages: %s", app.ID)
					}
					seen[app.ID] = true
				}
				if !page.HasNextPage {
					break
				}
				if len(page.Apps) == 0 {
					t.Fatal("next page reported without a cursor row")
				}
				last := page.Apps[len(page.Apps)-1]
				cursor := AppListPageCursor{Direction: AppListPageDirectionNext, ID: last.ID, Name: last.Name, Timestamp: last.CreatedAt}
				if sort == AppListSortUpdatedAtDesc {
					cursor.Timestamp = last.UpdatedAt
				}
				options.Cursor = &cursor
			}
			if len(seen) != count {
				t.Fatalf("pagination omitted rows: got %d want %d", len(seen), count)
			}
		})
	}
}
