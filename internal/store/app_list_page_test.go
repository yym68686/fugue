package store

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestAppPageWindowKeepsTenThousandRowsStableAndBounded(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2026, time.July, 12, 0, 0, 0, 0, time.UTC)
	for _, sortOrder := range []AppListSort{
		AppListSortCreatedAtDesc,
		AppListSortCreatedAtAsc,
		AppListSortUpdatedAtDesc,
		AppListSortNameAsc,
	} {
		t.Run(string(sortOrder), func(t *testing.T) {
			t.Parallel()
			apps := make([]model.App, 10_037)
			for index := range apps {
				apps[index] = model.App{
					ID:        fmt.Sprintf("app_%05d", index),
					Name:      fmt.Sprintf("service-%03d", index%97),
					CreatedAt: baseTime.Add(time.Duration(index%113) * time.Second),
					UpdatedAt: baseTime.Add(time.Duration(index%71) * time.Second),
				}
			}
			sort.Slice(apps, func(i, j int) bool {
				return compareAppPageOrder(apps[i], apps[j], sortOrder) < 0
			})

			const limit = 137
			seen := make(map[string]struct{}, len(apps))
			options := AppListPageOptions{Limit: limit, Sort: sortOrder}
			for {
				start, end := appPageWindow(apps, options)
				if end-start > limit {
					t.Fatalf("page exceeded limit: %d", end-start)
				}
				if start == end {
					break
				}
				for _, app := range apps[start:end] {
					if _, duplicate := seen[app.ID]; duplicate {
						t.Fatalf("duplicate app across pages: %s", app.ID)
					}
					seen[app.ID] = struct{}{}
				}
				last := apps[end-1]
				cursor := AppListPageCursor{
					Direction: AppListPageDirectionNext,
					ID:        last.ID,
					Name:      last.Name,
					Timestamp: last.CreatedAt,
				}
				if sortOrder == AppListSortUpdatedAtDesc {
					cursor.Timestamp = last.UpdatedAt
				}
				options.Cursor = &cursor
			}
			if len(seen) != len(apps) {
				t.Fatalf("pagination omitted rows: got %d want %d", len(seen), len(apps))
			}
		})
	}
}

func TestListAppsPagePreservesTenantProjectAndSoftDeleteFilters(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	now := time.Now().UTC()
	if err := stateStore.withLockedState(true, func(state *model.State) error {
		state.Apps = []model.App{
			{ID: "visible", TenantID: "tenant-a", ProjectID: "project-a", Name: "visible", Spec: model.AppSpec{Replicas: 1}, Status: model.AppStatus{Phase: "ready"}, CreatedAt: now, UpdatedAt: now},
			{ID: "wrong-project", TenantID: "tenant-a", ProjectID: "project-b", Name: "wrong-project", Spec: model.AppSpec{Replicas: 1}, Status: model.AppStatus{Phase: "ready"}, CreatedAt: now, UpdatedAt: now},
			{ID: "wrong-tenant", TenantID: "tenant-b", ProjectID: "project-a", Name: "wrong-tenant", Spec: model.AppSpec{Replicas: 1}, Status: model.AppStatus{Phase: "ready"}, CreatedAt: now, UpdatedAt: now},
			{ID: "deleted", TenantID: "tenant-a", ProjectID: "project-a", Name: "deleted", Status: model.AppStatus{Phase: "deleted"}, CreatedAt: now, UpdatedAt: now},
			{ID: "deleting", TenantID: "tenant-a", ProjectID: "project-a", Name: "deleting", Spec: model.AppSpec{Replicas: 1}, Status: model.AppStatus{Phase: "deleting"}, CreatedAt: now, UpdatedAt: now},
		}
		return nil
	}); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	page, err := stateStore.ListAppsPage(AppListPageOptions{
		Limit:              50,
		PlatformAdmin:      false,
		PrincipalProjectID: "project-a",
		Sort:               AppListSortCreatedAtDesc,
		TenantID:           "tenant-a",
	})
	if err != nil {
		t.Fatalf("list page: %v", err)
	}
	if len(page.Apps) != 1 || page.Apps[0].ID != "visible" {
		t.Fatalf("permission or soft-delete filter drifted: %+v", page.Apps)
	}
	if page.TotalItems != 1 {
		t.Fatalf("unexpected total %d", page.TotalItems)
	}
}

func TestListAppsPageBoundsTenThousandStoredApps(t *testing.T) {
	t.Parallel()

	stateStore := New(filepath.Join(t.TempDir(), "store.json"))
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	baseTime := time.Date(2026, time.July, 12, 0, 0, 0, 0, time.UTC)
	if err := stateStore.withLockedState(true, func(state *model.State) error {
		state.Apps = make([]model.App, 10_037)
		for index := range state.Apps {
			state.Apps[index] = model.App{
				ID:        fmt.Sprintf("app_%05d", index),
				TenantID:  "tenant-a",
				ProjectID: "project-a",
				Name:      fmt.Sprintf("service-%05d", index),
				Spec:      model.AppSpec{Replicas: 1},
				Status:    model.AppStatus{Phase: "ready"},
				CreatedAt: baseTime.Add(time.Duration(index%113) * time.Second),
				UpdatedAt: baseTime,
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed 10k apps: %v", err)
	}

	page, err := stateStore.ListAppsPage(AppListPageOptions{
		Limit:         137,
		PlatformAdmin: true,
		Sort:          AppListSortCreatedAtDesc,
	})
	if err != nil {
		t.Fatalf("list 10k app page: %v", err)
	}
	if len(page.Apps) != 137 {
		t.Fatalf("bounded page returned %d apps, want 137", len(page.Apps))
	}
	if page.TotalItems != 10_037 || !page.HasNextPage {
		t.Fatalf("unexpected page info: %+v", page)
	}
}
