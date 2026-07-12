package store

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestPostgresAppPageQueryPlanUsesBoundedSortIndex(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run app pagination query-plan integration test")
	}

	stateStore := New("", databaseURL)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init postgres store: %v", err)
	}
	defer stateStore.db.Close()

	page, err := stateStore.ListAppsPage(AppListPageOptions{
		Limit:         50,
		PlatformAdmin: true,
		Sort:          AppListSortCreatedAtDesc,
	})
	if err != nil {
		t.Fatalf("execute bounded app page query: %v", err)
	}
	if len(page.Apps) > 50 {
		t.Fatalf("page exceeded limit: %d", len(page.Apps))
	}

	tx, err := stateStore.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin explain transaction: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("SET LOCAL enable_seqscan = off"); err != nil {
		t.Fatalf("disable sequential scan for index viability check: %v", err)
	}
	rows, err := tx.Query(`
EXPLAIN (COSTS OFF)
SELECT id
FROM fugue_apps
WHERE lower(trim(COALESCE(status_json->>'phase', ''))) NOT IN ('deleted', 'deleting')
ORDER BY created_at DESC, id DESC
LIMIT 51
`)
	if err != nil {
		t.Fatalf("explain app page: %v", err)
	}
	defer rows.Close()
	planLines := make([]string, 0)
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan explain row: %v", err)
		}
		planLines = append(planLines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate explain rows: %v", err)
	}
	plan := strings.Join(planLines, "\n")
	if !strings.Contains(plan, "idx_fugue_apps_created_id_desc") {
		t.Fatalf("expected created/id pagination index, plan:\n%s", plan)
	}
}
