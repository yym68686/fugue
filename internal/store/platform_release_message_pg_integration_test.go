package store

import (
	"errors"
	"strings"
	"testing"
)

func TestPlatformReleaseMessagePostgresLatestLookupUsesOrderedIndex(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	ctx := t.Context()
	for _, statement := range []string{
		`INSERT INTO fugue_platform_artifacts
(id,artifact_kind,scope_key,generation,status,content_hash,created_at,updated_at)
VALUES ('artifact-neutral','edge_route_bundle','scope-neutral','generation-neutral','validated','hash-neutral',now(),now())`,
		`INSERT INTO fugue_platform_artifact_releases
(id,artifact_id,artifact_kind,scope_key,generation,release_channel,status,released_at,created_at,updated_at)
SELECT id,'artifact-neutral','edge_route_bundle','scope-neutral','generation-neutral','full','released',now(),now(),now()
FROM unnest(ARRAY['release-target','release-neighbor']) AS id`,
		`INSERT INTO fugue_platform_release_messages
(id,release_id,artifact_id,artifact_kind,scope_key,generation,release_channel,message_type,created_at)
SELECT 'neighbor-' || i,'release-neighbor','artifact-neutral','edge_route_bundle','scope-neutral','generation-neutral','full','release',now()
FROM generate_series(1,20000) AS i`,
		`INSERT INTO fugue_platform_release_messages
(id,release_id,artifact_id,artifact_kind,scope_key,generation,release_channel,message_type,created_at)
VALUES
('target-old','release-target','artifact-neutral','edge_route_bundle','scope-neutral','generation-neutral','full','release','2026-01-01'),
('target-z','release-target','artifact-neutral','edge_route_bundle','scope-neutral','generation-neutral','full','release','2026-01-02'),
('target-a','release-target','artifact-neutral','edge_route_bundle','scope-neutral','generation-neutral','full','release','2026-01-02')`,
		`ANALYZE fugue_platform_release_messages`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	message, err := pgGetLatestPlatformReleaseMessage(ctx, db, "release-target")
	if err != nil || message.ID != "target-a" {
		t.Fatalf("latest message must preserve release isolation, timestamp and ID tie-break: id=%q err=%v", message.ID, err)
	}
	if _, err := pgGetLatestPlatformReleaseMessage(ctx, db, "release-missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing release must remain not found: %v", err)
	}
	// Explain the actual production query without disabling sequential scans:
	// both filtering and ordering must be satisfied by the bounded lookup.
	rows, err := db.QueryContext(ctx, "EXPLAIN (COSTS OFF) "+pgLatestPlatformReleaseMessageSQL, "release-target")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(lines, "\n")
	if !strings.Contains(plan, "idx_fugue_platform_messages_release_created_id") || strings.Contains(plan, "Sort") || strings.Contains(plan, "Seq Scan") {
		t.Fatalf("latest message lookup must use the release/time/ID index without scanning or sorting history:\n%s", plan)
	}
}
