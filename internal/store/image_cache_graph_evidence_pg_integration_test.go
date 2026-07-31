package store

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/imagecacheevidence"
	"fugue/internal/model"
)

func TestImageCacheGraphEvidencePostgresIntegration(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run image-cache graph evidence Postgres integration test")
	}
	parsedDatabaseURL, err := url.Parse(databaseURL)
	if err != nil || parsedDatabaseURL.Scheme == "" || parsedDatabaseURL.Host == "" {
		t.Fatalf("image-cache graph evidence integration requires an absolute Postgres URL: %v", err)
	}
	databaseName := strings.Trim(parsedDatabaseURL.Path, "/")
	hostname := parsedDatabaseURL.Hostname()
	loopback := hostname == "127.0.0.1" || hostname == "localhost" || hostname == "::1"
	if !loopback || (!strings.Contains(databaseName, "fugue-pgtest") && !strings.Contains(databaseName, "fugue_test")) {
		t.Fatalf("refusing non-disposable Postgres integration target host=%q database=%q", hostname, databaseName)
	}

	schemaName := model.NewID("image_cache_graph")
	quotedSchema := quotePostgresIntegrationIdentifier(t, schemaName)
	schemaURL := postgresIntegrationURLWithSearchPath(t, databaseURL, schemaName)
	baseDB, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatalf("open disposable Postgres database: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := baseDB.PingContext(ctx); err != nil {
		_ = baseDB.Close()
		t.Fatalf("ping disposable Postgres database: %v", err)
	}
	if _, err := baseDB.ExecContext(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		_ = baseDB.Close()
		t.Fatalf("create disposable Postgres schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if _, err := baseDB.ExecContext(cleanupCtx, "DROP SCHEMA IF EXISTS "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop disposable Postgres schema: %v", err)
		}
		if err := baseDB.Close(); err != nil {
			t.Errorf("close disposable Postgres database: %v", err)
		}
	})

	legacyDB, err := sql.Open("pgx", schemaURL)
	if err != nil {
		t.Fatalf("open legacy-schema Postgres connection: %v", err)
	}
	t.Cleanup(func() { _ = legacyDB.Close() })
	if err := legacyDB.PingContext(ctx); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("ping legacy-schema Postgres connection: %v", err)
	}
	if _, err := legacyDB.ExecContext(ctx, `
CREATE TABLE fugue_image_cache_manifests (
	id TEXT PRIMARY KEY,
	node_id TEXT NOT NULL DEFAULT '',
	cluster_node_name TEXT NOT NULL DEFAULT '',
	runtime_id TEXT NOT NULL DEFAULT '',
	image_ref TEXT NOT NULL DEFAULT '',
	repo TEXT NOT NULL,
	target TEXT NOT NULL,
	digest TEXT NOT NULL DEFAULT '',
	media_type TEXT NOT NULL DEFAULT '',
	manifest_size_bytes BIGINT NOT NULL DEFAULT 0,
	total_blob_bytes BIGINT NOT NULL DEFAULT 0,
	referenced_blobs_json JSONB NULL,
	created_at_observed TIMESTAMPTZ NULL,
	last_seen_at TIMESTAMPTZ NOT NULL,
	pinned_locally BOOLEAN NOT NULL DEFAULT FALSE,
	present BOOLEAN NOT NULL DEFAULT TRUE,
	created_at TIMESTAMPTZ NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL
)
`); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("create legacy image-cache manifest table: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	const (
		legacyID     = "imgcacheman_legacy_graph_default"
		legacyNodeID = "machine-legacy-graph"
		legacyDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		legacyBlob   = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	if _, err := legacyDB.ExecContext(ctx, `
INSERT INTO fugue_image_cache_manifests (
	id, node_id, cluster_node_name, runtime_id, image_ref, repo, target,
	digest, media_type, manifest_size_bytes, total_blob_bytes,
	referenced_blobs_json, created_at_observed, last_seen_at,
	pinned_locally, present, created_at, updated_at
) VALUES (
	$1, $2, 'worker-legacy-graph', 'runtime-legacy-graph',
	'registry.fugue.internal:5000/fugue-apps/demo:legacy',
	'fugue-apps/demo', 'legacy', $3,
	'application/vnd.oci.image.manifest.v1+json', 12, 180,
	$4::jsonb, $5, $5, FALSE, TRUE, $5, $5
)
`, legacyID, legacyNodeID, legacyDigest, `["`+legacyBlob+`"]`, now); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("seed legacy image-cache manifest row: %v", err)
	}

	stateStore := New("", schemaURL)
	if err := stateStore.Init(); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("apply additive image-cache graph evidence migration: %v", err)
	}
	t.Cleanup(func() { _ = stateStore.db.Close() })

	var oldStatus, oldReason string
	var oldManifestBytes, oldBlobBytes int64
	if err := legacyDB.QueryRowContext(ctx, `
SELECT graph_status, graph_failure_reason, manifest_size_bytes, total_blob_bytes
FROM fugue_image_cache_manifests
WHERE id = $1
`, legacyID).Scan(&oldStatus, &oldReason, &oldManifestBytes, &oldBlobBytes); err != nil {
		_ = stateStore.db.Close()
		_ = legacyDB.Close()
		t.Fatalf("read migrated legacy image-cache manifest: %v", err)
	}
	if oldStatus != imagecacheevidence.GraphStatusComplete || oldReason != "" || oldManifestBytes != 12 || oldBlobBytes != 180 {
		_ = stateStore.db.Close()
		_ = legacyDB.Close()
		t.Fatalf("legacy manifest migration changed evidence: status=%q reason=%q manifest=%d blob=%d", oldStatus, oldReason, oldManifestBytes, oldBlobBytes)
	}

	const (
		incompleteNodeID = "machine-incomplete-graph"
		incompleteDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		incompleteBlob   = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	)
	if _, err := stateStore.UpsertImageCacheInventory(model.ImageCacheNodeInventory{
		NodeID:           incompleteNodeID,
		ClusterNodeName:  "worker-incomplete-graph",
		RuntimeID:        "runtime-incomplete-graph",
		ObservedAt:       now.Add(time.Second),
		SnapshotComplete: true,
	}, []model.ImageCacheManifest{{
		Repo:               "fugue-apps/demo",
		Target:             "incomplete",
		Digest:             incompleteDigest,
		ManifestSizeBytes:  999,
		TotalBlobBytes:     999,
		ReferencedBlobs:    []string{incompleteBlob},
		GraphStatus:        imagecacheevidence.GraphStatusIncomplete,
		GraphFailureReason: imagecacheevidence.ReasonMissingBlob,
		LastSeenAt:         now.Add(time.Second),
		Present:            true,
	}}); err != nil {
		_ = stateStore.db.Close()
		_ = legacyDB.Close()
		t.Fatalf("upsert incomplete image-cache graph evidence: %v", err)
	}

	defaultList, err := stateStore.ListImageCacheManifests(model.ImageCacheManifestFilter{
		NodeID:      incompleteNodeID,
		PresentOnly: true,
	})
	if err != nil {
		_ = stateStore.db.Close()
		_ = legacyDB.Close()
		t.Fatalf("list default image-cache manifests: %v", err)
	}
	if len(defaultList) != 0 {
		_ = stateStore.db.Close()
		_ = legacyDB.Close()
		t.Fatalf("default Postgres query exposed incomplete graph evidence: %+v", defaultList)
	}
	assertPostgresIncompleteGraphEvidence(t, stateStore, incompleteNodeID)

	if err := stateStore.db.Close(); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("close first Postgres store: %v", err)
	}
	reopened := New("", schemaURL)
	if err := reopened.Init(); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("reopen Postgres store after graph evidence migration: %v", err)
	}
	t.Cleanup(func() { _ = reopened.db.Close() })
	assertPostgresIncompleteGraphEvidence(t, reopened, incompleteNodeID)
	legacy, err := reopened.ListImageCacheManifests(model.ImageCacheManifestFilter{
		NodeID:      legacyNodeID,
		PresentOnly: true,
	})
	if err != nil {
		_ = reopened.db.Close()
		_ = legacyDB.Close()
		t.Fatalf("list migrated legacy manifest after reopen: %v", err)
	}
	if len(legacy) != 1 || legacy[0].GraphStatus != imagecacheevidence.GraphStatusComplete || legacy[0].GraphFailureReason != "" || legacy[0].ManifestSizeBytes != 12 || legacy[0].TotalBlobBytes != 180 || len(legacy[0].ReferencedBlobs) != 1 || legacy[0].ReferencedBlobs[0] != legacyBlob {
		_ = reopened.db.Close()
		_ = legacyDB.Close()
		t.Fatalf("migrated legacy manifest did not survive reopen: %+v", legacy)
	}
	if err := reopened.db.Close(); err != nil {
		_ = legacyDB.Close()
		t.Fatalf("close reopened Postgres store: %v", err)
	}
	if err := legacyDB.Close(); err != nil {
		t.Fatalf("close legacy-schema Postgres connection: %v", err)
	}
}

func assertPostgresIncompleteGraphEvidence(t *testing.T, stateStore *Store, nodeID string) {
	t.Helper()

	manifests, err := stateStore.ListImageCacheManifests(model.ImageCacheManifestFilter{
		NodeID:            nodeID,
		PresentOnly:       true,
		IncludeIncomplete: true,
	})
	if err != nil {
		t.Fatalf("list Postgres incomplete image-cache graph evidence: %v", err)
	}
	if len(manifests) != 1 {
		t.Fatalf("Postgres incomplete graph evidence count = %d, want 1: %+v", len(manifests), manifests)
	}
	manifest := manifests[0]
	if manifest.GraphStatus != imagecacheevidence.GraphStatusIncomplete || manifest.GraphFailureReason != imagecacheevidence.ReasonMissingBlob || manifest.ManifestSizeBytes != 0 || manifest.TotalBlobBytes != 0 || len(manifest.ReferencedBlobs) != 0 {
		t.Fatalf("Postgres incomplete graph evidence retained countable bytes: %+v", manifest)
	}
}

func postgresIntegrationURLWithSearchPath(t *testing.T, databaseURL, schema string) string {
	t.Helper()

	parsed, err := url.Parse(databaseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		t.Fatalf("Postgres integration URL must be absolute: %v", err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func quotePostgresIntegrationIdentifier(t *testing.T, value string) string {
	t.Helper()

	if value == "" {
		t.Fatal("Postgres integration identifier is empty")
	}
	for _, char := range value {
		if char != '_' && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			t.Fatalf("unsafe Postgres integration identifier %q", value)
		}
	}
	return `"` + value + `"`
}
