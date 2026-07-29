package store

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDemoteLegacyImageLocationPostgresIdentityCollisionIntegration(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run image location demotion Postgres integration test")
	}
	if !strings.Contains(databaseURL, "fugue-pgtest") && !strings.Contains(databaseURL, "fugue_test") {
		t.Fatalf("refusing to run image location demotion integration test against non-test database URL %q", databaseURL)
	}

	stateStore := New("", databaseURL)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init Postgres store: %v", err)
	}
	t.Cleanup(func() { _ = stateStore.db.Close() })

	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	suffix := model.NewID("image_location_demotion")
	legacyID := suffix + "_legacy"
	canonicalID := suffix + "_canonical"
	legacyAppID := "legacy-app-" + suffix
	canonicalAppID := "canonical-app-" + suffix
	imageRef := "registry.fugue.internal:5000/fugue-apps/" + suffix + "@" + digest
	expectedUpdatedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	observedAt := expectedUpdatedAt.Add(30 * time.Second)
	if _, err := stateStore.db.ExecContext(context.Background(), `
INSERT INTO fugue_image_locations (
	id, tenant_id, app_id, image_ref, digest, source_operation_id,
	node_id, runtime_id, cluster_node_name, cache_endpoint, status,
	last_seen_at, size_bytes, last_error, created_at, updated_at
)
VALUES
	($1, NULL, $3, $5, '', 'legacy-operation', $7, $8, $9, $10, $11, $12, 123, '', $13, $12),
	($2, NULL, $4, $5, $6, 'canonical-operation', $7, $8, $9, $10, $11, $12, 456, '', $13, $12)
`,
		legacyID,
		canonicalID,
		legacyAppID,
		canonicalAppID,
		imageRef,
		digest,
		"node-"+suffix,
		"runtime-"+suffix,
		"worker-"+suffix,
		"http://worker-"+suffix+":5000",
		model.ImageLocationStatusPresent,
		expectedUpdatedAt,
		expectedUpdatedAt.Add(-time.Hour),
	); err != nil {
		t.Fatalf("seed Postgres image location collision: %v", err)
	}
	t.Cleanup(func() {
		if _, err := stateStore.db.ExecContext(
			context.Background(),
			`DELETE FROM fugue_image_locations WHERE id = ANY($1::text[])`,
			[]string{legacyID, canonicalID},
		); err != nil {
			t.Errorf("delete Postgres image locations: %v", err)
		}
	})

	const reason = "superseded by canonical-digest image location"
	refreshedUpdatedAt := expectedUpdatedAt.Add(time.Second)
	if _, err := stateStore.db.ExecContext(
		context.Background(),
		`UPDATE fugue_image_locations SET updated_at = $2 WHERE id = $1`,
		legacyID,
		refreshedUpdatedAt,
	); err != nil {
		t.Fatalf("advance Postgres concurrent refresh fence: %v", err)
	}
	if _, err := stateStore.DemoteLegacyImageLocationIfUnchanged(
		legacyID,
		expectedUpdatedAt,
		observedAt,
		reason,
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("concurrent Postgres refresh compare-and-swap error = %v, want conflict", err)
	}
	var refreshedDigest, refreshedStatus string
	var observedRefreshedUpdatedAt time.Time
	if err := stateStore.db.QueryRowContext(context.Background(), `
SELECT digest, status, updated_at
FROM fugue_image_locations
WHERE id = $1
`, legacyID).Scan(
		&refreshedDigest,
		&refreshedStatus,
		&observedRefreshedUpdatedAt,
	); err != nil {
		t.Fatalf("read concurrently refreshed Postgres location: %v", err)
	}
	if refreshedDigest != "" || refreshedStatus != model.ImageLocationStatusPresent ||
		!observedRefreshedUpdatedAt.Equal(refreshedUpdatedAt) {
		t.Fatalf(
			"concurrently refreshed Postgres location changed: digest=%q status=%q updated_at=%v",
			refreshedDigest,
			refreshedStatus,
			observedRefreshedUpdatedAt,
		)
	}

	demoted, err := stateStore.DemoteLegacyImageLocationIfUnchanged(
		legacyID,
		refreshedUpdatedAt,
		observedAt,
		reason,
	)
	if err != nil {
		t.Fatalf("demote Postgres legacy location beside canonical identity: %v", err)
	}
	if demoted.ID != legacyID || demoted.AppID != legacyAppID || demoted.ImageRef != imageRef || demoted.Digest != "" ||
		demoted.Status != model.ImageLocationStatusMissing || demoted.LastError != reason {
		t.Fatalf("unexpected Postgres demoted location: %+v", demoted)
	}
	if demoted.LastSeenAt == nil || !demoted.LastSeenAt.Equal(observedAt) ||
		!demoted.UpdatedAt.After(refreshedUpdatedAt) {
		t.Fatalf("unexpected Postgres demotion timestamps: %+v", demoted)
	}

	var observedCanonicalAppID, canonicalDigest, canonicalStatus, canonicalLastError string
	var canonicalUpdatedAt time.Time
	if err := stateStore.db.QueryRowContext(context.Background(), `
SELECT app_id, digest, status, last_error, updated_at
FROM fugue_image_locations
WHERE id = $1
`, canonicalID).Scan(
		&observedCanonicalAppID,
		&canonicalDigest,
		&canonicalStatus,
		&canonicalLastError,
		&canonicalUpdatedAt,
	); err != nil {
		t.Fatalf("read canonical Postgres location: %v", err)
	}
	if observedCanonicalAppID != canonicalAppID || canonicalDigest != digest ||
		canonicalStatus != model.ImageLocationStatusPresent ||
		canonicalLastError != "" || !canonicalUpdatedAt.Equal(expectedUpdatedAt) {
		t.Fatalf(
			"canonical Postgres location changed: app_id=%q digest=%q status=%q error=%q updated_at=%v",
			observedCanonicalAppID,
			canonicalDigest,
			canonicalStatus,
			canonicalLastError,
			canonicalUpdatedAt,
		)
	}
}
