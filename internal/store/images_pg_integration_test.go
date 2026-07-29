package store

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestDemoteLegacyImagePostgresIntegration(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("FUGUE_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set FUGUE_TEST_DATABASE_URL to run image demotion Postgres integration test")
	}
	if !strings.Contains(databaseURL, "fugue-pgtest") && !strings.Contains(databaseURL, "fugue_test") {
		t.Fatalf("refusing to run image demotion integration test against non-test database URL %q", databaseURL)
	}

	stateStore := New("", databaseURL)
	if err := stateStore.Init(); err != nil {
		t.Fatalf("init Postgres store: %v", err)
	}
	t.Cleanup(func() { _ = stateStore.db.Close() })

	t.Run("available peer is locked through legacy demotion", func(t *testing.T) {
		legacyID, peerID, imageRef, digest := seedPostgresLegacyImagePair(
			t,
			stateStore,
			" AVAILABLE ",
		)
		defer deletePostgresImagePair(t, stateStore, legacyID, peerID)

		updated, err := stateStore.DemoteLegacyImageIfAvailableCanonicalPeer(legacyID, "", imageRef, digest)
		if err != nil {
			t.Fatalf("demote legacy image: %v", err)
		}
		if updated.CanonicalDigest != "" || updated.LifecycleState != model.ImageLifecycleLost {
			t.Fatalf("demoted legacy image = %+v", updated)
		}
	})

	for _, lifecycle := range []string{
		model.ImageLifecycleLost,
		model.ImageLifecycleDeleting,
		model.ImageLifecycleDeleted,
	} {
		lifecycle := lifecycle
		t.Run("non-serving peer "+lifecycle+" is rejected", func(t *testing.T) {
			legacyID, peerID, imageRef, digest := seedPostgresLegacyImagePair(t, stateStore, lifecycle)
			defer deletePostgresImagePair(t, stateStore, legacyID, peerID)

			if _, err := stateStore.DemoteLegacyImageIfAvailableCanonicalPeer(legacyID, "", imageRef, digest); !errors.Is(err, ErrConflict) {
				t.Fatalf("demotion error = %v, want conflict", err)
			}
			assertPostgresImageLifecycle(t, stateStore, legacyID, model.ImageLifecycleAvailable)
		})
	}

	t.Run("concurrent peer loss serializes before legacy demotion", func(t *testing.T) {
		legacyID, peerID, imageRef, digest := seedPostgresLegacyImagePair(
			t,
			stateStore,
			model.ImageLifecycleAvailable,
		)
		defer deletePostgresImagePair(t, stateStore, legacyID, peerID)
		demotionApplicationName := "fugue_image_demote_" + model.NewID("before")
		demotionStore := newPostgresIntegrationStoreWithApplicationName(
			t,
			databaseURL,
			demotionApplicationName,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		tx, err := stateStore.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin peer lifecycle transaction: %v", err)
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, `
UPDATE fugue_images
SET lifecycle_state = $2,
	updated_at = $3
WHERE id = $1
`, peerID, model.ImageLifecycleLost, time.Now().UTC()); err != nil {
			t.Fatalf("stage peer loss: %v", err)
		}

		type demotionResult struct {
			image model.Image
			err   error
		}
		resultCh := make(chan demotionResult, 1)
		go func() {
			image, err := demotionStore.DemoteLegacyImageIfAvailableCanonicalPeer(legacyID, "", imageRef, digest)
			resultCh <- demotionResult{image: image, err: err}
		}()

		waitForPostgresLockWait(t, stateStore, demotionApplicationName, "FOR SHARE")
		select {
		case result := <-resultCh:
			t.Fatalf("demotion completed while the peer lifecycle transition was uncommitted: image=%+v err=%v", result.image, result.err)
		default:
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit peer loss: %v", err)
		}

		select {
		case result := <-resultCh:
			if !errors.Is(result.err, ErrConflict) {
				t.Fatalf("demotion after peer loss = image=%+v err=%v, want conflict", result.image, result.err)
			}
		case <-ctx.Done():
			t.Fatalf("wait for serialized demotion: %v", ctx.Err())
		}
		assertPostgresImageLifecycle(t, stateStore, legacyID, model.ImageLifecycleAvailable)
		assertPostgresImageLifecycle(t, stateStore, peerID, model.ImageLifecycleLost)
	})

	t.Run("demotion peer lock serializes a later lifecycle update", func(t *testing.T) {
		legacyID, peerID, imageRef, digest := seedPostgresLegacyImagePair(
			t,
			stateStore,
			model.ImageLifecycleAvailable,
		)
		defer deletePostgresImagePair(t, stateStore, legacyID, peerID)
		demotionApplicationName := "fugue_image_demote_" + model.NewID("after")
		demotionStore := newPostgresIntegrationStoreWithApplicationName(
			t,
			databaseURL,
			demotionApplicationName,
		)
		updateApplicationName := "fugue_image_update_" + model.NewID("after")
		updateStore := newPostgresIntegrationStoreWithApplicationName(
			t,
			databaseURL,
			updateApplicationName,
		)

		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		legacyBlocker, err := stateStore.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin legacy blocker transaction: %v", err)
		}
		defer legacyBlocker.Rollback()
		var lockedLegacyID string
		if err := legacyBlocker.QueryRowContext(
			ctx,
			`SELECT id FROM fugue_images WHERE id = $1 FOR UPDATE`,
			legacyID,
		).Scan(&lockedLegacyID); err != nil {
			t.Fatalf("lock legacy image: %v", err)
		}

		type demotionResult struct {
			image model.Image
			err   error
		}
		demotionCh := make(chan demotionResult, 1)
		go func() {
			image, err := demotionStore.DemoteLegacyImageIfAvailableCanonicalPeer(legacyID, "", imageRef, digest)
			demotionCh <- demotionResult{image: image, err: err}
		}()
		waitForPostgresLockWait(t, stateStore, demotionApplicationName, "UPDATE fugue_images")

		updateCh := make(chan error, 1)
		go func() {
			_, err := updateStore.db.ExecContext(ctx, `
UPDATE fugue_images
SET lifecycle_state = $2,
	updated_at = $3
WHERE id = $1
`, peerID, model.ImageLifecycleLost, time.Now().UTC())
			updateCh <- err
		}()
		waitForPostgresLockWait(t, stateStore, updateApplicationName, "UPDATE fugue_images")

		select {
		case result := <-demotionCh:
			t.Fatalf("demotion completed while its legacy row was locked: image=%+v err=%v", result.image, result.err)
		default:
		}
		select {
		case err := <-updateCh:
			t.Fatalf("peer lifecycle update bypassed demotion's share lock: %v", err)
		default:
		}
		if err := legacyBlocker.Commit(); err != nil {
			t.Fatalf("release legacy blocker: %v", err)
		}

		select {
		case result := <-demotionCh:
			if result.err != nil || result.image.LifecycleState != model.ImageLifecycleLost {
				t.Fatalf("serialized demotion = image=%+v err=%v", result.image, result.err)
			}
		case <-ctx.Done():
			t.Fatalf("wait for demotion after releasing legacy row: %v", ctx.Err())
		}
		select {
		case err := <-updateCh:
			if err != nil {
				t.Fatalf("complete serialized peer lifecycle update: %v", err)
			}
		case <-ctx.Done():
			t.Fatalf("wait for peer lifecycle update: %v", ctx.Err())
		}
		assertPostgresImageLifecycle(t, stateStore, legacyID, model.ImageLifecycleLost)
		assertPostgresImageLifecycle(t, stateStore, peerID, model.ImageLifecycleLost)
	})
}

func newPostgresIntegrationStoreWithApplicationName(
	t *testing.T,
	databaseURL,
	applicationName string,
) *Store {
	t.Helper()
	parsed, err := url.Parse(databaseURL)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		t.Fatalf("parse Postgres test database URL %q: %v", databaseURL, err)
	}
	query := parsed.Query()
	query.Set("application_name", applicationName)
	parsed.RawQuery = query.Encode()
	stateStore := New("", parsed.String())
	if err := stateStore.ensureDatabaseReady(); err != nil {
		t.Fatalf("initialize Postgres test connection %q: %v", applicationName, err)
	}
	stateStore.db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = stateStore.db.Close() })
	return stateStore
}

func waitForPostgresLockWait(
	t *testing.T,
	observer *Store,
	applicationName,
	queryFragment string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		err := observer.db.QueryRowContext(ctx, `
SELECT EXISTS (
	SELECT 1
	FROM pg_stat_activity
	WHERE application_name = $1
	  AND state = 'active'
	  AND wait_event_type = 'Lock'
	  AND POSITION($2 IN query) > 0
)
`, applicationName, queryFragment).Scan(&waiting)
		if err != nil {
			t.Fatalf("observe Postgres lock wait for %q: %v", applicationName, err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf(
				"Postgres connection %q did not enter a %q lock wait: %v",
				applicationName,
				queryFragment,
				ctx.Err(),
			)
		case <-ticker.C:
		}
	}
}

func seedPostgresLegacyImagePair(
	t *testing.T,
	stateStore *Store,
	peerLifecycle string,
) (legacyID, peerID, imageRef, digest string) {
	t.Helper()
	suffix := model.NewID("image_demotion")
	legacyID = suffix + "_legacy"
	peerID = suffix + "_canonical"
	imageRef = "registry.fugue.internal:5000/fugue-apps/" + suffix + ":current"
	digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	now := time.Now().UTC()
	if _, err := stateStore.db.ExecContext(context.Background(), `
INSERT INTO fugue_images (
	id, tenant_id, app_id, image_ref, canonical_digest, lifecycle_state, created_at, updated_at
)
VALUES
	($1, NULL, '', $3, '', $5, $7, $7),
	($2, NULL, '', $3, $4, $6, $7, $7)
`, legacyID, peerID, imageRef, digest, model.ImageLifecycleAvailable, peerLifecycle, now); err != nil {
		t.Fatalf("seed Postgres image pair: %v", err)
	}
	return legacyID, peerID, imageRef, digest
}

func deletePostgresImagePair(t *testing.T, stateStore *Store, legacyID, peerID string) {
	t.Helper()
	if _, err := stateStore.db.ExecContext(
		context.Background(),
		`DELETE FROM fugue_images WHERE id = ANY($1::text[])`,
		[]string{legacyID, peerID},
	); err != nil {
		t.Errorf("delete Postgres image pair: %v", err)
	}
}

func assertPostgresImageLifecycle(t *testing.T, stateStore *Store, imageID, want string) {
	t.Helper()
	var got string
	if err := stateStore.db.QueryRowContext(
		context.Background(),
		`SELECT lifecycle_state FROM fugue_images WHERE id = $1`,
		imageID,
	).Scan(&got); err != nil {
		t.Fatalf("read Postgres image lifecycle: %v", err)
	}
	if got != want {
		t.Fatalf("image %s lifecycle = %q, want %q", imageID, got, want)
	}
}
