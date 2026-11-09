package store

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	"fugue/internal/inputarchive"
)

func TestHotObligationsPostgresOriginalExpiryAndStateRootsShareSnapshot(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	seedHotInventoryHistory(t, db)
	if _, err := db.Exec(`INSERT INTO fugue_platform_artifact_releases(id,artifact_id,artifact_kind,scope_key,generation,release_channel,status,released_at,created_at,updated_at)
 VALUES('release-neutral','artifact-neutral-1','edge_route_bundle','scope-neutral','artifact-neutral-1','stable','active',now(),now(),now());
 INSERT INTO fugue_platform_release_messages(id,release_id,artifact_id,artifact_kind,scope_key,generation,release_channel,message_type,created_at,expires_at)
 VALUES('message-neutral','release-neutral','artifact-neutral-1','edge_route_bundle','scope-neutral','artifact-neutral-1','stable','publish',now(),now()+interval '2 days');
 INSERT INTO fugue_platform_consumer_instances(id,consumer_id,artifact_kind,scope_key,lkg_generation,last_heartbeat_at,updated_at)
 VALUES('consumer-neutral','identity-neutral','edge_route_bundle','scope-neutral','missing-generation',now()-interval '30 days',now());
 INSERT INTO fugue_platform_lkg_snapshot_history(id,artifact_id,artifact_kind,scope_key,generation,content_hash,expires_at,created_at,updated_at)
 SELECT 'long-expiry',artifact_id,artifact_kind,scope_key,generation,content_hash,now()+interval '5 days',now(),now() FROM fugue_platform_lkg_snapshots;
 INSERT INTO fugue_platform_lkg_snapshot_history(id,artifact_id,artifact_kind,scope_key,generation,content_hash,expires_at,created_at,updated_at)
 SELECT 'short-expiry',artifact_id,artifact_kind,scope_key,generation,content_hash,now()+interval '1 day',now(),now() FROM fugue_platform_lkg_snapshots;`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var snapshot string
	var originalExpiry time.Time
	if err = tx.QueryRow(`SELECT pg_current_snapshot()::text,expires_at FROM fugue_platform_release_messages WHERE id='message-neutral'`).Scan(&snapshot, &originalExpiry); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE fugue_platform_release_messages SET expires_at=now()-interval '1 day' WHERE id='message-neutral'`); err != nil {
		t.Fatal(err)
	}
	target := inputarchive.HotReference{Kind: "generation", ID: "missing-generation", ArtifactKind: "edge_route_bundle", ScopeKey: "scope-neutral"}
	report, err := compilerInputHotObligationsSnapshot(t.Context(), tx, []inputarchive.HotReference{target})
	if err == nil || report.TraversalComplete || !report.PathsComplete || !report.CoherentMetadata || !report.RootInventoryComplete || report.Snapshot != snapshot || report.AllSourcesComplete || report.BodyVerification || report.RetirementAuthorized || report.PlanSHA256 != "" {
		t.Fatal("partial metadata granted proof or lost RR", report, err)
	}
	foundMessage, foundConsumer, foundLKG := false, false, false
	for _, item := range report.Roots {
		switch item.Root.Source {
		case "active-message":
			foundMessage = item.ReleaseID == "release-neutral" && item.ExpiresAt != nil && item.ExpiresAt.Equal(originalExpiry)
		case "consumer-lkg":
			foundConsumer = item.ConsumerID == "identity-neutral" && item.LastHeartbeatAt.Before(report.ObservedAt.Add(-29*24*time.Hour)) && strings.Contains(item.Rule, "heartbeat-age-does-not-release")
		case "unexpired-lkg":
			foundLKG = item.ExpiresAt != nil && item.ExpiresAt.After(report.ObservedAt.Add(4*24*time.Hour))
		}
	}
	if !foundMessage || !foundConsumer || !foundLKG || len(report.Paths) != 1 || report.Roots[report.Paths[0].RootIndex].Root.Source != "consumer-lkg" || report.Paths[0].DeclaredHashesMatch || len(report.Obligations) != 1 || !report.Obligations[0].StateRootProtection || report.Obligations[0].StrongestRootKnown || report.Obligations[0].DisposalAuthorized {
		t.Fatal("original expiry/max duplicate expiry/stale root or unresolved path lost", report)
	}
	// A new snapshot sees the expiration, but the stale consumer still retains
	// its exact missing generation and the report remains incomplete.
	next, err := RunCompilerInputHotObligations(t.Context(), address, []inputarchive.HotReference{target})
	if err == nil || next.TraversalComplete || len(next.Paths) != 1 {
		t.Fatal("incomplete new snapshot accepted", next, err)
	}
	for _, item := range next.Roots {
		if item.Root.Source == "active-message" {
			t.Fatal("expired message predicate crossed snapshots", item)
		}
	}
}

func TestHotObligationsPostgresCannotAcceptOrdinaryIsolation(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	report, err := compilerInputHotObligationsSnapshot(t.Context(), tx, nil)
	if err == nil || report.CoherentMetadata || report.RootInventoryComplete || len(report.Roots) != 0 {
		t.Fatal("non RR inventory accepted", report, err)
	}
}
