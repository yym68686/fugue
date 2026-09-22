package store

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformsafety"
	"fugue/internal/schemamigrate"
)

func TestPlatformRuntimeFactsFiltering(t *testing.T) { testPlatformRuntimeFactsFiltering(t, "") }
func TestPlatformRuntimeFactsFilteringPostgres(t *testing.T) {
	dsn := os.Getenv("FUGUE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable PostgreSQL not configured")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || !strings.Contains(u.Path, "fugue_test") {
		t.Fatal("disposable loopback PostgreSQL required")
	}
	testPlatformRuntimeFactsFiltering(t, dsn)
}

func testPlatformRuntimeFactsFiltering(t *testing.T, dsn string) {
	s := New(t.TempDir()+"/state.json", dsn)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	if s.db != nil {
		t.Cleanup(func() { s.db.Close() })
	}
	ctx := context.Background()
	prefix := model.NewID("facts")
	parent, baseline := prefix+"-parent", prefix+"-baseline"
	consumerID, instance := prefix+"-consumer", prefix+"-instance"
	at := time.Now().UTC().Truncate(time.Microsecond)
	consumer := model.PlatformConsumerInstance{ID: instance, ConsumerID: consumerID, ArtifactKind: "dns_answer_bundle", ScopeKey: "global", LastHeartbeatAt: at, UpdatedAt: at}
	if s.db != nil {
		_, err := s.db.Exec(`INSERT INTO fugue_platform_consumer_instances (id,consumer_id,artifact_kind,scope_key,last_heartbeat_at,updated_at) VALUES ($1,$2,$3,$4,$5,$5)`, instance, consumerID, consumer.ArtifactKind, consumer.ScopeKey, at)
		if err != nil {
			t.Fatal(err)
		}
	} else if err := s.withLockedState(true, func(st *model.State) error {
		st.PlatformConsumerInstances = append(st.PlatformConsumerInstances, consumer)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	events := []model.AuditEvent{}
	add := func(id, action, targetType, target string, meta map[string]string, when time.Time) {
		events = append(events, model.AuditEvent{ID: prefix + id, ActorType: "bootstrap", ActorID: "synthetic", Action: action, TargetType: targetType, TargetID: target, Metadata: meta, CreatedAt: when})
	}
	add("-01", "platform_config.gray_produced", "platform_release_set", parent, nil, at)
	add("-02", "platform_config.full_produced", "platform_release_set", parent, nil, at)
	add("-03", "platform_config.serving_verified", "platform_release_set", parent, nil, at)
	add("-04", "platform_config.serving_rolled_back", "platform_release_set", parent, map[string]string{"lkg_artifact_id": baseline}, at)
	add("-05", "platform_artifact.verified_lkg_promoted", "platform_artifact_release", prefix+"-release", map[string]string{"artifact_id": parent, "artifact_kind": "release_set"}, at)
	add("-06", "platform_artifact.full_released", "platform_artifact", parent, map[string]string{"artifact_kind": "release_set"}, at)
	add("-07", "platform_consumer.heartbeat_accepted", "platform_consumer", instance, map[string]string{"artifact_kind": "dns_answer_bundle", "release_set_id": parent}, at)
	keys := bundleauth.NewKeyring("synthetic-facts-key", "facts-key", "", "", nil)
	events[6].ChainID = prefix + "-chain"
	events[6].ChainSequence = 1
	signed, err := platformsafety.SignTamperEvidentAuditEvent(events[6], keys)
	if err != nil {
		t.Fatal(err)
	}
	events[6] = signed
	add("-08", "platform_consumer.heartbeat_accepted", "platform_consumer", prefix+"-new-instance", map[string]string{"consumer_id": consumerID, "artifact_kind": "dns_answer_bundle", "release_set_id": parent}, at)
	add("-09", "platform_config.full_produced", "app", parent, nil, at.Add(time.Second))
	add("-10", "platform_config.not_a_runtime_event", "platform_release_set", parent, nil, at.Add(time.Second))
	// More recent unrelated events used to hide all older matching facts.
	for i := 0; i < 30; i++ {
		add(fmt.Sprintf("-noise-%02d", i), "app.updated", "app", prefix+"-noise", nil, at.Add(time.Duration(i+1)*time.Second))
	}
	if s.db != nil {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range events {
			if err := s.pgAppendAuditEventTx(ctx, tx, e); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	} else if err := s.withLockedState(true, func(st *model.State) error { st.AuditEvents = append(st.AuditEvents, events...); return nil }); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		filter  PlatformRuntimeFactFilter
		indexes []int
	}{
		{"filter before limit", PlatformRuntimeFactFilter{ReleaseSetID: parent, ArtifactKind: "release_set", Limit: 2}, []int{5, 4}},
		{"producer and manual history", PlatformRuntimeFactFilter{ReleaseSetID: parent, ArtifactKind: "release_set", Limit: 20}, []int{5, 4, 3, 2, 1, 0}},
		{"all bound facts", PlatformRuntimeFactFilter{ReleaseSetID: parent, Limit: 20}, []int{7, 6, 5, 4, 3, 2, 1, 0}},
		{"rollback recovery target", PlatformRuntimeFactFilter{ReleaseSetID: baseline, Limit: 20}, []int{3}},
		{"canonical consumer history", PlatformRuntimeFactFilter{ConsumerID: consumerID, Limit: 20}, []int{7, 6}},
		{"historical instance", PlatformRuntimeFactFilter{ConsumerID: instance, Limit: 20}, []int{6}},
		{"combined", PlatformRuntimeFactFilter{ConsumerID: consumerID, ReleaseSetID: parent, ArtifactKind: "dns_answer_bundle", Limit: 1}, []int{7}},
		{"no consumer for publisher", PlatformRuntimeFactFilter{ConsumerID: consumerID, ArtifactKind: "release_set", Limit: 20}, nil},
		{"literal query string", PlatformRuntimeFactFilter{ReleaseSetID: parent + "' OR true --", Limit: 20}, nil},
		{"unknown", PlatformRuntimeFactFilter{ReleaseSetID: prefix + "-missing", Limit: 20}, nil},
	}
	if s.db != nil {
		if err := schemamigrate.MigratePlatformRuntimeFactIndexes(ctx, dsn); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.ExecContext(ctx, `INSERT INTO fugue_audit_events (id,actor_type,actor_id,action,target_type,target_id,metadata_json,created_at) SELECT $1||n::text,'bootstrap','synthetic','platform_consumer.heartbeat_accepted','platform_consumer','unrelated',jsonb_build_object('release_set_id','unrelated-'||(n/3)),NOW()+n*interval '1 second' FROM generate_series(1,100000)n`, prefix+"-bulk-"); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = s.db.Exec(`DELETE FROM fugue_audit_events WHERE left(id,length($1))=$1`, prefix+"-bulk-")
		})
		if _, err := s.db.Exec(`ANALYZE fugue_audit_events`); err != nil {
			t.Fatal(err)
		}
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			started := time.Now()
			got, err := s.ListPlatformRuntimeFacts(queryCtx, tt.filter)
			t.Logf("filtered query behind 100,000 newer heartbeats: %s", time.Since(started))
			if err != nil {
				t.Fatal(err)
			}
			want := []model.AuditEvent{}
			for _, i := range tt.indexes {
				want = append(want, events[i])
			}
			// SQL JSONB normalizes a nil metadata map to null/empty without changing
			// populated signed records. Compare serialized meaning for empty maps.
			for i := range want {
				if len(want[i].Metadata) == 0 {
					want[i].Metadata = nil
				}
			}
			for i := range got {
				got[i].CreatedAt = got[i].CreatedAt.UTC()
				if got[i].EventHash != "" && platformsafety.VerifyTamperEvidentAuditEvent(got[i], keys) != nil {
					t.Fatal("signature lost during projection")
				}
				if len(got[i].Metadata) == 0 {
					got[i].Metadata = nil
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("fact mismatch\ngot=%+v\nwant=%+v", got, want)
			}
		})
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ListPlatformRuntimeFacts(canceled, PlatformRuntimeFactFilter{}); err != context.Canceled {
		t.Fatal(err)
	}
	for _, limit := range []int{-1, 1001} {
		if _, err := s.ListPlatformRuntimeFacts(ctx, PlatformRuntimeFactFilter{Limit: limit}); err != ErrInvalidInput {
			t.Fatal("unbounded query accepted", limit, err)
		}
	}
	// Selection cannot rewrite signatures or mutate the historical source.
	var stored model.AuditEvent
	if s.db != nil {
		stored, err = scanAuditEvent(s.db.QueryRow(`SELECT id,tenant_id,actor_type,actor_id,action,target_type,target_id,metadata_json,chain_id,chain_sequence,previous_hash,event_hash,provenance_json,created_at FROM fugue_audit_events WHERE id=$1`, signed.ID))
	} else {
		err = s.withLockedState(false, func(st *model.State) error {
			for _, e := range st.AuditEvents {
				if e.ID == signed.ID {
					stored = e
				}
			}
			return nil
		})
	}
	stored.CreatedAt = stored.CreatedAt.UTC()
	if err != nil || !reflect.DeepEqual(stored, signed) {
		t.Fatal("query rewrote signed event", err)
	}
}
