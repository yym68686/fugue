package store

import (
	"errors"
	"testing"

	"fugue/internal/model"
)

func TestDataDeletionProtectsReferences(t *testing.T) {
	t.Parallel()
	s := newStoreTestDatabase(t, "")
	ctx := t.Context()
	seedStoreTestIdentity(t, s, "tenant-owner", "project-owner", "app-owner")
	seedStoreTestIdentity(t, s, "tenant-other", "project-other", "app-other")
	if _, err := s.db.ExecContext(ctx, `INSERT INTO fugue_data_workspaces
	(id,tenant_id,name,slug,assets_json,created_at,updated_at)
	VALUES ('workspace-1','tenant-owner','training','training','[]',now(),now());
	INSERT INTO fugue_data_snapshots (id,tenant_id,workspace_id,version,manifest_digest,manifest_json,created_at)
	VALUES ('snapshot-1','tenant-owner','workspace-1','v1','digest','{}',now())`); err != nil {
		t.Fatal(err)
	}
	snapshot := &model.DataSnapshot{ID: "snapshot-1", Version: "v1"}
	cases := []struct {
		name, seed string
		snapshot   *model.DataSnapshot
		blocked    bool
	}{
		{"retained snapshot blocks workspace deletion", "", nil, true},
		{"snapshot alone does not block its own deletion", "", snapshot, false},
		{"active transfer", `INSERT INTO fugue_data_transfers
			(id,workspace_id,direction,status,created_at,updated_at)
			VALUES ('transfer','workspace-1','upload','running',now(),now())`, snapshot, true},
		{"valid grant", `INSERT INTO fugue_data_grants
			(id,workspace_id,snapshot_id,mode,token_hash,created_at,expires_at)
			VALUES ('grant','workspace-1','snapshot-1','read','sha',now(),now()+interval '1 hour')`, snapshot, true},
		{"deployed app", `UPDATE fugue_apps SET spec_json='{"data":{"workspaces":[{"workspace_id":"workspace-1","version":"v1"}]}}'
			WHERE id='app-owner'`, snapshot, true},
		{"queued intent", `INSERT INTO fugue_operations
			(id,tenant_id,app_id,type,status,execution_mode,requested_by_type,requested_by_id,desired_spec_json,created_at,updated_at)
			VALUES ('operation','tenant-owner','app-owner','deploy','pending','managed','system','fixture',
			 '{"data":{"workspaces":[{"workspace":"training","version":"latest"}]}}',now(),now())`, snapshot, true},
		{"unrelated and expired", `INSERT INTO fugue_data_transfers
			(id,workspace_id,direction,status,created_at,updated_at)
			VALUES ('transfer','workspace-1','upload','completed',now(),now());
			INSERT INTO fugue_data_grants (id,workspace_id,mode,token_hash,created_at,expires_at)
			VALUES ('grant','workspace-1','read','sha',now()-interval '2 hours',now()-interval '1 hour');
			UPDATE fugue_apps SET spec_json='{"data":{"workspaces":[{"workspace":"training"}]}}' WHERE id='app-other'`, snapshot, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tx, err := s.db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if c.seed != "" {
				if _, err := tx.ExecContext(t.Context(), c.seed); err != nil {
					t.Fatal(err)
				}
			}
			err = guardPGDataDeletion(t.Context(), tx, "workspace-1", c.snapshot)
			if (c.blocked && !errors.Is(err, ErrConflict)) || (!c.blocked && err != nil) {
				t.Fatalf("blocked=%t, got %v", c.blocked, err)
			}
		})
	}
}
