package store

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestNodeDeepHealthEnforcementReadsRemainFreshAndOrdered(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		name := "file"
		if postgres {
			name = "postgres"
		}
		t.Run(name, func(t *testing.T) {
			s := New(filepath.Join(t.TempDir(), "state.json"))
			if postgres {
				s = billingBatchPGStore(t)
				if err := s.ensureDatabaseReady(); err != nil {
					t.Fatal(err)
				}
			}
			now := time.Now().UTC().Truncate(time.Microsecond)
			prefix := model.NewID("health_read")
			expiry := now.Add(-time.Hour)
			values := []model.NodeDeepHealthResult{
				{NodeUpdaterID: prefix + "-observed", ObservedOnly: true, QuarantineState: model.NodeQuarantineStateQuarantined, ReportedAt: now, UpdatedAt: now.Add(time.Second)},
				{NodeUpdaterID: prefix + "-b", ClusterNodeName: "node-b", RuntimeID: "runtime-b", MachineID: "machine-b", QuarantineState: model.NodeQuarantineStateQuarantined, QuarantineReason: "failed check", ReportedAt: now, UpdatedAt: now},
				{NodeUpdaterID: prefix + "-a", QuarantineState: model.NodeQuarantineStateClear, ReportedAt: now.Add(-time.Hour), UpdatedAt: now},
				{NodeUpdaterID: prefix + "-expired", QuarantineState: model.NodeQuarantineStateQuarantined, QuarantineExpiresAt: &expiry, ReportedAt: now, UpdatedAt: now.Add(-time.Second)},
			}
			for i := range values {
				values[i].OverallStatus = model.NodeDeepHealthStatusFail
				values[i].RecoveryConditions = []string{"fresh check required"}
				values[i].Checks = []model.NodeDeepHealthCheck{{Name: "synthetic", Status: model.NodeDeepHealthStatusFail, Message: strings.Repeat("evidence ", 1024), CheckedAt: now}}
			}
			persist := func() {
				t.Helper()
				if !postgres {
					if err := s.withLockedState(true, func(st *model.State) error {
						st.NodeDeepHealthResults = append([]model.NodeDeepHealthResult(nil), values...)
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					return
				}
				for _, v := range values {
					checks, _ := json.Marshal(v.Checks)
					recovery, _ := json.Marshal(v.RecoveryConditions)
					_, err := s.db.Exec(`INSERT INTO fugue_node_deep_health_results(node_updater_id,cluster_node_name,runtime_id,machine_id,observed_only,overall_status,quarantine_state,quarantine_reason,quarantine_expires_at,recovery_conditions_json,checks_json,reported_at,updated_at)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12,$13)
      ON CONFLICT(node_updater_id) DO UPDATE SET observed_only=EXCLUDED.observed_only,quarantine_state=EXCLUDED.quarantine_state,quarantine_expires_at=EXCLUDED.quarantine_expires_at,reported_at=EXCLUDED.reported_at,updated_at=EXCLUDED.updated_at`,
						v.NodeUpdaterID, v.ClusterNodeName, v.RuntimeID, v.MachineID, v.ObservedOnly, v.OverallStatus, v.QuarantineState, v.QuarantineReason, v.QuarantineExpiresAt, recovery, checks, v.ReportedAt, v.UpdatedAt)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			if postgres {
				t.Cleanup(func() {
					for _, v := range values {
						if _, err := s.db.Exec("DELETE FROM fugue_node_deep_health_results WHERE node_updater_id=$1", v.NodeUpdaterID); err != nil {
							t.Error(err)
						}
					}
				})
			}
			verify := func(wantCount int) []model.NodeDeepHealthResult {
				t.Helper()
				all, err := s.ListNodeDeepHealthResults()
				if err != nil {
					t.Fatal(err)
				}
				want := []model.NodeDeepHealthResult{}
				for _, v := range all {
					if !v.ObservedOnly {
						want = append(want, v)
					}
				}
				got, err := s.ListNodeDeepHealthEnforcementCandidates()
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatal("filtered read changed report contents, membership or ordering")
				}
				own := []model.NodeDeepHealthResult{}
				for _, v := range got {
					if strings.HasPrefix(v.NodeUpdaterID, prefix) {
						own = append(own, v)
					}
				}
				if len(own) != wantCount {
					t.Fatalf("membership=%d,want%d", len(own), wantCount)
				}
				observedFound := false
				for _, v := range all {
					if v.NodeUpdaterID == values[0].NodeUpdaterID {
						observedFound = true
					}
				}
				if !observedFound {
					t.Fatal("enforcement read removed report from full inventory")
				}
				return own
			}
			persist()
			got := verify(3)
			if got[0].NodeUpdaterID != prefix+"-a" || got[1].NodeUpdaterID != prefix+"-b" || got[2].QuarantineExpiresAt == nil {
				t.Fatal("tie order or expired candidate lost")
			}
			values[0].ObservedOnly = false
			values[1].ObservedOnly = true
			values[2].QuarantineState = model.NodeQuarantineStateQuarantined
			values[2].ReportedAt = now
			persist()
			got = verify(3)
			if got[0].NodeUpdaterID != values[0].NodeUpdaterID || got[1].QuarantineState != model.NodeQuarantineStateQuarantined || !got[1].ReportedAt.Equal(now) {
				t.Fatal("next call reused old authority or freshness")
			}
			for i := range values {
				values[i].ObservedOnly = true
			}
			persist()
			verify(0)
		})
	}
}
