package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

func activeRecovery(records []model.AppRecoveryRecord, appID string) bool {
	for _, r := range records {
		if r.AppID == appID && r.Active {
			return true
		}
	}
	return false
}

func deleteAppRecoveryHistory(records []model.AppRecoveryRecord, tenantID, appID string) []model.AppRecoveryRecord {
	out := make([]model.AppRecoveryRecord, 0, len(records))
	for _, r := range records {
		if tenantID != "" && r.TenantID == tenantID || appID != "" && r.AppID == appID {
			continue
		}
		out = append(out, r)
	}
	return out
}

// Reserve two independent app instances, subtracting the already accounted
// replicas. Backing databases are shared and must not be counted twice. This
// affects admission and available capacity only, never ledger activation.
func tenantRecoveryReservedResources(st *model.State, tenantID string) model.BillingResourceSpec {
	var total model.BillingResourceSpec
	if st == nil {
		return total
	}
	for _, app := range st.Apps {
		if app.TenantID != tenantID || isDeletedApp(app) || !(app.Status.RecoveryCapacityReserved || activeRecovery(st.AppRecoveries, app.ID)) {
			continue
		}
		for replicas := app.Status.CurrentReplicas; replicas < 2; replicas++ {
			total = addResourceSpec(total, appEffectiveResources(app.Spec))
		}
	}
	return total
}

func validateRecoveryCapacity(st *model.State, app model.App, record model.TenantBilling) error {
	current := addResourceSpec(tenantManagedCommittedResourcesForBilling(st, record), tenantRecoveryReservedResources(st, app.TenantID))
	projection := cloneTenantBillingState(st)
	i := findApp(&projection, app.ID)
	if i < 0 {
		return ErrNotFound
	}
	projection.Apps[i].Status.RecoveryCapacityReserved = true
	next := addResourceSpec(tenantManagedCommittedResourcesForBilling(&projection, record), tenantRecoveryReservedResources(&projection, app.TenantID))
	return validateTenantOperationBilling(record, current, next, 0, 0)
}

func (s *Store) ListAppRecoveries(appID string) ([]model.AppRecoveryRecord, error) {
	out := []model.AppRecoveryRecord{}
	if s.usingDatabase() {
		if err := s.ensureDatabaseReady(); err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rows, err := s.db.QueryContext(ctx, `SELECT record_json FROM fugue_app_recoveries WHERE app_id=$1 ORDER BY id DESC`, appID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var raw []byte
			var r model.AppRecoveryRecord
			if err := rows.Scan(&raw); err != nil {
				return nil, err
			}
			if err := json.Unmarshal(raw, &r); err != nil {
				return nil, err
			}
			out = append(out, r)
		}
		return out, rows.Err()
	}
	err := s.withLockedState(false, func(st *model.State) error {
		for _, r := range st.AppRecoveries {
			if r.AppID == appID {
				out = append(out, r)
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, err
}

func (s *Store) AppRecoveryHeld(appID string) (bool, error) {
	if s.usingDatabase() {
		if err := s.ensureDatabaseReady(); err != nil {
			return false, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return pgRecoveryHeld(ctx, s.db, appID)
	}
	var held bool
	err := s.withLockedState(false, func(st *model.State) error { held = activeRecovery(st.AppRecoveries, appID); return nil })
	return held, err
}

func pgRecoveryHeld(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, appID string) (bool, error) {
	var held bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_app_recoveries WHERE app_id=$1 AND active)`, appID).Scan(&held)
	return held, err
}

func (s *Store) recoveryGuardedTx(ctx context.Context, appID string) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err = s.pgGetAppTx(ctx, tx, appID, true); err != nil {
		tx.Rollback()
		return nil, mapDBErr(err)
	}
	held, err := pgRecoveryHeld(ctx, tx, appID)
	if err != nil || held {
		tx.Rollback()
		if err != nil {
			return nil, err
		}
		return nil, ErrConflict
	}
	return tx, nil
}

// Serialize intent, hold, operations and cutover on the same app row. The file
// backend uses its existing state lock, with identical transition logic.
func (s *Store) mutateAppRecovery(appID string, traffic bool, fn func(*model.State) error) error {
	if !s.usingDatabase() {
		return s.withLockedState(true, fn)
	}
	if err := s.ensureDatabaseReady(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	app, err := s.pgGetAppTx(ctx, tx, appID, true)
	if err != nil {
		return mapDBErr(err)
	}
	st := model.State{Apps: []model.App{app}}
	rows, err := tx.QueryContext(ctx, `SELECT record_json FROM fugue_app_recoveries WHERE app_id=$1 FOR UPDATE`, appID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var raw []byte
		var r model.AppRecoveryRecord
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return err
		}
		if err := json.Unmarshal(raw, &r); err != nil {
			rows.Close()
			return err
		}
		st.AppRecoveries = append(st.AppRecoveries, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	rows, err = tx.QueryContext(ctx, `SELECT id, status FROM fugue_operations WHERE app_id=$1 AND status IN ('pending','running','waiting-agent')`, appID)
	if err != nil {
		return err
	}
	for rows.Next() {
		var op model.Operation
		op.AppID = appID
		if err := rows.Scan(&op.ID, &op.Status); err != nil {
			rows.Close()
			return err
		}
		st.Operations = append(st.Operations, op)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if traffic {
		rows, err = tx.QueryContext(ctx, `SELECT `+appReleaseSelectColumns+` FROM fugue_app_releases WHERE app_id=$1 FOR UPDATE`, appID)
		if err != nil {
			return err
		}
		for rows.Next() {
			r, e := scanAppRelease(rows)
			if e != nil {
				rows.Close()
				return e
			}
			st.AppReleases = append(st.AppReleases, r)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		p, e := scanAppTrafficPolicy(tx.QueryRowContext(ctx, `SELECT `+appTrafficPolicySelectColumns+` FROM fugue_app_traffic_policies WHERE app_id=$1 FOR UPDATE`, appID))
		if e == nil {
			st.AppTrafficPolicies = append(st.AppTrafficPolicies, p)
		} else if e != sql.ErrNoRows {
			return e
		}
	}
	beforeJSON, _ := json.Marshal(st)
	var before model.State
	if err := json.Unmarshal(beforeJSON, &before); err != nil {
		return err
	}
	if err := fn(&st); err != nil {
		return err
	}
	if !activeRecovery(before.AppRecoveries, appID) && activeRecovery(st.AppRecoveries, appID) {
		now := time.Now().UTC()
		billing, err := s.pgEnsureTenantBillingRecordTx(ctx, tx, app.TenantID, true, now)
		if err != nil {
			return err
		}
		capacityState, err := s.pgLoadTenantBillingStateTx(ctx, tx, app.TenantID)
		if err != nil {
			return err
		}
		if err := validateRecoveryCapacity(&capacityState, app, billing); err != nil {
			return err
		}
	}
	if !reflect.DeepEqual(before.Apps, st.Apps) {
		if err := s.pgUpdateAppTx(ctx, tx, st.Apps[0]); err != nil {
			return err
		}
	}
	for _, r := range st.AppRecoveries {
		raw, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO fugue_app_recoveries (id,app_id,active,record_json) VALUES ($1,$2,$3,$4) ON CONFLICT(id) DO UPDATE SET active=EXCLUDED.active,record_json=EXCLUDED.record_json`, r.ID, r.AppID, r.Active, raw); err != nil {
			return err
		}
	}
	if traffic {
		for _, r := range st.AppReleases {
			i := findAppReleaseByID(before.AppReleases, r.ID)
			if i < 0 {
				_, err = s.pgCreateAppReleaseTx(ctx, tx, r)
			} else if !reflect.DeepEqual(before.AppReleases[i], r) {
				_, err = s.pgUpdateAppReleaseTx(ctx, tx, r)
			}
			if err != nil {
				return err
			}
		}
		for _, p := range st.AppTrafficPolicies {
			if _, err := s.pgUpsertAppTrafficPolicyTx(ctx, tx, p); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (s *Store) StartAppRecovery(app model.App, req model.AppRecoveryStartRequest) (model.AppRecoveryRecord, error) {
	if err := req.Validate(app.Spec); err != nil {
		return model.AppRecoveryRecord{}, fmt.Errorf("%w: %s", ErrInvalidInput, err)
	}
	var out model.AppRecoveryRecord
	err := s.mutateAppRecovery(app.ID, false, func(st *model.State) error {
		i := findApp(st, app.ID)
		if i < 0 {
			return ErrNotFound
		}
		current := st.Apps[i]
		if model.AppSpecSHA256(current.Spec) != model.AppSpecSHA256(app.Spec) || current.TenantID != app.TenantID || activeRecovery(st.AppRecoveries, app.ID) || hasInFlightOperationForApp(st.Operations, app.ID) {
			return ErrConflict
		}
		if err := req.Validate(current.Spec); err != nil {
			return ErrInvalidInput
		}
		now := time.Now().UTC()
		if !s.usingDatabase() {
			billing := ensureTenantBillingRecord(st, current.TenantID, now)
			if err := validateRecoveryCapacity(st, current, *billing); err != nil {
				return err
			}
		}
		out = model.AppRecoveryRecord{AppRecovery: model.AppRecovery{ID: model.NewID("recovery"), AppID: app.ID, Active: true, Phase: "pending", EvidenceStatus: "pending", CreatedAt: now, UpdatedAt: now}, Version: 1, TenantID: app.TenantID, SourceSpec: current.Spec, Request: req, Deadline: now.Add(time.Duration(req.TimeoutSeconds) * time.Second)}
		st.AppRecoveries = append(st.AppRecoveries, out)
		return nil
	})
	return out, err
}

func (s *Store) SaveAppRecovery(previous, next model.AppRecoveryRecord, cutover bool) (model.AppRecoveryRecord, error) {
	err := s.mutateAppRecovery(previous.AppID, cutover || !next.Active, func(st *model.State) error {
		for i, r := range st.AppRecoveries {
			if r.ID != previous.ID {
				continue
			}
			if r.Version != previous.Version || !r.Active {
				return ErrConflict
			}
			if !next.Active && r.CutoverAt != nil && r.SourceReleaseID != "" {
				if ri := findAppReleaseByID(st.AppReleases, r.SourceReleaseID); ri >= 0 {
					old := st.AppReleases[ri]
					old.Role = model.AppReleaseRoleRetired
					old.Status = model.AppReleaseStatusRetired
					now := time.Now().UTC()
					old.RetiredAt = &now
					old.UpdatedAt = now
					st.AppReleases[ri] = old
				}
			}
			if cutover {
				if r.Phase != "probing" || len(next.ProbeResults) != len(r.Request.Probes) || len(next.ProbeResults) == 0 || !reflect.DeepEqual(next.ReplacementSpec, r.ReplacementSpec) {
					return ErrConflict
				}
				for _, probe := range next.ProbeResults {
					if probe.Status != model.AppReleaseGateStatusPass {
						return ErrConflict
					}
				}
				ai := findApp(st, r.AppID)
				if ai < 0 {
					return ErrNotFound
				}
				app := st.Apps[ai]
				if r.ReleaseRequested || time.Now().After(r.Deadline) || r.CutoverAt != nil || next.ReplacementSpec == nil || model.AppSpecSHA256(app.Spec) != model.AppSpecSHA256(r.SourceSpec) || hasInFlightOperationForApp(st.Operations, app.ID) {
					return ErrConflict
				}
				pi := findAppTrafficPolicyByApp(st.AppTrafficPolicies, app.ID)
				if r.SourceTraffic == nil {
					if pi >= 0 {
						return ErrConflict
					}
				} else if pi < 0 || !reflect.DeepEqual(st.AppTrafficPolicies[pi], *r.SourceTraffic) {
					return ErrConflict
				}
				now := time.Now().UTC()
				app.Spec = *next.ReplacementSpec
				app.UpdatedAt = now
				st.Apps[ai] = app
				release, ok := currentStableReleaseForApp(app, now)
				if !ok {
					return ErrInvalidInput
				}
				release.ID = model.NewID("apprel")
				release.DeploymentName = runtime.RuntimeAppResourceName(app)
				release.ServiceName = runtime.RuntimeAppServiceName(app)
				release.RollbackTargetID = ""
				if r.SourceReleaseID != "" {
					if ri := findAppReleaseByID(st.AppReleases, r.SourceReleaseID); ri >= 0 {
						old := st.AppReleases[ri]
						old.Role = model.AppReleaseRolePrevious
						old.Status = model.AppReleaseStatusDraining
						old.RetentionUntil = nil
						old.UpdatedAt = now
						st.AppReleases[ri] = old
					}
				}
				st.AppReleases = append(st.AppReleases, release)
				p := model.AppTrafficPolicy{ID: model.NewID("apptraffic"), TenantID: app.TenantID, AppID: app.ID, CreatedAt: now}
				if pi >= 0 {
					p = st.AppTrafficPolicies[pi]
				}
				p = stableReleaseSyncedTrafficPolicy(p, release.ID, now)
				p.CandidateReleaseID = ""
				p.UpdatedByType = "system"
				p.UpdatedByID = r.ID
				if pi >= 0 {
					st.AppTrafficPolicies[pi] = p
				} else {
					st.AppTrafficPolicies = append(st.AppTrafficPolicies, p)
				}
				next.CutoverAt = &now
				next.Phase = "publishing"
			}
			next.Version = r.Version + 1
			next.UpdatedAt = time.Now().UTC()
			st.AppRecoveries[i] = next
			return nil
		}
		return ErrNotFound
	})
	return next, err
}

func (s *Store) ReleaseAppRecovery(appID, id string) (model.AppRecoveryRecord, error) {
	var out model.AppRecoveryRecord
	err := s.mutateAppRecovery(appID, false, func(st *model.State) error {
		for i, r := range st.AppRecoveries {
			if r.AppID == appID && r.ID == id {
				if r.Active && !r.ReleaseRequested {
					r.ReleaseRequested = true
					r.Version++
					r.UpdatedAt = time.Now().UTC()
					st.AppRecoveries[i] = r
				}
				out = r
				return nil
			}
		}
		return ErrNotFound
	})
	return out, err
}
