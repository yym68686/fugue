package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"strconv"
	"syscall"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/model"
)

type managedSSHControlFile struct {
	States    map[string]managedssh.ControlState `json:"states"`
	Artifacts map[string]managedssh.Artifact     `json:"artifacts"`
}

func (stateStore *Store) GetManagedSSHControl(tenantID, appID string) (managedssh.ControlState, error) {
	if _, err := stateStore.GetManagedSSHIntent(tenantID, appID); err != nil {
		return managedssh.ControlState{}, err
	}
	if stateStore.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var raw []byte
		if err := stateStore.db.QueryRowContext(ctx, `SELECT body FROM fugue_managed_ssh_control WHERE tenant_id=$1 AND app_id=$2`, tenantID, appID).Scan(&raw); err != nil {
			return managedssh.ControlState{}, mapDBErr(err)
		}
		return decodeManagedSSHControl(raw, tenantID, appID)
	}
	var result managedssh.ControlState
	err := stateStore.withManagedSSHControlFile(false, func(records *managedSSHControlFile) error {
		state, exists := records.States[appID]
		if !exists || state.TenantID != tenantID {
			return ErrNotFound
		}
		result = state
		return nil
	})
	return result, err
}

func (stateStore *Store) SaveManagedSSHControl(expected uint64, state managedssh.ControlState) error {
	if expected >= math.MaxInt64 || state.Revision != expected+1 || state.Generation > math.MaxInt64 || state.Validate() != nil {
		return ErrInvalidInput
	}
	identity, err := stateStore.GetAppSSHIdentity(state.TenantID, state.AppID)
	if err != nil {
		return err
	}
	if identity.ID != state.IdentityID {
		return ErrConflict
	}
	for _, slot := range state.Slots() {
		if !reflect.DeepEqual(slot.Artifact.Configuration.Identity, identity) {
			return ErrConflict
		}
	}
	if stateStore.usingDatabase() {
		return stateStore.pgSaveManagedSSHControl(expected, state)
	}
	return stateStore.withManagedSSHIntentFile(false, func(intents map[string]model.ManagedSSHIntent) error {
		intent, exists := intents[state.AppID]
		if !exists || intent.TenantID != state.TenantID || intent.Generation != state.IntentGeneration || intent.Spec.IdentityID != state.IdentityID {
			return ErrConflict
		}
		return stateStore.withManagedSSHControlFile(true, func(records *managedSSHControlFile) error {
			previous, exists := records.States[state.AppID]
			if err := validateManagedSSHControlChange(expected, previous, exists, state); err != nil {
				return err
			}
			for _, slot := range state.Slots() {
				key := state.AppID + ":" + strconv.FormatUint(slot.Artifact.Configuration.Generation, 10)
				if artifact, exists := records.Artifacts[key]; exists && !reflect.DeepEqual(artifact, slot.Artifact) {
					return ErrConflict
				}
				records.Artifacts[key] = slot.Artifact
			}
			records.States[state.AppID] = state
			return nil
		})
	})
}

func validateManagedSSHControlChange(expected uint64, previous managedssh.ControlState, exists bool, next managedssh.ControlState) error {
	if !exists {
		if expected != 0 || next.Active != nil || len(next.Draining) != 0 {
			return ErrConflict
		}
		return nil
	}
	if previous.Revision != expected || previous.TenantID != next.TenantID || previous.AppID != next.AppID || previous.IdentityID != next.IdentityID || previous.Generation > next.Generation || previous.IntentGeneration > next.IntentGeneration || previous.RouteGeneration > next.RouteGeneration {
		return ErrConflict
	}
	if previous.LegacyRouteDigest != "" && next.LegacyRouteDigest != previous.LegacyRouteDigest {
		return ErrConflict
	}
	if err := validateSSHRetirementChange(previous, next); err != nil {
		return err
	}
	oldSlots := map[string]managedssh.ServingSlot{}
	for _, slot := range previous.Slots() {
		oldSlots[slot.ID] = slot
	}
	for _, slot := range next.Slots() {
		old := oldSlots[slot.ID]
		if loaded := slot.LoadedArtifact; loaded != nil && !reflect.DeepEqual(*loaded, slot.Artifact) && !reflect.DeepEqual(*loaded, old.Artifact) && (old.LoadedArtifact == nil || !reflect.DeepEqual(*loaded, *old.LoadedArtifact)) {
			return ErrConflict // A receipt cannot introduce an unknown historical artifact.
		}
		if old.PendingApply != nil && slot.PendingApply != nil && *old.PendingApply != *slot.PendingApply {
			return ErrConflict // Retrying or superseding a publication cannot reset its clock.
		}
		if old, exists := oldSlots[slot.ID]; exists {
			if old.Namespace != slot.Namespace || old.Release != slot.Release || old.Artifact.Configuration.Target != slot.Artifact.Configuration.Target || !old.CreatedAt.Equal(slot.CreatedAt) || old.Artifact.Configuration.Generation > slot.Artifact.Configuration.Generation {
				return ErrConflict
			}
		}
	}
	if previous.Active != nil && (next.Active == nil || next.Active.ID != previous.Active.ID) {
		found := false
		for _, slot := range next.Draining {
			if slot.ID == previous.Active.ID {
				found = true
			}
		}
		if !found {
			return ErrConflict
		}
	}
	if next.Active != nil && (previous.Active == nil || previous.Active.ID != next.Active.ID) {
		approved := previous.Candidate != nil && reflect.DeepEqual(*previous.Candidate, *next.Active) && !next.RollbackHold
		if next.RollbackHold {
			for _, slot := range previous.Draining {
				slot.DrainingSince = nil
				if reflect.DeepEqual(slot, *next.Active) {
					approved = true
				}
			}
		}
		if !approved || !next.Active.Verified(time.Now().UTC()) || next.RouteGeneration != previous.RouteGeneration+1 {
			return ErrConflict
		}
	} else if next.RouteGeneration != previous.RouteGeneration {
		return ErrConflict
	}
	return nil
}

func validateSSHRetirementChange(previous, next managedssh.ControlState) error {
	if previous.Candidate != nil && (next.Candidate == nil || next.Candidate.ID != previous.Candidate.ID) {
		current := next.Slot(previous.Candidate.ID)
		if current == nil {
			return ErrConflict
		}
		if next.Active == nil || next.Active.ID != previous.Candidate.ID {
			if current.DrainingSince == nil {
				return ErrConflict
			}
		}
	}
	retired := map[string]managedssh.ServingSlot{}
	for _, slot := range next.Retired {
		retired[slot.ID] = slot
	}
	for _, slot := range previous.Draining {
		if current := next.Slot(slot.ID); current != nil {
			if slot.RetiringAt != nil && (current.RetiringAt == nil || !current.RetiringAt.Equal(*slot.RetiringAt) || !current.Artifact.Configuration.Draining) {
				return ErrConflict
			}
			continue
		}
		current, exists := retired[slot.ID]
		if !exists || slot.RetiringAt == nil || current.RetiredAt == nil {
			return ErrConflict
		}
		current.RetiredAt = nil
		if !reflect.DeepEqual(current, slot) {
			return ErrConflict
		}
	}
	for _, slot := range previous.Retired {
		if current, exists := retired[slot.ID]; exists {
			if !reflect.DeepEqual(slot, current) {
				return ErrConflict
			}
		} else if next.Candidate == nil || next.Candidate.ID != slot.ID || next.Candidate.Artifact.Configuration.Generation <= slot.Artifact.Configuration.Generation {
			return ErrConflict
		}
	}
	for _, slot := range next.Retired {
		found := false
		for _, old := range previous.Retired {
			if old.ID == slot.ID {
				found = true
			}
		}
		for _, old := range previous.Draining {
			if old.ID == slot.ID {
				found = true
			}
		}
		if !found {
			return ErrConflict
		}
	}
	return nil
}

func (stateStore *Store) pgSaveManagedSSHControl(expected uint64, state managedssh.ControlState) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transaction, err := stateStore.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer transaction.Rollback()
	app, err := stateStore.pgGetAppTx(ctx, transaction, state.AppID, true)
	if err != nil {
		return mapDBErr(err)
	}
	if app.TenantID != state.TenantID || isDeletedApp(app) {
		return ErrNotFound
	}
	var intentGeneration uint64
	if err := transaction.QueryRowContext(ctx, `SELECT generation FROM fugue_managed_ssh_intents WHERE app_id=$1 AND tenant_id=$2 FOR SHARE`, state.AppID, state.TenantID).Scan(&intentGeneration); err != nil {
		return mapDBErr(err)
	}
	if intentGeneration != state.IntentGeneration {
		return ErrConflict
	}
	if _, err := transaction.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "fugue.ssh-control/"+state.AppID); err != nil {
		return err
	}
	var previousRaw []byte
	err = transaction.QueryRowContext(ctx, `SELECT body FROM fugue_managed_ssh_control WHERE app_id=$1 FOR UPDATE`, state.AppID).Scan(&previousRaw)
	exists := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var previous managedssh.ControlState
	if exists {
		previous, err = decodeManagedSSHControl(previousRaw, state.TenantID, state.AppID)
		if err != nil {
			return err
		}
	}
	if err := validateManagedSSHControlChange(expected, previous, exists, state); err != nil {
		return err
	}
	for _, slot := range state.Slots() {
		raw, err := json.Marshal(slot.Artifact)
		if err != nil {
			return err
		}
		generation := slot.Artifact.Configuration.Generation
		result, err := transaction.ExecContext(ctx, `INSERT INTO fugue_managed_ssh_artifacts(app_id,generation,body) VALUES($1,$2,$3::jsonb) ON CONFLICT DO NOTHING`, state.AppID, generation, string(raw))
		if err != nil {
			return err
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if inserted == 0 {
			var equal bool
			if err := transaction.QueryRowContext(ctx, `SELECT body=$3::jsonb FROM fugue_managed_ssh_artifacts WHERE app_id=$1 AND generation=$2`, state.AppID, generation, string(raw)).Scan(&equal); err != nil {
				return err
			}
			if !equal {
				return ErrConflict
			}
		}
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return err
	}
	_, err = transaction.ExecContext(ctx, `INSERT INTO fugue_managed_ssh_control(app_id,tenant_id,revision,body) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(app_id) DO UPDATE SET revision=EXCLUDED.revision,body=EXCLUDED.body`, state.AppID, state.TenantID, state.Revision, string(raw))
	if err != nil {
		return err
	}
	if state.Active != nil && state.RouteGeneration != previous.RouteGeneration {
		if err := stateStore.pgPublishManagedSSHRoute(ctx, transaction, app, state); err != nil {
			return err
		}
	}
	return transaction.Commit()
}

func decodeManagedSSHControl(raw []byte, tenantID, appID string) (managedssh.ControlState, error) {
	var state managedssh.ControlState
	if err := json.Unmarshal(raw, &state); err != nil {
		return state, err
	}
	if state.TenantID != tenantID || state.AppID != appID || state.Validate() != nil {
		return managedssh.ControlState{}, ErrConflict
	}
	return state, nil
}

func (stateStore *Store) withManagedSSHControlFile(write bool, callback func(*managedSSHControlFile) error) error {
	path := stateStore.path + ".managed-ssh-control.json"
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	records := managedSSHControlFile{States: map[string]managedssh.ControlState{}, Artifacts: map[string]managedssh.Artifact{}}
	raw, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err == nil {
		if err := json.Unmarshal(raw, &records); err != nil {
			return err
		}
		if records.States == nil || records.Artifacts == nil {
			return ErrConflict
		}
		for appID, state := range records.States {
			if state.AppID != appID || state.Validate() != nil {
				return ErrConflict
			}
			for _, slot := range state.Slots() {
				key := appID + ":" + strconv.FormatUint(slot.Artifact.Configuration.Generation, 10)
				if artifact, exists := records.Artifacts[key]; !exists || !reflect.DeepEqual(artifact, slot.Artifact) {
					return ErrConflict
				}
			}
		}
	}
	if err := callback(&records); err != nil {
		return err
	}
	if !write {
		return nil
	}
	raw, err = json.Marshal(records)
	if err != nil {
		return err
	}
	return writeSSHIdentityFile(path, raw)
}
