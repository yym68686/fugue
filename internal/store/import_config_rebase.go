package store

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"fugue/internal/model"
)

// CreateDeployOperationAfterImport combines the immutable build result with
// configuration changes since acceptance under the same app lock used by all
// operation creation. Building code never restores an older serving config.
func (s *Store) CreateDeployOperationAfterImport(importID string, deploy model.Operation) (model.Operation, error) {
	parent, err := s.GetOperation(importID)
	if err != nil {
		return model.Operation{}, err
	}
	if parent.Type != model.OperationTypeImport || deploy.Type != model.OperationTypeDeploy ||
		parent.AppID != deploy.AppID || parent.TenantID != deploy.TenantID {
		return model.Operation{}, ErrInvalidInput
	}
	if parent.ConfigBaseSpec == nil {
		// Imports already accepted by an older API must remain executable during
		// rolling upgrades; only newly recorded baselines support a proven merge.
		return s.CreateOperation(deploy)
	}
	created, _, err := s.createOperationWithPolicy(deploy, operationCreatePolicy{ImportConfigBase: parent.ConfigBaseSpec})
	return created, err
}

func rebaseImportDeployConfiguration(op *model.Operation, current model.App, base *model.AppSpec) error {
	if base == nil {
		return nil
	}
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return ErrInvalidInput
	}
	toObject := func(spec model.AppSpec) (map[string]json.RawMessage, error) {
		raw, err := json.Marshal(spec)
		if err != nil {
			return nil, err
		}
		var object map[string]json.RawMessage
		err = json.Unmarshal(raw, &object)
		return object, err
	}
	baseObject, err := toObject(*base)
	if err != nil {
		return err
	}
	currentObject, err := toObject(current.Spec)
	if err != nil {
		return err
	}
	desiredObject, err := toObject(*op.DesiredSpec)
	if err != nil {
		return err
	}
	merged := mergeInterveningConfiguration(baseObject, desiredObject, currentObject)
	// Independent configuration edits preserve a newer serving artifact. Two
	// different artifact updates since the baseline require a fresh operation;
	// never silently roll the serving image back.
	artifactChanged := op.DesiredSpec.Image != base.Image
	if artifactChanged && current.Spec.Image != base.Image && current.Spec.Image != op.DesiredSpec.Image {
		return ErrConflict
	}
	if artifactChanged {
		merged["image"] = desiredObject["image"]
	} else if current.Spec.Image != base.Image {
		op.DesiredSource = model.CloneAppSource(model.AppBuildSource(current))
		op.DesiredOriginSource = model.CloneAppSource(model.AppOriginSource(current))
	}
	if restart, ok := desiredObject["restart_token"]; ok && op.DesiredSpec.RestartToken != base.RestartToken {
		merged["restart_token"] = restart
	}
	raw, err := json.Marshal(merged)
	if err != nil {
		return err
	}
	var spec model.AppSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return err
	}
	if err := normalizeAppSpecResources(&spec); err != nil {
		return err
	}
	if err := validateAppNetworkMode(spec); err != nil {
		return err
	}
	op.DesiredSpec = &spec
	// Record the configuration used for the queued deployment. A scale or env
	// operation can finish after build completion but before activation.
	op.ConfigBaseSpec = cloneAppSpec(&current.Spec)
	return nil
}

// Recurse into objects so an env-key edit does not erase unrelated build
// defaults. Deletions remain deletions; arrays are indivisible config values.
func mergeInterveningConfiguration(base, desired, current map[string]json.RawMessage) map[string]json.RawMessage {
	result := make(map[string]json.RawMessage, len(desired))
	for key, value := range desired {
		result[key] = value
	}
	keys := make(map[string]bool, len(base)+len(current))
	for key := range base {
		keys[key] = true
	}
	for key := range current {
		keys[key] = true
	}
	for key := range keys {
		before, hadBefore := base[key]
		now, hasNow := current[key]
		if hadBefore == hasNow && reflect.DeepEqual(before, now) {
			continue
		}
		if !hasNow {
			delete(result, key)
			continue
		}
		var beforeObject, desiredObject, currentObject map[string]json.RawMessage
		beforeOK := !hadBefore || json.Unmarshal(before, &beforeObject) == nil
		desiredRaw, hasDesired := desired[key]
		desiredOK := !hasDesired || json.Unmarshal(desiredRaw, &desiredObject) == nil
		currentOK := json.Unmarshal(now, &currentObject) == nil && currentObject != nil
		if beforeOK && desiredOK && currentOK {
			merged := mergeInterveningConfiguration(beforeObject, desiredObject, currentObject)
			result[key], _ = json.Marshal(merged)
		} else {
			result[key] = now
		}
	}
	return result
}

// RebaseDeployOperationForExecution closes the queue-time gap after a build.
// The app and operation are read and the merged snapshot is persisted in one
// transaction. A failed/cancelled operation can never be revived by this path.
func (s *Store) RebaseDeployOperationForExecution(id string) (model.Operation, error) {
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return model.Operation{}, err
		}
		defer tx.Rollback()
		op, err := s.pgGetOperationTx(ctx, tx, id, true)
		if err != nil {
			return model.Operation{}, mapDBErr(err)
		}
		if op.Type != model.OperationTypeDeploy || op.ConfigBaseSpec == nil {
			return op, nil
		}
		if op.Status != model.OperationStatusPending && op.Status != model.OperationStatusRunning {
			return model.Operation{}, ErrConflict
		}
		app, err := s.pgGetAppTx(ctx, tx, op.AppID, true)
		if err != nil {
			return model.Operation{}, mapDBErr(err)
		}
		if err = rebaseImportDeployConfiguration(&op, app, op.ConfigBaseSpec); err != nil {
			return model.Operation{}, err
		}
		if err = s.pgUpdateOperationTx(ctx, tx, op); err != nil {
			return model.Operation{}, err
		}
		if err = tx.Commit(); err != nil {
			return model.Operation{}, err
		}
		return op, nil
	}
	var op model.Operation
	err := s.withLockedState(true, func(state *model.State) error {
		idx := findOperation(state, id)
		if idx < 0 {
			return ErrNotFound
		}
		op = cloneOperation(state.Operations[idx])
		if op.Type != model.OperationTypeDeploy || op.ConfigBaseSpec == nil {
			return nil
		}
		if op.Status != model.OperationStatusPending && op.Status != model.OperationStatusRunning {
			return ErrConflict
		}
		ai := findApp(state, op.AppID)
		if ai < 0 {
			return ErrNotFound
		}
		if err := rebaseImportDeployConfiguration(&op, state.Apps[ai], op.ConfigBaseSpec); err != nil {
			return err
		}
		state.Operations[idx] = cloneOperation(op)
		return nil
	})
	return op, err
}
