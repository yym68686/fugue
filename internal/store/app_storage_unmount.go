package store

import "fugue/internal/model"

func validateStorageUnmountOperation(op model.Operation, app model.App, expected string) error {
	if op.Type != model.OperationTypeAppStorageUnmount {
		if op.StorageUnmount != nil {
			return ErrInvalidInput
		}
		return nil
	}
	if expected == "" || op.StorageUnmount == nil || op.DesiredSpec == nil ||
		op.DesiredSource != nil || op.DesiredOriginSource != nil {
		return ErrInvalidInput
	}
	desired, _, err := model.AppSpecWithoutPersistentMount(app.Spec, op.StorageUnmount.Path)
	if err != nil || !finalizationIntentEqual(desired, *op.DesiredSpec) ||
		op.StorageUnmount.Validate(app.Spec) != nil {
		return ErrInvalidInput
	}
	return nil
}

func applyStorageUnmountOperation(app *model.App, op model.Operation) error {
	if op.ConfigBaseSpec == nil || !finalizationIntentEqual(app.Spec, *op.ConfigBaseSpec) {
		return ErrConflict
	}
	if err := validateStorageUnmountOperation(op, *app, model.AppSpecSHA256(app.Spec)); err != nil {
		return err
	}
	app.Spec.PersistentStorage = op.DesiredSpec.DeepCopy().PersistentStorage
	app.Status.Phase = "deployed"
	app.Status.CurrentRuntimeID = app.Spec.RuntimeID
	app.Status.CurrentReplicas = app.Spec.Replicas
	return nil
}
