package appmail

import (
	"context"
	"encoding/json"
	"errors"

	"fugue/internal/model"
	"fugue/internal/store"
)

const ReconcilePageSize = 20

type ReconcileStore interface {
	ListAppMailBindings(after string, limit int) ([]model.AppMailBinding, error)
	GetAppMetadata(id string) (model.App, error)
	SaveAppMailBinding(model.AppMailBinding, uint64) (model.AppMailBinding, error)
}

type BindingSyncer interface {
	Sync(context.Context, model.AppMailBinding) (json.RawMessage, error)
}

type Reconciler struct {
	Store      ReconcileStore
	Provider   BindingSyncer
	BaseDomain string
	Log        interface{ Printf(string, ...any) }
}

// Pass advances one page; the caller owns writer exclusion and any durable
// checkpoint. A failed binding is revisited on the next sweep, without
// blocking later bindings behind it. Stop on cancellation before starting
// another binding, preserving the progress already made in this pass.
func (r Reconciler) Pass(ctx context.Context, after string) (string, error) {
	bindings, err := r.Store.ListAppMailBindings(after, ReconcilePageSize)
	if err != nil {
		return after, err
	}
	for _, binding := range bindings {
		if err := ctx.Err(); err != nil {
			return after, err
		}
		after = binding.AppID
		app, appErr := r.Store.GetAppMetadata(binding.AppID)
		if appErr != nil && !errors.Is(appErr, store.ErrNotFound) {
			r.Log.Printf("mail reconciliation app=%s lookup failed: %v", binding.AppID, appErr)
			continue
		}
		if binding.State != "revoked" && invalidOwner(app, appErr, binding, r.BaseDomain) {
			binding.State = "revoked"
			appID := binding.AppID
			binding, err = r.Store.SaveAppMailBinding(binding, binding.Generation)
			if err != nil {
				r.Log.Printf("mail reconciliation app=%s revoke failed: %v", appID, err)
				continue
			}
		}
		if _, err = r.Provider.Sync(ctx, binding); err != nil {
			r.Log.Printf("mail reconciliation app=%s sync failed: %v", binding.AppID, err)
		}
	}
	if len(bindings) < ReconcilePageSize {
		after = ""
	}
	return after, nil
}

func invalidOwner(app model.App, appErr error, binding model.AppMailBinding, base string) bool {
	host, _ := Hostname(app, base)
	return appErr != nil || app.TenantID != binding.TenantID || app.ProjectID != binding.ProjectID ||
		host != binding.Hostname || app.Status.Phase == "deleted" || app.Status.Phase == "deleting" || app.Status.Phase == "disabled"
}
