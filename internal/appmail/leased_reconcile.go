package appmail

import (
	"context"
	"errors"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

type LeasedReconcileStore interface {
	GetAppMetadata(string) (model.App, error)
	ClaimAppMailReconciliation(context.Context) (store.AppMailReconcileLease, bool, error)
	CheckAppMailReconciliationLease(context.Context, store.AppMailReconcileLease) error
	RevokeAppMailReconciliation(context.Context, store.AppMailReconcileLease) error
	SaveAppMailReconciliationDNS(context.Context, store.AppMailReconcileLease, []model.AppMailDNSRecord) error
	FinishAppMailReconciliation(context.Context, store.AppMailReconcileLease, store.AppMailReconcileResult) (bool, error)
}

type LeasedReconciler struct {
	store      LeasedReconcileStore
	sync       func(context.Context, model.AppMailBinding, func([]model.AppMailDNSRecord) error) error
	recheck    func() error
	baseDomain string
}

func NewLeasedReconciler(st LeasedReconcileStore, provider Provider, baseDomain string) (LeasedReconciler, error) {
	if normalizeDomain(baseDomain) == "" {
		return LeasedReconciler{}, errors.New("application base domain is required for mail reconciliation")
	}
	return LeasedReconciler{
		store:      st,
		baseDomain: baseDomain,
		recheck:    func() error { return provider.CheckReconciliationConfig(baseDomain) },
		sync: func(ctx context.Context, binding model.AppMailBinding, saveDNS func([]model.AppMailDNSRecord) error) error {
			_, err := provider.syncWithDNSWriter(ctx, binding, saveDNS)
			return err
		},
	}, nil
}

// RunOnce executes at most one claim. Failure is recorded even when the
// external request timed out; pending work remains recoverable if the process
// exits before it can record anything. A provider call has its own 15s cap.
func (r LeasedReconciler) RunOnce(ctx context.Context) (bool, error) {
	claimCtx, cancelClaim := context.WithTimeout(ctx, 3*time.Second)
	lease, claimed, err := r.store.ClaimAppMailReconciliation(claimCtx)
	cancelClaim()
	if err != nil || !claimed {
		return false, err
	}
	attemptCtx, cancelAttempt := context.WithTimeout(ctx, 38*time.Second)
	result, attemptErr := r.advance(attemptCtx, lease)
	cancelAttempt()

	finishCtx, cancelFinish := context.WithTimeout(ctx, 3*time.Second)
	defer cancelFinish()
	_, finishErr := r.store.FinishAppMailReconciliation(finishCtx, lease, result)
	return true, errors.Join(attemptErr, finishErr)
}

func (r LeasedReconciler) advance(ctx context.Context, lease store.AppMailReconcileLease) (store.AppMailReconcileResult, error) {
	binding := lease.Binding
	if binding.State != "revoked" {
		app, appErr := r.store.GetAppMetadata(binding.AppID)
		if appErr != nil && !errors.Is(appErr, store.ErrNotFound) {
			return store.AppMailReconcileMetadataUnavailable, appErr
		}
		if invalidOwner(app, appErr, binding, r.baseDomain) {
			if err := r.recheck(); err != nil {
				return store.AppMailReconcileMetadataUnavailable, err
			}
			// The tombstone is the next desired generation; never send the
			// retired active generation or fabricate a success for it.
			err := r.store.RevokeAppMailReconciliation(ctx, lease)
			if errors.Is(err, store.ErrConflict) {
				return store.AppMailReconcileDesiredChanged, nil
			}
			return store.AppMailReconcileDesiredChanged, err
		}
	}
	if err := r.store.CheckAppMailReconciliationLease(ctx, lease); err != nil {
		if errors.Is(err, store.ErrConflict) {
			return store.AppMailReconcileDesiredChanged, nil
		}
		return store.AppMailReconcileMetadataUnavailable, err
	}
	err := r.sync(ctx, binding, func(records []model.AppMailDNSRecord) error {
		return r.store.SaveAppMailReconciliationDNS(ctx, lease, records)
	})
	if errors.Is(err, store.ErrConflict) {
		return store.AppMailReconcileDesiredChanged, nil
	}
	if err != nil {
		return store.AppMailReconcileProviderUnavailable, err
	}
	return store.AppMailReconcileSynced, nil
}
