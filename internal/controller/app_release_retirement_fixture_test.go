package controller

import (
	"context"
	"errors"

	"fugue/internal/model"
	"fugue/internal/store"
)

// retireFixtureRelease constructs the same Store preconditions as a
// completed drain. These tests supply their own runtime observations.
func retireFixtureRelease(ctx context.Context, st *store.Store, release model.AppRelease) (model.AppRelease, error) {
	policy, err := st.GetAppTrafficPolicy(release.TenantID, false, release.AppID)
	if errors.Is(err, store.ErrNotFound) {
		stable, createErr := st.CreateAppRelease(model.AppRelease{
			TenantID: release.TenantID, AppID: release.AppID,
			Role: model.AppReleaseRoleStable, Status: model.AppReleaseStatusServing,
		})
		if createErr != nil {
			return model.AppRelease{}, createErr
		}
		policy, err = st.UpsertAppTrafficPolicy(model.AppTrafficPolicy{
			TenantID: release.TenantID, AppID: release.AppID, Mode: model.AppTrafficModeSingle,
			StableReleaseID: stable.ID, StableWeight: 100,
		})
	}
	if err != nil {
		return model.AppRelease{}, err
	}
	stable, err := st.GetAppRelease(release.TenantID, false, policy.StableReleaseID)
	if err != nil {
		return model.AppRelease{}, err
	}
	release.Role, release.Status = model.AppReleaseRolePrevious, model.AppReleaseStatusDraining
	release, err = st.UpdateAppRelease(release)
	if err != nil {
		return model.AppRelease{}, err
	}
	return st.AppReleaseRetirement(release.TenantID, release.AppID).RetireDrained(ctx, release, stable, policy)
}
