package store

import (
	"context"
	"strings"

	"fugue/internal/model"
	retirement "fugue/internal/releaseretirement/contract"
)

type appReleaseRetirementPort struct {
	store       *Store
	tenant, app string
}

func (s *Store) AppReleaseRetirement(tenantID, appID string) retirement.Repository {
	// The projection trims filters. Reject empty and whitespace-only scopes
	// before reaching that filter so a scoped read cannot become a global read.
	return appReleaseRetirementPort{store: s, tenant: strings.TrimSpace(tenantID), app: strings.TrimSpace(appID)}
}

func (p appReleaseRetirementPort) List(ctx context.Context) ([]model.AppRelease, error) {
	if p.tenant == "" || p.app == "" {
		return nil, ErrInvalidInput
	}
	return p.store.listAppReleaseRetirementState(ctx, model.AppReleaseFilter{TenantID: p.tenant, AppID: p.app, IncludeRetired: true})
}

func (p appReleaseRetirementPort) contains(previous, stable model.AppRelease, policy model.AppTrafficPolicy) bool {
	return p.tenant != "" && p.app != "" &&
		previous.TenantID == p.tenant && previous.AppID == p.app &&
		stable.TenantID == p.tenant && stable.AppID == p.app &&
		policy.TenantID == p.tenant && policy.AppID == p.app
}

func (p appReleaseRetirementPort) RetireDrained(ctx context.Context, previous, stable model.AppRelease, policy model.AppTrafficPolicy) (model.AppRelease, error) {
	if !p.contains(previous, stable, policy) {
		return model.AppRelease{}, ErrConflict
	}
	return p.store.retireDrainedAppRelease(ctx, previous, stable, policy, nil, nil)
}

func (p appReleaseRetirementPort) RetireStoppedHistorical(ctx context.Context, previous, stable model.AppRelease, policy model.AppTrafficPolicy, op model.Operation, workload model.AppReleaseWorkload) (model.AppRelease, error) {
	if !p.contains(previous, stable, policy) || op.TenantID != p.tenant || op.AppID != p.app {
		return model.AppRelease{}, ErrConflict
	}
	return p.store.retireStoppedHistoricalAppRelease(ctx, previous, stable, policy, op, workload)
}
