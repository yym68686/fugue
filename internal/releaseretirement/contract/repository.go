package contract

import (
	"context"

	"fugue/internal/model"
)

// Repository is a single app's retirement capability. It cannot change
// traffic, create releases, or commit against a different app's snapshots.
type Repository interface {
	List(context.Context) ([]model.AppRelease, error)
	RetireDrained(context.Context, model.AppRelease, model.AppRelease, model.AppTrafficPolicy) (model.AppRelease, error)
	RetireStoppedHistorical(context.Context, model.AppRelease, model.AppRelease, model.AppTrafficPolicy, model.Operation, model.AppReleaseWorkload) (model.AppRelease, error)
}
