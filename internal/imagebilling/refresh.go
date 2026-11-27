// Package imagebilling owns registry-backed image storage measurement and
// scheduling. It has no authority over balances, limits, or operation state.
package imagebilling

import (
	"context"

	"fugue/internal/appimages"
	"fugue/internal/imagebilling/contract"
)

func Refresh(ctx context.Context, repository contract.Repository, inspect appimages.InspectFunc, pushBase, pullBase string) error {
	apps, err := repository.Apps()
	if err != nil {
		return err
	}
	operations, err := repository.RetentionOperations()
	if err != nil {
		return err
	}
	storageBytes, err := appimages.MeasureTenantStorageBytes(ctx, inspect, apps, operations, pushBase, pullBase)
	if err != nil {
		return err
	}
	return repository.CommitStorage(appimages.StorageBytesToGibibytes(storageBytes))
}
