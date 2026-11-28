package appmail

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

// AwaitReconciliation preserves the foreground response after the Controller
// owns provider PUTs. Observe only the exact generation, never return stale
// credentials, and never perform a provider write from a waiting API request.
func AwaitReconciliation(
	ctx context.Context,
	binding model.AppMailBinding,
	readStatus func(context.Context, string) (store.AppMailReconcileStatus, error),
	observe func(context.Context) (json.RawMessage, error),
) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	tick := time.NewTicker(400 * time.Millisecond)
	defer tick.Stop()
	for {
		status, err := readStatus(ctx, binding.AppID)
		if ctx.Err() != nil {
			return nil, errors.New("mail reconciliation pending")
		}
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return nil, errors.New("mail reconciliation unavailable")
		}
		if status.Generation > binding.Generation {
			return nil, errors.New("mail binding changed")
		}
		if status.Generation == binding.Generation {
			switch status.LastResult {
			case store.AppMailReconcileSynced:
				observation, err := observe(ctx)
				if err != nil {
					return nil, err
				}
				if err = ValidateObservation(observation, binding); err != nil {
					return nil, err
				}
				return observation, nil
			case store.AppMailReconcileProviderUnavailable:
				return nil, errors.New("mail provider unavailable")
			case store.AppMailReconcileMetadataUnavailable:
				return nil, errors.New("mail reconciliation unavailable")
			case store.AppMailReconcileDesiredChanged:
				return nil, errors.New("mail binding changed")
			}
		}
		select {
		case <-ctx.Done():
			return nil, errors.New("mail reconciliation pending")
		case <-tick.C:
		}
	}
}
