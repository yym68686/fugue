package api

import (
	"context"
	"reflect"
	"strings"

	"fugue/internal/store"
)

// One synchronous assignment request owns this metadata. It is copied into the
// existing failure ring, never exposed by the public API or reused as authority.
type consumerInputReadFailure struct {
	Stage         string                         `json:"stage"`
	Store         store.PlatformInputReadFailure `json:"store,omitempty"`
	ChangedFields []string                       `json:"changed_fields,omitempty"`
}

type consumerInputReadObservationKey struct{}

func withConsumerInputReadObservation(ctx context.Context) context.Context {
	return context.WithValue(ctx, consumerInputReadObservationKey{}, &consumerInputReadFailure{})
}

func observeConsumerInputReadFailure(ctx context.Context, stage string, detail store.PlatformInputReadFailure, before, after *consumerArtifactLookup) {
	failure, _ := ctx.Value(consumerInputReadObservationKey{}).(*consumerInputReadFailure)
	if failure == nil || failure.Stage != "" {
		return
	}
	failure.Stage, failure.Store = stage, detail
	if before != nil && after != nil {
		// Only field names from these three fixed Go structs are recorded.
		// No arbitrary map key, value, identifier, proof or raw error is kept.
		for _, pair := range []struct {
			prefix string
			x, y   any
		}{{"artifact", before.Artifact, after.Artifact}, {"assignment", before.Assignment, after.Assignment}, {"release", before.Release, after.Release}} {
			x, y := reflect.ValueOf(pair.x), reflect.ValueOf(pair.y)
			for i := 0; i < x.NumField(); i++ {
				if !reflect.DeepEqual(x.Field(i).Interface(), y.Field(i).Interface()) {
					field := strings.Split(x.Type().Field(i).Tag.Get("json"), ",")[0]
					failure.ChangedFields = append(failure.ChangedFields, pair.prefix+"."+field)
				}
			}
		}
	}
}

func consumerInputReadFailureFromContext(ctx context.Context) *consumerInputReadFailure {
	failure, _ := ctx.Value(consumerInputReadObservationKey{}).(*consumerInputReadFailure)
	if failure == nil || failure.Stage == "" {
		return nil
	}
	copy := *failure
	copy.ChangedFields = append([]string(nil), failure.ChangedFields...)
	copy.Store.ChangedFields = append([]string(nil), failure.Store.ChangedFields...)
	return &copy
}
