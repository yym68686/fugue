package controller

import (
	"context"
	"strings"
)

const (
	managedAppApplySourceOperation           = "operation"
	managedAppApplySourceBackgroundReconcile = "background_reconcile"
)

type managedAppApplyContextKey struct{}

type managedAppApplyContext struct {
	Source      string
	OperationID string
}

func withManagedAppApplySource(ctx context.Context, source, operationID string) context.Context {
	source = strings.TrimSpace(source)
	if source == "" {
		source = managedAppApplySourceBackgroundReconcile
	}
	return context.WithValue(ctx, managedAppApplyContextKey{}, managedAppApplyContext{
		Source:      source,
		OperationID: strings.TrimSpace(operationID),
	})
}

func managedAppApplySourceFromContext(ctx context.Context) managedAppApplyContext {
	if ctx == nil {
		return managedAppApplyContext{Source: managedAppApplySourceBackgroundReconcile}
	}
	if value, ok := ctx.Value(managedAppApplyContextKey{}).(managedAppApplyContext); ok {
		value.Source = strings.TrimSpace(value.Source)
		value.OperationID = strings.TrimSpace(value.OperationID)
		if value.Source == "" {
			value.Source = managedAppApplySourceBackgroundReconcile
		}
		return value
	}
	return managedAppApplyContext{Source: managedAppApplySourceBackgroundReconcile}
}

func (s *Service) managedAppOperationTypeFromContext(ctx context.Context) string {
	apply := managedAppApplySourceFromContext(ctx)
	if apply.Source != managedAppApplySourceOperation || apply.OperationID == "" || s == nil || s.Store == nil {
		return ""
	}
	op, err := s.Store.GetOperation(apply.OperationID)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(op.Type)
}
