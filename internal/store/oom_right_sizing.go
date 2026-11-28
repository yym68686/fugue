package store

import (
	"encoding/hex"
	"strings"

	"fugue/internal/model"
)

const oomRightSizingPrefix = model.OperationRequestedByOOMRightSizing + "/"

func isOOMRightSizingActor(op model.Operation) bool {
	actor := strings.TrimSpace(op.RequestedByID)
	return strings.TrimSpace(op.RequestedByType) == model.ActorTypeSystem &&
		(actor == model.OperationRequestedByOOMRightSizing || strings.HasPrefix(actor, oomRightSizingPrefix))
}

// EnqueueOOMRightSizingDeploy can only queue a system deployment with the
// historical event fingerprint. It uses the same app lock, checks and unique
// index as the former general-purpose operation write.
func (s *Store) EnqueueOOMRightSizingDeploy(app model.App, requestID string, spec model.AppSpec, source *model.AppSource) (model.Operation, error) {
	fingerprint, prefixed := strings.CutPrefix(requestID, oomRightSizingPrefix)
	if !prefixed || len(fingerprint) != 24 {
		return model.Operation{}, ErrInvalidInput
	}
	if _, err := hex.DecodeString(fingerprint); err != nil || fingerprint != strings.ToLower(fingerprint) {
		return model.Operation{}, ErrInvalidInput
	}
	op, _, err := s.createOperationWithPolicy(model.Operation{
		TenantID:            app.TenantID,
		Type:                model.OperationTypeDeploy,
		RequestedByType:     model.ActorTypeSystem,
		RequestedByID:       requestID,
		AppID:               app.ID,
		DesiredSpec:         &spec,
		DesiredSource:       source,
		DesiredOriginSource: model.AppOriginSource(app),
	}, operationCreatePolicy{AllowOOMRightSizingDeploy: true})
	return op, err
}
