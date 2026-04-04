package api

import (
	"strings"

	"fugue/internal/model"
)

func (s *Server) recoverAppDeployBaseline(app model.App) (model.AppSpec, *model.AppSource, error) {
	spec := cloneAppSpec(app.Spec)
	source := cloneAppSource(app.Source)
	if !appDeployBaselineNeedsRecovery(spec, source) {
		return spec, source, nil
	}

	ops, err := s.store.ListOperationsByApp(app.TenantID, false, app.ID)
	if err != nil {
		return model.AppSpec{}, nil, err
	}

	recoveredSpec := spec
	recoveredSource := source
	recoveredImage := strings.TrimSpace(recoveredSpec.Image)

	for _, op := range ops {
		if !isRecoverableDeployBaselineOperation(op) || op.DesiredSpec == nil {
			continue
		}

		nextSpec := cloneAppSpec(*op.DesiredSpec)
		if strings.TrimSpace(nextSpec.Image) == "" && recoveredImage != "" {
			nextSpec.Image = recoveredImage
		}
		recoveredSpec = nextSpec
		if image := strings.TrimSpace(recoveredSpec.Image); image != "" {
			recoveredImage = image
		}
		if op.DesiredSource != nil {
			recoveredSource = cloneAppSource(op.DesiredSource)
		}
	}

	if strings.TrimSpace(recoveredSpec.Image) == "" && recoveredSource != nil {
		if runtimeImageRef := s.runtimeImageRefFromManagedRef(recoveredSource.ResolvedImageRef); runtimeImageRef != "" {
			recoveredSpec.Image = runtimeImageRef
		}
	}

	recoveredSpec.Replicas = app.Spec.Replicas
	recoveredSpec.ImageMirrorLimit = app.Spec.ImageMirrorLimit
	model.ApplyAppSpecDefaults(&recoveredSpec)
	return recoveredSpec, recoveredSource, nil
}

func appDeployBaselineNeedsRecovery(spec model.AppSpec, source *model.AppSource) bool {
	if strings.TrimSpace(spec.Image) == "" {
		return true
	}
	return source == nil
}

func isRecoverableDeployBaselineOperation(op model.Operation) bool {
	switch op.Type {
	case model.OperationTypeDeploy:
		return true
	case model.OperationTypeImport:
		return op.Status == model.OperationStatusCompleted
	default:
		return false
	}
}
