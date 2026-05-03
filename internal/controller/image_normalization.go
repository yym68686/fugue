package controller

import (
	"strings"

	"fugue/internal/appimages"
	"fugue/internal/model"
)

func (s *Service) normalizeManagedAppRuntimeImageRefs(app model.App) (model.App, bool) {
	if s == nil {
		return app, false
	}

	normalizedImage := appimages.NormalizeRuntimeImageRefForSource(
		app.Spec.Image,
		model.AppBuildSource(app),
		s.registryPushBase,
		s.registryPullBase,
	)
	if normalizedImage == "" || normalizedImage == strings.TrimSpace(app.Spec.Image) {
		return app, false
	}

	app.Spec.Image = normalizedImage
	return app, true
}
