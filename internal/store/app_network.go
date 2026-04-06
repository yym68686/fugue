package store

import (
	"strings"

	"fugue/internal/model"
)

func validateAppNetworkMode(spec model.AppSpec) error {
	raw := strings.TrimSpace(spec.NetworkMode)
	if raw != "" && model.NormalizeAppNetworkMode(raw) == "" {
		return ErrInvalidInput
	}
	if model.AppUsesBackgroundNetwork(spec) && firstPositiveSpecPort(spec.Ports) > 0 {
		return ErrInvalidInput
	}
	return nil
}
