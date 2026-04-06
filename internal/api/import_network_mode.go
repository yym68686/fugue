package api

import (
	"fmt"
	"strings"

	"fugue/internal/model"
)

func resolveImportNetworkMode(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	normalized := model.NormalizeAppNetworkMode(raw)
	if normalized == "" {
		return "", fmt.Errorf("network_mode must be background when set")
	}
	return normalized, nil
}

func applyImportedNetworkMode(spec *model.AppSpec, networkMode string) {
	if spec == nil {
		return
	}
	if normalized := model.NormalizeAppNetworkMode(networkMode); normalized != "" {
		spec.NetworkMode = normalized
	}
	if model.AppUsesBackgroundNetwork(*spec) {
		spec.Ports = nil
	}
}
