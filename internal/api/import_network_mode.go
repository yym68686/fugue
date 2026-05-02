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
		return "", fmt.Errorf("network_mode must be background or internal when set")
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

func applyImportedNetworkPolicy(spec *model.AppSpec, networkPolicy *model.AppNetworkPolicySpec) {
	if spec == nil || networkPolicy == nil {
		return
	}
	normalized := model.NormalizeAppNetworkPolicySpec(*networkPolicy)
	spec.NetworkPolicy = &normalized
}

func applyImportedGeneratedEnv(spec *model.AppSpec, generatedEnv map[string]model.AppGeneratedEnvSpec) {
	if spec == nil || len(generatedEnv) == 0 {
		return
	}
	spec.GeneratedEnv = model.NormalizeAppGeneratedEnvSpecs(generatedEnv)
}
