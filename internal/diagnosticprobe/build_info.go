package diagnosticprobe

import (
	"errors"
	"regexp"
	"runtime/debug"
)

var goModuleSelector = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._~+/-]{0,255}$`)

func validateGoModules(modules []string) error {
	if len(modules) > 32 {
		return errors.New("Go dependency selection exceeds 32 modules")
	}
	seen := map[string]bool{}
	for _, name := range modules {
		if !goModuleSelector.MatchString(name) || seen[name] {
			return errors.New("Go dependency selectors must be distinct bounded module names")
		}
		seen[name] = true
	}
	return nil
}

func selectedGoBuild(build *debug.BuildInfo, requested []string) (map[string]any, []string) {
	selected := map[string]any{}
	missing := []string{}
	for _, name := range requested {
		for _, dep := range build.Deps {
			if dep != nil && dep.Path == name {
				if value, ok := goModuleIdentity(dep); ok {
					selected[name] = value
				}
				break
			}
		}
		if selected[name] == nil {
			missing = append(missing, name)
		}
	}
	settings := map[string]string{}
	for _, setting := range build.Settings {
		switch setting.Key {
		case "GOOS", "GOARCH", "GOAMD64", "CGO_ENABLED", "vcs", "vcs.revision", "vcs.time", "vcs.modified":
			if len(setting.Value) <= 128 {
				settings[setting.Key] = setting.Value
			}
		}
	}
	return map[string]any{"dependencies": selected, "settings": settings,
		"scope": "embedded Go build metadata for selected module names; local replacement paths, build flags and environment values are excluded"}, missing
}

func goModuleIdentity(module *debug.Module) (map[string]any, bool) {
	if len(module.Path) > 256 || len(module.Version) > 256 || len(module.Sum) > 128 {
		return nil, false
	}
	result := map[string]any{"path": module.Path, "version": module.Version, "sum": module.Sum}
	if replacement := module.Replace; replacement != nil {
		if replacement.Version == "" {
			result["local_replacement"] = true
		} else if len(replacement.Path) <= 256 && len(replacement.Version) <= 256 && len(replacement.Sum) <= 128 {
			result["replacement"] = map[string]string{"path": replacement.Path, "version": replacement.Version, "sum": replacement.Sum}
		} else {
			return nil, false
		}
	}
	return result, true
}
