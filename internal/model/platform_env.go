package model

import "strings"

var fugueInjectedAppEnvNames = map[string]struct{}{
	"FUGUE_API_URL":      {},
	"FUGUE_APP_HOSTNAME": {},
	"FUGUE_APP_ID":       {},
	"FUGUE_APP_NAME":     {},
	"FUGUE_APP_URL":      {},
	"FUGUE_BASE_URL":     {},
	"FUGUE_PROJECT_ID":   {},
	"FUGUE_RUNTIME_ID":   {},
	"FUGUE_TENANT_ID":    {},
	"FUGUE_TOKEN":        {},
}

func IsFugueInjectedAppEnvName(name string) bool {
	_, ok := fugueInjectedAppEnvNames[strings.TrimSpace(name)]
	return ok
}

func StripFugueInjectedAppEnv(in map[string]string) (map[string]string, bool) {
	if len(in) == 0 {
		return nil, false
	}
	out := make(map[string]string, len(in))
	changed := false
	for key, value := range in {
		if IsFugueInjectedAppEnvName(key) {
			changed = true
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		out = nil
	}
	return out, changed
}

func StripFugueInjectedAppEnvFromSpec(spec AppSpec) (AppSpec, bool) {
	env, changed := StripFugueInjectedAppEnv(spec.Env)
	spec.Env = env
	return spec, changed
}
