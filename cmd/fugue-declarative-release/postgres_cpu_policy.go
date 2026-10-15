package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"fugue/internal/declarativerelease"
)

const postgresCPUPolicyPath = "deploy/environments/production/managed-postgres/cpu-upscale-policy.json"
const postgresCPUPolicyAnnotation = "fugue.pro/postgres-cpu-policy-sha256"

// CPU direction gates have an independent, fenced configuration writer. Bind
// that reviewed configuration into both code targets: a code rollback must not
// undo configuration recovery. The predecessor annotation and exact gate values
// are checked against live state by the normal prewrite convergence/CAS checks.
func bindPostgresCPUPolicy(release declarativerelease.PlanRelease, rendered declarativerelease.RenderedManifests) (declarativerelease.RenderedManifests, error) {
	if release.ComponentID != "controller" {
		return rendered, nil
	}
	raw, err := os.ReadFile(postgresCPUPolicyPath)
	if err != nil {
		return rendered, fmt.Errorf("read independent Postgres CPU policy: %w", err)
	}
	return bindPostgresCPUPolicyBytes(release, rendered, raw)
}

func bindPostgresCPUPolicyBytes(release declarativerelease.PlanRelease, rendered declarativerelease.RenderedManifests, raw []byte) (declarativerelease.RenderedManifests, error) {
	var policy map[string]any
	if err := json.Unmarshal(raw, &policy); err != nil {
		return rendered, err
	}
	keys := []string{"schema", "namespace", "deployment", "container", "expectedImage", "expectedConfigSha", "cpuRequestUpscale", "cpuLimitUpscale"}
	if len(policy) != len(keys) {
		return rendered, fmt.Errorf("unsupported independent CPU policy fields")
	}
	for _, key := range keys {
		if _, ok := policy[key]; !ok {
			return rendered, fmt.Errorf("missing independent CPU policy field %s", key)
		}
	}
	w := release.Workload
	if policy["schema"] != "fugue.managed-postgres-cpu-policy/v1" || policy["namespace"] != w.Namespace || policy["deployment"] != w.Name || policy["container"] != w.Container || w.Kind != "Deployment" {
		return rendered, fmt.Errorf("independent CPU policy workload identity differs")
	}
	canonical, err := declarativerelease.CanonicalJSON(policy)
	if err != nil {
		return rendered, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(canonical))
	gates := map[string]string{}
	for key, name := range map[string]string{"cpuRequestUpscale": "FUGUE_MANAGED_POSTGRES_IN_PLACE_CPU_REQUEST_UPSCALE_ENABLED", "cpuLimitUpscale": "FUGUE_MANAGED_POSTGRES_IN_PLACE_CPU_LIMIT_UPSCALE_ENABLED"} {
		value, ok := policy[key].(bool)
		if !ok {
			return rendered, fmt.Errorf("independent CPU gate must be a boolean")
		}
		gates[name] = strconv.FormatBool(value)
	}
	bind := func(manifest []byte) ([]byte, error) {
		set, err := declarativerelease.DecodeResourceSet(bytes.NewReader(manifest))
		if err != nil {
			return nil, err
		}
		found := 0
		for _, item := range set.Items {
			metadata := mapField(item, "metadata")
			if item["kind"] != w.Kind || metadata["namespace"] != w.Namespace || metadata["name"] != w.Name {
				continue
			}
			found++
			containers := anySlice(mapField(mapField(mapField(item, "spec"), "template"), "spec")["containers"])
			matched := 0
			for _, raw := range containers {
				container, _ := raw.(map[string]any)
				if container["name"] != w.Container {
					continue
				}
				matched++
				for name, value := range gates {
					if old, ok := declaredEnvValue(container, name); !ok || (old != "true" && old != "false") || !setDeclaredEnvValue(container, name, value) {
						return nil, fmt.Errorf("independent CPU gate declaration is absent or ambiguous")
					}
				}
			}
			if matched != 1 {
				return nil, fmt.Errorf("independent CPU policy container is ambiguous")
			}
			annotations := mapField(metadata, "annotations")
			if annotations == nil {
				annotations = map[string]any{}
				metadata["annotations"] = annotations
			}
			annotations[postgresCPUPolicyAnnotation] = digest
		}
		if found != 1 {
			return nil, fmt.Errorf("independent CPU policy deployment is absent or ambiguous")
		}
		return declarativerelease.CanonicalJSON(set)
	}
	forward, err := bind(rendered.Forward)
	if err != nil {
		return rendered, err
	}
	lkg, err := bind(rendered.LKG)
	if err != nil {
		return rendered, err
	}
	return declarativerelease.RenderedManifests{Forward: forward, LKG: lkg, ForwardDigest: fmt.Sprintf("sha256:%x", sha256.Sum256(forward)), LKGDigest: fmt.Sprintf("sha256:%x", sha256.Sum256(lkg))}, nil
}
