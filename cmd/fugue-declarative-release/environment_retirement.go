package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"fugue/internal/declarativerelease"
)

type retiredEnvironment struct {
	Field, Container string
	Entry            map[string]any
}

var errRetirementContainerAbsent = errors.New("retirement container absent")

// BindManifestTransition derives deletion authority only from the two sealed
// manifests, in both directions so compensation retains the same guarantees.
func (cluster *kubectlCluster) BindManifestTransition(release declarativerelease.PlanRelease, forward, lkg []byte) error {
	cluster.envRetirements = nil
	if !release.ExpectedPreviousPresent || len(bytes.TrimSpace(lkg)) == 0 {
		return nil
	}
	result := map[string][]retiredEnvironment{}
	for _, pair := range [][2][]byte{{lkg, forward}, {forward, lkg}} {
		source, err := declarativerelease.DecodeResourceSet(bytes.NewReader(pair[0]))
		if err != nil {
			return err
		}
		target, err := declarativerelease.DecodeResourceSet(bytes.NewReader(pair[1]))
		if err != nil {
			return err
		}
		for _, desired := range target.Items {
			metadata := mapField(desired, "metadata")
			identity := declarativerelease.ResourceIdentity{APIVersion: stringValue(desired["apiVersion"]), Kind: stringValue(desired["kind"]), Namespace: stringValue(metadata["namespace"]), Name: stringValue(metadata["name"])}
			if !releaseDeclaresResource(release, identity) {
				continue
			}
			var previous map[string]any
			for _, item := range source.Items {
				m := mapField(item, "metadata")
				if stringValue(item["apiVersion"]) == identity.APIVersion && stringValue(item["kind"]) == identity.Kind && stringValue(m["name"]) == identity.Name && stringValue(m["namespace"]) == identity.Namespace {
					previous = item
					break
				}
			}
			if previous == nil {
				continue
			}
			for _, field := range []string{"containers", "initContainers"} {
				for _, raw := range anySlice(mapField(mapField(mapField(desired, "spec"), "template"), "spec")[field]) {
					container, _ := raw.(map[string]any)
					name := stringValue(container["name"])
					if !retirementContainerOwned(release, identity, field, name) {
						continue
					}
					old, _, err := retirementContainer(previous, field, name)
					if errors.Is(err, errRetirementContainerAbsent) {
						continue
					}
					if err != nil {
						return err
					}
					oldEnv, err := retirementEnvIndex(old)
					if err != nil {
						return err
					}
					newEnv, err := retirementEnvIndex(container)
					if err != nil {
						return err
					}
					for n, e := range oldEnv {
						if _, kept := newEnv[n]; kept {
							continue
						}
						if !runtimeEnvNamePattern.MatchString(n) || len(e) > 2 {
							return errors.New("environment retirement supports literal values only")
						}
						if _, ok := e["valueFrom"]; ok {
							// Reference fields keep ordinary SSA semantics. This bridge
							// must not prevent adding a reference merely because the
							// inverse rollback would remove it.
							continue
						}
						if v, ok := e["value"]; ok {
							if _, literal := v.(string); !literal {
								return errors.New("retired environment value is not literal")
							}
						}
						key := retirementResourceKey(desired)
						result[key] = append(result[key], retiredEnvironment{field, name, e})
					}
				}
			}
		}
	}
	cluster.envRetirements = result
	return nil
}

func retirementResourceKey(value map[string]any) string {
	raw, _ := declarativerelease.CanonicalJSON(value)
	copy, _ := decodeJSONObject(raw)
	m := mapField(copy, "metadata")
	delete(m, "uid")
	delete(m, "resourceVersion")
	return digestJSON(copy)
}
func retirementContainerOwned(r declarativerelease.PlanRelease, id declarativerelease.ResourceIdentity, field, name string) bool {
	if field == "containers" && r.Workload.APIVersion == id.APIVersion && r.Workload.Kind == id.Kind && r.Workload.Namespace == id.Namespace && r.Workload.Name == id.Name && r.Workload.Container == name {
		return true
	}
	for _, t := range r.ArtifactTargets {
		f := "containers"
		if t.ContainerType == "init-container" {
			f = "initContainers"
		}
		if f == field && t.APIVersion == id.APIVersion && t.Kind == id.Kind && t.Namespace == id.Namespace && t.Name == id.Name && t.Container == name {
			return true
		}
	}
	return false
}
func retirementContainer(value map[string]any, field, name string) (map[string]any, int, error) {
	var found map[string]any
	index := -1
	for i, raw := range anySlice(mapField(mapField(mapField(value, "spec"), "template"), "spec")[field]) {
		c, _ := raw.(map[string]any)
		if stringValue(c["name"]) == name {
			if found != nil {
				return nil, 0, errors.New("ambiguous retirement container")
			}
			found, index = c, i
		}
	}
	if found == nil {
		return nil, 0, errRetirementContainerAbsent
	}
	return found, index, nil
}
func retirementEnvIndex(c map[string]any) (map[string]map[string]any, error) {
	out := map[string]map[string]any{}
	for _, raw := range anySlice(c["env"]) {
		e, ok := raw.(map[string]any)
		name := stringValue(e["name"])
		if !ok || name == "" || out[name] != nil {
			return nil, errors.New("ambiguous environment entries")
		}
		out[name] = e
	}
	return out, nil
}

// The only extra write is deletion of the exact reviewed predecessor literals.
// No desired value is written by this bridge; ordinary SSA still applies the
// target and retains its existing conflict/ownership checks.
func retirementPatch(desired, live map[string]any, manager string, retired []retiredEnvironment) ([]map[string]any, map[string]any, error) {
	dm, lm := mapField(desired, "metadata"), mapField(live, "metadata")
	uid, rv := stringValue(lm["uid"]), stringValue(lm["resourceVersion"])
	if uid == "" || uid != stringValue(dm["uid"]) || !validKubernetesResourceVersion(rv) || rv != stringValue(dm["resourceVersion"]) {
		return nil, nil, errors.New("environment retirement UID/RV changed")
	}
	raw, _ := declarativerelease.CanonicalJSON(live)
	expected, _ := decodeJSONObject(raw)
	type removal struct {
		path      string
		index     int
		entry     map[string]any
		container map[string]any
	}
	removals := []removal{}
	for _, r := range retired {
		c, ci, err := retirementContainer(expected, r.Field, r.Container)
		if err != nil {
			return nil, nil, err
		}
		entries, err := retirementEnvIndex(c)
		if err != nil {
			return nil, nil, err
		}
		name := stringValue(r.Entry["name"])
		entry, present := entries[name]
		if !present {
			continue
		}
		if literalEnvironmentDigest(entry) != literalEnvironmentDigest(r.Entry) {
			return nil, nil, errors.New("retired environment differs from reviewed predecessor")
		}
		pointer := "/spec/template/spec/" + r.Field + "[name=" + r.Container + "]/env[name=" + name + "]"
		if !managedFieldsOwnPointers(lm, manager, []string{pointer}) {
			return nil, nil, errors.New("retired environment is not owned by this component")
		}
		for _, raw := range anySlice(lm["managedFields"]) {
			m, _ := raw.(map[string]any)
			if stringValue(m["subresource"]) != "" {
				continue
			}
			owner := stringValue(m["manager"])
			if owner != manager && managedFieldsEntryOwnsPointers(mapField(m, "fieldsV1"), []string{pointer}, false) && !(owner == "helm" && stringValue(m["operation"]) == "Update") {
				return nil, nil, errors.New("retired environment has an unreviewed owner")
			}
		}
		for i, raw := range anySlice(c["env"]) {
			if stringValue(mapFieldValue(raw, "name")) == name {
				removals = append(removals, removal{"/spec/template/spec/" + r.Field + "/" + strconv.Itoa(ci) + "/env", i, entry, c})
				break
			}
		}
	}
	if len(removals) == 0 {
		return nil, expected, nil
	}
	sort.Slice(removals, func(i, j int) bool {
		if removals[i].path == removals[j].path {
			return removals[i].index > removals[j].index
		}
		return removals[i].path < removals[j].path
	})
	patch := []map[string]any{{"op": "test", "path": "/metadata/uid", "value": uid}, {"op": "test", "path": "/metadata/resourceVersion", "value": rv}}
	for _, r := range removals {
		path := r.path + "/" + strconv.Itoa(r.index)
		patch = append(patch, map[string]any{"op": "test", "path": path, "value": r.entry}, map[string]any{"op": "remove", "path": path})
		env := anySlice(r.container["env"])
		r.container["env"] = append(env[:r.index], env[r.index+1:]...)
	}
	return patch, expected, nil
}
func literalEnvironmentDigest(entry map[string]any) string {
	copy := make(map[string]any, len(entry))
	for key, value := range entry {
		// Kubernetes omits the zero-value string on serialization.
		if key != "value" || value != "" {
			copy[key] = value
		}
	}
	return digestJSON(copy)
}

func mapFieldValue(raw any, key string) any { m, _ := raw.(map[string]any); return m[key] }

func (cluster *kubectlCluster) retireEnvironment(ctx context.Context, release declarativerelease.PlanRelease, identity declarativerelease.ResourceIdentity, desired map[string]any, encoded []byte, dryRun bool) (map[string]any, []byte, map[string]any, error) {
	retired := cluster.envRetirements[retirementResourceKey(desired)]
	if len(retired) == 0 {
		return desired, encoded, nil, nil
	}
	raw, err := cluster.getResource(ctx, identity)
	if err != nil {
		return nil, nil, nil, err
	}
	live, err := decodeJSONObject(raw)
	if err != nil {
		return nil, nil, nil, err
	}
	patch, expected, err := retirementPatch(desired, live, release.Workload.FieldManager, retired)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(patch) == 0 {
		return desired, encoded, nil, nil
	}
	data, err := declarativerelease.CanonicalJSON(patch)
	if err != nil {
		return nil, nil, nil, err
	}
	args := []string{"patch", strings.ToLower(identity.Kind), identity.Name, "--namespace", identity.Namespace, "--type=json", "--field-manager", release.Workload.FieldManager, "--patch", string(data), "--output", "json"}
	if dryRun {
		args = append(args, "--dry-run=server")
	}
	patched, err := cluster.kubectlRun(ctx, nil, args...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("retire reviewed environment: %w", err)
	}
	fresh, err := decodeJSONObject(patched)
	if err != nil {
		return nil, nil, nil, err
	}
	fm, lm := mapField(fresh, "metadata"), mapField(live, "metadata")
	if stringValue(fm["uid"]) != stringValue(lm["uid"]) || !validKubernetesResourceVersion(stringValue(fm["resourceVersion"])) || digestJSON(sanitizeObservedResource(expected)) != digestJSON(sanitizeObservedResource(fresh)) {
		return nil, nil, nil, errors.New("environment retirement changed unreviewed resource fields")
	}
	if dryRun {
		return desired, encoded, nil, nil
	}
	if stringValue(fm["resourceVersion"]) == stringValue(lm["resourceVersion"]) || int64Value(fm["generation"]) != int64Value(lm["generation"])+1 {
		return nil, nil, nil, errors.New("environment retirement generation witness invalid")
	}
	rebound, err := decodeJSONObject(encoded)
	if err != nil {
		return nil, nil, nil, err
	}
	mapField(rebound, "metadata")["resourceVersion"] = fm["resourceVersion"]
	output, err := declarativerelease.CanonicalJSON(rebound)
	return rebound, output, fresh, err
}
