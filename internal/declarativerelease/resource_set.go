package declarativerelease

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// ReferencedRequiredSecrets returns only Secret names required by Pod specs.
// Callers can use the Kubernetes metadata API to prove presence without ever
// requesting or materializing Secret data.
func ReferencedRequiredSecrets(manifest []byte) ([]string, error) {
	set, err := DecodeResourceSet(bytes.NewReader(manifest))
	if err != nil {
		return nil, err
	}
	names := make(map[string]struct{})
	add := func(value any, optional bool) error {
		if optional {
			return nil
		}
		name, _ := value.(string)
		name = strings.TrimSpace(name)
		if !componentIDPattern.MatchString(name) {
			return errors.New("required Secret name is invalid")
		}
		names[name] = struct{}{}
		return nil
	}
	for _, item := range set.Items {
		identity, identityErr := resourceIdentity(item)
		if identityErr != nil {
			return nil, identityErr
		}
		if identity.Kind != "Deployment" && identity.Kind != "DaemonSet" && identity.Kind != "Job" {
			continue
		}
		spec, specErr := objectField(item, "spec")
		if specErr != nil {
			return nil, specErr
		}
		template, specErr := objectField(spec, "template")
		if specErr != nil {
			return nil, specErr
		}
		podSpec, specErr := objectField(template, "spec")
		if specErr != nil {
			return nil, specErr
		}
		if pullSecrets, ok := podSpec["imagePullSecrets"].([]any); ok {
			for _, raw := range pullSecrets {
				secret, ok := raw.(map[string]any)
				if !ok {
					return nil, errors.New("imagePullSecrets entry is invalid")
				}
				if err := add(secret["name"], false); err != nil {
					return nil, err
				}
			}
		}
		if volumes, ok := podSpec["volumes"].([]any); ok {
			for _, raw := range volumes {
				volume, ok := raw.(map[string]any)
				if !ok {
					return nil, errors.New("Pod volume is invalid")
				}
				secret, ok := volume["secret"].(map[string]any)
				if !ok {
					continue
				}
				optional, _ := secret["optional"].(bool)
				if err := add(secret["secretName"], optional); err != nil {
					return nil, err
				}
			}
		}
		for _, field := range []string{"initContainers", "containers"} {
			containers, _ := podSpec[field].([]any)
			for _, raw := range containers {
				container, ok := raw.(map[string]any)
				if !ok {
					return nil, errors.New("Pod container is invalid")
				}
				env, _ := container["env"].([]any)
				for _, rawEnv := range env {
					variable, ok := rawEnv.(map[string]any)
					if !ok {
						return nil, errors.New("container environment is invalid")
					}
					valueFrom, _ := variable["valueFrom"].(map[string]any)
					secret, ok := valueFrom["secretKeyRef"].(map[string]any)
					if !ok {
						continue
					}
					optional, _ := secret["optional"].(bool)
					if err := add(secret["name"], optional); err != nil {
						return nil, err
					}
				}
				envFrom, _ := container["envFrom"].([]any)
				for _, rawEnvFrom := range envFrom {
					source, ok := rawEnvFrom.(map[string]any)
					if !ok {
						return nil, errors.New("container envFrom is invalid")
					}
					secret, ok := source["secretRef"].(map[string]any)
					if !ok {
						continue
					}
					optional, _ := secret["optional"].(bool)
					if err := add(secret["name"], optional); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	out := make([]string, 0, len(names))
	for name := range names {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

const (
	ResourceSetAPIVersion = "release.fugue.dev/v2"
	ResourceSetKind       = "ComponentResourceSet"
)

// ResourceSet is the sole Git-owned deployment input. It deliberately uses
// ordinary Kubernetes objects and preserves their declarative semantics; the
// release entrypoint only adds identity, image and CAS bindings.
type ResourceSet struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	Items      []map[string]any `json:"items"`
}

type ResourceIdentity struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
}

func DecodeResourceSet(reader io.Reader) (ResourceSet, error) {
	if reader == nil {
		return ResourceSet{}, errors.New("component resource set reader is nil")
	}
	decoder := json.NewDecoder(io.LimitReader(reader, maxWorkloadManifestBytes))
	decoder.UseNumber()
	var set ResourceSet
	if err := decoder.Decode(&set); err != nil {
		return ResourceSet{}, fmt.Errorf("decode component resource set: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return ResourceSet{}, errors.New("component resource set must contain exactly one JSON document")
	}
	if err := set.Validate(); err != nil {
		return ResourceSet{}, err
	}
	return set, nil
}

func (set ResourceSet) Validate() error {
	if set.APIVersion != ResourceSetAPIVersion || set.Kind != ResourceSetKind {
		return errors.New("component resource set identity is invalid")
	}
	if len(set.Items) == 0 || len(set.Items) > 32 {
		return errors.New("component resource set item count is invalid")
	}
	previous := ""
	for index, item := range set.Items {
		identity, err := resourceIdentity(item)
		if err != nil {
			return fmt.Errorf("component resource set item %d: %w", index, err)
		}
		key := identity.key()
		if previous != "" && previous >= key {
			return errors.New("component resource set items must be strictly identity ordered")
		}
		metadata, _ := objectField(item, "metadata")
		retain := ensureReadStringMap(metadata, "annotations")["fugue.pro/release-retain-on-rollback"]
		if retain != "" && (retain != "true" || identity.APIVersion != "v1" || identity.Kind != "PersistentVolumeClaim") {
			return errors.New("release-retain-on-rollback is only allowed on a PersistentVolumeClaim")
		}
		previous = key
	}
	return nil
}

func ensureReadStringMap(value map[string]any, name string) map[string]string {
	result := map[string]string{}
	existing, ok := value[name].(map[string]any)
	if !ok {
		return result
	}
	for key, raw := range existing {
		if typed, ok := raw.(string); ok {
			result[key] = typed
		}
	}
	return result
}

func (set ResourceSet) Primary(workload Workload) (map[string]any, error) {
	var selected map[string]any
	for _, item := range set.Items {
		identity, err := resourceIdentity(item)
		if err != nil {
			return nil, err
		}
		if identity.APIVersion == workload.APIVersion && identity.Kind == workload.Kind &&
			identity.Namespace == workload.Namespace && identity.Name == workload.Name {
			if selected != nil {
				return nil, errors.New("component resource set primary workload is ambiguous")
			}
			selected = item
		}
	}
	if selected == nil {
		return nil, errors.New("component resource set does not contain its primary workload")
	}
	if err := validatePrimaryShape(selected, workload); err != nil {
		return nil, err
	}
	return selected, nil
}

func validatePrimaryShape(primary map[string]any, workload Workload) error {
	spec, err := objectField(primary, "spec")
	if err != nil {
		return err
	}
	switch workload.Kind {
	case "Deployment":
		replicas, ok := integerField(spec["replicas"])
		if !ok || replicas != int64(workload.Replicas) {
			return errors.New("Deployment resource-set replicas do not match the component registry")
		}
		strategy, err := objectField(spec, "strategy")
		expected := map[string]string{"rolling": "RollingUpdate", "recreate": "Recreate"}[workload.RolloutMode]
		if err != nil || expected == "" || stringField(strategy, "type") != expected {
			return errors.New("Deployment resource-set strategy does not match rolling mode")
		}
	case "DaemonSet":
		strategy, err := objectField(spec, "updateStrategy")
		if err != nil {
			return err
		}
		expected := map[string]string{"rolling": "RollingUpdate", "on-delete": "OnDelete"}[workload.RolloutMode]
		if expected == "" || stringField(strategy, "type") != expected {
			return errors.New("DaemonSet resource-set strategy does not match the component registry")
		}
	case "Job":
		if workload.RolloutMode != "job" {
			return errors.New("Job resource-set rollout mode is invalid")
		}
	default:
		return errors.New("component resource-set primary kind is unsupported")
	}
	return nil
}

func integerField(value any) (int64, bool) {
	switch typed := value.(type) {
	case json.Number:
		result, err := typed.Int64()
		return result, err == nil
	case float64:
		result := int64(typed)
		return result, float64(result) == typed
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	default:
		return 0, false
	}
}

func ResourceSetIdentities(manifest []byte) ([]ResourceIdentity, error) {
	set, err := DecodeResourceSet(bytes.NewReader(manifest))
	if err != nil {
		return nil, err
	}
	identities := make([]ResourceIdentity, 0, len(set.Items))
	for _, item := range set.Items {
		identity, err := resourceIdentity(item)
		if err != nil {
			return nil, err
		}
		identities = append(identities, identity)
	}
	return identities, nil
}

func ResourceSetItem(manifest []byte, identity ResourceIdentity) (map[string]any, error) {
	set, err := DecodeResourceSet(bytes.NewReader(manifest))
	if err != nil {
		return nil, err
	}
	for _, item := range set.Items {
		candidate, identityErr := resourceIdentity(item)
		if identityErr != nil {
			return nil, identityErr
		}
		if candidate == identity {
			return deepCopyMap(item), nil
		}
	}
	return nil, errors.New("resource identity is not in component resource set")
}

func ResourceDesiredSubset(desired, live map[string]any) bool {
	return desiredSubset(desired, live, "")
}

// RuntimeResourcesRollbackWitness removes only explicitly reviewed container
// resources from a predecessor convergence witness. This allows a recovery
// atom to repair a resource limit on an unhealthy old LKG before the first
// forward write; all code, image, identity and other workload fields remain
// part of the witness.
func RuntimeResourcesRollbackWitness(manifest []byte, targets []RuntimeResourceTarget) ([]byte, error) {
	set, err := DecodeResourceSet(bytes.NewReader(manifest))
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		artifactTarget := ArtifactTarget{APIVersion: target.APIVersion, Kind: target.Kind, Namespace: target.Namespace, Name: target.Name, Container: target.Container, ContainerType: target.ContainerType}
		workload, err := resourceSetTarget(&set, artifactTarget)
		if err != nil {
			return nil, err
		}
		container, err := workloadContainerObject(workload, target.Container, target.ContainerType)
		if err != nil {
			return nil, err
		}
		delete(container, "resources")
	}
	return CanonicalJSON(set)
}

// ResourceSetsEquivalentExceptRuntimeResources proves that two resource sets
// are byte-equivalent after removing only the explicitly reviewed container
// resources objects. It is used at Guardian admission to distinguish a
// deliberate runtime-capacity recovery from an unreviewed LKG drift.
func ResourceSetsEquivalentExceptRuntimeResources(left, right []byte, targets []RuntimeResourceTarget) (bool, error) {
	leftWitness, err := RuntimeResourcesRollbackWitness(left, targets)
	if err != nil {
		return false, err
	}
	rightWitness, err := RuntimeResourcesRollbackWitness(right, targets)
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftWitness, rightWitness), nil
}

// PredecessorConvergenceManifest removes only the ownership/receipt metadata
// introduced by this release entrypoint. Every operational field remains in
// the witness and must already match the live LKG before a first handoff.
func PredecessorConvergenceManifest(manifest []byte) ([]byte, error) {
	set, err := DecodeResourceSet(bytes.NewReader(manifest))
	if err != nil {
		return nil, err
	}
	for _, item := range set.Items {
		metadata, metadataErr := objectField(item, "metadata")
		if metadataErr != nil {
			return nil, metadataErr
		}
		if labels, ok := metadata["labels"].(map[string]any); ok {
			delete(labels, "app.kubernetes.io/managed-by")
		}
		if annotations, ok := metadata["annotations"].(map[string]any); ok {
			delete(annotations, "fugue.pro/production-config-sha")
			delete(annotations, "fugue.pro/release-plan-digest")
			delete(annotations, "fugue.pro/artifact-receipt-digest")
		}
		if spec, ok := item["spec"].(map[string]any); ok {
			if template, ok := spec["template"].(map[string]any); ok {
				if templateMetadata, ok := template["metadata"].(map[string]any); ok {
					if templateAnnotations, ok := templateMetadata["annotations"].(map[string]any); ok {
						delete(templateAnnotations, "fugue.pro/production-config-sha")
						delete(templateAnnotations, "fugue.pro/oci-revision")
					}
				}
			}
		}
	}
	return CanonicalJSON(set)
}

// BootstrapPredecessorConvergenceManifest compares every reviewed legacy
// operational field while leaving container identity to the registry and Pod
// imageID checks. This is required when a safe immutable bootstrap LKG records
// digests but the legacy workload spec still contains the corresponding tag.
func BootstrapPredecessorConvergenceManifest(manifest []byte, release PlanRelease) ([]byte, error) {
	witness, err := PredecessorConvergenceManifest(manifest)
	if err != nil {
		return nil, err
	}
	set, err := DecodeResourceSet(bytes.NewReader(witness))
	if err != nil {
		return nil, err
	}
	for _, item := range set.Items {
		spec, ok := item["spec"].(map[string]any)
		if !ok {
			continue
		}
		template, ok := spec["template"].(map[string]any)
		if !ok {
			continue
		}
		metadata, ok := template["metadata"].(map[string]any)
		if !ok {
			continue
		}
		if annotations, ok := metadata["annotations"].(map[string]any); ok {
			delete(annotations, "fugue.pro/source-commit")
		}
		// Legacy bootstrap workloads may represent sidecar images as mutable
		// tags while the reviewed LKG stores the same image immutably. Image
		// identity is verified separately from Pod imageIDs and the declared
		// artifact targets; no container image is part of this structural witness.
		templateSpec, specErr := objectField(template, "spec")
		if specErr != nil {
			return nil, specErr
		}
		for _, field := range []string{"initContainers", "containers"} {
			if containers, ok := templateSpec[field].([]any); ok {
				for _, raw := range containers {
					container, containerOK := raw.(map[string]any)
					if !containerOK {
						return nil, errors.New("bootstrap predecessor container is invalid")
					}
					delete(container, "image")
				}
			}
		}
	}
	targets := release.ArtifactTargets
	if len(targets) == 0 {
		targets = []ArtifactTarget{{
			APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind,
			Namespace: release.Workload.Namespace, Name: release.Workload.Name,
			Container: release.Workload.Container, ContainerType: "container",
		}}
	}
	for _, target := range targets {
		item, targetErr := resourceSetTarget(&set, target)
		if targetErr != nil {
			return nil, targetErr
		}
		spec, targetErr := objectField(item, "spec")
		if targetErr != nil {
			return nil, targetErr
		}
		template, targetErr := objectField(spec, "template")
		if targetErr != nil {
			return nil, targetErr
		}
		templateSpec, targetErr := objectField(template, "spec")
		if targetErr != nil {
			return nil, targetErr
		}
		field := "containers"
		if target.ContainerType == "init-container" {
			field = "initContainers"
		}
		rawContainers, exists := templateSpec[field]
		if !exists && target.ContainerType == "init-container" {
			continue
		}
		containers, ok := rawContainers.([]any)
		if !ok {
			return nil, errors.New("bootstrap predecessor container list is invalid")
		}
		matches := 0
		for _, raw := range containers {
			container, ok := raw.(map[string]any)
			if !ok {
				return nil, errors.New("bootstrap predecessor container is invalid")
			}
			if stringField(container, "name") == target.Container {
				delete(container, "image")
				matches++
			}
		}
		if matches == 0 {
			// A bootstrap LKG may predate a forward-only init container. The
			// forward manifest is still required to contain every artifact
			// target by RenderManifests.
			continue
		}
		if matches != 1 {
			return nil, errors.New("bootstrap predecessor container is ambiguous")
		}
	}
	return CanonicalJSON(set)
}

// RetryPredecessorConvergenceManifest binds an unhealthy prior declarative
// attempt to the current reviewed resource shape while excluding only the
// immutable image and release identity. The caller separately
// verifies that the live object is owned by this component's field manager and
// that its immutable image carries the same OCI revision as its source
// annotations.
func RetryPredecessorConvergenceManifest(manifest []byte, release PlanRelease) ([]byte, error) {
	set, err := DecodeResourceSet(bytes.NewReader(manifest))
	if err != nil {
		return nil, err
	}
	for _, item := range set.Items {
		metadata, metadataErr := objectField(item, "metadata")
		if metadataErr != nil {
			return nil, metadataErr
		}
		if annotations, ok := metadata["annotations"].(map[string]any); ok {
			delete(annotations, "fugue.pro/production-config-sha")
			delete(annotations, "fugue.pro/release-plan-digest")
			delete(annotations, "fugue.pro/artifact-receipt-digest")
		}
	}
	targets := release.ArtifactTargets
	if len(targets) == 0 {
		targets = []ArtifactTarget{{
			APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind,
			Namespace: release.Workload.Namespace, Name: release.Workload.Name,
			Container: release.Workload.Container, ContainerType: "container",
		}}
	}
	cleanedWorkloads := make(map[string]struct{})
	for _, target := range targets {
		item, targetErr := resourceSetTarget(&set, target)
		if targetErr != nil {
			return nil, targetErr
		}
		workloadKey := target.APIVersion + "\x00" + target.Kind + "\x00" + target.Namespace + "\x00" + target.Name
		if _, exists := cleanedWorkloads[workloadKey]; !exists {
			spec, specErr := objectField(item, "spec")
			if specErr != nil {
				return nil, specErr
			}
			template, templateErr := objectField(spec, "template")
			if templateErr != nil {
				return nil, templateErr
			}
			templateMetadata, metadataErr := objectField(template, "metadata")
			if metadataErr != nil {
				return nil, metadataErr
			}
			if annotations, ok := templateMetadata["annotations"].(map[string]any); ok {
				for _, key := range []string{
					"fugue.pro/artifact-image",
					"fugue.pro/artifact-receipt-digest",
					"fugue.pro/oci-revision",
					"fugue.pro/production-config-sha",
					"fugue.pro/release-plan-digest",
					"fugue.pro/source-commit",
				} {
					delete(annotations, key)
				}
			}
			cleanedWorkloads[workloadKey] = struct{}{}
		}
		spec, specErr := objectField(item, "spec")
		if specErr != nil {
			return nil, specErr
		}
		template, templateErr := objectField(spec, "template")
		if templateErr != nil {
			return nil, templateErr
		}
		podSpec, podSpecErr := objectField(template, "spec")
		if podSpecErr != nil {
			return nil, podSpecErr
		}
		field := "containers"
		if target.ContainerType == "init-container" {
			field = "initContainers"
		}
		containers, ok := podSpec[field].([]any)
		if !ok {
			return nil, fmt.Errorf("artifact target %s are invalid", field)
		}
		matches := 0
		for _, raw := range containers {
			container, ok := raw.(map[string]any)
			if !ok {
				return nil, errors.New("artifact target container is not an object")
			}
			if stringField(container, "name") == target.Container {
				delete(container, "image")
				matches++
			}
		}
		if matches != 1 {
			return nil, fmt.Errorf("workload manifest must contain exactly one %q %s", target.Container, target.ContainerType)
		}
	}
	return CanonicalJSON(set)
}

func desiredSubset(desired, live any, path string) bool {
	switch typed := desired.(type) {
	case map[string]any:
		candidate, ok := live.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range typed {
			if path == "metadata" && (key == "uid" || key == "resourceVersion" || key == "generation" || key == "creationTimestamp" || key == "managedFields") {
				continue
			}
			liveValue, exists := candidate[key]
			if !exists || !desiredSubset(value, liveValue, joinJSONPath(path, key)) {
				return false
			}
		}
		return true
	case []any:
		candidate, ok := live.([]any)
		if !ok || len(candidate) != len(typed) {
			return false
		}
		if key := kubernetesMapListKey(path); key != "" {
			return desiredMapListSubset(typed, candidate, path, key)
		}
		for index := range typed {
			if !desiredSubset(typed[index], candidate[index], fmt.Sprintf("%s[%d]", path, index)) {
				return false
			}
		}
		return true
	case json.Number:
		candidate, ok := live.(json.Number)
		return ok && candidate.String() == typed.String()
	default:
		return fmt.Sprint(live) == fmt.Sprint(desired)
	}
}

func kubernetesMapListKey(path string) string {
	switch {
	case strings.HasSuffix(path, ".volumes"):
		return "name"
	case strings.HasSuffix(path, ".volumeMounts"):
		return "mountPath"
	default:
		return ""
	}
}

func desiredMapListSubset(desired, live []any, path, key string) bool {
	indexed := make(map[string]map[string]any, len(live))
	for _, raw := range live {
		item, ok := raw.(map[string]any)
		value, valueOK := item[key].(string)
		if !ok || !valueOK || strings.TrimSpace(value) == "" {
			return false
		}
		if _, duplicate := indexed[value]; duplicate {
			return false
		}
		indexed[value] = item
	}
	seen := make(map[string]struct{}, len(desired))
	for _, raw := range desired {
		item, ok := raw.(map[string]any)
		value, valueOK := item[key].(string)
		if !ok || !valueOK || strings.TrimSpace(value) == "" {
			return false
		}
		if _, duplicate := seen[value]; duplicate {
			return false
		}
		seen[value] = struct{}{}
		candidate, exists := indexed[value]
		if !exists || !desiredSubset(item, candidate, path+"["+key+"="+value+"]") {
			return false
		}
	}
	return len(seen) == len(indexed)
}

func joinJSONPath(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}

func resourceIdentity(value map[string]any) (ResourceIdentity, error) {
	metadata, err := objectField(value, "metadata")
	if err != nil {
		return ResourceIdentity{}, err
	}
	identity := ResourceIdentity{
		APIVersion: stringField(value, "apiVersion"), Kind: stringField(value, "kind"),
		Namespace: stringField(metadata, "namespace"), Name: stringField(metadata, "name"),
	}
	if identity.APIVersion == "" || identity.Kind == "" || !componentIDPattern.MatchString(identity.Namespace) ||
		!componentIDPattern.MatchString(identity.Name) {
		return ResourceIdentity{}, errors.New("Kubernetes resource identity is invalid")
	}
	return identity, nil
}

func (identity ResourceIdentity) key() string {
	return identity.APIVersion + "\x00" + identity.Kind + "\x00" + identity.Namespace + "\x00" + identity.Name
}

func sortResourceSet(set *ResourceSet) {
	sort.Slice(set.Items, func(i, j int) bool {
		left, _ := resourceIdentity(set.Items[i])
		right, _ := resourceIdentity(set.Items[j])
		return left.key() < right.key()
	})
}
