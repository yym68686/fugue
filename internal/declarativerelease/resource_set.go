package declarativerelease

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

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
