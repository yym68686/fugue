package declarativerelease

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// BuildOwnershipAdoptionManifest materializes the only document that may be
// applied with force-conflicts. It contains live LKG values for reviewed
// fields and per-resource UID/RV preconditions; it cannot change runtime
// behavior during ownership transfer.
func BuildOwnershipAdoptionManifest(lkgManifest []byte, adoption OwnershipAdoptionPlan) ([]byte, error) {
	if !digestPattern.MatchString(adoption.BootstrapLKGDigest) || adoption.BootstrapLKGDigest != digestOf(lkgManifest) ||
		len(adoption.Resources) == 0 {
		return nil, errors.New("ownership adoption is not bound to its bootstrap LKG")
	}
	return buildOwnershipScopedManifest(lkgManifest, adoption)
}

// BuildOwnershipTakeoverManifest materializes only the reviewed fields from
// the immutable forward target after an equal-value bootstrap adoption. This
// is the bounded adopting-only path for transferring legacy leaf ownership.
func BuildOwnershipTakeoverManifest(targetManifest []byte, adoption OwnershipAdoptionPlan, target TargetIdentity) ([]byte, error) {
	if !adoption.AlreadyConverged || !target.Present || !digestPattern.MatchString(target.ManifestDigest) ||
		target.ManifestDigest != digestOf(targetManifest) || !shaPattern.MatchString(target.ConfigSHA) ||
		target.ManifestSHA != target.ConfigSHA || target.OCIRevision != target.ConfigSHA ||
		!strings.Contains(target.ImageRef, "@sha256:") || len(adoption.Resources) == 0 {
		return nil, errors.New("ownership takeover is not bound to its immutable forward target")
	}
	return buildOwnershipScopedManifest(targetManifest, adoption)
}

func buildOwnershipScopedManifest(sourceManifest []byte, adoption OwnershipAdoptionPlan) ([]byte, error) {
	set, err := DecodeResourceSet(bytes.NewReader(sourceManifest))
	if err != nil {
		return nil, err
	}
	result := ResourceSet{APIVersion: ResourceSetAPIVersion, Kind: ResourceSetKind}
	for _, scope := range adoption.Resources {
		if scope.UID == "" || !resourceVersionPattern.MatchString(scope.ResourceVersion) || scope.Generation < 1 || len(scope.Fields) == 0 {
			return nil, fmt.Errorf("ownership adoption resource %s/%s has invalid CAS", scope.Identity.Kind, scope.Identity.Name)
		}
		source, err := ResourceSetItem(sourceManifest, scope.Identity)
		if err != nil {
			return nil, err
		}
		item := map[string]any{
			"apiVersion": scope.Identity.APIVersion,
			"kind":       scope.Identity.Kind,
			"metadata": map[string]any{
				"name": scope.Identity.Name, "namespace": scope.Identity.Namespace,
				"uid": scope.UID, "resourceVersion": scope.ResourceVersion,
			},
		}
		for _, pointer := range scope.Fields {
			value, err := adoptionPointerValue(source, pointer)
			if err != nil {
				return nil, fmt.Errorf("ownership adoption field %s: %w", pointer, err)
			}
			if err := setAdoptionPointer(item, pointer, value); err != nil {
				return nil, err
			}
		}
		result.Items = append(result.Items, item)
	}
	sortResourceSet(&result)
	if len(result.Items) != len(set.Items) && len(result.Items) > len(set.Items) {
		return nil, errors.New("ownership adoption resource set expanded unexpectedly")
	}
	return CanonicalJSON(result)
}

func adoptionPointerValue(value map[string]any, pointer string) (any, error) {
	current := any(value)
	for _, token := range adoptionPointerTokens(pointer) {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, errors.New("pointer crosses a non-object value")
		}
		current, ok = object[token]
		if !ok {
			return nil, errors.New("pointer is absent from bootstrap LKG")
		}
	}
	raw, err := json.Marshal(current)
	if err != nil {
		return nil, err
	}
	var copied any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&copied); err != nil {
		return nil, err
	}
	return copied, nil
}

func setAdoptionPointer(value map[string]any, pointer string, field any) error {
	tokens := adoptionPointerTokens(pointer)
	if len(tokens) == 0 {
		return errors.New("ownership adoption cannot claim the whole resource")
	}
	current := value
	for _, token := range tokens[:len(tokens)-1] {
		next, exists := current[token]
		if !exists {
			object := map[string]any{}
			current[token] = object
			current = object
			continue
		}
		object, ok := next.(map[string]any)
		if !ok {
			return errors.New("ownership adoption fields overlap incompatibly")
		}
		current = object
	}
	current[tokens[len(tokens)-1]] = field
	return nil
}

func adoptionPointerTokens(pointer string) []string {
	if pointer == "" || pointer[0] != '/' {
		return nil
	}
	raw := strings.Split(pointer[1:], "/")
	for index := range raw {
		raw[index] = strings.ReplaceAll(strings.ReplaceAll(raw[index], "~1", "/"), "~0", "~")
	}
	return raw
}
