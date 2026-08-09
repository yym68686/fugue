package declarativerelease

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	return buildOwnershipScopedManifest(lkgManifest, adoption, false)
}

// BuildOwnershipTakeoverManifest materializes only the reviewed fields from
// the immutable forward target after an equal-value bootstrap adoption. This
// is the bounded adopting-only path for transferring legacy leaf ownership.
func BuildOwnershipTakeoverManifest(targetManifest []byte, adoption OwnershipAdoptionPlan, target TargetIdentity) ([]byte, error) {
	if (!adoption.AlreadyConverged && !adoption.ResumeTakeover) || adoption.AlreadyConverged && adoption.ResumeTakeover ||
		!target.Present || !digestPattern.MatchString(target.ManifestDigest) ||
		target.ManifestDigest != digestOf(targetManifest) || !shaPattern.MatchString(target.ConfigSHA) ||
		target.ManifestSHA != target.ConfigSHA || target.OCIRevision != target.ConfigSHA ||
		!strings.Contains(target.ImageRef, "@sha256:") || len(adoption.Resources) == 0 {
		return nil, errors.New("ownership takeover is not bound to its immutable forward target")
	}
	return buildOwnershipScopedManifest(targetManifest, adoption, true)
}

func buildOwnershipScopedManifest(sourceManifest []byte, adoption OwnershipAdoptionPlan, takeover bool) ([]byte, error) {
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
		if ownershipAdoptionUsesOnlyArtifactImages(scope.Fields) || ownershipAdoptionUsesExactAPISourceAndImage(adoption.Component, scope) {
			metadata, ok := source["metadata"].(map[string]any)
			if !ok {
				return nil, errors.New("ownership adoption resource metadata is invalid")
			}
			metadata["uid"] = scope.UID
			metadata["resourceVersion"] = scope.ResourceVersion
			result.Items = append(result.Items, source)
			continue
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
		if takeover {
			scaffolds, err := validateOwnershipValidationScaffolds(scope)
			if err != nil {
				return nil, err
			}
			for _, scaffold := range scaffolds {
				if adoptionScopeContainsPointer(scope.Fields, scaffold.Pointer) {
					// A reviewed field already carries its immutable forward
					// value. Its scaffold is prewrite validation metadata only;
					// replacing the field here would erase the takeover.
					continue
				}
				if err := setAdoptionPointer(item, scaffold.Pointer, scaffold.Value); err != nil {
					return nil, err
				}
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

func ownershipAdoptionUsesExactAPISourceAndImage(component string, scope OwnershipAdoptionResourcePlan) bool {
	return component == "api" && scope.Identity == (ResourceIdentity{
		APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api",
	}) && equalStrings(scope.Fields, []string{
		"/spec/template/metadata/annotations/fugue.pro~1source-commit",
		"/spec/template/spec/containers[name=api]/image",
	})
}

func ownershipAdoptionUsesOnlyArtifactImages(fields []string) bool {
	if len(fields) == 0 {
		return false
	}
	for _, pointer := range fields {
		steps, err := parseAdoptionPointer(pointer)
		if err != nil || len(steps) != 5 || steps[0].Field != "spec" || steps[1].Field != "template" ||
			steps[2].Field != "spec" || (steps[3].Field != "containers" && steps[3].Field != "initContainers") ||
			steps[3].Name == "" || steps[4].Field != "image" || steps[4].Name != "" {
			return false
		}
	}
	return true
}

func adoptionScopeContainsPointer(fields []string, pointer string) bool {
	for _, field := range fields {
		if field == pointer {
			return true
		}
	}
	return false
}

func validateOwnershipValidationScaffolds(scope OwnershipAdoptionResourcePlan) ([]OwnershipValidationScaffold, error) {
	pointers, err := OwnershipTakeoverValidationScaffolds(scope.Fields)
	if err != nil {
		return nil, err
	}
	if len(pointers) != len(scope.ValidationScaffolds) {
		return nil, errors.New("ownership takeover validation scaffolds are incomplete")
	}
	result := append([]OwnershipValidationScaffold(nil), scope.ValidationScaffolds...)
	sort.Slice(result, func(i, j int) bool { return result[i].Pointer < result[j].Pointer })
	for index, pointer := range pointers {
		if result[index].Pointer != pointer || !strings.Contains(result[index].Value, "@sha256:") ||
			(index > 0 && result[index-1].Pointer == pointer) {
			return nil, errors.New("ownership takeover validation scaffold is invalid")
		}
	}
	return result, nil
}

// OwnershipTakeoverValidationScaffolds returns fields required only to make a
// reviewed leaf-level Pod container apply structurally valid. They are not
// takeover fields unless the exact pointer is itself reviewed. The Kubernetes
// adapter proves the live scaffold value and declarative ownership; a reviewed
// scaffold may retain only an explicitly bound legacy Update owner until the
// typed takeover conflict is forced.
func OwnershipTakeoverValidationScaffolds(fields []string) ([]string, error) {
	set := map[string]struct{}{}
	for _, pointer := range fields {
		steps, err := parseAdoptionPointer(pointer)
		if err != nil {
			return nil, err
		}
		if len(steps) < 5 || steps[0].Field != "spec" || steps[1].Field != "template" ||
			steps[2].Field != "spec" || steps[3].Field != "containers" || steps[3].Name == "" ||
			steps[4].Field == "image" {
			continue
		}
		pointer := "/spec/template/spec/containers[name=" + steps[3].Name + "]/image"
		set[pointer] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for pointer := range set {
		result = append(result, pointer)
	}
	sort.Strings(result)
	return result, nil
}

// OwnershipAdoptionPointerValue resolves one validated reviewed pointer from a
// Kubernetes object. It is exposed for the adapter's exact live conflict-set
// proof; callers still cannot construct or widen an adoption scope here.
func OwnershipAdoptionPointerValue(resource map[string]any, pointer string) (any, error) {
	return adoptionPointerValue(resource, pointer)
}

func adoptionPointerValue(value map[string]any, pointer string) (any, error) {
	current := any(value)
	steps, err := parseAdoptionPointer(pointer)
	if err != nil {
		return nil, err
	}
	for _, step := range steps {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, errors.New("pointer crosses a non-object value")
		}
		current, ok = object[step.Field]
		if !ok {
			return nil, errors.New("pointer is absent from bootstrap LKG")
		}
		if step.Name != "" {
			items, ok := current.([]any)
			if !ok {
				return nil, errors.New("ownership adoption selector crosses a non-list value")
			}
			current = nil
			for _, item := range items {
				candidate, ok := item.(map[string]any)
				if ok && candidate["name"] == step.Name {
					current = candidate
					break
				}
			}
			if current == nil {
				return nil, errors.New("ownership adoption selector is absent from bootstrap LKG")
			}
		}
	}
	if pointer == "/spec/template/spec/containers" {
		containers, ok := current.([]any)
		if !ok || len(containers) == 0 {
			return nil, errors.New("ownership adoption containers are invalid")
		}
		scoped := make([]any, 0, len(containers))
		for _, raw := range containers {
			container, ok := raw.(map[string]any)
			if !ok {
				return nil, errors.New("ownership adoption container is invalid")
			}
			name, nameOK := container["name"].(string)
			image, imageOK := container["image"].(string)
			if !nameOK || name == "" || !imageOK || image == "" {
				return nil, errors.New("ownership adoption container identity is incomplete")
			}
			scoped = append(scoped, map[string]any{"name": name, "image": image})
		}
		current = scoped
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
	steps, err := parseAdoptionPointer(pointer)
	if err != nil {
		return err
	}
	if steps[len(steps)-1].Name != "" {
		return errors.New("ownership adoption selector cannot terminate a pointer")
	}
	current := value
	for _, step := range steps[:len(steps)-1] {
		next, exists := current[step.Field]
		if step.Name != "" {
			var items []any
			if exists {
				var ok bool
				items, ok = next.([]any)
				if !ok {
					return errors.New("ownership adoption fields overlap incompatibly")
				}
			}
			var selected map[string]any
			for _, item := range items {
				candidate, ok := item.(map[string]any)
				if ok && candidate["name"] == step.Name {
					selected = candidate
					break
				}
			}
			if selected == nil {
				selected = map[string]any{"name": step.Name}
				items = append(items, selected)
				current[step.Field] = items
			}
			current = selected
			continue
		}
		if !exists {
			object := map[string]any{}
			current[step.Field] = object
			current = object
			continue
		}
		object, ok := next.(map[string]any)
		if !ok {
			return errors.New("ownership adoption fields overlap incompatibly")
		}
		current = object
	}
	current[steps[len(steps)-1].Field] = field
	return nil
}

type adoptionPointerStep struct {
	Field string
	Name  string
}

// parseAdoptionPointer accepts ordinary JSON pointers plus Kubernetes
// associative-list selectors such as containers[name=dns]. Selectors are
// intentionally limited to the canonical "name" merge key so an adoption
// manifest can claim one exact container/env leaf without claiming the whole
// Pod template.
func parseAdoptionPointer(pointer string) ([]adoptionPointerStep, error) {
	if pointer == "" || pointer[0] != '/' {
		return nil, errors.New("ownership adoption pointer is invalid")
	}
	raw := strings.Split(pointer[1:], "/")
	steps := make([]adoptionPointerStep, 0, len(raw))
	for _, encoded := range raw {
		token := strings.ReplaceAll(strings.ReplaceAll(encoded, "~1", "/"), "~0", "~")
		if token == "" {
			return nil, errors.New("ownership adoption pointer contains an empty token")
		}
		step := adoptionPointerStep{Field: token}
		if open := strings.Index(token, "[name="); open >= 0 {
			if open == 0 || !strings.HasSuffix(token, "]") || strings.Count(token, "[name=") != 1 {
				return nil, errors.New("ownership adoption selector is invalid")
			}
			step.Field = token[:open]
			step.Name = token[open+len("[name=") : len(token)-1]
			if !validAdoptionSelectorName(step.Name) {
				return nil, errors.New("ownership adoption selector name is invalid")
			}
		} else if strings.ContainsAny(token, "[]") {
			return nil, errors.New("ownership adoption selector is invalid")
		}
		steps = append(steps, step)
	}
	return steps, nil
}

func validAdoptionSelectorName(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}
