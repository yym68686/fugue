package releasedomain

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// RenderedOptions supplies identities resolved by the same render builder that
// produced base and target manifests.
type RenderedOptions struct {
	DefaultNamespace    string
	Bindings            map[string]string
	IgnoreHelmTestHooks bool
}

type manifestObject struct {
	Identity    ObjectIdentity
	Labels      map[string]string
	Annotations map[string]string
	Object      map[string]any
}

// ClassifyRendered performs a structural base-to-target manifest diff and
// classifies every changed JSON Pointer through one unique ownership matcher.
func ClassifyRendered(baseManifest, targetManifest []byte, spec *OwnershipSpec, options RenderedOptions) RenderedClassification {
	classification := RenderedClassification{}
	if spec == nil {
		classification.Unknown = []Evidence{{Source: "rendered-object", Subject: "ownership", Reason: "ownership is nil"}}
		return classification
	}
	if err := spec.ValidateBindings(options.Bindings); err != nil {
		classification.Unknown = []Evidence{{Source: "rendered-object", Subject: "ownership-bindings", Reason: err.Error()}}
		return classification
	}

	baseObjects, baseUnknown := decodeManifest(baseManifest, spec, options.DefaultNamespace, "base")
	targetObjects, targetUnknown := decodeManifest(targetManifest, spec, options.DefaultNamespace, "target")
	classification.Unknown = append(classification.Unknown, baseUnknown...)
	classification.Unknown = append(classification.Unknown, targetUnknown...)

	baseByIdentity, duplicateBase := indexManifestObjects(baseObjects, "base")
	targetByIdentity, duplicateTarget := indexManifestObjects(targetObjects, "target")
	classification.Unknown = append(classification.Unknown, duplicateBase...)
	classification.Unknown = append(classification.Unknown, duplicateTarget...)

	keys := make([]string, 0, len(baseByIdentity)+len(targetByIdentity))
	seen := map[string]struct{}{}
	for key := range baseByIdentity {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for key := range targetByIdentity {
		if _, exists := seen[key]; !exists {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	domainSet := map[Domain]struct{}{}
	for _, key := range keys {
		base, baseExists := baseByIdentity[key]
		target, targetExists := targetByIdentity[key]
		baseNormalized := normalizedObject(base)
		targetNormalized := normalizedObject(target)
		if baseExists && targetExists && reflect.DeepEqual(baseNormalized, targetNormalized) {
			continue
		}

		representative := target
		if !targetExists {
			representative = base
		}
		subject := representative.Identity.String()
		if isCustomResourceDefinition(representative.Identity) {
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "rendered-object", Subject: subject, Reason: "CustomResourceDefinition changes are outside every single-domain adapter",
			})
			continue
		}
		baseHooks := helmHooks(base, baseExists)
		targetHooks := helmHooks(target, targetExists)
		if len(baseHooks) > 0 || len(targetHooks) > 0 {
			if options.IgnoreHelmTestHooks && ignorableTestHookTransition(baseHooks, baseExists, targetHooks, targetExists) {
				classification.Evidence = append(classification.Evidence, Evidence{
					Source: "rendered-object", Subject: subject, Reason: "explicitly ignored non-upgrade Helm test hook", Ignored: true,
				})
				continue
			}
			hooks := uniqueSortedStrings(append(append([]string(nil), baseHooks...), targetHooks...))
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "rendered-object", Subject: subject, Reason: "changed Helm hook is not a stable, explicitly ignored test hook: " + strings.Join(hooks, ","),
			})
			continue
		}

		matchingRules := make([]ObjectRule, 0, 1)
		matchError := error(nil)
		for _, rule := range spec.ObjectRules {
			baseMatches := true
			targetMatches := true
			var err error
			if baseExists {
				baseMatches, err = rule.matches(base, options.DefaultNamespace, options.Bindings)
				if err != nil {
					matchError = err
					break
				}
			}
			if targetExists {
				targetMatches, err = rule.matches(target, options.DefaultNamespace, options.Bindings)
				if err != nil {
					matchError = err
					break
				}
			}
			if baseMatches && targetMatches {
				matchingRules = append(matchingRules, rule)
			}
		}
		if matchError != nil {
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "rendered-object", Subject: subject, Reason: "ownership matcher failed: " + matchError.Error(),
			})
			continue
		}
		if len(matchingRules) == 0 {
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "rendered-object", Subject: subject, Reason: "no exact ownership matcher with required labels matched both base and target",
			})
			continue
		}
		if len(matchingRules) > 1 {
			ruleIDs := make([]string, 0, len(matchingRules))
			for _, rule := range matchingRules {
				ruleIDs = append(ruleIDs, rule.ID)
			}
			sort.Strings(ruleIDs)
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "rendered-object", Subject: subject, Reason: "overlapping ownership matchers: " + strings.Join(ruleIDs, ", "),
			})
			continue
		}

		pointers := make([]string, 0)
		diffJSON(baseNormalized, baseExists, targetNormalized, targetExists, "", &pointers)
		pointers = uniqueSortedStrings(pointers)
		if len(pointers) == 0 {
			classification.Unknown = append(classification.Unknown, Evidence{
				Source: "rendered-object", Subject: subject, Reason: "object changed but produced no structural JSON Pointer diff",
			})
			continue
		}
		rule := matchingRules[0]
		objectDomains := map[Domain]struct{}{}
		for _, pointer := range pointers {
			domain := rule.domainForPointer(pointer)
			objectDomains[domain] = struct{}{}
			domainSet[domain] = struct{}{}
		}
		classification.Evidence = append(classification.Evidence, Evidence{
			Source: "rendered-object", Subject: subject, Paths: pointers, Domains: domainsFromSet(objectDomains), RuleID: rule.ID,
		})
	}

	classification.Domains = domainsFromSet(domainSet)
	classification.Evidence = canonicalEvidence(classification.Evidence)
	classification.Unknown = canonicalEvidence(classification.Unknown)
	return classification
}

func decodeManifest(data []byte, spec *OwnershipSpec, defaultNamespace, side string) ([]manifestObject, []Evidence) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	objects := make([]manifestObject, 0)
	unknown := make([]Evidence, 0)
	for document := 1; ; document++ {
		var documentNode yaml.Node
		err := decoder.Decode(&documentNode)
		if err == io.EOF {
			break
		}
		if err != nil {
			unknown = append(unknown, Evidence{
				Source: "rendered-object", Subject: fmt.Sprintf("%s manifest document %d", side, document), Reason: "YAML parse failed: " + err.Error(),
			})
			break
		}
		// Helm can emit separator-delimited, comment-only documents for
		// disabled templates. They contain no YAML value and are not evidence.
		// Explicit null values ("null" or "~") are intentionally not skipped.
		if isImplicitEmptyDocument(&documentNode) {
			continue
		}
		decoded, err := strictManifestValue(&documentNode, "$")
		if err != nil {
			unknown = append(unknown, Evidence{
				Source: "rendered-object", Subject: fmt.Sprintf("%s manifest document %d", side, document), Reason: "YAML validation failed: " + err.Error(),
			})
			continue
		}
		raw, ok := decoded.(map[string]any)
		if !ok {
			reason := "document root must be an object"
			if decoded == nil {
				reason = "document root must not be null or empty"
			}
			unknown = append(unknown, Evidence{
				Source: "rendered-object", Subject: fmt.Sprintf("%s manifest document %d", side, document), Reason: reason,
			})
			continue
		}
		if len(raw) == 0 {
			unknown = append(unknown, Evidence{
				Source: "rendered-object", Subject: fmt.Sprintf("%s manifest document %d", side, document), Reason: "document root object must not be empty",
			})
			continue
		}
		expanded, err := expandManifestMap(raw, spec, defaultNamespace)
		if err != nil {
			unknown = append(unknown, Evidence{
				Source: "rendered-object", Subject: fmt.Sprintf("%s manifest document %d", side, document), Reason: err.Error(),
			})
			continue
		}
		objects = append(objects, expanded...)
	}
	return objects, unknown
}

func isImplicitEmptyDocument(document *yaml.Node) bool {
	if document == nil || document.Kind != yaml.DocumentNode || len(document.Content) != 1 {
		return false
	}
	root := document.Content[0]
	// yaml.v3 represents a separator-delimited or comment-only document as an
	// unstyled implicit null scalar. An explicitly tagged null can have the same
	// tag and value (for example `!!null ""`), but TaggedStyle records that it
	// was real input and therefore must be validated and rejected below.
	return root != nil && root.Kind == yaml.ScalarNode && root.Tag == "!!null" && root.Value == "" && root.Style == 0
}

// manifestNumber is an exact, canonical rational representation of a YAML
// number. Keeping it out of float64 is important: rendered release evidence
// must distinguish adjacent integers above 2^53.
type manifestNumber string

func strictManifestValue(node *yaml.Node, path string) (any, error) {
	if node == nil {
		return nil, fmt.Errorf("%s is missing", path)
	}
	if node.Anchor != "" {
		return nil, fmt.Errorf("%s uses YAML anchor %q", path, node.Anchor)
	}
	if node.Kind == yaml.AliasNode {
		return nil, fmt.Errorf("%s uses a YAML alias", path)
	}
	if node.Tag == "!!merge" {
		return nil, fmt.Errorf("%s uses a YAML merge key", path)
	}

	switch node.Kind {
	case yaml.DocumentNode:
		if len(node.Content) != 1 {
			return nil, fmt.Errorf("%s document must contain exactly one root value", path)
		}
		return strictManifestValue(node.Content[0], path)
	case yaml.MappingNode:
		if node.Tag != "!!map" {
			return nil, fmt.Errorf("%s mapping has unsupported tag %q", path, node.Tag)
		}
		if len(node.Content)%2 != 0 {
			return nil, fmt.Errorf("%s mapping has an incomplete key/value pair", path)
		}
		result := make(map[string]any, len(node.Content)/2)
		seen := make(map[string]struct{}, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			keyNode := node.Content[index]
			if keyNode == nil || keyNode.Kind != yaml.ScalarNode || keyNode.Tag != "!!str" || keyNode.Anchor != "" {
				return nil, fmt.Errorf("%s mapping key %d must be an unanchored string", path, index/2)
			}
			key := keyNode.Value
			if key == "<<" || keyNode.Tag == "!!merge" {
				return nil, fmt.Errorf("%s uses a YAML merge key", path)
			}
			if _, duplicate := seen[key]; duplicate {
				return nil, fmt.Errorf("%s contains duplicate mapping key %q", path, key)
			}
			seen[key] = struct{}{}
			childPath := path + "/" + escapeJSONPointerToken(key)
			value, err := strictManifestValue(node.Content[index+1], childPath)
			if err != nil {
				return nil, err
			}
			result[key] = value
		}
		return result, nil
	case yaml.SequenceNode:
		if node.Tag != "!!seq" {
			return nil, fmt.Errorf("%s sequence has unsupported tag %q", path, node.Tag)
		}
		result := make([]any, 0, len(node.Content))
		for index, child := range node.Content {
			value, err := strictManifestValue(child, path+"/"+strconv.Itoa(index))
			if err != nil {
				return nil, err
			}
			result = append(result, value)
		}
		return result, nil
	case yaml.ScalarNode:
		if len(node.Content) != 0 {
			return nil, fmt.Errorf("%s scalar has child nodes", path)
		}
		switch node.Tag {
		case "!!str":
			return node.Value, nil
		case "!!null":
			return nil, nil
		case "!!bool":
			switch strings.ToLower(node.Value) {
			case "true":
				return true, nil
			case "false":
				return false, nil
			default:
				return nil, fmt.Errorf("%s has invalid boolean %q", path, node.Value)
			}
		case "!!int":
			value := strings.ReplaceAll(node.Value, "_", "")
			integer, ok := new(big.Int).SetString(value, 0)
			if !ok {
				return nil, fmt.Errorf("%s has invalid integer %q", path, node.Value)
			}
			return manifestNumber(integer.String()), nil
		case "!!float":
			value := strings.ReplaceAll(node.Value, "_", "")
			number, ok := new(big.Rat).SetString(value)
			if !ok {
				return nil, fmt.Errorf("%s has non-finite or invalid number %q", path, node.Value)
			}
			return manifestNumber(number.RatString()), nil
		default:
			return nil, fmt.Errorf("%s scalar has unsupported tag %q", path, node.Tag)
		}
	default:
		return nil, fmt.Errorf("%s has unsupported YAML node kind %d", path, node.Kind)
	}
}

func expandManifestMap(raw map[string]any, spec *OwnershipSpec, defaultNamespace string) ([]manifestObject, error) {
	kind, _ := raw["kind"].(string)
	if kind == "List" {
		apiVersion, ok := raw["apiVersion"].(string)
		if !ok || apiVersion != "v1" {
			return nil, fmt.Errorf("List apiVersion must be exactly v1")
		}
		for key, value := range raw {
			switch key {
			case "apiVersion", "kind", "items":
			case "metadata":
				metadata, ok := value.(map[string]any)
				if !ok || len(metadata) != 0 {
					return nil, fmt.Errorf("List metadata must be an empty object when present")
				}
			default:
				return nil, fmt.Errorf("List contains unsupported field %q", key)
			}
		}
		items, ok := raw["items"].([]any)
		if !ok {
			return nil, fmt.Errorf("List items must be an array")
		}
		objects := make([]manifestObject, 0, len(items))
		for index, item := range items {
			itemMap, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("List item %d must be an object", index)
			}
			expanded, err := expandManifestMap(itemMap, spec, defaultNamespace)
			if err != nil {
				return nil, fmt.Errorf("List item %d: %w", index, err)
			}
			objects = append(objects, expanded...)
		}
		return objects, nil
	}
	object, err := newManifestObject(raw, spec, defaultNamespace)
	if err != nil {
		return nil, err
	}
	return []manifestObject{object}, nil
}

func newManifestObject(raw map[string]any, spec *OwnershipSpec, defaultNamespace string) (manifestObject, error) {
	apiVersion, ok := raw["apiVersion"].(string)
	if !ok || apiVersion == "" {
		return manifestObject{}, fmt.Errorf("object apiVersion is required")
	}
	kind, ok := raw["kind"].(string)
	if !ok || kind == "" {
		return manifestObject{}, fmt.Errorf("object kind is required")
	}
	group, version, err := splitAPIVersion(apiVersion)
	if err != nil {
		return manifestObject{}, err
	}
	metadata, ok := raw["metadata"].(map[string]any)
	if !ok {
		return manifestObject{}, fmt.Errorf("%s %s metadata must be an object", apiVersion, kind)
	}
	nameValue, namePresent := metadata["name"]
	name, nameIsString := nameValue.(string)
	if !namePresent || !nameIsString || name == "" {
		return manifestObject{}, fmt.Errorf("%s %s metadata.name is required", apiVersion, kind)
	}
	if generateNameValue, present := metadata["generateName"]; present {
		generateName, isString := generateNameValue.(string)
		if !isString {
			return manifestObject{}, fmt.Errorf("%s %s %s metadata.generateName must be a string", apiVersion, kind, name)
		}
		if generateName != "" {
			return manifestObject{}, fmt.Errorf("%s %s %s uses generateName", apiVersion, kind, name)
		}
	}
	namespace := ""
	if namespaceValue, present := metadata["namespace"]; present {
		var isString bool
		namespace, isString = namespaceValue.(string)
		if !isString {
			return manifestObject{}, fmt.Errorf("%s %s %s metadata.namespace must be a string", apiVersion, kind, name)
		}
	}
	if namespace == "" && !isClusterScoped(spec, group, version, kind) {
		namespace = defaultNamespace
	}
	labels, err := stringMap(metadata["labels"], "metadata.labels")
	if err != nil {
		return manifestObject{}, fmt.Errorf("%s %s %s: %w", apiVersion, kind, name, err)
	}
	annotations, err := stringMap(metadata["annotations"], "metadata.annotations")
	if err != nil {
		return manifestObject{}, fmt.Errorf("%s %s %s: %w", apiVersion, kind, name, err)
	}
	return manifestObject{
		Identity: ObjectIdentity{APIGroup: group, Version: version, Kind: kind, Namespace: namespace, Name: name},
		Labels:   labels, Annotations: annotations, Object: raw,
	}, nil
}

func splitAPIVersion(apiVersion string) (string, string, error) {
	parts := strings.Split(apiVersion, "/")
	switch len(parts) {
	case 1:
		if parts[0] == "" {
			return "", "", fmt.Errorf("apiVersion is empty")
		}
		return "", parts[0], nil
	case 2:
		if parts[0] == "" || parts[1] == "" {
			return "", "", fmt.Errorf("invalid apiVersion %q", apiVersion)
		}
		return parts[0], parts[1], nil
	default:
		return "", "", fmt.Errorf("invalid apiVersion %q", apiVersion)
	}
}

func stringMap(value any, field string) (map[string]string, error) {
	if value == nil {
		return map[string]string{}, nil
	}
	input, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object", field)
	}
	result := make(map[string]string, len(input))
	for key, raw := range input {
		text, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("%s[%q] must be a string", field, key)
		}
		result[key] = text
	}
	return result, nil
}

func isClusterScoped(spec *OwnershipSpec, group, version, kind string) bool {
	for _, rule := range spec.ObjectRules {
		if rule.APIGroup == group && rule.Version == version && rule.Kind == kind && rule.Scope == ScopeCluster {
			return true
		}
	}
	key := group + "/" + kind
	switch key {
	case "apiextensions.k8s.io/CustomResourceDefinition",
		"scheduling.k8s.io/PriorityClass",
		"rbac.authorization.k8s.io/ClusterRole",
		"rbac.authorization.k8s.io/ClusterRoleBinding",
		"storage.k8s.io/StorageClass",
		"snapshot.storage.k8s.io/VolumeSnapshotClass",
		"/Namespace",
		"/Node":
		return true
	default:
		return false
	}
}

func indexManifestObjects(objects []manifestObject, side string) (map[string]manifestObject, []Evidence) {
	indexed := make(map[string]manifestObject, len(objects))
	duplicates := make(map[string]struct{})
	unknown := make([]Evidence, 0)
	for _, object := range objects {
		key := identityKey(object.Identity)
		if _, duplicate := duplicates[key]; duplicate {
			continue
		}
		if _, exists := indexed[key]; exists {
			unknown = append(unknown, Evidence{
				Source: "rendered-object", Subject: object.Identity.String(), Reason: side + " manifest contains duplicate object identity",
			})
			delete(indexed, key)
			duplicates[key] = struct{}{}
			continue
		}
		indexed[key] = object
	}
	return indexed, unknown
}

func identityKey(identity ObjectIdentity) string {
	return strings.Join([]string{identity.APIGroup, identity.Version, identity.Kind, identity.Namespace, identity.Name}, "\x00")
}

func normalizedObject(object manifestObject) map[string]any {
	if object.Object == nil {
		return nil
	}
	result := cloneManifestMap(object.Object)
	delete(result, "status")
	metadata, _ := result["metadata"].(map[string]any)
	if metadata != nil {
		if value, exists := metadata["creationTimestamp"]; exists && (value == nil || value == "") {
			delete(metadata, "creationTimestamp")
		}
		if object.Identity.Namespace != "" {
			metadata["namespace"] = object.Identity.Namespace
		}
	}
	return result
}

func cloneManifestMap(input map[string]any) map[string]any {
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = cloneManifestValue(value)
	}
	return result
}

func cloneManifestValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneManifestMap(typed)
	case []any:
		result := make([]any, len(typed))
		for index, item := range typed {
			result[index] = cloneManifestValue(item)
		}
		return result
	default:
		return typed
	}
}

func isCustomResourceDefinition(identity ObjectIdentity) bool {
	return identity.APIGroup == "apiextensions.k8s.io" && identity.Kind == "CustomResourceDefinition"
}

func helmHooks(object manifestObject, exists bool) []string {
	values := map[string]struct{}{}
	if !exists {
		return nil
	}
	for _, value := range strings.Split(object.Annotations["helm.sh/hook"], ",") {
		value = strings.TrimSpace(value)
		if value != "" {
			values[value] = struct{}{}
		}
	}
	return uniqueSortedMapKeys(values)
}

func ignorableTestHookTransition(baseHooks []string, baseExists bool, targetHooks []string, targetExists bool) bool {
	if baseExists && !onlyTestHooks(baseHooks) {
		return false
	}
	if targetExists && !onlyTestHooks(targetHooks) {
		return false
	}
	if baseExists && targetExists && !reflect.DeepEqual(baseHooks, targetHooks) {
		return false
	}
	return (baseExists && len(baseHooks) > 0) || (targetExists && len(targetHooks) > 0)
}

func onlyTestHooks(hooks []string) bool {
	if len(hooks) == 0 {
		return false
	}
	for _, hook := range hooks {
		if hook != "test" && hook != "test-success" && hook != "test-failure" {
			return false
		}
	}
	return true
}

func diffJSON(base any, baseExists bool, target any, targetExists bool, pointer string, output *[]string) {
	if baseExists && targetExists && reflect.DeepEqual(base, target) {
		return
	}
	baseMap, baseIsMap := base.(map[string]any)
	targetMap, targetIsMap := target.(map[string]any)
	if (baseIsMap || !baseExists) && (targetIsMap || !targetExists) && (baseIsMap || targetIsMap) {
		keys := map[string]struct{}{}
		for key := range baseMap {
			keys[key] = struct{}{}
		}
		for key := range targetMap {
			keys[key] = struct{}{}
		}
		ordered := uniqueSortedMapKeys(keys)
		if len(ordered) == 0 {
			*output = append(*output, pointer)
			return
		}
		for _, key := range ordered {
			baseValue, baseValueExists := baseMap[key]
			targetValue, targetValueExists := targetMap[key]
			diffJSON(baseValue, baseValueExists, targetValue, targetValueExists, pointer+"/"+escapeJSONPointerToken(key), output)
		}
		return
	}
	baseArray, baseIsArray := base.([]any)
	targetArray, targetIsArray := target.([]any)
	if baseExists && targetExists && baseIsArray && targetIsArray && len(baseArray) == len(targetArray) {
		if len(baseArray) == 0 {
			*output = append(*output, pointer)
			return
		}
		for index := range baseArray {
			diffJSON(baseArray[index], true, targetArray[index], true, pointer+"/"+strconv.Itoa(index), output)
		}
		return
	}
	*output = append(*output, pointer)
}

func escapeJSONPointerToken(value string) string {
	value = strings.ReplaceAll(value, "~", "~0")
	return strings.ReplaceAll(value, "/", "~1")
}

func uniqueSortedStrings(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return uniqueSortedMapKeys(set)
}

func uniqueSortedMapKeys[T any](values map[string]T) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
