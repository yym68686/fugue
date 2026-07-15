package releasedomain

import (
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	OwnershipAPIVersion = "release-domain.fugue.dev/v1"
	OwnershipKind       = "ReleaseDomainOwnership"
)

// OwnershipSpec is the declarative release-time ownership allowlist. It does
// not contain deployment state or SSoT data.
type OwnershipSpec struct {
	APIVersion       string       `yaml:"apiVersion" json:"apiVersion"`
	Kind             string       `yaml:"kind" json:"kind"`
	Domains          []Domain     `yaml:"domains" json:"domains"`
	RequiredBindings []string     `yaml:"requiredBindings" json:"requiredBindings"`
	FileRules        []FileRule   `yaml:"fileRules" json:"fileRules"`
	ValueRules       []ValueRule  `yaml:"valueRules" json:"valueRules"`
	ObjectRules      []ObjectRule `yaml:"objectRules" json:"objectRules"`
}

// FileRule classifies a path. Exactly one selector and exactly one result
// (domains, nonRuntime, or unknownReason) must be configured.
type FileRule struct {
	ID            string   `yaml:"id" json:"id"`
	Exact         string   `yaml:"exact,omitempty" json:"exact,omitempty"`
	Prefix        string   `yaml:"prefix,omitempty" json:"prefix,omitempty"`
	Suffix        string   `yaml:"suffix,omitempty" json:"suffix,omitempty"`
	Glob          string   `yaml:"glob,omitempty" json:"glob,omitempty"`
	Domains       []Domain `yaml:"domains,omitempty" json:"domains,omitempty"`
	NonRuntime    bool     `yaml:"nonRuntime,omitempty" json:"nonRuntime,omitempty"`
	UnknownReason string   `yaml:"unknownReason,omitempty" json:"unknownReason,omitempty"`
}

// ValueRule owns a JSON Pointer subtree in a versioned values file. Longest
// pointer wins so a narrower ownership or fail-closed exception is decisive.
type ValueRule struct {
	ID            string `yaml:"id" json:"id"`
	Exact         string `yaml:"exact,omitempty" json:"exact,omitempty"`
	Prefix        string `yaml:"prefix,omitempty" json:"prefix,omitempty"`
	Suffix        string `yaml:"suffix,omitempty" json:"suffix,omitempty"`
	Glob          string `yaml:"glob,omitempty" json:"glob,omitempty"`
	Pointer       string `yaml:"pointer" json:"pointer"`
	Domain        Domain `yaml:"domain,omitempty" json:"domain,omitempty"`
	UnknownReason string `yaml:"unknownReason,omitempty" json:"unknownReason,omitempty"`
}

// Scope is the Kubernetes identity scope required by an object rule.
type Scope string

const (
	ScopeCluster    Scope = "Cluster"
	ScopeNamespaced Scope = "Namespaced"
)

// NameSuffixLabel binds a dynamic object's name suffix to one required label.
// It is used for the chart's normalized authoritative-DNS group objects.
type NameSuffixLabel struct {
	Key         string `yaml:"key" json:"key"`
	ValuePrefix string `yaml:"valuePrefix" json:"valuePrefix"`
}

// PathOwner overrides the default object domain for one JSON Pointer subtree.
type PathOwner struct {
	Pointer string `yaml:"pointer" json:"pointer"`
	Domain  Domain `yaml:"domain" json:"domain"`
}

// ObjectRule is an exact GVK/scope/name allowlist entry with required labels.
// NamePrefix is permitted only with NameSuffixLabel, preserving a deterministic
// relation between the normalized name and component label.
type ObjectRule struct {
	ID              string            `yaml:"id" json:"id"`
	Domain          Domain            `yaml:"domain" json:"domain"`
	APIGroup        string            `yaml:"apiGroup" json:"apiGroup"`
	Version         string            `yaml:"version" json:"version"`
	Kind            string            `yaml:"kind" json:"kind"`
	Scope           Scope             `yaml:"scope" json:"scope"`
	Namespace       string            `yaml:"namespace,omitempty" json:"namespace,omitempty"`
	Name            string            `yaml:"name,omitempty" json:"name,omitempty"`
	NamePrefix      string            `yaml:"namePrefix,omitempty" json:"namePrefix,omitempty"`
	RequiredLabels  map[string]string `yaml:"requiredLabels,omitempty" json:"requiredLabels,omitempty"`
	NameSuffixLabel *NameSuffixLabel  `yaml:"nameSuffixLabel,omitempty" json:"nameSuffixLabel,omitempty"`
	PathOverrides   []PathOwner       `yaml:"pathOverrides,omitempty" json:"pathOverrides,omitempty"`
}

// LoadOwnership parses and validates exactly one strict YAML document.
func LoadOwnership(reader io.Reader) (*OwnershipSpec, error) {
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)
	var spec OwnershipSpec
	if err := decoder.Decode(&spec); err != nil {
		return nil, fmt.Errorf("decode ownership: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, fmt.Errorf("decode trailing ownership document: %w", err)
		}
		return nil, errors.New("ownership must contain exactly one YAML document")
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return &spec, nil
}

// Validate verifies that the allowlist is deterministic and fail closed.
func (spec *OwnershipSpec) Validate() error {
	if spec == nil {
		return errors.New("ownership is nil")
	}
	if spec.APIVersion != OwnershipAPIVersion {
		return fmt.Errorf("ownership apiVersion must be %q", OwnershipAPIVersion)
	}
	if spec.Kind != OwnershipKind {
		return fmt.Errorf("ownership kind must be %q", OwnershipKind)
	}
	if len(spec.Domains) != len(orderedDomains) || !equalDomains(spec.Domains, orderedDomains) {
		return fmt.Errorf("ownership domains must contain exactly %v", orderedDomains)
	}

	bindings := map[string]struct{}{}
	for _, binding := range spec.RequiredBindings {
		if strings.TrimSpace(binding) == "" || strings.ContainsAny(binding, "${}") {
			return fmt.Errorf("invalid required binding %q", binding)
		}
		if _, exists := bindings[binding]; exists {
			return fmt.Errorf("duplicate required binding %q", binding)
		}
		bindings[binding] = struct{}{}
	}
	validationBindings := make(map[string]string, len(bindings))
	for binding := range bindings {
		validationBindings[binding] = "validation"
	}

	fileIDs := map[string]struct{}{}
	for index := range spec.FileRules {
		rule := &spec.FileRules[index]
		if err := validateRuleID(rule.ID, fileIDs, "file"); err != nil {
			return err
		}
		if err := validatePathSelector(rule.Exact, rule.Prefix, rule.Suffix, rule.Glob); err != nil {
			return fmt.Errorf("file rule %q: %w", rule.ID, err)
		}
		results := 0
		if len(rule.Domains) > 0 {
			results++
			if len(canonicalDomains(rule.Domains)) != len(rule.Domains) {
				return fmt.Errorf("file rule %q contains duplicate domains", rule.ID)
			}
			for _, domain := range rule.Domains {
				if _, err := ParseDomain(string(domain)); err != nil {
					return fmt.Errorf("file rule %q: %w", rule.ID, err)
				}
			}
			rule.Domains = canonicalDomains(rule.Domains)
		}
		if rule.NonRuntime {
			results++
		}
		if rule.UnknownReason != "" {
			results++
		}
		if results != 1 {
			return fmt.Errorf("file rule %q must define exactly one classification", rule.ID)
		}
	}

	valueIDs := map[string]struct{}{}
	for index := range spec.ValueRules {
		rule := &spec.ValueRules[index]
		if err := validateRuleID(rule.ID, valueIDs, "value"); err != nil {
			return err
		}
		if err := validatePathSelector(rule.Exact, rule.Prefix, rule.Suffix, rule.Glob); err != nil {
			return fmt.Errorf("value rule %q: %w", rule.ID, err)
		}
		if err := validateJSONPointer(rule.Pointer); err != nil {
			return fmt.Errorf("value rule %q: %w", rule.ID, err)
		}
		if (rule.Domain == "") == (rule.UnknownReason == "") {
			return fmt.Errorf("value rule %q must define exactly one domain or unknownReason", rule.ID)
		}
		if rule.Domain != "" {
			if _, err := ParseDomain(string(rule.Domain)); err != nil {
				return fmt.Errorf("value rule %q: %w", rule.ID, err)
			}
		}
	}

	objectIDs := map[string]struct{}{}
	exactIdentities := map[string]string{}
	for index := range spec.ObjectRules {
		rule := &spec.ObjectRules[index]
		if err := validateRuleID(rule.ID, objectIDs, "object"); err != nil {
			return err
		}
		if _, err := ParseDomain(string(rule.Domain)); err != nil {
			return fmt.Errorf("object rule %q: %w", rule.ID, err)
		}
		if rule.Version == "" || rule.Kind == "" {
			return fmt.Errorf("object rule %q must define version and kind", rule.ID)
		}
		if rule.Kind == "CustomResourceDefinition" {
			return fmt.Errorf("object rule %q must not own CustomResourceDefinition", rule.ID)
		}
		if rule.Scope != ScopeCluster && rule.Scope != ScopeNamespaced {
			return fmt.Errorf("object rule %q has invalid scope %q", rule.ID, rule.Scope)
		}
		if rule.Scope == ScopeCluster && rule.Namespace != "" {
			return fmt.Errorf("cluster-scoped object rule %q must not define namespace", rule.ID)
		}
		if rule.Scope == ScopeNamespaced && rule.Namespace == "" {
			return fmt.Errorf("namespaced object rule %q must define namespace", rule.ID)
		}
		if (rule.Name == "") == (rule.NamePrefix == "") {
			return fmt.Errorf("object rule %q must define exactly one name or namePrefix", rule.ID)
		}
		if rule.NamePrefix != "" && rule.NameSuffixLabel == nil {
			return fmt.Errorf("prefix object rule %q must define nameSuffixLabel", rule.ID)
		}
		if rule.NameSuffixLabel != nil {
			if rule.NamePrefix == "" || rule.NameSuffixLabel.Key == "" || rule.NameSuffixLabel.ValuePrefix == "" {
				return fmt.Errorf("object rule %q has incomplete nameSuffixLabel", rule.ID)
			}
			if strings.Contains(rule.NameSuffixLabel.ValuePrefix, "${") {
				return fmt.Errorf("object rule %q nameSuffixLabel valuePrefix must be literal", rule.ID)
			}
		}
		for field, value := range map[string]string{
			"namespace":  rule.Namespace,
			"name":       rule.Name,
			"namePrefix": rule.NamePrefix,
		} {
			if _, err := expandBindings(value, validationBindings); err != nil {
				return fmt.Errorf("object rule %q %s: %w", rule.ID, field, err)
			}
		}
		for key, value := range rule.RequiredLabels {
			if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
				return fmt.Errorf("object rule %q has empty required label", rule.ID)
			}
			if _, err := expandBindings(value, validationBindings); err != nil {
				return fmt.Errorf("object rule %q required label %q: %w", rule.ID, key, err)
			}
		}
		if rule.Name != "" {
			key := strings.Join([]string{rule.APIGroup, rule.Version, rule.Kind, string(rule.Scope), rule.Namespace, rule.Name}, "\x00")
			if existing, ok := exactIdentities[key]; ok {
				return fmt.Errorf("object rules %q and %q have the same exact identity", existing, rule.ID)
			}
			exactIdentities[key] = rule.ID
		}
		overrides := map[string]struct{}{}
		for _, override := range rule.PathOverrides {
			if override.Pointer == "" {
				return fmt.Errorf("object rule %q must not override the root pointer", rule.ID)
			}
			if err := validateJSONPointer(override.Pointer); err != nil {
				return fmt.Errorf("object rule %q override: %w", rule.ID, err)
			}
			if _, err := ParseDomain(string(override.Domain)); err != nil {
				return fmt.Errorf("object rule %q override: %w", rule.ID, err)
			}
			if _, exists := overrides[override.Pointer]; exists {
				return fmt.Errorf("object rule %q has duplicate override %q", rule.ID, override.Pointer)
			}
			overrides[override.Pointer] = struct{}{}
		}
	}
	return nil
}

func validateRuleID(id string, seen map[string]struct{}, kind string) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("%s rule id is required", kind)
	}
	if _, exists := seen[id]; exists {
		return fmt.Errorf("duplicate %s rule id %q", kind, id)
	}
	seen[id] = struct{}{}
	return nil
}

func validatePathSelector(exact, prefix, suffix, glob string) error {
	count := 0
	for _, value := range []string{exact, prefix, suffix, glob} {
		if value != "" {
			count++
		}
	}
	if count != 1 {
		return errors.New("exactly one exact, prefix, suffix, or glob selector is required")
	}
	if exact != "" && !validRepositoryPath(exact) {
		return fmt.Errorf("invalid exact path %q", exact)
	}
	if prefix != "" && (strings.HasPrefix(prefix, "/") || strings.Contains(prefix, "..") || strings.Contains(prefix, "\\")) {
		return fmt.Errorf("invalid path prefix %q", prefix)
	}
	if suffix != "" && strings.ContainsAny(suffix, "\x00/\\") {
		return fmt.Errorf("invalid path suffix %q", suffix)
	}
	if glob != "" {
		if strings.HasPrefix(glob, "/") || strings.Contains(glob, "..") || strings.Contains(glob, "\\") {
			return fmt.Errorf("invalid path glob %q", glob)
		}
		if _, err := path.Match(glob, "probe"); err != nil {
			return fmt.Errorf("invalid path glob %q: %w", glob, err)
		}
	}
	return nil
}

func pathSelectorMatches(repositoryPath, exact, prefix, suffix, glob string) bool {
	switch {
	case exact != "":
		return repositoryPath == exact
	case prefix != "":
		return strings.HasPrefix(repositoryPath, prefix)
	case suffix != "":
		return strings.HasSuffix(repositoryPath, suffix)
	case glob != "":
		matched, _ := path.Match(glob, repositoryPath)
		return matched
	default:
		return false
	}
}

// ValidateBindings requires all names to have been resolved from the same
// render inputs before ownership matching begins.
func (spec *OwnershipSpec) ValidateBindings(bindings map[string]string) error {
	missing := make([]string, 0)
	for _, key := range spec.RequiredBindings {
		if strings.TrimSpace(bindings[key]) == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("missing ownership bindings: %s", strings.Join(missing, ", "))
	}
	return nil
}

func expandBindings(value string, bindings map[string]string) (string, error) {
	var result strings.Builder
	for {
		start := strings.Index(value, "${")
		if start < 0 {
			result.WriteString(value)
			break
		}
		result.WriteString(value[:start])
		value = value[start+2:]
		end := strings.IndexByte(value, '}')
		if end < 0 {
			return "", errors.New("unterminated ownership binding")
		}
		key := value[:end]
		resolved, ok := bindings[key]
		if !ok || resolved == "" {
			return "", fmt.Errorf("ownership binding %q is not set", key)
		}
		result.WriteString(resolved)
		value = value[end+1:]
	}
	return result.String(), nil
}

func (rule ObjectRule) matches(object manifestObject, defaultNamespace string, bindings map[string]string) (bool, error) {
	if object.Identity.APIGroup != rule.APIGroup || object.Identity.Version != rule.Version || object.Identity.Kind != rule.Kind {
		return false, nil
	}
	if rule.Scope == ScopeCluster {
		if object.Identity.Namespace != "" {
			return false, nil
		}
	} else {
		namespace, err := expandBindings(rule.Namespace, bindings)
		if err != nil {
			return false, err
		}
		objectNamespace := object.Identity.Namespace
		if objectNamespace == "" {
			objectNamespace = defaultNamespace
		}
		if objectNamespace != namespace {
			return false, nil
		}
	}

	nameSuffix := ""
	if rule.Name != "" {
		name, err := expandBindings(rule.Name, bindings)
		if err != nil {
			return false, err
		}
		if object.Identity.Name != name {
			return false, nil
		}
	} else {
		prefix, err := expandBindings(rule.NamePrefix, bindings)
		if err != nil {
			return false, err
		}
		if !strings.HasPrefix(object.Identity.Name, prefix) || object.Identity.Name == prefix {
			return false, nil
		}
		nameSuffix = strings.TrimPrefix(object.Identity.Name, prefix)
	}

	for key, expectedTemplate := range rule.RequiredLabels {
		expected, err := expandBindings(expectedTemplate, bindings)
		if err != nil {
			return false, err
		}
		if object.Labels[key] != expected {
			return false, nil
		}
	}
	if rule.NameSuffixLabel != nil {
		expected := rule.NameSuffixLabel.ValuePrefix + nameSuffix
		if object.Labels[rule.NameSuffixLabel.Key] != expected {
			return false, nil
		}
	}
	return true, nil
}

func (rule ObjectRule) domainForPointer(pointer string) Domain {
	domain := rule.Domain
	longest := -1
	for _, override := range rule.PathOverrides {
		if pointerHasPrefix(pointer, override.Pointer) {
			depth := pointerDepth(override.Pointer)
			if depth > longest {
				longest = depth
				domain = override.Domain
			}
		}
	}
	return domain
}

func validateJSONPointer(pointer string) error {
	if pointer == "" {
		return nil
	}
	if !strings.HasPrefix(pointer, "/") {
		return fmt.Errorf("JSON Pointer %q must start with /", pointer)
	}
	for index := 0; index < len(pointer); index++ {
		if pointer[index] != '~' {
			continue
		}
		if index+1 >= len(pointer) || (pointer[index+1] != '0' && pointer[index+1] != '1') {
			return fmt.Errorf("JSON Pointer %q has invalid escape", pointer)
		}
		index++
	}
	return nil
}

func pointerHasPrefix(pointer, prefix string) bool {
	return prefix == "" || pointer == prefix || strings.HasPrefix(pointer, prefix+"/")
}

func pointerDepth(pointer string) int {
	if pointer == "" {
		return 0
	}
	return strings.Count(pointer, "/")
}
