// Package releasedomain classifies a release change into a fail-closed
// ownership domain. It is deliberately side-effect free: callers are
// responsible for rendering manifests and for enforcing a returned plan.
package releasedomain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// Domain is a release-time mutation boundary, not a runtime RBAC role.
type Domain string

const (
	DomainNodeLocal        Domain = "node-local"
	DomainAuthoritativeDNS Domain = "authoritative-dns"
	DomainControlPlane     Domain = "control-plane"
	DomainImageCache       Domain = "image-cache"
	DomainBackup           Domain = "backup"
)

var orderedDomains = []Domain{
	DomainNodeLocal,
	DomainAuthoritativeDNS,
	DomainControlPlane,
	DomainImageCache,
	DomainBackup,
}

var domainRank = map[Domain]int{
	DomainNodeLocal:        0,
	DomainAuthoritativeDNS: 1,
	DomainControlPlane:     2,
	DomainImageCache:       3,
	DomainBackup:           4,
}

// KnownDomains returns the canonical domain order used in plans and digests.
func KnownDomains() []Domain {
	return append([]Domain(nil), orderedDomains...)
}

// ParseDomain validates a domain name.
func ParseDomain(value string) (Domain, error) {
	domain := Domain(value)
	if _, ok := domainRank[domain]; !ok {
		return "", fmt.Errorf("unknown release domain %q", value)
	}
	return domain, nil
}

func canonicalDomains(values []Domain) []Domain {
	seen := make(map[Domain]struct{}, len(values))
	result := make([]Domain, 0, len(values))
	for _, domain := range values {
		if _, ok := domainRank[domain]; !ok {
			continue
		}
		if _, ok := seen[domain]; ok {
			continue
		}
		seen[domain] = struct{}{}
		result = append(result, domain)
	}
	sort.Slice(result, func(i, j int) bool {
		return domainRank[result[i]] < domainRank[result[j]]
	})
	return result
}

func equalDomains(left, right []Domain) bool {
	left = canonicalDomains(left)
	right = canonicalDomains(right)
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

// ChangeStatus is a no-rename Git name-status classification.
type ChangeStatus string

const (
	ChangeAdded    ChangeStatus = "A"
	ChangeModified ChangeStatus = "M"
	ChangeDeleted  ChangeStatus = "D"
)

// ChangedFile is the pure input to the file classifier. Go consumer and
// versioned-values evidence is supplied by the future integration boundary;
// this package does not guess dependency graphs or values diffs.
type ChangedFile struct {
	Status           ChangeStatus `json:"status"`
	Path             string       `json:"path"`
	ValuePointers    []string     `json:"valuePointers,omitempty"`
	ConsumerDomains  []Domain     `json:"consumerDomains,omitempty"`
	SemanticDomains  []Domain     `json:"semanticDomains,omitempty"`
	OutsideConsumers []string     `json:"outsideConsumers,omitempty"`
}

// Evidence is a deterministic explanation of a classification decision.
type Evidence struct {
	Source  string   `json:"source"`
	Subject string   `json:"subject"`
	Status  string   `json:"status,omitempty"`
	Paths   []string `json:"paths,omitempty"`
	Domains []Domain `json:"domains,omitempty"`
	Reason  string   `json:"reason,omitempty"`
	RuleID  string   `json:"ruleId,omitempty"`
	Ignored bool     `json:"ignored,omitempty"`
}

// FileClassification is the complete changed-file evidence set.
type FileClassification struct {
	Domains       []Domain   `json:"domains"`
	Evidence      []Evidence `json:"evidence"`
	Unknown       []Evidence `json:"unknown,omitempty"`
	AllNonRuntime bool       `json:"allNonRuntime"`
}

// ObjectIdentity is the canonical Kubernetes object identity used by the
// ownership matcher.
type ObjectIdentity struct {
	APIGroup  string `json:"apiGroup"`
	Version   string `json:"version"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

func (identity ObjectIdentity) String() string {
	groupVersion := identity.Version
	if identity.APIGroup != "" {
		groupVersion = identity.APIGroup + "/" + identity.Version
	}
	name := identity.Name
	if identity.Namespace != "" {
		name = identity.Namespace + "/" + name
	}
	return fmt.Sprintf("%s %s %s", groupVersion, identity.Kind, name)
}

// RenderedClassification is the structured base-to-target manifest evidence.
type RenderedClassification struct {
	Domains  []Domain   `json:"domains"`
	Evidence []Evidence `json:"evidence"`
	Unknown  []Evidence `json:"unknown,omitempty"`
}

// ClassificationBinding is one resolved ownership binding from the immutable
// render context shared by the base and target manifests. A slice, rather than
// a map, makes the order explicit in persisted evidence.
type ClassificationBinding struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// ClassificationContextEvidence records the exact namespace, resolved
// bindings, and Helm-hook policy used to classify both rendered manifests.
// Digest covers the other fields in their canonical (binding-name-sorted)
// representation; the enclosing planDigest then makes this evidence
// immutable with the rest of the plan.
type ClassificationContextEvidence struct {
	DefaultNamespace    string                  `json:"defaultNamespace"`
	Bindings            []ClassificationBinding `json:"bindings"`
	IgnoreHelmTestHooks bool                    `json:"ignoreHelmTestHooks"`
	Digest              string                  `json:"digest"`
}

// NewClassificationContextEvidence snapshots one shared base/target
// classification context. Callers must not construct separate contexts for
// the two manifests.
func NewClassificationContextEvidence(defaultNamespace string, bindings map[string]string, ignoreHelmTestHooks bool) (ClassificationContextEvidence, error) {
	if !utf8.ValidString(defaultNamespace) {
		return ClassificationContextEvidence{}, fmt.Errorf("classification default namespace is not valid UTF-8")
	}
	if strings.TrimSpace(defaultNamespace) == "" {
		return ClassificationContextEvidence{}, fmt.Errorf("classification default namespace is empty")
	}
	if releaseNamespace := bindings["releaseNamespace"]; releaseNamespace != defaultNamespace {
		return ClassificationContextEvidence{}, fmt.Errorf("classification releaseNamespace binding %q differs from default namespace %q", releaseNamespace, defaultNamespace)
	}

	keys := make([]string, 0, len(bindings))
	for name, value := range bindings {
		if !utf8.ValidString(name) || !utf8.ValidString(value) {
			return ClassificationContextEvidence{}, fmt.Errorf("classification binding names and values must be valid UTF-8")
		}
		if strings.TrimSpace(name) == "" || strings.TrimSpace(value) == "" {
			return ClassificationContextEvidence{}, fmt.Errorf("classification binding names and values must be non-empty")
		}
		keys = append(keys, name)
	}
	sort.Strings(keys)

	context := ClassificationContextEvidence{
		DefaultNamespace:    defaultNamespace,
		Bindings:            make([]ClassificationBinding, 0, len(keys)),
		IgnoreHelmTestHooks: ignoreHelmTestHooks,
	}
	for _, name := range keys {
		context.Bindings = append(context.Bindings, ClassificationBinding{Name: name, Value: bindings[name]})
	}
	context.Digest = classificationContextDigest(context)
	return context, nil
}

// BindingMap returns a fresh map so classifiers cannot mutate the persisted
// context or observe later mutation of the caller's input map.
func (context ClassificationContextEvidence) BindingMap() map[string]string {
	bindings := make(map[string]string, len(context.Bindings))
	for _, binding := range context.Bindings {
		bindings[binding.Name] = binding.Value
	}
	return bindings
}

// VerifyClassificationContextEvidence rejects mutation, duplicate/unsorted
// bindings, or a namespace/binding mismatch in persisted evidence.
func VerifyClassificationContextEvidence(context ClassificationContextEvidence) error {
	if !utf8.ValidString(context.DefaultNamespace) {
		return fmt.Errorf("classification default namespace is not valid UTF-8")
	}
	if strings.TrimSpace(context.DefaultNamespace) == "" {
		return fmt.Errorf("classification default namespace is empty")
	}
	previous := ""
	for index, binding := range context.Bindings {
		if !utf8.ValidString(binding.Name) || !utf8.ValidString(binding.Value) {
			return fmt.Errorf("classification binding names and values must be valid UTF-8")
		}
		if strings.TrimSpace(binding.Name) == "" || strings.TrimSpace(binding.Value) == "" {
			return fmt.Errorf("classification binding names and values must be non-empty")
		}
		if index > 0 && binding.Name <= previous {
			return fmt.Errorf("classification bindings are not uniquely sorted by name")
		}
		previous = binding.Name
	}
	if releaseNamespace := context.BindingMap()["releaseNamespace"]; releaseNamespace != context.DefaultNamespace {
		return fmt.Errorf("classification releaseNamespace binding %q differs from default namespace %q", releaseNamespace, context.DefaultNamespace)
	}
	expected := classificationContextDigest(context)
	if context.Digest != expected {
		return fmt.Errorf("classification context digest mismatch: got %s, want %s", context.Digest, expected)
	}
	return nil
}

func classificationContextDigest(context ClassificationContextEvidence) string {
	payload := struct {
		DefaultNamespace    string                  `json:"defaultNamespace"`
		Bindings            []ClassificationBinding `json:"bindings"`
		IgnoreHelmTestHooks bool                    `json:"ignoreHelmTestHooks"`
	}{
		DefaultNamespace:    context.DefaultNamespace,
		Bindings:            context.Bindings,
		IgnoreHelmTestHooks: context.IgnoreHelmTestHooks,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		panic(fmt.Sprintf("marshal classification context: %v", err))
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

// Outcome is the planner's write-gate result.
type Outcome string

const (
	OutcomeZero     Outcome = "zero"
	OutcomeSingle   Outcome = "single"
	OutcomeMultiple Outcome = "multiple"
	OutcomeUnknown  Outcome = "unknown"
)

// DigestEvidence carries opaque SSoT identities, locally computed input
// digests, and the exact rendered-classification context. Opaque SSoT values
// are compared as strings; the planner defines no SSoT schema.
type DigestEvidence struct {
	Base                   string                        `json:"base"`
	Target                 string                        `json:"target"`
	Live                   string                        `json:"live"`
	BaseManifest           string                        `json:"baseManifest"`
	TargetManifest         string                        `json:"targetManifest"`
	RepeatedTargetManifest string                        `json:"repeatedTargetManifest"`
	Ownership              string                        `json:"ownership"`
	ChangedFiles           string                        `json:"changedFiles"`
	ClassificationContext  ClassificationContextEvidence `json:"classificationContext"`
}

// PlanInput combines the two independent classification channels.
type PlanInput struct {
	Files    FileClassification     `json:"files"`
	Rendered RenderedClassification `json:"rendered"`
	Digests  DigestEvidence         `json:"digests"`
}

// Plan is an immutable, digest-bound release-domain-plan.json payload.
type Plan struct {
	APIVersion     string                 `json:"apiVersion"`
	Kind           string                 `json:"kind"`
	Result         Outcome                `json:"result"`
	SelectedDomain Domain                 `json:"selectedDomain,omitempty"`
	Domains        []Domain               `json:"domains"`
	Digests        DigestEvidence         `json:"digests"`
	Files          FileClassification     `json:"files"`
	Rendered       RenderedClassification `json:"rendered"`
	Unknown        []Evidence             `json:"unknown,omitempty"`
	PlanDigest     string                 `json:"planDigest"`
}

func canonicalEvidence(values []Evidence) []Evidence {
	result := append([]Evidence(nil), values...)
	for index := range result {
		result[index].Domains = canonicalDomains(result[index].Domains)
		result[index].Paths = append([]string(nil), result[index].Paths...)
		sort.Strings(result[index].Paths)
	}
	sort.SliceStable(result, func(i, j int) bool {
		left, right := result[i], result[j]
		if left.Source != right.Source {
			return left.Source < right.Source
		}
		if left.Subject != right.Subject {
			return left.Subject < right.Subject
		}
		if left.RuleID != right.RuleID {
			return left.RuleID < right.RuleID
		}
		return left.Reason < right.Reason
	})
	return result
}
