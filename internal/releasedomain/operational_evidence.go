package releasedomain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	OperationalImagePlanAPIVersion = "release-domain.fugue.dev/v1"
	OperationalImagePlanKind       = "OperationalImageRolloutPlan"
	OperationalImagePlanPolicy     = "exact-image-rollout-targets-v1"

	OperationalEvidenceAPIVersion = "release-domain.fugue.dev/v1"
	OperationalEvidenceKind       = "OperationalDomainEvidence"
	OperationalEvidencePolicy     = "consumer-build-render-adapter-intersection-v1"

	maxOperationalEvidenceBytes = 8 << 20
)

// OperationalImageRolloutPlan is a revision-bound description of the exact
// image targets selected by the existing build/rollout planner. This contract
// is additive: the release-domain planner does not consume it.
type OperationalImageRolloutPlan struct {
	APIVersion         string                          `json:"apiVersion"`
	Kind               string                          `json:"kind"`
	Policy             string                          `json:"policy"`
	BaseCommit         string                          `json:"baseCommit"`
	TargetCommit       string                          `json:"targetCommit"`
	ChangedFilesDigest string                          `json:"changedFilesDigest"`
	Targets            []OperationalImageRolloutTarget `json:"targets"`
	Digest             string                          `json:"digest"`
}

// OperationalImageRolloutTarget binds a selected formal image target to the
// exact component source baseline and verified produced OCI artifact digest.
// A target name alone is insufficient evidence of an actual build/rollout.
type OperationalImageRolloutTarget struct {
	Name             string `json:"name"`
	SourceBaseCommit string `json:"sourceBaseCommit"`
	ArtifactDigest   string `json:"artifactDigest"`
}

// OperationalAdapterBinding names the literal production adapter associated
// with one fixed release domain. The complete ordered set is embedded in every
// report so a later report-only consumer can detect binding drift.
type OperationalAdapterBinding struct {
	Domain  Domain `json:"domain"`
	Adapter string `json:"adapter"`
}

// OperationalDomainEvidence is a report-only conjunction of four independent
// witness channels. AuthorizationEligible is permanently false; no production
// authorization constructor accepts this type.
type OperationalDomainEvidence struct {
	APIVersion            string                          `json:"apiVersion"`
	Kind                  string                          `json:"kind"`
	Policy                string                          `json:"policy"`
	BaseCommit            string                          `json:"baseCommit"`
	TargetCommit          string                          `json:"targetCommit"`
	ChangedFilesDigest    string                          `json:"changedFilesDigest"`
	ImagePlanDigest       string                          `json:"imagePlanDigest"`
	PlanDigest            string                          `json:"planDigest"`
	AdapterBindingDigest  string                          `json:"adapterBindingDigest"`
	ImageTargets          []OperationalImageRolloutTarget `json:"imageTargets"`
	ConsumerDomains       []Domain                        `json:"consumerDomains"`
	ImageRolloutDomains   []Domain                        `json:"imageRolloutDomains"`
	RenderedDomains       []Domain                        `json:"renderedDomains"`
	AdapterDomains        []Domain                        `json:"adapterDomains"`
	IntersectionDomains   []Domain                        `json:"intersectionDomains"`
	AdapterBindings       []OperationalAdapterBinding     `json:"adapterBindings"`
	Observation           Outcome                         `json:"observation"`
	CandidateDomain       Domain                          `json:"candidateDomain,omitempty"`
	Issues                []string                        `json:"issues"`
	AuthorizationEligible bool                            `json:"authorizationEligible"`
	Digest                string                          `json:"digest"`
}

var operationalImageTargetDomains = map[string][]Domain{
	"api":             {DomainControlPlane},
	"controller":      {DomainControlPlane},
	"drain_agent":     {DomainControlPlane},
	"edge":            {DomainAuthoritativeDNS},
	"image_cache":     {DomainImageCache},
	"telemetry_agent": nil,
	"app_ssh":         nil,
}

var operationalCommandImageTargets = map[string]string{
	"cmd/fugue-api":                  "api",
	"cmd/fugue-controller":           "controller",
	"cmd/fugue-registry-maintenance": "controller",
	"cmd/fugue-drain-agent":          "drain_agent",
	"cmd/fugue-telemetry-agent":      "telemetry_agent",
	"cmd/fugue-image-cache":          "image_cache",
	"cmd/fugue-edge":                 "edge",
	"cmd/fugue-edge-front":           "edge",
	"cmd/fugue-ssh-front":            "edge",
	"cmd/fugue-dns":                  "edge",
	"cmd/fugue-mesh-agent":           "edge",
	"cmd/fugue-mesh-recovery":        "edge",
}

// NewOperationalImageRolloutPlan creates the canonical digest-bound input
// that a later workflow checkpoint will populate from the actual build plan.
func NewOperationalImageRolloutPlan(baseCommit, targetCommit, changedFilesDigest string, targets []OperationalImageRolloutTarget) (OperationalImageRolloutPlan, error) {
	if err := validateTrustedGitCommit(baseCommit, "operational image plan base commit"); err != nil {
		return OperationalImageRolloutPlan{}, err
	}
	if err := validateTrustedGitCommit(targetCommit, "operational image plan target commit"); err != nil {
		return OperationalImageRolloutPlan{}, err
	}
	if err := validateCanonicalSHA256Digest(changedFilesDigest, "operational image plan changed-files digest"); err != nil {
		return OperationalImageRolloutPlan{}, err
	}
	canonicalTargets, err := canonicalOperationalImageTargets(targets)
	if err != nil {
		return OperationalImageRolloutPlan{}, err
	}
	plan := OperationalImageRolloutPlan{
		APIVersion:         OperationalImagePlanAPIVersion,
		Kind:               OperationalImagePlanKind,
		Policy:             OperationalImagePlanPolicy,
		BaseCommit:         baseCommit,
		TargetCommit:       targetCommit,
		ChangedFilesDigest: changedFilesDigest,
		Targets:            canonicalTargets,
	}
	plan.Digest = operationalImagePlanDigest(plan)
	return plan, nil
}

// MarshalOperationalImageRolloutPlan returns canonical report input bytes.
func MarshalOperationalImageRolloutPlan(plan OperationalImageRolloutPlan) ([]byte, error) {
	if err := VerifyOperationalImageRolloutPlan(plan); err != nil {
		return nil, err
	}
	return marshalOperationalJSON(plan)
}

// DecodeAndVerifyOperationalImageRolloutPlan strictly verifies a plan and its
// independent revision/digest bindings.
func DecodeAndVerifyOperationalImageRolloutPlan(reader io.Reader, trustedBase, trustedTarget, trustedChangedDigest string) (OperationalImageRolloutPlan, error) {
	data, err := readOperationalEvidence(reader, "operational image plan")
	if err != nil {
		return OperationalImageRolloutPlan{}, err
	}
	if err := validateStrictOperationalJSON(data, reflect.TypeOf(OperationalImageRolloutPlan{}), "operationalImagePlan"); err != nil {
		return OperationalImageRolloutPlan{}, err
	}
	var plan OperationalImageRolloutPlan
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return OperationalImageRolloutPlan{}, fmt.Errorf("decode operational image plan: %w", err)
	}
	if err := VerifyOperationalImageRolloutPlan(plan); err != nil {
		return OperationalImageRolloutPlan{}, err
	}
	if plan.BaseCommit != trustedBase || plan.TargetCommit != trustedTarget || plan.ChangedFilesDigest != trustedChangedDigest {
		return OperationalImageRolloutPlan{}, fmt.Errorf("operational image plan trusted binding mismatch")
	}
	return plan, nil
}

// VerifyOperationalImageRolloutPlan detects non-canonical target order,
// unknown targets, revision drift and payload mutation.
func VerifyOperationalImageRolloutPlan(plan OperationalImageRolloutPlan) error {
	if plan.APIVersion != OperationalImagePlanAPIVersion || plan.Kind != OperationalImagePlanKind || plan.Policy != OperationalImagePlanPolicy {
		return fmt.Errorf("operational image plan identity is unsupported")
	}
	if err := validateTrustedGitCommit(plan.BaseCommit, "operational image plan base commit"); err != nil {
		return err
	}
	if err := validateTrustedGitCommit(plan.TargetCommit, "operational image plan target commit"); err != nil {
		return err
	}
	if err := validateCanonicalSHA256Digest(plan.ChangedFilesDigest, "operational image plan changed-files digest"); err != nil {
		return err
	}
	canonical, err := canonicalOperationalImageTargets(plan.Targets)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(canonical, plan.Targets) {
		return fmt.Errorf("operational image plan targets are not uniquely sorted")
	}
	if err := validateCanonicalSHA256Digest(plan.Digest, "operational image plan digest"); err != nil {
		return err
	}
	if plan.Digest != operationalImagePlanDigest(plan) {
		return fmt.Errorf("operational image plan digest mismatch")
	}
	return nil
}

// BuildOperationalDomainEvidence creates a non-authorizing report. The
// existing changed-file and rendered planners remain the only production
// authorization inputs.
func BuildOperationalDomainEvidence(changed ChangedFileEvidence, imagePlan OperationalImageRolloutPlan, plan Plan) (OperationalDomainEvidence, error) {
	if err := VerifyOperationalImageRolloutPlan(imagePlan); err != nil {
		return OperationalDomainEvidence{}, err
	}
	if plan.APIVersion != PlanAPIVersion || plan.Kind != PlanKind {
		return OperationalDomainEvidence{}, fmt.Errorf("operational evidence release-domain plan identity is unsupported")
	}
	if err := VerifyPlanDigest(plan); err != nil {
		return OperationalDomainEvidence{}, err
	}
	if changed.BaseCommit() != imagePlan.BaseCommit || changed.TargetCommit() != imagePlan.TargetCommit || changed.Digest() != imagePlan.ChangedFilesDigest {
		return OperationalDomainEvidence{}, fmt.Errorf("operational evidence revision binding mismatch")
	}
	if plan.Digests.ChangedFiles != changed.Digest() {
		return OperationalDomainEvidence{}, fmt.Errorf("operational evidence planner changed-files digest mismatch")
	}

	issues := make([]string, 0)
	for _, evidence := range validateDigestEvidence(plan.Digests) {
		issues = append(issues, "rendered-object plan integrity failed: "+evidence.Subject+": "+evidence.Reason)
	}
	consumerDomains := operationalConsumerDomains(changed.Changes(), imagePlan.Targets, &issues)
	imageDomains := operationalImageDomains(imagePlan.Targets, &issues)
	renderedDomains := checkedOperationalDomains(plan.Rendered.Domains, "rendered", &issues)
	if len(plan.Rendered.Unknown) != 0 {
		issues = append(issues, "rendered-object evidence is incomplete")
	}
	if len(classificationEvidenceErrors("operational rendered-object", plan.Rendered.Domains, plan.Rendered.Evidence)) != 0 {
		issues = append(issues, "rendered-object declared domains differ from their evidence")
	}
	bindings := fixedOperationalBindings()
	adapterDomains := operationalAdapterDomains(renderedDomains, bindings, &issues)
	intersection := intersectOperationalDomains(consumerDomains, imageDomains, renderedDomains, adapterDomains)

	if !equalDomains(intersectOperationalDomains(consumerDomains, imageDomains), imageDomains) {
		issues = append(issues, "changed-package consumers do not cover every selected image rollout domain")
	}
	if !equalDomains(imageDomains, renderedDomains) {
		issues = append(issues, "selected image rollout domains differ from rendered-object domains")
	}
	if !equalDomains(adapterDomains, renderedDomains) {
		issues = append(issues, "fixed adapter domains differ from rendered-object domains")
	}
	issues = canonicalOperationalStrings(issues)

	report := OperationalDomainEvidence{
		APIVersion:            OperationalEvidenceAPIVersion,
		Kind:                  OperationalEvidenceKind,
		Policy:                OperationalEvidencePolicy,
		BaseCommit:            changed.BaseCommit(),
		TargetCommit:          changed.TargetCommit(),
		ChangedFilesDigest:    changed.Digest(),
		ImagePlanDigest:       imagePlan.Digest,
		PlanDigest:            plan.PlanDigest,
		AdapterBindingDigest:  operationalAdapterBindingDigest(bindings),
		ImageTargets:          append([]OperationalImageRolloutTarget(nil), imagePlan.Targets...),
		ConsumerDomains:       canonicalDomains(consumerDomains),
		ImageRolloutDomains:   canonicalDomains(imageDomains),
		RenderedDomains:       canonicalDomains(renderedDomains),
		AdapterDomains:        canonicalDomains(adapterDomains),
		IntersectionDomains:   canonicalDomains(intersection),
		AdapterBindings:       bindings,
		Observation:           OutcomeUnknown,
		Issues:                issues,
		AuthorizationEligible: false,
	}
	if len(report.Issues) == 0 {
		switch len(report.IntersectionDomains) {
		case 0:
			report.Observation = OutcomeZero
		case 1:
			report.Observation = OutcomeSingle
			report.CandidateDomain = report.IntersectionDomains[0]
		default:
			report.Observation = OutcomeMultiple
		}
	}
	report.Digest = operationalEvidenceDigest(report)
	return report, nil
}

// MarshalOperationalDomainEvidence verifies and emits canonical JSON bytes.
func MarshalOperationalDomainEvidence(report OperationalDomainEvidence) ([]byte, error) {
	if err := VerifyOperationalDomainEvidence(report); err != nil {
		return nil, err
	}
	return marshalOperationalJSON(report)
}

// DecodeAndVerifyOperationalDomainEvidence strictly decodes a report and
// binds it to an independently supplied digest. This still creates no
// ExecutionAuthorization.
func DecodeAndVerifyOperationalDomainEvidence(reader io.Reader, expectedDigest string) (OperationalDomainEvidence, error) {
	data, err := readOperationalEvidence(reader, "operational domain evidence")
	if err != nil {
		return OperationalDomainEvidence{}, err
	}
	if err := validateStrictOperationalJSON(data, reflect.TypeOf(OperationalDomainEvidence{}), "operationalDomainEvidence"); err != nil {
		return OperationalDomainEvidence{}, err
	}
	var report OperationalDomainEvidence
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&report); err != nil {
		return OperationalDomainEvidence{}, fmt.Errorf("decode operational domain evidence: %w", err)
	}
	if report.Digest != expectedDigest {
		return OperationalDomainEvidence{}, fmt.Errorf("operational domain evidence external digest mismatch")
	}
	if err := VerifyOperationalDomainEvidence(report); err != nil {
		return OperationalDomainEvidence{}, err
	}
	return report, nil
}

// VerifyOperationalDomainEvidence enforces canonical report structure and the
// permanent non-authorization boundary.
func VerifyOperationalDomainEvidence(report OperationalDomainEvidence) error {
	if report.APIVersion != OperationalEvidenceAPIVersion || report.Kind != OperationalEvidenceKind || report.Policy != OperationalEvidencePolicy {
		return fmt.Errorf("operational domain evidence identity is unsupported")
	}
	if report.AuthorizationEligible {
		return fmt.Errorf("operational domain evidence must never be authorization eligible")
	}
	if err := validateTrustedGitCommit(report.BaseCommit, "operational evidence base commit"); err != nil {
		return err
	}
	if err := validateTrustedGitCommit(report.TargetCommit, "operational evidence target commit"); err != nil {
		return err
	}
	for label, digest := range map[string]string{
		"changed-files":   report.ChangedFilesDigest,
		"image-plan":      report.ImagePlanDigest,
		"plan":            report.PlanDigest,
		"adapter-binding": report.AdapterBindingDigest,
		"report":          report.Digest,
	} {
		if err := validateCanonicalSHA256Digest(digest, "operational evidence "+label+" digest"); err != nil {
			return err
		}
	}
	for label, domains := range map[string][]Domain{
		"consumer":      report.ConsumerDomains,
		"image rollout": report.ImageRolloutDomains,
		"rendered":      report.RenderedDomains,
		"adapter":       report.AdapterDomains,
		"intersection":  report.IntersectionDomains,
	} {
		if err := verifyCanonicalOperationalDomains(domains, label); err != nil {
			return err
		}
	}
	embeddedImagePlan := OperationalImageRolloutPlan{
		APIVersion:         OperationalImagePlanAPIVersion,
		Kind:               OperationalImagePlanKind,
		Policy:             OperationalImagePlanPolicy,
		BaseCommit:         report.BaseCommit,
		TargetCommit:       report.TargetCommit,
		ChangedFilesDigest: report.ChangedFilesDigest,
		Targets:            append([]OperationalImageRolloutTarget(nil), report.ImageTargets...),
		Digest:             report.ImagePlanDigest,
	}
	if err := VerifyOperationalImageRolloutPlan(embeddedImagePlan); err != nil {
		return fmt.Errorf("operational evidence embedded image plan: %w", err)
	}
	if !reflect.DeepEqual(report.AdapterBindings, fixedOperationalBindings()) {
		return fmt.Errorf("operational evidence fixed adapter bindings differ")
	}
	if report.AdapterBindingDigest != operationalAdapterBindingDigest(report.AdapterBindings) {
		return fmt.Errorf("operational evidence adapter binding digest mismatch")
	}
	if !reflect.DeepEqual(report.Issues, canonicalOperationalStrings(report.Issues)) {
		return fmt.Errorf("operational evidence issues are not uniquely sorted")
	}
	expectedIntersection := intersectOperationalDomains(report.ConsumerDomains, report.ImageRolloutDomains, report.RenderedDomains, report.AdapterDomains)
	if !equalDomains(report.IntersectionDomains, expectedIntersection) {
		return fmt.Errorf("operational evidence intersection mismatch")
	}
	if len(report.Issues) != 0 {
		if report.Observation != OutcomeUnknown || report.CandidateDomain != "" {
			return fmt.Errorf("incomplete operational evidence must remain unknown")
		}
	} else {
		switch len(report.IntersectionDomains) {
		case 0:
			if report.Observation != OutcomeZero || report.CandidateDomain != "" {
				return fmt.Errorf("zero operational evidence observation mismatch")
			}
		case 1:
			if report.Observation != OutcomeSingle || report.CandidateDomain != report.IntersectionDomains[0] {
				return fmt.Errorf("single operational evidence observation mismatch")
			}
		default:
			if report.Observation != OutcomeMultiple || report.CandidateDomain != "" {
				return fmt.Errorf("multiple operational evidence observation mismatch")
			}
		}
	}
	if report.Digest != operationalEvidenceDigest(report) {
		return fmt.Errorf("operational domain evidence digest mismatch")
	}
	return nil
}

func operationalConsumerDomains(changes []ChangedFile, targets []OperationalImageRolloutTarget, issues *[]string) []Domain {
	selectedTargets := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		selectedTargets[target.Name] = struct{}{}
	}
	set := map[Domain]struct{}{}
	for _, change := range changes {
		if !strings.HasSuffix(change.Path, ".go") || strings.HasSuffix(change.Path, "_test.go") {
			continue
		}
		for _, domain := range append(append([]Domain(nil), change.ConsumerDomains...), change.SemanticDomains...) {
			if _, err := ParseDomain(string(domain)); err != nil {
				*issues = append(*issues, "changed-package consumer evidence contains an unknown domain")
				continue
			}
			set[domain] = struct{}{}
		}
		for _, command := range change.OutsideConsumers {
			target, mapped := operationalCommandImageTargets[command]
			if !mapped {
				continue
			}
			if _, selected := selectedTargets[target]; !selected {
				continue
			}
			domains := operationalImageTargetDomains[target]
			if len(domains) == 0 {
				*issues = append(*issues, "selected image target "+target+" has no fixed operational domain")
				continue
			}
			for _, domain := range domains {
				set[domain] = struct{}{}
			}
		}
	}
	return domainsFromSet(set)
}

func operationalImageDomains(targets []OperationalImageRolloutTarget, issues *[]string) []Domain {
	set := map[Domain]struct{}{}
	for _, target := range targets {
		domains := operationalImageTargetDomains[target.Name]
		if len(domains) == 0 {
			*issues = append(*issues, "selected image target "+target.Name+" has no fixed operational domain")
			continue
		}
		for _, domain := range domains {
			set[domain] = struct{}{}
		}
	}
	return domainsFromSet(set)
}

func checkedOperationalDomains(values []Domain, label string, issues *[]string) []Domain {
	set := map[Domain]struct{}{}
	for _, domain := range values {
		if _, err := ParseDomain(string(domain)); err != nil {
			*issues = append(*issues, label+" evidence contains an unknown domain")
			continue
		}
		set[domain] = struct{}{}
	}
	return domainsFromSet(set)
}

func operationalAdapterDomains(rendered []Domain, bindings []OperationalAdapterBinding, issues *[]string) []Domain {
	bound := map[Domain]struct{}{}
	for _, binding := range bindings {
		bound[binding.Domain] = struct{}{}
	}
	set := map[Domain]struct{}{}
	for _, domain := range rendered {
		if _, ok := bound[domain]; !ok {
			*issues = append(*issues, "rendered domain has no fixed production adapter")
			continue
		}
		set[domain] = struct{}{}
	}
	return domainsFromSet(set)
}

func intersectOperationalDomains(groups ...[]Domain) []Domain {
	if len(groups) == 0 {
		return []Domain{}
	}
	counts := map[Domain]int{}
	for _, group := range groups {
		seen := map[Domain]struct{}{}
		for _, domain := range group {
			if _, duplicate := seen[domain]; duplicate {
				continue
			}
			seen[domain] = struct{}{}
			counts[domain]++
		}
	}
	set := map[Domain]struct{}{}
	for domain, count := range counts {
		if count == len(groups) {
			set[domain] = struct{}{}
		}
	}
	return domainsFromSet(set)
}

func canonicalOperationalImageTargets(targets []OperationalImageRolloutTarget) ([]OperationalImageRolloutTarget, error) {
	result := append([]OperationalImageRolloutTarget(nil), targets...)
	sort.Slice(result, func(left, right int) bool { return result[left].Name < result[right].Name })
	for index, target := range result {
		if !utf8.ValidString(target.Name) || strings.TrimSpace(target.Name) != target.Name || target.Name == "" {
			return nil, fmt.Errorf("operational image target is invalid")
		}
		if _, known := operationalImageTargetDomains[target.Name]; !known {
			return nil, fmt.Errorf("operational image target %q is unknown", target.Name)
		}
		if index > 0 && result[index-1].Name == target.Name {
			return nil, fmt.Errorf("operational image target %q is duplicated", target.Name)
		}
		if err := validateTrustedGitCommit(target.SourceBaseCommit, "operational image target source base commit"); err != nil {
			return nil, err
		}
		if err := validateCanonicalSHA256Digest(target.ArtifactDigest, "operational image target artifact digest"); err != nil {
			return nil, err
		}
	}
	if result == nil {
		result = []OperationalImageRolloutTarget{}
	}
	return result, nil
}

func verifyCanonicalOperationalDomains(domains []Domain, label string) error {
	canonical := canonicalDomains(domains)
	if !reflect.DeepEqual(canonical, domains) {
		return fmt.Errorf("operational evidence %s domains are not uniquely canonical", label)
	}
	for _, domain := range domains {
		if _, err := ParseDomain(string(domain)); err != nil {
			return fmt.Errorf("operational evidence %s domain: %w", label, err)
		}
	}
	return nil
}

func fixedOperationalBindings() []OperationalAdapterBinding {
	return []OperationalAdapterBinding{
		{Domain: DomainNodeLocal, Adapter: "control_plane_release_adapter_node_local"},
		{Domain: DomainAuthoritativeDNS, Adapter: "control_plane_release_adapter_authoritative_dns"},
		{Domain: DomainControlPlane, Adapter: "control_plane_release_adapter_control_plane"},
		{Domain: DomainImageCache, Adapter: "control_plane_release_adapter_image_cache"},
		{Domain: DomainBackup, Adapter: "control_plane_release_adapter_backup"},
	}
}

func canonicalOperationalStrings(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	compacted := result[:0]
	for _, value := range result {
		if value == "" || (len(compacted) > 0 && compacted[len(compacted)-1] == value) {
			continue
		}
		compacted = append(compacted, value)
	}
	if compacted == nil {
		return []string{}
	}
	return compacted
}

func operationalAdapterBindingDigest(bindings []OperationalAdapterBinding) string {
	encoded, err := json.Marshal(bindings)
	if err != nil {
		panic(fmt.Sprintf("marshal operational adapter bindings: %v", err))
	}
	return digestOperationalBytes(encoded)
}

func operationalImagePlanDigest(plan OperationalImageRolloutPlan) string {
	plan.Digest = ""
	encoded, err := json.Marshal(plan)
	if err != nil {
		panic(fmt.Sprintf("marshal operational image plan: %v", err))
	}
	return digestOperationalBytes(encoded)
}

func operationalEvidenceDigest(report OperationalDomainEvidence) string {
	report.Digest = ""
	encoded, err := json.Marshal(report)
	if err != nil {
		panic(fmt.Sprintf("marshal operational evidence: %v", err))
	}
	return digestOperationalBytes(encoded)
}

func digestOperationalBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func marshalOperationalJSON(value any) ([]byte, error) {
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}
	if len(encoded)+1 > maxOperationalEvidenceBytes {
		return nil, fmt.Errorf("operational evidence exceeds %d-byte limit", maxOperationalEvidenceBytes)
	}
	return append(encoded, '\n'), nil
}

func readOperationalEvidence(reader io.Reader, label string) ([]byte, error) {
	if isNilReader(reader) {
		return nil, fmt.Errorf("%s reader is nil", label)
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxOperationalEvidenceBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", label, err)
	}
	if len(data) > maxOperationalEvidenceBytes {
		return nil, fmt.Errorf("%s exceeds %d-byte limit", label, maxOperationalEvidenceBytes)
	}
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("%s contains invalid UTF-8", label)
	}
	if err := validateJSONUnicodeEscapes(data); err != nil {
		return nil, fmt.Errorf("decode %s: %w", label, err)
	}
	return data, nil
}

func validateStrictOperationalJSON(data []byte, valueType reflect.Type, path string) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := validateJSONValueForType(decoder, valueType, path); err != nil {
		return err
	}
	if trailing, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return fmt.Errorf("decode trailing operational evidence: %w", err)
		}
		return fmt.Errorf("operational evidence must contain one JSON value; found trailing token %v", trailing)
	}
	return nil
}
