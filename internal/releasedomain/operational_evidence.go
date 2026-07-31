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

	OperationalEvidenceAPIVersion           = "release-domain.fugue.dev/v1"
	OperationalEvidenceKind                 = "OperationalDomainEvidence"
	OperationalEvidencePolicy               = "consumer-build-render-adapter-intersection-v1"
	OperationalActivationEvidencePolicy     = "consumer-activation-render-adapter-intersection-v2"
	OperationalActivationReportPolicy       = "consumer-activation-render-adapter-intersection-v3-report-only"
	OperationalRenderedOnlyActivationPolicy = "consumer-activation-render-adapter-intersection-v4-rendered-only"
	OperationalObservedLiveActivationPolicy = "consumer-activation-render-adapter-intersection-v5-observed-live"
	OperationalImageCacheConvergencePolicy  = "authorized-image-cache-convergence-v1"
	OperationalRenderedOnlyPolicy           = "empty-build-activation-render-adapter-v1"
	operationalRenderedOnlyMismatchIssue    = "image activation domains differ from immutable rendered-object domains"

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

// OperationalDomainEvidence is a conjunction of four independent witness
// channels. AuthorizationEligible is true only for a complete single-domain
// intersection that may be consumed by ResolveOperationalPlan.
type OperationalDomainEvidence struct {
	APIVersion               string                               `json:"apiVersion"`
	Kind                     string                               `json:"kind"`
	Policy                   string                               `json:"policy"`
	BaseCommit               string                               `json:"baseCommit"`
	TargetCommit             string                               `json:"targetCommit"`
	ChangedFilesDigest       string                               `json:"changedFilesDigest"`
	ImagePlanDigest          string                               `json:"imagePlanDigest"`
	PlanDigest               string                               `json:"planDigest"`
	AdapterBindingDigest     string                               `json:"adapterBindingDigest"`
	ImageTargets             []OperationalImageRolloutTarget      `json:"imageTargets"`
	ConsumerDomains          []Domain                             `json:"consumerDomains"`
	ImageRolloutDomains      []Domain                             `json:"imageRolloutDomains"`
	RenderedDomains          []Domain                             `json:"renderedDomains"`
	AdapterDomains           []Domain                             `json:"adapterDomains"`
	IntersectionDomains      []Domain                             `json:"intersectionDomains"`
	AdapterBindings          []OperationalAdapterBinding          `json:"adapterBindings"`
	ConservativeOutcome      Outcome                              `json:"conservativeOutcome"`
	ConservativeDomains      []Domain                             `json:"conservativeDomains"`
	ConservativeDomain       Domain                               `json:"conservativeDomain,omitempty"`
	Observation              Outcome                              `json:"observation"`
	CandidateDomain          Domain                               `json:"candidateDomain,omitempty"`
	ClassificationAgrees     bool                                 `json:"classificationAgrees"`
	Issues                   []string                             `json:"issues"`
	AuthorizationEligible    bool                                 `json:"authorizationEligible"`
	ActivationWitness        []OperationalActivationWitness       `json:"activationWitness,omitempty"`
	RenderedOnlyObservations []RenderedOnlyOperationalObservation `json:"renderedOnlyObservations,omitempty"`
	Digest                   string                               `json:"digest"`
}

// RenderedOnlyOperationalObservation is a parallel classification for
// chart/config-only changes. It is derived from the exact activation witness
// only when no artifact was built or activated. The v3 policy is report-only;
// only the explicit v4 policy may consume a complete single-domain result.
type RenderedOnlyOperationalObservation struct {
	Policy                     string   `json:"policy"`
	BuildArtifactsEmpty        bool     `json:"buildArtifactsEmpty"`
	ImageActivationsEmpty      bool     `json:"imageActivationsEmpty"`
	BuiltOnlyArtifactsEmpty    bool     `json:"builtOnlyArtifactsEmpty"`
	UnresolvedActivationsEmpty bool     `json:"unresolvedActivationsEmpty"`
	ActivationEvidenceComplete bool     `json:"activationEvidenceComplete"`
	Applicable                 bool     `json:"applicable"`
	RenderedDomains            []Domain `json:"renderedDomains"`
	AdapterDomains             []Domain `json:"adapterDomains"`
	IntersectionDomains        []Domain `json:"intersectionDomains"`
	Observation                Outcome  `json:"observation"`
	CandidateDomain            Domain   `json:"candidateDomain,omitempty"`
	Issues                     []string `json:"issues"`
}

// OperationalActivationWitness embeds the exact build/activation partition
// and the independently reclassified immutable rendered target consumed by
// the v2 authorizer. Built artifacts absent from Plan.Activations are audit
// evidence only and cannot create a production rollout domain.
type OperationalActivationWitness struct {
	BuildPlan                     BuildArtifactPlan       `json:"buildPlan"`
	Plan                          ImageActivationPlan     `json:"plan"`
	Evidence                      ImageActivationEvidence `json:"evidence"`
	Rendered                      RenderedClassification  `json:"rendered"`
	BaseManifestDigest            string                  `json:"baseManifestDigest"`
	ObservedLiveManifestDigest    string                  `json:"observedLiveManifestDigest,omitempty"`
	TargetManifestDigest          string                  `json:"targetManifestDigest"`
	ImmutableTargetManifestDigest string                  `json:"immutableTargetManifestDigest"`
	OwnershipDigest               string                  `json:"ownershipDigest"`
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
		ConservativeOutcome:   plan.Result,
		ConservativeDomains:   canonicalDomains(plan.Domains),
		ConservativeDomain:    plan.SelectedDomain,
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
	report.ClassificationAgrees = operationalClassificationAgrees(report)
	report.AuthorizationEligible = operationalAuthorizationEligible(report)
	report.Digest = operationalEvidenceDigest(report)
	return report, nil
}

// BuildOperationalDomainEvidenceFromActivation replaces the build-target
// witness with a live-relative activation witness. It remains side-effect
// free: the returned report can authorize only after ResolveOperationalPlan
// rebinds it to the exact conservative predecessor.
func BuildOperationalDomainEvidenceFromActivation(
	changed ChangedFileEvidence,
	buildPlan BuildArtifactPlan,
	activationPlan ImageActivationPlan,
	activationEvidence ImageActivationEvidence,
	activationRendered RenderedClassification,
	baseManifestDigest string,
	targetManifestDigest string,
	immutableTargetManifestDigest string,
	ownershipDigest string,
	plan Plan,
) (OperationalDomainEvidence, error) {
	return buildOperationalDomainEvidenceFromActivation(
		changed, buildPlan, activationPlan, activationEvidence, activationRendered,
		baseManifestDigest, targetManifestDigest, immutableTargetManifestDigest,
		ownershipDigest, "", plan, OperationalActivationEvidencePolicy, false,
	)
}

// BuildOperationalDomainEvidenceFromActivationReportOnly emits the v3
// parallel rendered-only observation while preserving the v2 authorization
// inputs and decision. It is intentionally separate so existing callers can
// continue producing and verifying the exact v2 contract.
func BuildOperationalDomainEvidenceFromActivationReportOnly(
	changed ChangedFileEvidence,
	buildPlan BuildArtifactPlan,
	activationPlan ImageActivationPlan,
	activationEvidence ImageActivationEvidence,
	activationRendered RenderedClassification,
	baseManifestDigest string,
	targetManifestDigest string,
	immutableTargetManifestDigest string,
	ownershipDigest string,
	plan Plan,
) (OperationalDomainEvidence, error) {
	return buildOperationalDomainEvidenceFromActivation(
		changed, buildPlan, activationPlan, activationEvidence, activationRendered,
		baseManifestDigest, targetManifestDigest, immutableTargetManifestDigest,
		ownershipDigest, "", plan, OperationalActivationReportPolicy, false,
	)
}

// BuildOperationalDomainEvidenceFromRenderedOnlyActivation emits the v4
// evidence policy. It preserves the existing live-relative activation path
// and additionally permits a complete rendered-only observation to authorize
// exactly one operational domain.
func BuildOperationalDomainEvidenceFromRenderedOnlyActivation(
	changed ChangedFileEvidence,
	buildPlan BuildArtifactPlan,
	activationPlan ImageActivationPlan,
	activationEvidence ImageActivationEvidence,
	activationRendered RenderedClassification,
	baseManifestDigest string,
	targetManifestDigest string,
	immutableTargetManifestDigest string,
	ownershipDigest string,
	plan Plan,
) (OperationalDomainEvidence, error) {
	return buildOperationalDomainEvidenceFromActivation(
		changed, buildPlan, activationPlan, activationEvidence, activationRendered,
		baseManifestDigest, targetManifestDigest, immutableTargetManifestDigest,
		ownershipDigest, "", plan, OperationalRenderedOnlyActivationPolicy, false,
	)
}

// BuildOperationalDomainEvidenceFromObservedLiveActivation emits the v5
// rendered-only policy. It keeps the Helm base/target digests intact while
// binding image activation to a separately captured image-only live witness.
// Consumers must also supply those exact observed-live bytes when resolving
// the report; the embedded digest alone is never sufficient authorization.
func BuildOperationalDomainEvidenceFromObservedLiveActivation(
	changed ChangedFileEvidence,
	buildPlan BuildArtifactPlan,
	activationPlan ImageActivationPlan,
	activationEvidence ImageActivationEvidence,
	activationRendered RenderedClassification,
	baseManifestDigest string,
	observedLiveManifestDigest string,
	targetManifestDigest string,
	immutableTargetManifestDigest string,
	ownershipDigest string,
	plan Plan,
) (OperationalDomainEvidence, error) {
	return buildOperationalDomainEvidenceFromActivation(
		changed, buildPlan, activationPlan, activationEvidence, activationRendered,
		baseManifestDigest, targetManifestDigest, immutableTargetManifestDigest,
		ownershipDigest, observedLiveManifestDigest, plan, OperationalObservedLiveActivationPolicy, false,
	)
}

// BuildOperationalDomainEvidenceFromAuthorizedImageCacheConvergence is the
// narrow staged-successor variant. Callers must first authenticate the source
// run's convergence artifact; this constructor still requires the sealed
// build and activation witnesses to prove one deferred image-cache transition.
func BuildOperationalDomainEvidenceFromAuthorizedImageCacheConvergence(
	changed ChangedFileEvidence,
	buildPlan BuildArtifactPlan,
	activationPlan ImageActivationPlan,
	activationEvidence ImageActivationEvidence,
	activationRendered RenderedClassification,
	baseManifestDigest string,
	targetManifestDigest string,
	immutableTargetManifestDigest string,
	ownershipDigest string,
	plan Plan,
) (OperationalDomainEvidence, error) {
	if !authorizedImageCacheConvergenceWitnessMatches(buildPlan, activationPlan.Activations) ||
		!authorizedImageCacheConvergenceContextMatches(
			activationPlan.Activations[0].Workload,
			plan.Digests.ClassificationContext,
		) {
		return OperationalDomainEvidence{}, fmt.Errorf("authorized image-cache convergence witness is not exact")
	}
	return buildOperationalDomainEvidenceFromActivation(
		changed, buildPlan, activationPlan, activationEvidence, activationRendered,
		baseManifestDigest, targetManifestDigest, immutableTargetManifestDigest,
		ownershipDigest, "", plan, OperationalImageCacheConvergencePolicy, true,
	)
}

func buildOperationalDomainEvidenceFromActivation(
	changed ChangedFileEvidence,
	buildPlan BuildArtifactPlan,
	activationPlan ImageActivationPlan,
	activationEvidence ImageActivationEvidence,
	activationRendered RenderedClassification,
	baseManifestDigest string,
	targetManifestDigest string,
	immutableTargetManifestDigest string,
	ownershipDigest string,
	observedLiveManifestDigest string,
	plan Plan,
	policy string,
	authorizedImageCacheConvergence bool,
) (OperationalDomainEvidence, error) {
	if err := VerifyBuildArtifactPlan(buildPlan); err != nil {
		return OperationalDomainEvidence{}, fmt.Errorf("operational activation build plan: %w", err)
	}
	if err := VerifyImageActivationPlan(activationPlan); err != nil {
		return OperationalDomainEvidence{}, fmt.Errorf("operational activation plan: %w", err)
	}
	if err := VerifyImageActivationEvidence(activationEvidence); err != nil {
		return OperationalDomainEvidence{}, fmt.Errorf("operational activation evidence: %w", err)
	}
	if plan.APIVersion != PlanAPIVersion || plan.Kind != PlanKind {
		return OperationalDomainEvidence{}, fmt.Errorf("operational evidence release-domain plan identity is unsupported")
	}
	if err := VerifyPlanDigest(plan); err != nil {
		return OperationalDomainEvidence{}, err
	}
	if changed.BaseCommit() != buildPlan.BaseCommit || changed.TargetCommit() != buildPlan.TargetCommit ||
		changed.Digest() != buildPlan.ChangedFilesDigest {
		return OperationalDomainEvidence{}, fmt.Errorf("operational activation revision binding mismatch")
	}
	if activationPlan.BaseCommit != buildPlan.BaseCommit || activationPlan.TargetCommit != buildPlan.TargetCommit ||
		activationPlan.BuildArtifactPlanDigest != buildPlan.Digest ||
		activationEvidence.BaseCommit != buildPlan.BaseCommit || activationEvidence.TargetCommit != buildPlan.TargetCommit ||
		activationEvidence.BuildArtifactPlanDigest != buildPlan.Digest ||
		activationEvidence.ResolvedImageActivationPlanDigest != activationPlan.Digest {
		return OperationalDomainEvidence{}, fmt.Errorf("operational activation witness binding mismatch")
	}
	expectedLiveStateDigest := baseManifestDigest
	if policy == OperationalObservedLiveActivationPolicy {
		if err := validateCanonicalSHA256Digest(observedLiveManifestDigest, "operational activation observed live manifest digest"); err != nil {
			return OperationalDomainEvidence{}, err
		}
		expectedLiveStateDigest = observedLiveManifestDigest
	} else if observedLiveManifestDigest != "" {
		return OperationalDomainEvidence{}, fmt.Errorf("legacy operational activation must not include an observed live manifest")
	}
	if plan.Digests.ChangedFiles != changed.Digest() ||
		plan.Digests.BaseManifest != baseManifestDigest ||
		plan.Digests.TargetManifest != targetManifestDigest ||
		plan.Digests.Ownership != ownershipDigest ||
		activationPlan.LiveStateDigest != expectedLiveStateDigest {
		return OperationalDomainEvidence{}, fmt.Errorf("operational activation rendered binding mismatch")
	}
	for label, digest := range map[string]string{
		"base manifest":             baseManifestDigest,
		"target manifest":           targetManifestDigest,
		"immutable target manifest": immutableTargetManifestDigest,
		"ownership":                 ownershipDigest,
	} {
		if err := validateCanonicalSHA256Digest(digest, "operational activation "+label+" digest"); err != nil {
			return OperationalDomainEvidence{}, err
		}
	}

	imageTargets, err := operationalActivatedImageTargets(buildPlan, activationPlan)
	if err != nil {
		return OperationalDomainEvidence{}, err
	}
	if err := verifyOperationalActivationPartition(buildPlan, activationPlan, activationEvidence); err != nil {
		return OperationalDomainEvidence{}, err
	}

	issues := make([]string, 0)
	for _, evidence := range validateDigestEvidence(plan.Digests) {
		issues = append(issues, "rendered-object plan integrity failed: "+evidence.Subject+": "+evidence.Reason)
	}
	activationDomains := operationalActivationDomains(activationPlan.Activations, &issues)
	consumerDomains := operationalConsumerDomainsFromActivations(
		changed.Changes(), buildPlan, activationPlan.Activations,
		plan.Digests.ClassificationContext,
		authorizedImageCacheConvergence, &issues,
	)
	activationRendered.Domains = canonicalDomains(activationRendered.Domains)
	activationRendered.Evidence = canonicalEvidence(activationRendered.Evidence)
	// Unknown is optional on RenderedClassification. Preserve its canonical
	// omitted representation when there are no gaps so a marshal/decode cycle
	// cannot turn an empty non-nil slice into nil and drift the sealed witness.
	if len(activationRendered.Unknown) != 0 {
		activationRendered.Unknown = canonicalEvidence(activationRendered.Unknown)
	} else {
		activationRendered.Unknown = nil
	}
	renderedDomains := checkedOperationalDomains(activationRendered.Domains, "rendered", &issues)
	if len(activationRendered.Unknown) != 0 {
		issues = append(issues, "immutable rendered-object evidence is incomplete")
	}
	if len(classificationEvidenceErrors("operational immutable rendered-object", activationRendered.Domains, activationRendered.Evidence)) != 0 {
		issues = append(issues, "immutable rendered-object declared domains differ from their evidence")
	}
	bindings := fixedOperationalBindings()
	adapterDomains := operationalActivationAdapterDomains(activationPlan.Activations, bindings, &issues)
	intersection := intersectOperationalDomains(consumerDomains, activationDomains, renderedDomains, adapterDomains)
	if !equalDomains(intersectOperationalDomains(consumerDomains, activationDomains), activationDomains) {
		issues = append(issues, "changed-package consumers do not cover every activated production domain")
	}
	if !equalDomains(activationDomains, renderedDomains) {
		issues = append(issues, operationalRenderedOnlyMismatchIssue)
	}
	if !equalDomains(adapterDomains, activationDomains) {
		issues = append(issues, "fixed adapter domains differ from image activation domains")
	}
	if !activationEvidence.Complete || len(activationEvidence.Unresolved) != 0 {
		issues = append(issues, "image activation evidence is incomplete")
	}
	issues = canonicalOperationalStrings(issues)

	witness := OperationalActivationWitness{
		BuildPlan: buildPlan, Plan: activationPlan, Evidence: activationEvidence,
		Rendered:           activationRendered,
		BaseManifestDigest: baseManifestDigest, ObservedLiveManifestDigest: observedLiveManifestDigest,
		TargetManifestDigest:          targetManifestDigest,
		ImmutableTargetManifestDigest: immutableTargetManifestDigest, OwnershipDigest: ownershipDigest,
	}
	report := OperationalDomainEvidence{
		APIVersion: OperationalEvidenceAPIVersion, Kind: OperationalEvidenceKind,
		Policy:     policy,
		BaseCommit: changed.BaseCommit(), TargetCommit: changed.TargetCommit(),
		ChangedFilesDigest: changed.Digest(), ImagePlanDigest: activationPlan.Digest,
		PlanDigest: plan.PlanDigest, AdapterBindingDigest: operationalAdapterBindingDigest(bindings),
		ImageTargets: imageTargets, ConsumerDomains: canonicalDomains(consumerDomains),
		ImageRolloutDomains: canonicalDomains(activationDomains), RenderedDomains: canonicalDomains(renderedDomains),
		AdapterDomains: canonicalDomains(adapterDomains), IntersectionDomains: canonicalDomains(intersection),
		AdapterBindings: bindings, ConservativeOutcome: plan.Result,
		ConservativeDomains: canonicalDomains(plan.Domains), ConservativeDomain: plan.SelectedDomain,
		Observation: OutcomeUnknown, Issues: issues, ActivationWitness: []OperationalActivationWitness{witness},
	}
	if policy == OperationalActivationReportPolicy || policy == OperationalRenderedOnlyActivationPolicy ||
		policy == OperationalObservedLiveActivationPolicy ||
		policy == OperationalImageCacheConvergencePolicy {
		observation := buildRenderedOnlyOperationalObservation(
			buildPlan, activationPlan, activationEvidence, activationRendered, bindings,
		)
		report.RenderedOnlyObservations = []RenderedOnlyOperationalObservation{observation}
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
	report.ClassificationAgrees = operationalClassificationAgrees(report)
	report.AuthorizationEligible = operationalAuthorizationEligible(report)
	report.Digest = operationalEvidenceDigest(report)
	if err := VerifyOperationalDomainEvidence(report); err != nil {
		return OperationalDomainEvidence{}, err
	}
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
// binds it to an independently supplied digest. The report alone is never an
// ExecutionAuthorization; resolution still requires its conservative plan.
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
// exact activation eligibility predicate.
func VerifyOperationalDomainEvidence(report OperationalDomainEvidence) error {
	if report.APIVersion != OperationalEvidenceAPIVersion || report.Kind != OperationalEvidenceKind ||
		(report.Policy != OperationalEvidencePolicy && report.Policy != OperationalActivationEvidencePolicy &&
			report.Policy != OperationalActivationReportPolicy &&
			report.Policy != OperationalRenderedOnlyActivationPolicy &&
			report.Policy != OperationalObservedLiveActivationPolicy &&
			report.Policy != OperationalImageCacheConvergencePolicy) {
		return fmt.Errorf("operational domain evidence identity is unsupported")
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
		"conservative":  report.ConservativeDomains,
	} {
		if err := verifyCanonicalOperationalDomains(domains, label); err != nil {
			return err
		}
	}
	switch report.Policy {
	case OperationalEvidencePolicy:
		if len(report.ActivationWitness) != 0 {
			return fmt.Errorf("legacy operational evidence contains an activation witness")
		}
		if len(report.RenderedOnlyObservations) != 0 {
			return fmt.Errorf("legacy operational evidence contains a rendered-only observation")
		}
		embeddedImagePlan := OperationalImageRolloutPlan{
			APIVersion: OperationalImagePlanAPIVersion, Kind: OperationalImagePlanKind,
			Policy: OperationalImagePlanPolicy, BaseCommit: report.BaseCommit,
			TargetCommit: report.TargetCommit, ChangedFilesDigest: report.ChangedFilesDigest,
			Targets: append([]OperationalImageRolloutTarget(nil), report.ImageTargets...), Digest: report.ImagePlanDigest,
		}
		if err := VerifyOperationalImageRolloutPlan(embeddedImagePlan); err != nil {
			return fmt.Errorf("operational evidence embedded image plan: %w", err)
		}
	case OperationalActivationEvidencePolicy, OperationalActivationReportPolicy,
		OperationalRenderedOnlyActivationPolicy, OperationalObservedLiveActivationPolicy,
		OperationalImageCacheConvergencePolicy:
		if len(report.ActivationWitness) != 1 {
			return fmt.Errorf("operational activation evidence requires one exact witness")
		}
		if err := verifyOperationalActivationWitness(report, report.ActivationWitness[0]); err != nil {
			return err
		}
		if report.Policy == OperationalActivationEvidencePolicy {
			if len(report.RenderedOnlyObservations) != 0 {
				return fmt.Errorf("v2 operational activation evidence contains a rendered-only observation")
			}
		} else {
			if len(report.RenderedOnlyObservations) != 1 {
				return fmt.Errorf("v3 operational activation evidence requires one rendered-only observation")
			}
			expected := buildRenderedOnlyOperationalObservation(
				report.ActivationWitness[0].BuildPlan,
				report.ActivationWitness[0].Plan,
				report.ActivationWitness[0].Evidence,
				report.ActivationWitness[0].Rendered,
				fixedOperationalBindings(),
			)
			if !reflect.DeepEqual(report.RenderedOnlyObservations[0], expected) {
				return fmt.Errorf("v3 operational rendered-only observation mismatch")
			}
		}
	}
	if report.Policy == OperationalImageCacheConvergencePolicy {
		if err := verifyAuthorizedImageCacheConvergenceReport(report); err != nil {
			return err
		}
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
	if err := verifyOperationalConservativeClassification(report); err != nil {
		return err
	}
	if report.ClassificationAgrees != operationalClassificationAgrees(report) {
		return fmt.Errorf("operational evidence classification comparison mismatch")
	}
	if report.AuthorizationEligible != operationalAuthorizationEligible(report) {
		return fmt.Errorf("operational evidence authorization eligibility mismatch")
	}
	if report.Digest != operationalEvidenceDigest(report) {
		return fmt.Errorf("operational domain evidence digest mismatch")
	}
	return nil
}

func verifyOperationalActivationWitness(report OperationalDomainEvidence, witness OperationalActivationWitness) error {
	if err := VerifyBuildArtifactPlan(witness.BuildPlan); err != nil {
		return fmt.Errorf("operational activation witness build plan: %w", err)
	}
	if err := VerifyImageActivationPlan(witness.Plan); err != nil {
		return fmt.Errorf("operational activation witness plan: %w", err)
	}
	if err := VerifyImageActivationEvidence(witness.Evidence); err != nil {
		return fmt.Errorf("operational activation witness evidence: %w", err)
	}
	if (!witness.Evidence.Complete || len(witness.Evidence.Unresolved) != 0) &&
		!hasOperationalIssue(report.Issues, "image activation evidence is incomplete") {
		return fmt.Errorf("operational activation witness incompleteness is not recorded")
	}
	for label, digest := range map[string]string{
		"base manifest":             witness.BaseManifestDigest,
		"target manifest":           witness.TargetManifestDigest,
		"immutable target manifest": witness.ImmutableTargetManifestDigest,
		"ownership":                 witness.OwnershipDigest,
	} {
		if err := validateCanonicalSHA256Digest(digest, "operational activation witness "+label+" digest"); err != nil {
			return err
		}
	}
	expectedLiveStateDigest := witness.BaseManifestDigest
	if report.Policy == OperationalObservedLiveActivationPolicy {
		if err := validateCanonicalSHA256Digest(
			witness.ObservedLiveManifestDigest,
			"operational activation witness observed live manifest digest",
		); err != nil {
			return err
		}
		expectedLiveStateDigest = witness.ObservedLiveManifestDigest
	} else if witness.ObservedLiveManifestDigest != "" {
		return fmt.Errorf("legacy operational activation witness contains observed live evidence")
	}
	if witness.BuildPlan.BaseCommit != report.BaseCommit || witness.BuildPlan.TargetCommit != report.TargetCommit ||
		witness.BuildPlan.ChangedFilesDigest != report.ChangedFilesDigest ||
		witness.Plan.BaseCommit != report.BaseCommit || witness.Plan.TargetCommit != report.TargetCommit ||
		witness.Plan.BuildArtifactPlanDigest != witness.BuildPlan.Digest ||
		witness.Plan.Digest != report.ImagePlanDigest || witness.Plan.LiveStateDigest != expectedLiveStateDigest ||
		witness.Evidence.BaseCommit != report.BaseCommit || witness.Evidence.TargetCommit != report.TargetCommit ||
		witness.Evidence.BuildArtifactPlanDigest != witness.BuildPlan.Digest ||
		witness.Evidence.ResolvedImageActivationPlanDigest != witness.Plan.Digest {
		return fmt.Errorf("operational activation witness identity binding mismatch")
	}
	if err := verifyOperationalActivationPartition(witness.BuildPlan, witness.Plan, witness.Evidence); err != nil {
		return err
	}
	targets, err := operationalActivatedImageTargets(witness.BuildPlan, witness.Plan)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(targets, report.ImageTargets) {
		return fmt.Errorf("operational activation witness image targets differ")
	}
	issues := make([]string, 0)
	activationDomains := operationalActivationDomains(witness.Plan.Activations, &issues)
	adapterDomains := operationalActivationAdapterDomains(witness.Plan.Activations, fixedOperationalBindings(), &issues)
	if len(issues) != 0 || !equalDomains(activationDomains, report.ImageRolloutDomains) ||
		!equalDomains(adapterDomains, report.AdapterDomains) {
		return fmt.Errorf("operational activation witness domain or adapter binding mismatch")
	}
	if (len(witness.Rendered.Domains) != 0 && !reflect.DeepEqual(witness.Rendered.Domains, canonicalDomains(witness.Rendered.Domains))) ||
		(len(witness.Rendered.Evidence) != 0 && !reflect.DeepEqual(witness.Rendered.Evidence, canonicalEvidence(witness.Rendered.Evidence))) ||
		(len(witness.Rendered.Unknown) != 0 && !reflect.DeepEqual(witness.Rendered.Unknown, canonicalEvidence(witness.Rendered.Unknown))) ||
		!equalDomains(witness.Rendered.Domains, report.RenderedDomains) {
		return fmt.Errorf("operational activation witness rendered classification mismatch")
	}
	if len(witness.Rendered.Unknown) != 0 &&
		!hasOperationalIssue(report.Issues, "immutable rendered-object evidence is incomplete") {
		return fmt.Errorf("operational activation witness rendered incompleteness is not recorded")
	}
	if len(classificationEvidenceErrors("operational immutable rendered-object", witness.Rendered.Domains, witness.Rendered.Evidence)) != 0 &&
		!hasOperationalIssue(report.Issues, "immutable rendered-object declared domains differ from their evidence") {
		return fmt.Errorf("operational activation witness rendered disagreement is not recorded")
	}
	if !equalDomains(activationDomains, witness.Rendered.Domains) &&
		!hasOperationalIssue(report.Issues, operationalRenderedOnlyMismatchIssue) {
		return fmt.Errorf("operational activation witness rendered-domain mismatch is not recorded")
	}
	if !equalDomains(adapterDomains, activationDomains) &&
		!hasOperationalIssue(report.Issues, "fixed adapter domains differ from image activation domains") {
		return fmt.Errorf("operational activation witness adapter mismatch is not recorded")
	}
	if !equalDomains(intersectOperationalDomains(report.ConsumerDomains, activationDomains), activationDomains) &&
		!hasOperationalIssue(report.Issues, "changed-package consumers do not cover every activated production domain") {
		return fmt.Errorf("operational activation witness consumer gap is not recorded")
	}
	return nil
}

func hasOperationalIssue(issues []string, expected string) bool {
	for _, issue := range issues {
		if issue == expected {
			return true
		}
	}
	return false
}

func buildRenderedOnlyOperationalObservation(
	buildPlan BuildArtifactPlan,
	activationPlan ImageActivationPlan,
	activationEvidence ImageActivationEvidence,
	rendered RenderedClassification,
	bindings []OperationalAdapterBinding,
) RenderedOnlyOperationalObservation {
	issues := make([]string, 0)
	renderedDomains := checkedOperationalDomains(rendered.Domains, "rendered-only", &issues)
	if len(rendered.Unknown) != 0 {
		issues = append(issues, "rendered-only immutable rendered-object evidence is incomplete")
	}
	if len(classificationEvidenceErrors("rendered-only immutable rendered-object", rendered.Domains, rendered.Evidence)) != 0 {
		issues = append(issues, "rendered-only declared domains differ from their evidence")
	}
	adapterDomains := operationalAdapterDomains(renderedDomains, bindings, &issues)
	intersection := intersectOperationalDomains(renderedDomains, adapterDomains)

	if len(buildPlan.Artifacts) != 0 {
		issues = append(issues, "rendered-only build artifact plan is not empty")
	}
	if len(activationPlan.Activations) != 0 {
		issues = append(issues, "rendered-only image activation plan is not empty")
	}
	if len(activationEvidence.BuiltOnlyArtifacts) != 0 {
		issues = append(issues, "rendered-only built-only artifact set is not empty")
	}
	if !activationEvidence.Complete || len(activationEvidence.Unresolved) != 0 {
		issues = append(issues, "rendered-only image activation evidence is incomplete")
	}
	issues = canonicalOperationalStrings(issues)

	observation := RenderedOnlyOperationalObservation{
		Policy:                     OperationalRenderedOnlyPolicy,
		BuildArtifactsEmpty:        len(buildPlan.Artifacts) == 0,
		ImageActivationsEmpty:      len(activationPlan.Activations) == 0,
		BuiltOnlyArtifactsEmpty:    len(activationEvidence.BuiltOnlyArtifacts) == 0,
		UnresolvedActivationsEmpty: len(activationEvidence.Unresolved) == 0,
		ActivationEvidenceComplete: activationEvidence.Complete,
		Applicable: len(buildPlan.Artifacts) == 0 && len(activationPlan.Activations) == 0 &&
			len(activationEvidence.BuiltOnlyArtifacts) == 0 && len(activationEvidence.Unresolved) == 0 &&
			activationEvidence.Complete,
		RenderedDomains: canonicalDomains(renderedDomains), AdapterDomains: canonicalDomains(adapterDomains),
		IntersectionDomains: canonicalDomains(intersection), Observation: OutcomeUnknown, Issues: issues,
	}
	if observation.Applicable && len(observation.Issues) == 0 {
		switch len(observation.IntersectionDomains) {
		case 0:
			observation.Observation = OutcomeZero
		case 1:
			observation.Observation = OutcomeSingle
			observation.CandidateDomain = observation.IntersectionDomains[0]
		default:
			observation.Observation = OutcomeMultiple
		}
	}
	return observation
}

// ResolveOperationalPlan returns a canonically reproducible operational plan
// when a conservative multiple/unknown result is paired with complete live-
// relative evidence. It can resolve either one executable domain or a proven
// zero-write result: built artifacts are all absent from the target, or the
// empty build plan proves the immutable target already equals live state.
func ResolveOperationalPlan(conservative Plan, report OperationalDomainEvidence) (Plan, error) {
	return resolveOperationalPlan(conservative, report, nil)
}

// ResolveOperationalPlanWithObservedLiveManifest is the only resolver for a
// v5 observed-live report. The caller must provide the exact private canonical
// image-only witness whose digest was embedded by the report producer.
func ResolveOperationalPlanWithObservedLiveManifest(
	conservative Plan,
	report OperationalDomainEvidence,
	observedLiveManifest []byte,
) (Plan, error) {
	if len(observedLiveManifest) == 0 {
		return Plan{}, fmt.Errorf("operational activation observed live manifest is required")
	}
	return resolveOperationalPlan(conservative, report, observedLiveManifest)
}

func resolveOperationalPlan(
	conservative Plan,
	report OperationalDomainEvidence,
	observedLiveManifest []byte,
) (Plan, error) {
	if err := VerifyPlanDigest(conservative); err != nil {
		return Plan{}, fmt.Errorf("operational activation conservative plan: %w", err)
	}
	if len(conservative.OperationalEvidence) != 0 {
		return Plan{}, fmt.Errorf("operational activation requires a conservative predecessor")
	}
	if err := VerifyOperationalDomainEvidence(report); err != nil {
		return Plan{}, fmt.Errorf("operational activation report: %w", err)
	}
	if report.Policy == OperationalObservedLiveActivationPolicy {
		witness := report.ActivationWitness[0]
		if len(observedLiveManifest) == 0 ||
			digestBytesSHA256(observedLiveManifest) != witness.ObservedLiveManifestDigest ||
			witness.Plan.LiveStateDigest != witness.ObservedLiveManifestDigest {
			return Plan{}, fmt.Errorf("operational activation observed live manifest binding mismatch")
		}
	} else if len(observedLiveManifest) != 0 {
		return Plan{}, fmt.Errorf("legacy operational activation does not accept an observed live manifest")
	}
	authorizedDomain, domainEligible := operationalAuthorizationCandidate(report)
	zeroEligible := operationalZeroResolutionEligible(report)
	if report.PlanDigest != conservative.PlanDigest || report.ChangedFilesDigest != conservative.Digests.ChangedFiles {
		return Plan{}, fmt.Errorf("operational activation evidence binding mismatch")
	}
	if (!report.AuthorizationEligible || !domainEligible) && !zeroEligible {
		return Plan{}, fmt.Errorf("operational activation evidence does not resolve to a safe outcome")
	}
	if report.ConservativeOutcome != conservative.Result ||
		!equalDomains(report.ConservativeDomains, conservative.Domains) ||
		report.ConservativeDomain != conservative.SelectedDomain {
		return Plan{}, fmt.Errorf("operational activation conservative classification mismatch")
	}
	if conservative.Result != OutcomeMultiple && conservative.Result != OutcomeUnknown {
		return Plan{}, fmt.Errorf("operational activation requires a blocked conservative outcome")
	}
	if conservative.Files.AllNonRuntime {
		return Plan{}, fmt.Errorf("operational activation single-domain evidence mismatch")
	}
	if report.Policy == OperationalActivationEvidencePolicy || report.Policy == OperationalActivationReportPolicy ||
		report.Policy == OperationalRenderedOnlyActivationPolicy ||
		report.Policy == OperationalObservedLiveActivationPolicy ||
		report.Policy == OperationalImageCacheConvergencePolicy {
		witness := report.ActivationWitness[0]
		expectedLiveStateDigest := conservative.Digests.BaseManifest
		if report.Policy == OperationalObservedLiveActivationPolicy {
			expectedLiveStateDigest = witness.ObservedLiveManifestDigest
		}
		if witness.BaseManifestDigest != conservative.Digests.BaseManifest ||
			witness.TargetManifestDigest != conservative.Digests.TargetManifest ||
			witness.OwnershipDigest != conservative.Digests.Ownership ||
			witness.Plan.LiveStateDigest != expectedLiveStateDigest ||
			witness.BuildPlan.ChangedFilesDigest != conservative.Digests.ChangedFiles {
			return Plan{}, fmt.Errorf("operational activation immutable rendered witness mismatch")
		}
	}
	if report.Policy == OperationalImageCacheConvergencePolicy &&
		!authorizedImageCacheConvergenceContextMatches(
			report.ActivationWitness[0].Plan.Activations[0].Workload,
			conservative.Digests.ClassificationContext,
		) {
		return Plan{}, fmt.Errorf("authorized image-cache convergence classification context mismatch")
	}
	rebuiltConservative := BuildPlan(PlanInput{
		Files: conservative.Files, Rendered: conservative.Rendered, Digests: conservative.Digests,
	})
	if !reflect.DeepEqual(rebuiltConservative, conservative) {
		return Plan{}, fmt.Errorf("operational activation conservative plan is not canonically reproducible")
	}

	clonedPlan := rebuiltConservative
	encodedReport, err := MarshalOperationalDomainEvidence(report)
	if err != nil {
		return Plan{}, fmt.Errorf("operational activation marshal report: %w", err)
	}
	clonedReport, err := DecodeAndVerifyOperationalDomainEvidence(bytes.NewReader(encodedReport), report.Digest)
	if err != nil {
		return Plan{}, fmt.Errorf("operational activation clone report: %w", err)
	}

	clonedPlan.OperationalEvidence = []OperationalDomainEvidence{clonedReport}
	if zeroEligible {
		if !operationalZeroResolutionEligible(clonedReport) {
			return Plan{}, fmt.Errorf("operational activation cloned zero-write evidence mismatch")
		}
		clonedPlan.Result = OutcomeZero
		clonedPlan.SelectedDomain = ""
		clonedPlan.Domains = []Domain{}
	} else {
		clonedPlan.Result = OutcomeSingle
		clonedDomain, ok := operationalAuthorizationCandidate(clonedReport)
		if !ok || clonedDomain != authorizedDomain {
			return Plan{}, fmt.Errorf("operational activation cloned authorization domain mismatch")
		}
		clonedPlan.SelectedDomain = clonedDomain
		clonedPlan.Domains = []Domain{clonedDomain}
	}
	clonedPlan.PlanDigest = computePlanDigest(clonedPlan)
	return clonedPlan, nil
}

// ActivateOperationalPlan preserves the single-domain execution API. A
// proven zero-write result is intentionally consumable only by the dispatcher,
// which never creates an execution authorization for zero outcomes.
func ActivateOperationalPlan(conservative Plan, report OperationalDomainEvidence) (Plan, error) {
	resolved, err := ResolveOperationalPlan(conservative, report)
	if err != nil {
		return Plan{}, err
	}
	if resolved.Result != OutcomeSingle {
		return Plan{}, fmt.Errorf("operational activation requires a single-domain result")
	}
	return resolved, nil
}

func operationalZeroResolutionEligible(report OperationalDomainEvidence) bool {
	if !isConsumableRenderedOnlyPolicy(report.Policy) || report.AuthorizationEligible ||
		len(report.Issues) != 0 || report.Observation != OutcomeZero || report.CandidateDomain != "" ||
		len(report.ImageTargets) != 0 || len(report.ImageRolloutDomains) != 0 ||
		len(report.RenderedDomains) != 0 || len(report.AdapterDomains) != 0 ||
		len(report.IntersectionDomains) != 0 || len(report.ActivationWitness) != 1 ||
		len(report.RenderedOnlyObservations) != 1 {
		return false
	}
	witness := report.ActivationWitness[0]
	if len(witness.Plan.Activations) != 0 || !witness.Evidence.Complete ||
		len(witness.Evidence.Unresolved) != 0 ||
		len(witness.Evidence.BuiltOnlyArtifacts) != len(witness.BuildPlan.Artifacts) ||
		len(witness.Rendered.Domains) != 0 || len(witness.Rendered.Evidence) != 0 ||
		len(witness.Rendered.Unknown) != 0 {
		return false
	}
	if witness.BaseManifestDigest != witness.TargetManifestDigest ||
		witness.BaseManifestDigest != witness.ImmutableTargetManifestDigest {
		return false
	}
	if len(witness.BuildPlan.Artifacts) > 0 {
		return true
	}
	observation := report.RenderedOnlyObservations[0]
	return observation.Applicable && len(observation.Issues) == 0 &&
		observation.Observation == OutcomeZero && observation.CandidateDomain == "" &&
		len(observation.RenderedDomains) == 0 && len(observation.AdapterDomains) == 0 &&
		len(observation.IntersectionDomains) == 0
}

func operationalAuthorizationEligible(report OperationalDomainEvidence) bool {
	_, candidate := operationalAuthorizationCandidate(report)
	return candidate && (report.ConservativeOutcome == OutcomeMultiple || report.ConservativeOutcome == OutcomeUnknown)
}

func operationalAuthorizationCandidate(report OperationalDomainEvidence) (Domain, bool) {
	if len(report.Issues) == 0 && report.Observation == OutcomeSingle &&
		len(report.IntersectionDomains) == 1 && report.CandidateDomain == report.IntersectionDomains[0] {
		return report.CandidateDomain, true
	}
	if !isConsumableRenderedOnlyPolicy(report.Policy) || len(report.RenderedOnlyObservations) != 1 ||
		!reflect.DeepEqual(report.Issues, []string{operationalRenderedOnlyMismatchIssue}) ||
		len(report.ImageTargets) != 0 || len(report.ConsumerDomains) != 0 ||
		len(report.ImageRolloutDomains) != 0 || len(report.AdapterDomains) != 0 ||
		len(report.IntersectionDomains) != 0 || report.Observation != OutcomeUnknown || report.CandidateDomain != "" {
		return "", false
	}
	observation := report.RenderedOnlyObservations[0]
	if !observation.Applicable || len(observation.Issues) != 0 ||
		observation.Observation != OutcomeSingle || len(observation.IntersectionDomains) != 1 ||
		observation.CandidateDomain != observation.IntersectionDomains[0] {
		return "", false
	}
	return observation.CandidateDomain, true
}

func isConsumableRenderedOnlyPolicy(policy string) bool {
	return policy == OperationalRenderedOnlyActivationPolicy ||
		policy == OperationalObservedLiveActivationPolicy
}

func verifyOperationalConservativeClassification(report OperationalDomainEvidence) error {
	switch report.ConservativeOutcome {
	case OutcomeZero:
		if len(report.ConservativeDomains) != 0 || report.ConservativeDomain != "" {
			return fmt.Errorf("conservative zero classification mismatch")
		}
	case OutcomeSingle:
		if len(report.ConservativeDomains) != 1 || report.ConservativeDomain != report.ConservativeDomains[0] {
			return fmt.Errorf("conservative single classification mismatch")
		}
	case OutcomeMultiple:
		if len(report.ConservativeDomains) < 2 || report.ConservativeDomain != "" {
			return fmt.Errorf("conservative multiple classification mismatch")
		}
	case OutcomeUnknown:
		if report.ConservativeDomain != "" {
			return fmt.Errorf("conservative unknown classification selected a domain")
		}
	default:
		return fmt.Errorf("conservative classification outcome is unsupported")
	}
	return nil
}

func operationalClassificationAgrees(report OperationalDomainEvidence) bool {
	if report.ConservativeOutcome != report.Observation ||
		!equalDomains(report.ConservativeDomains, report.IntersectionDomains) {
		return false
	}
	if report.Observation == OutcomeSingle {
		return report.ConservativeDomain == report.CandidateDomain
	}
	return report.ConservativeDomain == "" && report.CandidateDomain == ""
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

func operationalConsumerDomainsFromActivations(
	changes []ChangedFile,
	buildPlan BuildArtifactPlan,
	activations []ImageActivation,
	classificationContext ClassificationContextEvidence,
	authorizedImageCacheConvergence bool,
	issues *[]string,
) []Domain {
	activatedArtifacts := make(map[string]map[Domain]struct{})
	for _, activation := range activations {
		domains := activatedArtifacts[activation.ArtifactName]
		if domains == nil {
			domains = map[Domain]struct{}{}
			activatedArtifacts[activation.ArtifactName] = domains
		}
		domains[activation.Domain] = struct{}{}
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
			artifact, mapped := operationalCommandImageTargets[command]
			if !mapped {
				continue
			}
			for domain := range activatedArtifacts[artifact] {
				set[domain] = struct{}{}
			}
		}
	}
	// A staged image-cache convergence successor activates an artifact whose
	// component baseline deliberately predates the release-wide baseline. The
	// release-wide changed-file witness therefore cannot contain the source
	// delta that selected image_cache. Admit that one missing consumer only
	// when the sealed build and activation witnesses prove the exact singleton
	// image-cache transition. Ordinary same-baseline activations and every
	// other artifact still require changed-package consumer evidence.
	if authorizedImageCacheConvergence &&
		authorizedImageCacheConvergenceWitnessMatches(buildPlan, activations) &&
		authorizedImageCacheConvergenceContextMatches(activations[0].Workload, classificationContext) {
		set[DomainImageCache] = struct{}{}
	}
	return domainsFromSet(set)
}

func authorizedImageCacheConvergenceWitnessMatches(buildPlan BuildArtifactPlan, activations []ImageActivation) bool {
	if len(buildPlan.Artifacts) != 1 || len(activations) != 1 {
		return false
	}
	artifact := buildPlan.Artifacts[0]
	activation := activations[0]
	expectedAdapter, adapterExists := fixedAdapterForDomain(DomainImageCache)
	return artifact.Name == "image_cache" && artifact.SourceBaseCommit != buildPlan.BaseCommit &&
		activation.ArtifactName == artifact.Name && activation.ArtifactDigest == artifact.ArtifactDigest &&
		activation.Domain == DomainImageCache && adapterExists && activation.Adapter == expectedAdapter &&
		activation.Workload.APIVersion == "apps/v1" && activation.Workload.Kind == "DaemonSet" &&
		strings.TrimSpace(activation.Workload.Namespace) != "" &&
		strings.TrimSpace(activation.Workload.Name) != "" &&
		activation.Workload.Container == "image-cache"
}

func authorizedImageCacheConvergenceContextMatches(
	workload ActivationWorkload,
	context ClassificationContextEvidence,
) bool {
	bindings := context.BindingMap()
	releaseNamespace := bindings["releaseNamespace"]
	imageCacheName := bindings["imageCacheName"]
	return strings.TrimSpace(releaseNamespace) != "" && releaseNamespace == context.DefaultNamespace &&
		strings.TrimSpace(imageCacheName) != "" && workload.Namespace == releaseNamespace &&
		workload.Name == imageCacheName
}

func verifyAuthorizedImageCacheConvergenceReport(report OperationalDomainEvidence) error {
	if len(report.ActivationWitness) != 1 ||
		!authorizedImageCacheConvergenceWitnessMatches(
			report.ActivationWitness[0].BuildPlan,
			report.ActivationWitness[0].Plan.Activations,
		) {
		return fmt.Errorf("authorized image-cache convergence witness is not exact")
	}
	if len(report.Issues) != 0 || report.Observation != OutcomeSingle ||
		report.CandidateDomain != DomainImageCache || !report.AuthorizationEligible ||
		!equalDomains(report.ImageRolloutDomains, []Domain{DomainImageCache}) ||
		!equalDomains(report.RenderedDomains, []Domain{DomainImageCache}) ||
		!equalDomains(report.AdapterDomains, []Domain{DomainImageCache}) ||
		!equalDomains(report.IntersectionDomains, []Domain{DomainImageCache}) ||
		!domainSetContains(report.ConsumerDomains, DomainImageCache) {
		return fmt.Errorf("authorized image-cache convergence report does not prove one exact domain")
	}
	return nil
}

func domainSetContains(domains []Domain, wanted Domain) bool {
	for _, domain := range domains {
		if domain == wanted {
			return true
		}
	}
	return false
}

func operationalActivationDomains(activations []ImageActivation, issues *[]string) []Domain {
	set := map[Domain]struct{}{}
	for _, activation := range activations {
		if _, err := ParseDomain(string(activation.Domain)); err != nil {
			*issues = append(*issues, "image activation contains an unknown domain")
			continue
		}
		expected, ok := fixedAdapterForDomain(activation.Domain)
		if !ok || activation.Adapter != expected {
			*issues = append(*issues, "image activation fixed adapter binding differs")
			continue
		}
		set[activation.Domain] = struct{}{}
	}
	return domainsFromSet(set)
}

func operationalActivationAdapterDomains(activations []ImageActivation, bindings []OperationalAdapterBinding, issues *[]string) []Domain {
	bound := make(map[Domain]string, len(bindings))
	for _, binding := range bindings {
		bound[binding.Domain] = binding.Adapter
	}
	set := map[Domain]struct{}{}
	for _, activation := range activations {
		adapter, ok := bound[activation.Domain]
		if !ok || adapter != activation.Adapter {
			*issues = append(*issues, "activated domain has no exact fixed production adapter")
			continue
		}
		set[activation.Domain] = struct{}{}
	}
	return domainsFromSet(set)
}

func operationalActivatedImageTargets(buildPlan BuildArtifactPlan, activationPlan ImageActivationPlan) ([]OperationalImageRolloutTarget, error) {
	artifacts := make(map[string]BuildArtifact, len(buildPlan.Artifacts))
	for _, artifact := range buildPlan.Artifacts {
		artifacts[artifact.Name] = artifact
	}
	used := map[string]struct{}{}
	targets := make([]OperationalImageRolloutTarget, 0, len(activationPlan.Activations))
	for _, activation := range activationPlan.Activations {
		artifact, ok := artifacts[activation.ArtifactName]
		if !ok || artifact.ArtifactDigest != activation.ArtifactDigest {
			return nil, fmt.Errorf("operational activation artifact binding mismatch")
		}
		if _, duplicate := used[artifact.Name]; duplicate {
			continue
		}
		used[artifact.Name] = struct{}{}
		targets = append(targets, OperationalImageRolloutTarget{
			Name: artifact.Name, SourceBaseCommit: artifact.SourceBaseCommit, ArtifactDigest: artifact.ArtifactDigest,
		})
	}
	return canonicalActivationImageTargets(targets)
}

func verifyOperationalActivationPartition(buildPlan BuildArtifactPlan, activationPlan ImageActivationPlan, evidence ImageActivationEvidence) error {
	activated := map[string]struct{}{}
	for _, activation := range activationPlan.Activations {
		activated[activation.ArtifactName] = struct{}{}
	}
	builtOnly := map[string]struct{}{}
	for _, name := range evidence.BuiltOnlyArtifacts {
		if _, overlap := activated[name]; overlap {
			return fmt.Errorf("operational activation artifact is both activated and built-only")
		}
		builtOnly[name] = struct{}{}
	}
	unresolved := map[string]struct{}{}
	for _, gap := range evidence.Unresolved {
		for _, name := range gap.MatchingBuildArtifacts {
			if _, active := activated[name]; active {
				return fmt.Errorf("operational activation artifact is both activated and unresolved")
			}
			if _, only := builtOnly[name]; only {
				return fmt.Errorf("operational activation artifact is both built-only and unresolved")
			}
			unresolved[name] = struct{}{}
		}
	}
	for _, artifact := range buildPlan.Artifacts {
		_, active := activated[artifact.Name]
		_, only := builtOnly[artifact.Name]
		_, blocked := unresolved[artifact.Name]
		memberships := 0
		for _, present := range []bool{active, only, blocked} {
			if present {
				memberships++
			}
		}
		if memberships != 1 {
			return fmt.Errorf("operational activation build partition is incomplete")
		}
		delete(activated, artifact.Name)
		delete(builtOnly, artifact.Name)
		delete(unresolved, artifact.Name)
	}
	if len(activated) != 0 || len(builtOnly) != 0 || len(unresolved) != 0 {
		return fmt.Errorf("operational activation build partition contains unknown artifacts")
	}
	return nil
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

func canonicalActivationImageTargets(targets []OperationalImageRolloutTarget) ([]OperationalImageRolloutTarget, error) {
	result := append([]OperationalImageRolloutTarget(nil), targets...)
	sort.Slice(result, func(left, right int) bool { return result[left].Name < result[right].Name })
	for index, target := range result {
		if !validContractIdentifier(target.Name) || (index > 0 && result[index-1].Name == target.Name) {
			return nil, fmt.Errorf("operational activation image target is invalid or duplicated")
		}
		if err := validateTrustedGitCommit(target.SourceBaseCommit, "operational activation image target source base commit"); err != nil {
			return nil, err
		}
		if err := validateCanonicalSHA256Digest(target.ArtifactDigest, "operational activation image target artifact digest"); err != nil {
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
