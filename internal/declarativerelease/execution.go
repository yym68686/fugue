package declarativerelease

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	ExecutionPlanAPIVersion = "release.fugue.dev/v2"
	ExecutionPlanKind       = "DeclarativeComponentExecutionPlan"
	ExecutionResultKind     = "DeclarativeComponentExecutionResult"
)

var resourceVersionPattern = regexp.MustCompile(`^[1-9][0-9]*$`)

type Observation struct {
	Present            bool                  `json:"present"`
	Primary            ResourceIdentity      `json:"primary"`
	UID                string                `json:"uid"`
	ResourceVersion    string                `json:"resourceVersion"`
	Generation         int64                 `json:"generation"`
	ObservedGeneration int64                 `json:"observedGeneration"`
	Desired            int32                 `json:"desired"`
	Updated            int32                 `json:"updated"`
	Ready              int32                 `json:"ready"`
	Available          int32                 `json:"available"`
	Unavailable        int32                 `json:"unavailable"`
	ImageRef           string                `json:"imageRef"`
	ImageID            string                `json:"imageId"`
	ConfigSHA          string                `json:"configSha"`
	ManifestSHA        string                `json:"manifestSha"`
	OCIRevision        string                `json:"ociRevision"`
	TemplateDigest     string                `json:"templateDigest"`
	HealthDigest       string                `json:"healthDigest"`
	FieldManagers      []string              `json:"fieldManagers"`
	Resources          []ResourceObservation `json:"resources"`
}

type ResourceObservation struct {
	Identity                   ResourceIdentity `json:"identity"`
	Present                    bool             `json:"present"`
	RetainOnRollback           bool             `json:"retainOnRollback"`
	UID                        string           `json:"uid,omitempty"`
	ResourceVersion            string           `json:"resourceVersion,omitempty"`
	Generation                 int64            `json:"generation,omitempty"`
	ObjectDigest               string           `json:"objectDigest,omitempty"`
	FieldManagers              []string         `json:"fieldManagers,omitempty"`
	ReviewedOwnershipApplied   bool             `json:"reviewedOwnershipApplied,omitempty"`
	ReviewedOwnershipExclusive bool             `json:"reviewedOwnershipExclusive,omitempty"`
}

type TargetIdentity struct {
	Present        bool   `json:"present"`
	ImageRef       string `json:"imageRef"`
	ConfigSHA      string `json:"configSha"`
	ManifestSHA    string `json:"manifestSha"`
	OCIRevision    string `json:"ociRevision"`
	ManifestDigest string `json:"manifestDigest"`
}

type ExecutionPlan struct {
	APIVersion          string                 `json:"apiVersion"`
	Kind                string                 `json:"kind"`
	Component           string                 `json:"component"`
	ConfigSHA           string                 `json:"configSha"`
	ReleasePlanDigest   string                 `json:"releasePlanDigest"`
	IntentDigest        string                 `json:"intentDigest"`
	ArtifactDigest      string                 `json:"artifactDigest"`
	Forward             TargetIdentity         `json:"forward"`
	LKG                 TargetIdentity         `json:"lkg"`
	Prewrite            Observation            `json:"prewrite"`
	AlreadyConverged    bool                   `json:"alreadyConverged"`
	DegradedPredecessor bool                   `json:"degradedPredecessor,omitempty"`
	OwnershipAdoption   *OwnershipAdoptionPlan `json:"ownershipAdoption,omitempty"`
	PreparedAt          string                 `json:"preparedAt"`
	PlanDigest          string                 `json:"planDigest"`
}

type OwnershipAdoptionPlan struct {
	Component           string                          `json:"component"`
	BootstrapLKGDigest  string                          `json:"bootstrapLkgDigest"`
	UID                 string                          `json:"uid"`
	ResourceVersion     string                          `json:"resourceVersion"`
	Generation          int64                           `json:"generation"`
	LegacyFieldManager  string                          `json:"legacyFieldManager"`
	LegacyFieldManagers []string                        `json:"legacyFieldManagers"`
	Resources           []OwnershipAdoptionResourcePlan `json:"resources"`
	ImageRef            string                          `json:"imageRef"`
	ConfigSHA           string                          `json:"configSha"`
	ManifestSHA         string                          `json:"manifestSha"`
	OCIRevision         string                          `json:"ociRevision"`
	AlreadyConverged    bool                            `json:"alreadyConverged,omitempty"`
	ResumeTakeover      bool                            `json:"resumeTakeover,omitempty"`
}

type OwnershipAdoptionResourcePlan struct {
	Identity            ResourceIdentity              `json:"identity"`
	Fields              []string                      `json:"fields"`
	ValidationScaffolds []OwnershipValidationScaffold `json:"validationScaffolds,omitempty"`
	UID                 string                        `json:"uid"`
	ResourceVersion     string                        `json:"resourceVersion"`
	Generation          int64                         `json:"generation"`
}

type OwnershipValidationScaffold struct {
	Pointer string `json:"pointer"`
	Value   string `json:"value"`
}

type ExecutionResult struct {
	APIVersion          string      `json:"apiVersion"`
	Kind                string      `json:"kind"`
	Component           string      `json:"component"`
	ConfigSHA           string      `json:"configSha"`
	ExecutionPlanDigest string      `json:"executionPlanDigest"`
	Status              string      `json:"status"`
	Reason              string      `json:"reason"`
	FailureClass        string      `json:"failureClass,omitempty"`
	FailureDetail       string      `json:"failureDetail,omitempty"`
	ForwardApplyCount   int         `json:"forwardApplyCount"`
	LKGApplyCount       int         `json:"lkgApplyCount"`
	Final               Observation `json:"final"`
	ReceiptDigest       string      `json:"receiptDigest"`
}

type Cluster interface {
	Observe(context.Context, PlanRelease, TargetIdentity, []byte) (Observation, error)
	ObserveCAS(context.Context, PlanRelease, []byte) (Observation, error)
	ObserveDegraded(context.Context, PlanRelease, []byte) (Observation, error)
	VerifyTarget(context.Context, TargetIdentity) error
	DryRunApply(context.Context, PlanRelease, []byte) error
	DryRunOwnershipAdoption(context.Context, PlanRelease, OwnershipAdoptionPlan, []byte) error
	DryRunOwnershipTakeover(context.Context, PlanRelease, OwnershipAdoptionPlan, TargetIdentity, []byte) error
	AdoptOwnership(context.Context, PlanRelease, OwnershipAdoptionPlan, TargetIdentity, []byte) (Observation, error)
	TakeoverOwnership(context.Context, PlanRelease, OwnershipAdoptionPlan, TargetIdentity, []byte) (Observation, error)
	Apply(context.Context, PlanRelease, TargetIdentity, []byte) error
	Delete(context.Context, PlanRelease, []byte, Observation) error
	DeleteCreated(context.Context, PlanRelease, []byte, Observation, Observation) error
	WaitHealthy(context.Context, PlanRelease, TargetIdentity, []byte) (Observation, error)
	Converged(context.Context, PlanRelease, []byte) error
}

var ErrDegradedPredecessorHealth = errors.New("declarative predecessor health is degraded")

type prewritePredecessorHealthWaitKey struct{}

// WithPrewritePredecessorHealthWait marks only the immutable predecessor
// health wait performed before any production mutation. Kubernetes adapters
// may use the marker to return an already-typed degraded-health result without
// consuming the remaining command budget. Forward and rollback waits never
// carry this marker.
func WithPrewritePredecessorHealthWait(ctx context.Context) context.Context {
	return context.WithValue(ctx, prewritePredecessorHealthWaitKey{}, true)
}

func IsPrewritePredecessorHealthWait(ctx context.Context) bool {
	marked, _ := ctx.Value(prewritePredecessorHealthWaitKey{}).(bool)
	return marked
}

// InitialExplicitBootstrapAdoption is the only first-generation adoption that
// may recover a typed unhealthy predecessor. The explicit runtime binds the
// live legacy Pod while the expected predecessor remains bound to the
// canonical immutable bootstrap LKG.
func InitialExplicitBootstrapAdoption(release PlanRelease) bool {
	if release.MigrationState != "adopting" || release.IntentGeneration != 1 || release.RetrySameLKG ||
		release.SupersedesFailedConfigSHA != "" || !release.ExpectedPreviousPresent || release.BootstrapRuntime == nil ||
		release.BootstrapLKGPath == "" || release.OwnershipAdoption == nil || len(release.OwnershipAdoption.Resources) == 0 {
		return false
	}
	bootstrap := release.BootstrapRuntime
	return bootstrap.Resource == (ResourceIdentity{
		APIVersion: release.Workload.APIVersion, Kind: release.Workload.Kind,
		Namespace: release.Workload.Namespace, Name: release.Workload.Name,
	}) && bootstrap.Container == release.Workload.Container && digestPattern.MatchString(bootstrap.ImageDigest) &&
		shaPattern.MatchString(bootstrap.OCIRevision)
}

// ownershipTakeoverCompensator is intentionally narrower than Cluster. Only
// the Kubernetes adapter implements it; test/fake clusters must opt in so a
// migration rollback cannot silently skip forward-only field cleanup.
type ownershipTakeoverCompensator interface {
	ClearOwnershipTakeoverForwardOnlyFields(context.Context, PlanRelease, []byte, []byte, Observation) error
}

// ownershipTakeoverCreatedResourceCompensator preserves explicit workload
// dependencies that are introduced by the forward resource set. A bootstrap
// LKG may predate such a dependency even though the live workload already
// references it; deleting it during compensation can strand a recreated pod.
type ownershipTakeoverCreatedResourceCompensator interface {
	DeleteCreatedForOwnershipTakeover(context.Context, PlanRelease, []byte, []byte, Observation, Observation) error
}

func DecodeExecutionPlan(reader io.Reader, releasePlan Plan, forwardManifest, lkgManifest []byte) (ExecutionPlan, error) {
	var plan ExecutionPlan
	if err := decodeStrict(reader, &plan); err != nil {
		return ExecutionPlan{}, fmt.Errorf("decode execution plan: %w", err)
	}
	if err := plan.Validate(releasePlan, forwardManifest, lkgManifest); err != nil {
		return ExecutionPlan{}, err
	}
	preparedAt, err := time.Parse(time.RFC3339Nano, plan.PreparedAt)
	if err != nil {
		return ExecutionPlan{}, errors.New("execution plan preparedAt is invalid")
	}
	now := time.Now().UTC()
	if preparedAt.After(now.Add(30*time.Second)) || now.Sub(preparedAt) > 15*time.Minute {
		return ExecutionPlan{}, errors.New("execution plan is stale or from the future")
	}
	return plan, nil
}

func DecodeExecutionResult(reader io.Reader) (ExecutionResult, error) {
	var result ExecutionResult
	if err := decodeStrict(reader, &result); err != nil {
		return ExecutionResult{}, fmt.Errorf("decode execution result: %w", err)
	}
	if result.APIVersion != ExecutionPlanAPIVersion || result.Kind != ExecutionResultKind ||
		!componentIDPattern.MatchString(result.Component) || !shaPattern.MatchString(result.ConfigSHA) ||
		!digestPattern.MatchString(result.ExecutionPlanDigest) || !digestPattern.MatchString(result.ReceiptDigest) ||
		result.ForwardApplyCount < 0 || result.ForwardApplyCount > 1 || result.LKGApplyCount < 0 || result.LKGApplyCount > 1 {
		return ExecutionResult{}, errors.New("execution result identity is invalid")
	}
	allowed := map[string]bool{"verified": true, "compensated": true, "failed-no-write": true, "recovery-required": true}
	if !allowed[result.Status] || result.Reason == "" {
		return ExecutionResult{}, errors.New("execution result status is invalid")
	}
	copy := result
	copy.ReceiptDigest = ""
	unsigned, err := CanonicalJSON(copy)
	if err != nil || result.ReceiptDigest != digestOf(unsigned) {
		return ExecutionResult{}, errors.New("execution result digest is invalid")
	}
	return result, nil
}

func PrepareExecution(ctx context.Context, cluster Cluster, releasePlan Plan, componentID string, artifact ArtifactReceipt, rendered RenderedManifests, now time.Time) (ExecutionPlan, error) {
	if cluster == nil {
		return ExecutionPlan{}, errors.New("cluster is nil")
	}
	if err := releasePlan.ValidateBound(); err != nil {
		return ExecutionPlan{}, err
	}
	release, err := releaseByID(releasePlan, componentID)
	if err != nil {
		return ExecutionPlan{}, err
	}
	if _, err := DecodeArtifactReceipt(bytes.NewReader(mustCanonical(artifact))); err != nil {
		return ExecutionPlan{}, err
	}
	if artifact.Component != componentID || artifact.ConfigSHA != releasePlan.HeadSHA ||
		artifact.PlanDigest != releasePlan.PlanDigest || artifact.IntentDigest != release.IntentDigest {
		return ExecutionPlan{}, errors.New("artifact is not bound to release plan")
	}
	if digestOf(rendered.Forward) != rendered.ForwardDigest || digestOf(rendered.LKG) != rendered.LKGDigest {
		return ExecutionPlan{}, errors.New("rendered manifest digest mismatch")
	}
	lkg := TargetIdentity{
		Present:        release.ExpectedPreviousPresent,
		ManifestDigest: rendered.LKGDigest,
	}
	if release.ExpectedPreviousPresent {
		lkg.ImageRef = artifact.Repository + "@" + release.ExpectedPreviousImageDigest
		lkg.ConfigSHA = release.ExpectedPreviousConfigSHA
		lkg.ManifestSHA = release.ExpectedPreviousManifestSHA
		lkg.OCIRevision = release.ExpectedPreviousOCIRevision
	}
	forward := TargetIdentity{
		Present:  true,
		ImageRef: artifact.ImmutableRef, ConfigSHA: releasePlan.HeadSHA,
		ManifestSHA: releasePlan.HeadSHA, OCIRevision: releasePlan.HeadSHA,
		ManifestDigest: rendered.ForwardDigest,
	}
	var prewrite Observation
	alreadyConverged := false
	degradedPredecessor := false
	if release.RetrySameLKG && release.ExpectedPreviousPresent {
		prewrite, err = cluster.Observe(ctx, release, forward, rendered.Forward)
		if err == nil && prewrite.Matches(forward, release, false) {
			prewrite, err = cluster.WaitHealthy(ctx, release, forward, rendered.Forward)
			if err == nil {
				err = cluster.Converged(ctx, release, rendered.Forward)
				alreadyConverged = err == nil
			}
		} else if release.MigrationState == "adopting" && release.OwnershipAdoption != nil && release.BootstrapLKGPath != "" {
			prewrite, err = prepareAdoptingRetryPredecessor(ctx, cluster, release, lkg, rendered.LKG)
			if errors.Is(err, ErrDegradedPredecessorHealth) {
				prewrite, err = prepareDegradedPredecessor(ctx, cluster, release, lkg, rendered.Forward, rendered.LKG)
				degradedPredecessor = err == nil
			}
		} else {
			prewrite, err = prepareDegradedPredecessor(ctx, cluster, release, lkg, rendered.Forward, rendered.LKG)
			degradedPredecessor = err == nil && !prewrite.Matches(lkg, release, true)
		}
	} else {
		var lkgObserveErr error
		// A first-install LKG has no resources by definition. Observe against
		// the forward resource schema so the CAS witness covers every declared
		// object while still requiring each one to be absent.
		lkgObservationManifest := rendered.LKG
		if !release.ExpectedPreviousPresent {
			lkgObservationManifest = rendered.Forward
		}
		prewrite, lkgObserveErr = cluster.Observe(ctx, release, lkg, lkgObservationManifest)
		lkgMatched := lkgObserveErr == nil && prewrite.Matches(lkg, release, true)
		lkgHealthVerified := false
		if release.ExpectedPreviousPresent && errors.Is(lkgObserveErr, ErrDegradedPredecessorHealth) {
			prewrite, err = prepareDegradedPredecessor(ctx, cluster, release, lkg, rendered.Forward, rendered.LKG)
			degradedPredecessor = err == nil
		} else if release.ExpectedPreviousPresent && lkgObserveErr != nil {
			var healthyLKG Observation
			healthyLKG, err = cluster.WaitHealthy(WithPrewritePredecessorHealthWait(ctx), release, lkg, rendered.LKG)
			if errors.Is(err, ErrDegradedPredecessorHealth) {
				prewrite, err = prepareDegradedPredecessor(ctx, cluster, release, lkg, rendered.Forward, rendered.LKG)
				degradedPredecessor = err == nil
			} else if err == nil && healthyLKG.Matches(lkg, release, true) {
				prewrite, err = cluster.Observe(ctx, release, lkg, rendered.LKG)
				lkgMatched = err == nil && prewrite.Matches(lkg, release, true)
				lkgHealthVerified = lkgMatched
			}
		}
		if degradedPredecessor {
			// The exact immutable predecessor is stable at the resource CAS but
			// its Pod health cannot recover without replacing the workload.
		} else if lkgMatched {
			if release.ExpectedPreviousPresent {
				if !lkgHealthVerified {
					_, err = cluster.WaitHealthy(WithPrewritePredecessorHealthWait(ctx), release, lkg, rendered.LKG)
					if errors.Is(err, ErrDegradedPredecessorHealth) {
						prewrite, err = prepareDegradedPredecessor(ctx, cluster, release, lkg, rendered.Forward, rendered.LKG)
						degradedPredecessor = err == nil
					}
				}
				if err == nil && !degradedPredecessor {
					var predecessorWitness []byte
					if release.MigrationState == "adopting" && release.OwnershipAdoption != nil && release.BootstrapLKGPath != "" {
						predecessorWitness, err = BootstrapPredecessorConvergenceManifest(rendered.LKG, release)
					} else {
						predecessorWitness, err = PredecessorConvergenceManifest(rendered.LKG)
					}
					if err == nil {
						err = cluster.Converged(ctx, release, predecessorWitness)
					}
					if err == nil {
						var freshLKG Observation
						freshLKG, err = cluster.Observe(ctx, release, lkg, rendered.LKG)
						if err == nil && freshLKG.HealthDigest != prewrite.HealthDigest {
							err = errors.New("LKG pod health changed after convergence validation")
						}
						prewrite = freshLKG
					}
				}
			} else {
				err = prewrite.ValidateMustBeStable()
			}
		} else {
			prewrite, err = cluster.WaitHealthy(ctx, release, forward, rendered.Forward)
			if err == nil && prewrite.Matches(forward, release, false) {
				err = cluster.Converged(ctx, release, rendered.Forward)
				alreadyConverged = err == nil
			}
		}
	}
	if err != nil {
		return ExecutionPlan{}, fmt.Errorf("observe prewrite state: %w", err)
	}
	if alreadyConverged {
		if err := prewrite.Validate(release); err != nil {
			return ExecutionPlan{}, fmt.Errorf("validate converged prewrite state: %w", err)
		}
	} else if degradedPredecessor {
		if err := prewrite.ValidateDegradedPredecessor(release); err != nil {
			return ExecutionPlan{}, fmt.Errorf("validate degraded LKG prewrite state: %w", err)
		}
	} else if release.ExpectedPreviousPresent {
		if err := prewrite.Validate(release); err != nil {
			return ExecutionPlan{}, fmt.Errorf("validate LKG prewrite state: %w", err)
		}
	} else if err := prewrite.ValidateMustBeStable(); err != nil {
		return ExecutionPlan{}, fmt.Errorf("validate absent prewrite state: %w", err)
	}
	if !alreadyConverged && !degradedPredecessor && !prewrite.Matches(lkg, release, true) {
		return ExecutionPlan{}, errors.New("live workload matches neither declared LKG nor immutable target")
	}
	adoption, err := bindOwnershipAdoption(release, lkg, rendered.LKG, prewrite, degradedPredecessor)
	if err != nil {
		return ExecutionPlan{}, err
	}
	if adoption != nil {
		if !adoption.AlreadyConverged && !adoption.ResumeTakeover {
			if err := cluster.DryRunOwnershipAdoption(ctx, release, *adoption, rendered.LKG); err != nil {
				return ExecutionPlan{}, fmt.Errorf("server-side dry-run ownership adoption: %w", err)
			}
		} else {
			expanded, err := cluster.ObserveCAS(ctx, release, rendered.Forward)
			if err != nil || !expanded.ExtendsResourceCAS(prewrite) {
				return ExecutionPlan{}, errors.New("already-adopted forward resource CAS is invalid")
			}
			if err := cluster.DryRunOwnershipTakeover(ctx, release, *adoption, forward, rendered.Forward); err != nil {
				return ExecutionPlan{}, fmt.Errorf("server-side dry-run reviewed ownership takeover: %w", err)
			}
			lkgDryRun, bindErr := BindManifestCAS(rendered.LKG, expanded)
			if bindErr != nil {
				return ExecutionPlan{}, bindErr
			}
			if err := cluster.DryRunApply(ctx, release, lkgDryRun); err != nil {
				return ExecutionPlan{}, fmt.Errorf("server-side dry-run already-adopted LKG: %w", err)
			}
		}
	} else {
		forwardDryRun, bindErr := BindManifestCAS(rendered.Forward, prewrite)
		if bindErr != nil {
			return ExecutionPlan{}, bindErr
		}
		if err := cluster.DryRunApply(ctx, release, forwardDryRun); err != nil {
			return ExecutionPlan{}, fmt.Errorf("server-side dry-run forward: %w", err)
		}
		if release.ExpectedPreviousPresent {
			lkgDryRun, bindErr := BindManifestCAS(rendered.LKG, prewrite)
			if bindErr != nil {
				return ExecutionPlan{}, bindErr
			}
			if err := cluster.DryRunApply(ctx, release, lkgDryRun); err != nil {
				return ExecutionPlan{}, fmt.Errorf("server-side dry-run LKG: %w", err)
			}
		}
	}
	plan := ExecutionPlan{
		APIVersion: ExecutionPlanAPIVersion, Kind: ExecutionPlanKind,
		Component: componentID, ConfigSHA: releasePlan.HeadSHA,
		ReleasePlanDigest: releasePlan.PlanDigest, IntentDigest: release.IntentDigest,
		ArtifactDigest: artifact.ReceiptDigest, Forward: forward, LKG: lkg,
		Prewrite: prewrite, AlreadyConverged: alreadyConverged, DegradedPredecessor: degradedPredecessor,
		OwnershipAdoption: adoption,
		PreparedAt:        now.UTC().Format(time.RFC3339Nano),
	}
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		return ExecutionPlan{}, err
	}
	plan.PlanDigest = digestOf(unsigned)
	return plan, nil
}

func prepareAdoptingRetryPredecessor(ctx context.Context, cluster Cluster, release PlanRelease, lkg TargetIdentity, lkgManifest []byte) (Observation, error) {
	first, err := cluster.Observe(ctx, release, lkg, lkgManifest)
	if err != nil || !first.Matches(lkg, release, true) {
		if err == nil {
			err = errors.New("adopting retry does not match the bootstrap LKG")
		}
		return Observation{}, err
	}
	if healthy, healthErr := cluster.WaitHealthy(WithPrewritePredecessorHealthWait(ctx), release, lkg, lkgManifest); healthErr != nil || !healthy.Matches(lkg, release, true) {
		if healthErr != nil {
			return Observation{}, fmt.Errorf("wait for adopting retry bootstrap LKG: %w", healthErr)
		}
		return Observation{}, fmt.Errorf("%w: adopting retry bootstrap LKG is unhealthy", ErrDegradedPredecessorHealth)
	}
	witness, err := BootstrapPredecessorConvergenceManifest(lkgManifest, release)
	if err != nil {
		return Observation{}, err
	}
	if err := cluster.Converged(ctx, release, witness); err != nil {
		return Observation{}, fmt.Errorf("adopting retry bootstrap LKG drift: %w", err)
	}
	second, err := cluster.Observe(ctx, release, lkg, lkgManifest)
	if err != nil || !second.Matches(lkg, release, true) {
		if err == nil {
			err = errors.New("adopting retry bootstrap LKG changed")
		}
		return Observation{}, err
	}
	if second.HealthDigest != first.HealthDigest {
		return Observation{}, errors.New("adopting retry bootstrap LKG health changed")
	}
	return second, nil
}

func bindOwnershipAdoption(release PlanRelease, lkg TargetIdentity, lkgManifest []byte, prewrite Observation, degraded ...bool) (*OwnershipAdoptionPlan, error) {
	if release.MigrationState != "adopting" || !release.ExpectedPreviousPresent {
		if release.OwnershipAdoption != nil {
			return nil, errors.New("ownership adoption is present outside an adopting predecessor")
		}
		return nil, nil
	}
	degradedPredecessor := len(degraded) > 0 && degraded[0]
	lkgIdentityBound := prewrite.Matches(lkg, release, true)
	initialExplicitBootstrap := degradedPredecessor && InitialExplicitBootstrapAdoption(release) &&
		prewrite.ImageRef == "" && prewrite.ConfigSHA == "" && prewrite.ManifestSHA == "" && prewrite.OCIRevision == "" &&
		prewrite.TemplateDigest == "" && prewrite.ValidateDegradedPredecessor(release) == nil
	if degradedPredecessor {
		lkgIdentityBound = initialExplicitBootstrap || (prewrite.Present && prewrite.ImageRef == lkg.ImageRef && prewrite.ConfigSHA == lkg.ConfigSHA &&
			prewrite.ManifestSHA == lkg.ManifestSHA && prewrite.OCIRevision == lkg.OCIRevision && prewrite.TemplateDigest != "")
	}
	if release.OwnershipAdoption == nil || !lkg.Present || !lkgIdentityBound {
		return nil, errors.New("ownership adoption is not bound to the exact bootstrap LKG")
	}
	legacyManager := false
	declarativeManager := false
	for _, manager := range prewrite.FieldManagers {
		legacyManager = legacyManager || manager == release.OwnershipAdoption.LegacyFieldManager
		declarativeManager = declarativeManager || manager == release.Workload.FieldManager
	}
	if initialExplicitBootstrap && !legacyManager && !declarativeManager {
		for _, resource := range prewrite.Resources {
			if resource.Identity != prewrite.Primary {
				continue
			}
			for _, manager := range resource.FieldManagers {
				legacyManager = legacyManager || manager == release.OwnershipAdoption.LegacyFieldManager
				declarativeManager = declarativeManager || manager == release.Workload.FieldManager
			}
		}
	}
	if !legacyManager && !declarativeManager {
		return nil, errors.New("ownership adoption live field manager identity is invalid")
	}
	adoptionAlreadyConverged := true
	resumeTakeover := InitialExplicitBootstrapFailedAtomSuccessor(release)
	resources := make(map[ResourceIdentity]ResourceObservation, len(prewrite.Resources))
	for _, resource := range prewrite.Resources {
		resources[resource.Identity] = resource
	}
	boundResources := make([]OwnershipAdoptionResourcePlan, 0, len(release.OwnershipAdoption.Resources))
	for _, scope := range release.OwnershipAdoption.Resources {
		resource, exists := resources[scope.Identity]
		if !exists || !resource.Present || resource.UID == "" || !resourceVersionPattern.MatchString(resource.ResourceVersion) {
			return nil, fmt.Errorf("ownership adoption resource %s/%s is not CAS-bound", scope.Identity.Kind, scope.Identity.Name)
		}
		legacyManager := false
		declarativeManager := false
		for _, manager := range resource.FieldManagers {
			legacyManager = legacyManager || manager == release.OwnershipAdoption.LegacyFieldManager
			declarativeManager = declarativeManager || manager == release.Workload.FieldManager
		}
		if !legacyManager && !declarativeManager {
			return nil, fmt.Errorf("ownership adoption resource %s/%s field manager identity is invalid", scope.Identity.Kind, scope.Identity.Name)
		}
		adoptionAlreadyConverged = adoptionAlreadyConverged && resource.ReviewedOwnershipExclusive
		resumeTakeover = resumeTakeover && resource.ReviewedOwnershipApplied
		scaffolds, err := bindOwnershipValidationScaffolds(lkgManifest, scope)
		if err != nil {
			return nil, err
		}
		boundResources = append(boundResources, OwnershipAdoptionResourcePlan{
			Identity: scope.Identity, Fields: append([]string(nil), scope.Fields...),
			ValidationScaffolds: scaffolds,
			UID:                 resource.UID, ResourceVersion: resource.ResourceVersion, Generation: resource.Generation,
		})
	}
	result := &OwnershipAdoptionPlan{
		Component: release.ComponentID, BootstrapLKGDigest: lkg.ManifestDigest,
		UID: prewrite.UID, ResourceVersion: prewrite.ResourceVersion, Generation: prewrite.Generation,
		LegacyFieldManager:  release.OwnershipAdoption.LegacyFieldManager,
		LegacyFieldManagers: append([]string(nil), release.OwnershipAdoption.legacyManagers()...),
		Resources:           boundResources,
		ImageRef:            lkg.ImageRef, ConfigSHA: lkg.ConfigSHA, ManifestSHA: lkg.ManifestSHA, OCIRevision: lkg.OCIRevision,
		AlreadyConverged: adoptionAlreadyConverged,
		ResumeTakeover:   resumeTakeover && !adoptionAlreadyConverged,
	}
	return result, nil
}

func bindOwnershipValidationScaffolds(lkgManifest []byte, scope OwnershipAdoptionScope) ([]OwnershipValidationScaffold, error) {
	pointers, err := OwnershipTakeoverValidationScaffolds(scope.Fields)
	if err != nil || len(pointers) == 0 {
		return nil, err
	}
	resource, err := ResourceSetItem(lkgManifest, scope.Identity)
	if err != nil {
		return nil, err
	}
	result := make([]OwnershipValidationScaffold, 0, len(pointers))
	for _, pointer := range pointers {
		value, err := adoptionPointerValue(resource, pointer)
		if err != nil {
			return nil, fmt.Errorf("ownership validation scaffold %s: %w", pointer, err)
		}
		image, ok := value.(string)
		if !ok || !strings.Contains(image, "@sha256:") {
			return nil, fmt.Errorf("ownership validation scaffold %s is not an immutable image", pointer)
		}
		result = append(result, OwnershipValidationScaffold{Pointer: pointer, Value: image})
	}
	return result, nil
}

func prepareDegradedPredecessor(ctx context.Context, cluster Cluster, release PlanRelease, lkg TargetIdentity, forwardManifest, lkgManifest []byte) (Observation, error) {
	initialExplicitBootstrap := InitialExplicitBootstrapAdoption(release)
	failedInitialBootstrap := InitialExplicitBootstrapFailedAtomSuccessor(release)
	if !release.ExpectedPreviousPresent || !lkg.Present ||
		(release.MigrationState == "adopting" && !release.RetrySameLKG && !initialExplicitBootstrap && !failedInitialBootstrap) {
		return Observation{}, errors.New("degraded predecessor recovery is not authorized")
	}
	if verifyErr := cluster.VerifyTarget(ctx, lkg); verifyErr != nil {
		return Observation{}, fmt.Errorf("verify degraded predecessor artifact: %w", verifyErr)
	}
	var witness []byte
	var err error
	if initialExplicitBootstrap || failedInitialBootstrap {
		witness, err = BootstrapPredecessorConvergenceManifest(lkgManifest, release)
	} else {
		witness, err = PredecessorConvergenceManifest(lkgManifest)
	}
	if err != nil {
		return Observation{}, err
	}
	first, err := cluster.ObserveCAS(ctx, release, forwardManifest)
	if err != nil {
		return Observation{}, err
	}
	if err := first.ValidateDegradedPredecessor(release); err != nil {
		return Observation{}, err
	}
	if err := cluster.Converged(ctx, release, witness); err != nil {
		return prepareOwnedDegradedPredecessor(ctx, cluster, release, forwardManifest, err)
	}
	if release.MigrationState == "adopting" && release.OwnershipAdoption != nil && !initialExplicitBootstrap {
		return prepareAdoptingDegradedLKG(ctx, cluster, release, lkg, lkgManifest, witness)
	}
	second, err := cluster.ObserveCAS(ctx, release, forwardManifest)
	if err != nil {
		return Observation{}, err
	}
	if err := second.ValidateDegradedPredecessor(release); err != nil {
		return Observation{}, err
	}
	if !second.SameSpecIdentity(first) {
		return Observation{}, errors.New("degraded predecessor identity changed during validation")
	}
	return second, nil
}

// prepareAdoptingDegradedLKG replaces the deliberately CAS-only degraded
// observation with the registry-bound immutable identity required by the
// ownership adapter. It is reached only after the exact LKG artifact and
// desired object bytes have already been verified. Two observations around a
// second LKG convergence witness prevent status movement from being confused
// with source, image, spec, or manager drift.
func prepareAdoptingDegradedLKG(ctx context.Context, cluster Cluster, release PlanRelease, lkg TargetIdentity, lkgManifest, witness []byte) (Observation, error) {
	first, err := cluster.ObserveDegraded(ctx, release, lkgManifest)
	if err != nil {
		return Observation{}, fmt.Errorf("observe adopting degraded LKG: %w", err)
	}
	if err := first.ValidateDegradedPredecessor(release); err != nil {
		return Observation{}, err
	}
	if !degradedObservationMatchesTarget(first, lkg) {
		return Observation{}, errors.New("adopting degraded predecessor is not the exact LKG")
	}
	if err := cluster.Converged(ctx, release, witness); err != nil {
		return Observation{}, fmt.Errorf("adopting degraded LKG drift: %w", err)
	}
	second, err := cluster.ObserveDegraded(ctx, release, lkgManifest)
	if err != nil {
		return Observation{}, fmt.Errorf("reobserve adopting degraded LKG: %w", err)
	}
	if err := second.ValidateDegradedPredecessor(release); err != nil {
		return Observation{}, err
	}
	if !degradedObservationMatchesTarget(second, lkg) || !second.SameSpecIdentity(first) {
		return Observation{}, errors.New("adopting degraded LKG identity changed during validation")
	}
	return second, nil
}

func degradedObservationMatchesTarget(observation Observation, target TargetIdentity) bool {
	return observation.Present == target.Present && observation.ImageRef == target.ImageRef &&
		observation.ConfigSHA == target.ConfigSHA && observation.ManifestSHA == target.ManifestSHA &&
		observation.OCIRevision == target.OCIRevision && observation.TemplateDigest != ""
}

func prepareOwnedDegradedPredecessor(ctx context.Context, cluster Cluster, release PlanRelease, forwardManifest []byte, lkgDrift error) (Observation, error) {
	witness, err := RetryPredecessorConvergenceManifest(forwardManifest, release)
	if err != nil {
		return Observation{}, err
	}
	first, err := cluster.ObserveDegraded(ctx, release, forwardManifest)
	if err != nil {
		return Observation{}, fmt.Errorf("observe owned degraded predecessor: %w", err)
	}
	if err := first.ValidateDegradedPredecessor(release); err != nil {
		return Observation{}, err
	}
	if first.ImageRef == "" {
		return Observation{}, errors.New("owned degraded predecessor identity is absent")
	}
	if err := cluster.Converged(ctx, release, witness); err != nil {
		return Observation{}, fmt.Errorf("degraded predecessor manifest drift: LKG=%v retry=%w", lkgDrift, err)
	}
	second, err := cluster.ObserveDegraded(ctx, release, forwardManifest)
	if err != nil {
		return Observation{}, err
	}
	if err := second.ValidateDegradedPredecessor(release); err != nil {
		return Observation{}, err
	}
	if !second.SameSpecIdentity(first) {
		return Observation{}, errors.New("degraded predecessor identity changed during validation")
	}
	return second, nil
}

func Execute(ctx context.Context, cluster Cluster, releasePlan Plan, prepared ExecutionPlan, forwardManifest, lkgManifest []byte) ExecutionResult {
	result := ExecutionResult{
		APIVersion: ExecutionPlanAPIVersion, Kind: ExecutionResultKind,
		Component: prepared.Component, ConfigSHA: prepared.ConfigSHA,
		ExecutionPlanDigest: prepared.PlanDigest,
		Status:              "recovery-required", Reason: "execution-not-started",
	}
	release, err := releaseByID(releasePlan, prepared.Component)
	if err != nil || prepared.Validate(releasePlan, forwardManifest, lkgManifest) != nil {
		result.Reason = "execution-plan-invalid"
		return sealResult(result)
	}
	observationManifest := forwardManifest
	if prepared.OwnershipAdoption != nil {
		observationManifest = lkgManifest
	}
	var current Observation
	if prepared.DegradedPredecessor {
		if prepared.Prewrite.ImageRef == "" {
			current, err = cluster.ObserveCAS(ctx, release, observationManifest)
		} else {
			current, err = cluster.ObserveDegraded(ctx, release, observationManifest)
		}
		if err == nil && !current.SameSpecIdentity(prepared.Prewrite) {
			err = errors.New("degraded predecessor identity changed")
		}
		if err == nil {
			var witness []byte
			if prepared.OwnershipAdoption != nil {
				witness, err = BootstrapPredecessorConvergenceManifest(lkgManifest, release)
			} else if prepared.Prewrite.ImageRef == "" {
				witness, err = PredecessorConvergenceManifest(lkgManifest)
			} else {
				witness, err = RetryPredecessorConvergenceManifest(forwardManifest, release)
			}
			if err == nil {
				err = cluster.Converged(ctx, release, witness)
			}
		}
	} else {
		current, err = cluster.ObserveCAS(ctx, release, observationManifest)
		if err == nil && !current.SameResourceCAS(prepared.Prewrite) {
			err = errors.New("prewrite CAS changed")
		}
	}
	if err != nil {
		result.Status = "failed-no-write"
		result.Reason = "prewrite-cas-drift"
		return sealResult(result)
	}
	if prepared.OwnershipAdoption != nil {
		adopted := prepared.Prewrite
		if !prepared.OwnershipAdoption.AlreadyConverged && !prepared.OwnershipAdoption.ResumeTakeover {
			var adoptionErr error
			adopted, adoptionErr = cluster.AdoptOwnership(ctx, release, *prepared.OwnershipAdoption, prepared.LKG, lkgManifest)
			if adoptionErr != nil || !ownershipAdoptionConverged(prepared.Prewrite, adopted, release.Workload.FieldManager, *prepared.OwnershipAdoption) {
				result.Status = "recovery-required"
				result.Reason = "ownership-adoption-unknown"
				result.Final = adopted
				return sealResult(result)
			}
		} else {
			var takeoverErr error
			adopted, takeoverErr = cluster.TakeoverOwnership(ctx, release, *prepared.OwnershipAdoption, prepared.Forward, forwardManifest)
			if takeoverErr != nil || !adopted.CompletesOwnershipTakeover(prepared.Prewrite, release.Workload.FieldManager, *prepared.OwnershipAdoption) {
				return compensateOwnershipTakeover(ctx, cluster, release, prepared, forwardManifest, lkgManifest, result, adopted)
			}
		}
		current = adopted
		if !prepared.OwnershipAdoption.AlreadyConverged && !prepared.OwnershipAdoption.ResumeTakeover {
			expanded, expandErr := cluster.ObserveCAS(ctx, release, forwardManifest)
			if expandErr != nil || !expanded.ExtendsResourceCAS(adopted) {
				result.Status = "recovery-required"
				result.Reason = "post-adoption-forward-cas-drift"
				result.Final = adopted
				return sealResult(result)
			}
			current = expanded
		}
	}
	if prepared.AlreadyConverged {
		forwardObservation, healthErr := cluster.WaitHealthy(ctx, release, prepared.Forward, forwardManifest)
		convergedErr := cluster.Converged(ctx, release, forwardManifest)
		if healthErr == nil && convergedErr == nil && forwardObservation.Matches(prepared.Forward, release, false) {
			result.Status = "verified"
			result.Reason = "forward-already-converged"
			result.Final = forwardObservation
			return sealResult(result)
		}
		result.Reason = "already-converged-evidence-drift"
		return sealResult(result)
	}
	forwardCAS, err := BindManifestCAS(forwardManifest, current)
	if err != nil {
		result.Reason = "forward-cas-manifest-invalid"
		return sealResult(result)
	}
	if prepared.OwnershipAdoption != nil {
		if err := cluster.DryRunApply(ctx, release, forwardCAS); err != nil {
			result.Status = "recovery-required"
			result.Reason = "post-adoption-forward-dry-run-rejected"
			result.Final = current
			return sealResult(result)
		}
	}
	result.ForwardApplyCount = 1
	applyErr := cluster.Apply(ctx, release, prepared.Forward, forwardCAS)
	forwardObservation, healthErr := cluster.WaitHealthy(ctx, release, prepared.Forward, forwardManifest)
	convergedErr := cluster.Converged(ctx, release, forwardManifest)
	if healthErr == nil && convergedErr == nil && forwardObservation.Matches(prepared.Forward, release, false) {
		result.Status = "verified"
		if applyErr != nil {
			result.Reason = "forward-commit-unknown-reconciled"
		} else {
			result.Reason = "forward-verified"
		}
		result.Final = forwardObservation
		return sealResult(result)
	}
	if applyErr != nil {
		var observed Observation
		var observeErr error
		unchanged := false
		if prepared.DegradedPredecessor {
			if prepared.Prewrite.ImageRef == "" {
				observed, observeErr = cluster.ObserveCAS(ctx, release, observationManifest)
			} else {
				observed, observeErr = cluster.ObserveDegraded(ctx, release, observationManifest)
			}
			unchanged = observeErr == nil && observed.SameSpecIdentity(prepared.Prewrite)
		} else if prepared.OwnershipAdoption == nil {
			observed, observeErr = cluster.Observe(ctx, release, prepared.LKG, observationManifest)
			unchanged = observeErr == nil && observed.SameCAS(prepared.Prewrite)
		}
		if unchanged {
			result.Status = "failed-no-write"
			result.Reason = "forward-apply-rejected-before-commit"
			result.Final = observed
			return sealResult(result)
		}
	}
	rollbackBase := forwardObservation
	result.FailureClass = forwardFailureClass(applyErr, healthErr, convergedErr, forwardObservation, prepared.Forward, release)
	result.FailureDetail = forwardFailureDetail(healthErr, convergedErr)
	if !rollbackBase.Present || rollbackBase.UID == "" || !resourceVersionPattern.MatchString(rollbackBase.ResourceVersion) {
		rollbackBase, err = cluster.ObserveCAS(ctx, release, forwardManifest)
		if err != nil {
			result.Reason = "rollback-prewrite-observation-failed"
			return sealResult(result)
		}
	}
	result.LKGApplyCount = 1
	var rollbackErr error
	if prepared.LKG.Present {
		lkgCAS, bindErr := BindManifestCAS(lkgManifest, rollbackBase)
		if bindErr != nil {
			result.Reason = "rollback-cas-manifest-invalid"
			return sealResult(result)
		}
		rollbackErr = cluster.Apply(ctx, release, prepared.LKG, lkgCAS)
		createdDeleteErr := cluster.DeleteCreated(ctx, release, forwardManifest, prepared.Prewrite, rollbackBase)
		rollbackErr = errors.Join(rollbackErr, createdDeleteErr)
	} else {
		rollbackErr = cluster.Delete(ctx, release, forwardManifest, rollbackBase)
	}
	var lkgObservation Observation
	var lkgHealthErr, lkgConvergedErr error
	if prepared.LKG.Present {
		lkgObservation, lkgHealthErr = cluster.WaitHealthy(ctx, release, prepared.LKG, lkgManifest)
		lkgConvergedErr = cluster.Converged(ctx, release, lkgManifest)
	} else {
		lkgObservation, lkgHealthErr = cluster.Observe(ctx, release, prepared.LKG, forwardManifest)
	}
	if lkgHealthErr == nil && lkgConvergedErr == nil && lkgObservation.Matches(prepared.LKG, release, true) {
		result.Status = "compensated"
		if rollbackErr != nil {
			result.Reason = "lkg-commit-unknown-reconciled"
		} else {
			result.Reason = "forward-unhealthy-lkg-restored"
		}
		result.Final = lkgObservation
		return sealResult(result)
	}
	result.Reason = "lkg-unproven"
	result.Final = lkgObservation
	return sealResult(result)
}

func forwardFailureDetail(healthErr, convergedErr error) string {
	var detail string
	if healthErr != nil {
		detail = healthErr.Error()
	} else if convergedErr != nil {
		detail = convergedErr.Error()
	}
	detail = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, detail)
	if len(detail) > 512 {
		detail = detail[:512]
	}
	return detail
}

func forwardFailureClass(applyErr, healthErr, convergedErr error, observed Observation, target TargetIdentity, release PlanRelease) string {
	switch {
	case applyErr != nil:
		return "forward_apply"
	case healthErr != nil:
		return "forward_health"
	case convergedErr != nil:
		return "forward_convergence"
	case !observed.Matches(target, release, false):
		return "forward_identity"
	default:
		return "forward_unknown"
	}
}

func compensateOwnershipTakeover(ctx context.Context, cluster Cluster, release PlanRelease, prepared ExecutionPlan, forwardManifest, lkgManifest []byte, result ExecutionResult, observed Observation) ExecutionResult {
	if !observed.Present || observed.UID == "" || !resourceVersionPattern.MatchString(observed.ResourceVersion) {
		var observeErr error
		observed, observeErr = cluster.ObserveCAS(ctx, release, forwardManifest)
		if observeErr != nil {
			result.Status = "recovery-required"
			result.Reason = "ownership-takeover-observation-failed"
			return sealResult(result)
		}
	}
	if observed.SameResourceCAS(prepared.Prewrite) {
		result.Status = "failed-no-write"
		result.Reason = "ownership-takeover-rejected-before-commit"
		result.Final = observed
		return sealResult(result)
	}
	lkgCAS, err := BindManifestCAS(lkgManifest, observed)
	if err != nil {
		result.Status = "recovery-required"
		result.Reason = "ownership-takeover-lkg-cas-invalid"
		result.Final = observed
		return sealResult(result)
	}
	result.LKGApplyCount = 1
	applyErr := cluster.Apply(ctx, release, prepared.LKG, lkgCAS)
	clearErr := error(nil)
	if compensator, ok := cluster.(ownershipTakeoverCompensator); ok {
		clearErr = compensator.ClearOwnershipTakeoverForwardOnlyFields(ctx, release, forwardManifest, lkgManifest, observed)
	} else {
		clearErr = errors.New("ownership takeover compensation adapter is unavailable")
	}
	deleteErr := error(nil)
	if compensator, ok := cluster.(ownershipTakeoverCreatedResourceCompensator); ok {
		deleteErr = compensator.DeleteCreatedForOwnershipTakeover(ctx, release, forwardManifest, lkgManifest, prepared.Prewrite, observed)
	} else {
		deleteErr = cluster.DeleteCreated(ctx, release, forwardManifest, prepared.Prewrite, observed)
	}
	lkgObservation, healthErr := cluster.WaitHealthy(ctx, release, prepared.LKG, lkgManifest)
	convergedErr := cluster.Converged(ctx, release, lkgManifest)
	if healthErr == nil && convergedErr == nil && clearErr == nil && lkgObservation.Matches(prepared.LKG, release, true) {
		result.Status = "compensated"
		if applyErr != nil || deleteErr != nil {
			result.Reason = "ownership-takeover-lkg-commit-unknown-reconciled"
		} else {
			result.Reason = "ownership-takeover-lkg-restored"
		}
		result.Final = lkgObservation
		return sealResult(result)
	}
	result.Status = "recovery-required"
	result.Reason = "ownership-takeover-lkg-unproven"
	if clearErr != nil {
		result.Reason = "ownership-takeover-lkg-fields-unproven"
	}
	result.Final = lkgObservation
	return sealResult(result)
}

func ownershipAdoptionConverged(before, after Observation, manager string, plan OwnershipAdoptionPlan) bool {
	if before.Present != after.Present || before.Primary != after.Primary || before.UID != after.UID || after.Generation < before.Generation ||
		after.ImageRef != plan.ImageRef || after.ConfigSHA != plan.ConfigSHA || after.ManifestSHA != plan.ManifestSHA ||
		after.OCIRevision != plan.OCIRevision || !receiptContainsString(after.FieldManagers, manager) ||
		len(after.Resources) < len(before.Resources) {
		return false
	}
	beforeResources := make(map[ResourceIdentity]ResourceObservation, len(before.Resources))
	afterResources := make(map[ResourceIdentity]ResourceObservation, len(after.Resources))
	for _, resource := range before.Resources {
		beforeResources[resource.Identity] = resource
	}
	for _, resource := range after.Resources {
		afterResources[resource.Identity] = resource
	}
	for identity, resource := range afterResources {
		if _, exists := beforeResources[identity]; !exists && (!isForwardDependencyResource(identity) || !resource.Present) {
			return false
		}
	}
	for identity, prior := range beforeResources {
		current, exists := afterResources[identity]
		if !exists || prior.Present != current.Present || prior.UID != current.UID || current.Generation < prior.Generation ||
			prior.RetainOnRollback != current.RetainOnRollback {
			return false
		}
	}
	for _, scope := range plan.Resources {
		if !receiptContainsString(afterResources[scope.Identity].FieldManagers, manager) {
			return false
		}
	}
	return true
}

// ReconcileExecution is the read-only terminal path used when the executor
// process itself fails. It never invokes Apply/Delete and can only verify the
// immutable forward target or report that LKG/partial state still requires a
// failed terminal result.
func ReconcileExecution(ctx context.Context, cluster Cluster, releasePlan Plan, prepared ExecutionPlan, forwardManifest, lkgManifest []byte) ExecutionResult {
	result := ExecutionResult{APIVersion: ExecutionPlanAPIVersion, Kind: ExecutionResultKind, Component: prepared.Component, ConfigSHA: prepared.ConfigSHA,
		ExecutionPlanDigest: prepared.PlanDigest, Status: "recovery-required", Reason: "terminal-reconcile-unproven"}
	release, err := releaseByID(releasePlan, prepared.Component)
	if err != nil || prepared.Validate(releasePlan, forwardManifest, lkgManifest) != nil {
		result.Reason = "execution-plan-invalid"
		return sealResult(result)
	}
	forwardObserved, forwardObserveErr := cluster.Observe(ctx, release, prepared.Forward, forwardManifest)
	if forwardObserveErr == nil && forwardObserved.Matches(prepared.Forward, release, false) {
		forward, forwardHealthErr := cluster.WaitHealthy(ctx, release, prepared.Forward, forwardManifest)
		forwardConvergedErr := cluster.Converged(ctx, release, forwardManifest)
		if forwardHealthErr == nil && forwardConvergedErr == nil && forward.Matches(prepared.Forward, release, false) {
			result.Status = "verified"
			result.Reason = "forward-reconciled-after-executor-failure"
			result.Final = forward
			return sealResult(result)
		}
	}
	current, observeErr := cluster.ObserveCAS(ctx, release, forwardManifest)
	if observeErr != nil {
		result.Reason = "terminal-reconcile-observation-failed"
		return sealResult(result)
	}
	if prepared.LKG.Present {
		lkg, lkgHealthErr := cluster.WaitHealthy(ctx, release, prepared.LKG, lkgManifest)
		lkgConvergedErr := cluster.Converged(ctx, release, lkgManifest)
		if lkgHealthErr == nil && lkgConvergedErr == nil && lkg.Matches(prepared.LKG, release, true) && !forwardOnlyResourcesRemain(prepared.Prewrite, current) {
			result.Status = "compensated"
			result.Reason = "executor-failure-lkg-observed"
			if current.SameCAS(prepared.Prewrite) {
				result.Status = "failed-no-write"
				result.Reason = "executor-failure-no-write-observed"
			}
			result.Final = lkg
			return sealResult(result)
		}
	} else if current.Matches(prepared.LKG, release, true) {
		result.Status = "compensated"
		result.Reason = "executor-failure-absent-lkg-observed"
		if current.SameCAS(prepared.Prewrite) {
			result.Status = "failed-no-write"
			result.Reason = "executor-failure-no-write-observed"
		}
		result.Final = current
		return sealResult(result)
	}
	result.Final = current
	return sealResult(result)
}

func forwardOnlyResourcesRemain(before, current Observation) bool {
	currentByIdentity := make(map[ResourceIdentity]ResourceObservation, len(current.Resources))
	for _, resource := range current.Resources {
		currentByIdentity[resource.Identity] = resource
	}
	for _, prior := range before.Resources {
		observed, exists := currentByIdentity[prior.Identity]
		if !exists || (!prior.Present && observed.Present && !observed.RetainOnRollback) {
			return true
		}
	}
	return false
}

func (plan ExecutionPlan) Validate(releasePlan Plan, forwardManifest, lkgManifest []byte) error {
	if plan.APIVersion != ExecutionPlanAPIVersion || plan.Kind != ExecutionPlanKind ||
		!componentIDPattern.MatchString(plan.Component) || plan.ConfigSHA != releasePlan.HeadSHA ||
		plan.ReleasePlanDigest != releasePlan.PlanDigest || !digestPattern.MatchString(plan.IntentDigest) ||
		!digestPattern.MatchString(plan.ArtifactDigest) || !digestPattern.MatchString(plan.PlanDigest) ||
		plan.Forward.ManifestDigest != digestOf(forwardManifest) || plan.LKG.ManifestDigest != digestOf(lkgManifest) {
		return errors.New("execution plan identity is invalid")
	}
	copy := plan
	copy.PlanDigest = ""
	unsigned, err := CanonicalJSON(copy)
	if err != nil || plan.PlanDigest != digestOf(unsigned) {
		return errors.New("execution plan digest is invalid")
	}
	release, err := releaseByID(releasePlan, plan.Component)
	if err != nil {
		return err
	}
	if !plan.Forward.Present || plan.Forward.ConfigSHA != releasePlan.HeadSHA ||
		plan.Forward.ManifestSHA != releasePlan.HeadSHA || plan.Forward.OCIRevision != releasePlan.HeadSHA ||
		!strings.HasPrefix(plan.Forward.ImageRef, release.Artifact.Repository+"@sha256:") {
		return errors.New("execution forward identity is invalid")
	}
	if plan.LKG.Present != release.ExpectedPreviousPresent {
		return errors.New("execution LKG presence is invalid")
	}
	if release.ExpectedPreviousPresent {
		if plan.LKG.ImageRef != release.Artifact.Repository+"@"+release.ExpectedPreviousImageDigest ||
			plan.LKG.ConfigSHA != release.ExpectedPreviousConfigSHA || plan.LKG.ManifestSHA != release.ExpectedPreviousManifestSHA ||
			plan.LKG.OCIRevision != release.ExpectedPreviousOCIRevision {
			return errors.New("execution LKG identity is invalid")
		}
	} else if plan.LKG.ImageRef != "" || plan.LKG.ConfigSHA != "" || plan.LKG.ManifestSHA != "" || plan.LKG.OCIRevision != "" {
		return errors.New("absent execution LKG carries runtime identity")
	}
	if err := plan.validateOwnershipAdoption(release); err != nil {
		return err
	}
	if plan.DegradedPredecessor {
		failedAtomSuccessor := release.MigrationState == "independent" && shaPattern.MatchString(release.SupersedesFailedConfigSHA)
		failedInitialBootstrap := InitialExplicitBootstrapFailedAtomSuccessor(release) && plan.OwnershipAdoption != nil && plan.OwnershipAdoption.ResumeTakeover
		initialExplicitBootstrap := InitialExplicitBootstrapAdoption(release) && plan.OwnershipAdoption != nil
		if !release.ExpectedPreviousPresent || plan.AlreadyConverged ||
			(!release.RetrySameLKG && !failedAtomSuccessor && !initialExplicitBootstrap && !failedInitialBootstrap) {
			return errors.New("degraded predecessor execution is not authorized")
		}
		if err := plan.Prewrite.ValidateDegradedPredecessor(release); err != nil {
			return err
		}
	} else if err := plan.Prewrite.ValidateMustBeStable(); err != nil {
		return err
	}
	if plan.AlreadyConverged {
		if !plan.Prewrite.Matches(plan.Forward, release, false) {
			return errors.New("already-converged execution plan is not bound to the forward target")
		}
	} else if !plan.DegradedPredecessor && !plan.Prewrite.Matches(plan.LKG, release, true) {
		return errors.New("execution plan prewrite is not bound to the LKG")
	}
	return nil
}

func (plan ExecutionPlan) validateOwnershipAdoption(release PlanRelease) error {
	required := release.MigrationState == "adopting" && release.ExpectedPreviousPresent
	if !required {
		if plan.OwnershipAdoption != nil || release.OwnershipAdoption != nil {
			return errors.New("execution retained ownership adoption outside an adopting predecessor")
		}
		return nil
	}
	adoption := plan.OwnershipAdoption
	if adoption == nil || release.OwnershipAdoption == nil || adoption.Component != release.ComponentID ||
		adoption.BootstrapLKGDigest != plan.LKG.ManifestDigest || adoption.UID != plan.Prewrite.UID ||
		adoption.ResourceVersion != plan.Prewrite.ResourceVersion || adoption.Generation != plan.Prewrite.Generation ||
		adoption.LegacyFieldManager != release.OwnershipAdoption.LegacyFieldManager ||
		!equalStrings(adoption.LegacyFieldManagers, release.OwnershipAdoption.legacyManagers()) ||
		adoption.ImageRef != plan.LKG.ImageRef || adoption.ConfigSHA != plan.LKG.ConfigSHA ||
		adoption.ManifestSHA != plan.LKG.ManifestSHA || adoption.OCIRevision != plan.LKG.OCIRevision {
		return errors.New("execution ownership adoption identity is invalid")
	}
	resources := make(map[ResourceIdentity]ResourceObservation, len(plan.Prewrite.Resources))
	for _, resource := range plan.Prewrite.Resources {
		resources[resource.Identity] = resource
	}
	scopes := make([]OwnershipAdoptionScope, 0, len(adoption.Resources))
	allReviewedApplied := true
	allReviewedExclusive := true
	for _, resource := range adoption.Resources {
		prewrite, exists := resources[resource.Identity]
		if !exists || resource.UID != prewrite.UID || resource.ResourceVersion != prewrite.ResourceVersion || resource.Generation != prewrite.Generation {
			return errors.New("execution ownership adoption resource CAS is invalid")
		}
		if _, err := validateOwnershipValidationScaffolds(resource); err != nil {
			return errors.New("execution ownership adoption validation scaffold is invalid")
		}
		allReviewedApplied = allReviewedApplied && prewrite.ReviewedOwnershipApplied
		allReviewedExclusive = allReviewedExclusive && prewrite.ReviewedOwnershipExclusive
		scopes = append(scopes, OwnershipAdoptionScope{Identity: resource.Identity, Fields: resource.Fields})
	}
	wantResume := InitialExplicitBootstrapFailedAtomSuccessor(release) && allReviewedApplied && !allReviewedExclusive
	if adoption.AlreadyConverged != allReviewedExclusive || adoption.ResumeTakeover != wantResume ||
		adoption.AlreadyConverged && adoption.ResumeTakeover {
		return errors.New("execution ownership adoption convergence evidence is invalid")
	}
	want, err := CanonicalJSON(release.OwnershipAdoption.Resources)
	if err != nil {
		return err
	}
	got, err := CanonicalJSON(scopes)
	if err != nil || !bytes.Equal(got, want) {
		return errors.New("execution ownership adoption scope is invalid")
	}
	return nil
}

func (observation Observation) ValidateDegradedPredecessor(release PlanRelease) error {
	if !observation.Present || observation.UID == "" || !resourceVersionPattern.MatchString(observation.ResourceVersion) || observation.Generation < 1 {
		return errors.New("degraded predecessor CAS is invalid")
	}
	if observation.ObservedGeneration != 0 || observation.Desired != 0 || observation.Updated != 0 || observation.Ready != 0 ||
		observation.Available != 0 || observation.Unavailable != 0 || observation.ImageID != "" || observation.HealthDigest != "" {
		return errors.New("degraded predecessor observation carries unverified health state")
	}
	owned := observation.ImageRef != "" || observation.ConfigSHA != "" || observation.ManifestSHA != "" ||
		observation.OCIRevision != "" || observation.TemplateDigest != "" || len(observation.FieldManagers) != 0
	if owned {
		separator := strings.LastIndex(observation.ImageRef, "@")
		if separator < 1 || !digestPattern.MatchString(observation.ImageRef[separator+1:]) ||
			!shaPattern.MatchString(observation.ConfigSHA) || observation.ManifestSHA != observation.ConfigSHA ||
			observation.OCIRevision != observation.ConfigSHA || !digestPattern.MatchString(observation.TemplateDigest) ||
			!sort.StringsAreSorted(observation.FieldManagers) {
			return errors.New("owned degraded predecessor identity is invalid")
		}
		ownedByComponent := false
		for _, manager := range observation.FieldManagers {
			if manager == release.Workload.FieldManager {
				ownedByComponent = true
				break
			}
		}
		if !ownedByComponent {
			return errors.New("owned degraded predecessor field manager is invalid")
		}
	}
	return observation.validateResourceCAS()
}

func (observation Observation) Validate(release PlanRelease) error {
	if !observation.Present || observation.UID == "" || !resourceVersionPattern.MatchString(observation.ResourceVersion) ||
		observation.Generation < 1 || observation.ObservedGeneration != observation.Generation ||
		observation.Desired < 1 || !digestPattern.MatchString(observation.TemplateDigest) ||
		!digestPattern.MatchString(observation.HealthDigest) || !digestPattern.MatchString(observation.ImageID) {
		return errors.New("workload observation is not stable and healthy")
	}
	if int(observation.Desired) != release.Workload.Replicas && release.Workload.Kind == "Deployment" {
		return errors.New("workload observation replica count mismatch")
	}
	activeDesired := observation.Desired - int32(release.Workload.PreservedUnavailable)
	if activeDesired < 1 || observation.Updated != activeDesired || observation.Ready != activeDesired ||
		observation.Available != activeDesired || observation.Unavailable != int32(release.Workload.PreservedUnavailable) {
		return errors.New("workload observation active cohort mismatch")
	}
	if !shaPattern.MatchString(observation.ConfigSHA) || !shaPattern.MatchString(observation.ManifestSHA) ||
		!shaPattern.MatchString(observation.OCIRevision) {
		return errors.New("workload observation source identity is invalid")
	}
	if len(observation.FieldManagers) == 0 || !sort.StringsAreSorted(observation.FieldManagers) {
		return errors.New("workload observation field managers are invalid")
	}
	return observation.validateResourceCAS()
}

func (observation Observation) ValidateMustBeStable() error {
	if observation.Present {
		if observation.UID == "" || !resourceVersionPattern.MatchString(observation.ResourceVersion) ||
			observation.Generation < 1 || !digestPattern.MatchString(observation.TemplateDigest) {
			return errors.New("prewrite observation CAS is invalid")
		}
	} else if observation.UID != "" || observation.ResourceVersion != "" || observation.Generation != 0 ||
		observation.TemplateDigest != "" || observation.ImageRef != "" || observation.ImageID != "" ||
		observation.ConfigSHA != "" || observation.ManifestSHA != "" || observation.OCIRevision != "" {
		return errors.New("absent prewrite observation carries workload state")
	}
	return observation.validateResourceCAS()
}

func (observation Observation) Matches(target TargetIdentity, release PlanRelease, allowLegacyManager bool) bool {
	if observation.Present != target.Present {
		return false
	}
	if !target.Present {
		for _, resource := range observation.Resources {
			if resource.Present && !resource.RetainOnRollback {
				return false
			}
		}
		return true
	}
	imageMatches := observation.ImageRef == target.ImageRef
	if !imageMatches && allowLegacyManager {
		separator := strings.LastIndex(target.ImageRef, "@")
		imageMatches = separator > 0 && observation.ImageID == target.ImageRef[separator+1:]
	}
	activeDesired := observation.Desired - int32(release.Workload.PreservedUnavailable)
	if !imageMatches || observation.ConfigSHA != target.ConfigSHA ||
		observation.ManifestSHA != target.ManifestSHA || observation.OCIRevision != target.OCIRevision ||
		observation.ObservedGeneration != observation.Generation || activeDesired < 1 || observation.Updated != activeDesired ||
		observation.Ready != activeDesired || observation.Available != activeDesired ||
		observation.Unavailable != int32(release.Workload.PreservedUnavailable) {
		return false
	}
	for _, manager := range observation.FieldManagers {
		if manager == release.Workload.FieldManager {
			return true
		}
	}
	return allowLegacyManager
}

func (observation Observation) SameCAS(other Observation) bool {
	if !observation.SameResourceCAS(other) ||
		observation.TemplateDigest != other.TemplateDigest || observation.ImageRef != other.ImageRef ||
		observation.ConfigSHA != other.ConfigSHA || observation.ManifestSHA != other.ManifestSHA ||
		observation.OCIRevision != other.OCIRevision {
		return false
	}
	return true
}

// SameResourceCAS compares only Kubernetes object identity, desired bytes, and
// managed-field ownership. Prepare has already verified image provenance and
// workload health; execute uses this bounded recapture to avoid repeating
// network and Pod-health observations before the first mutation.
func (observation Observation) SameResourceCAS(other Observation) bool {
	if observation.Present != other.Present || observation.Primary != other.Primary ||
		observation.UID != other.UID || observation.ResourceVersion != other.ResourceVersion ||
		observation.Generation != other.Generation ||
		len(observation.Resources) != len(other.Resources) {
		return false
	}
	for index := range observation.Resources {
		left, right := observation.Resources[index], other.Resources[index]
		if left.Identity != right.Identity || left.Present != right.Present || left.UID != right.UID ||
			left.RetainOnRollback != right.RetainOnRollback || left.ResourceVersion != right.ResourceVersion || left.Generation != right.Generation ||
			left.ObjectDigest != right.ObjectDigest || !equalStrings(left.FieldManagers, right.FieldManagers) ||
			left.ReviewedOwnershipApplied != right.ReviewedOwnershipApplied || left.ReviewedOwnershipExclusive != right.ReviewedOwnershipExclusive {
			return false
		}
	}
	return true
}

// ExtendsResourceCAS permits a forward manifest to add only resources that are
// still absent after ownership adoption. Every bootstrap resource must retain
// its exact post-adoption UID/RV/generation/object/manager identity.
func (observation Observation) ExtendsResourceCAS(base Observation) bool {
	if observation.Present != base.Present || observation.Primary != base.Primary ||
		observation.UID != base.UID || observation.ResourceVersion != base.ResourceVersion ||
		observation.Generation != base.Generation || len(observation.Resources) < len(base.Resources) {
		return false
	}
	baseResources := make(map[ResourceIdentity]ResourceObservation, len(base.Resources))
	for _, resource := range base.Resources {
		baseResources[resource.Identity] = resource
	}
	for _, current := range observation.Resources {
		prior, exists := baseResources[current.Identity]
		if !exists {
			if current.Present && !isForwardDependencyResource(current.Identity) {
				return false
			}
			continue
		}
		if current.Present != prior.Present || current.UID != prior.UID || current.ResourceVersion != prior.ResourceVersion ||
			current.Generation != prior.Generation || current.ObjectDigest != prior.ObjectDigest ||
			current.RetainOnRollback != prior.RetainOnRollback || !equalStrings(current.FieldManagers, prior.FieldManagers) ||
			current.ReviewedOwnershipApplied != prior.ReviewedOwnershipApplied || current.ReviewedOwnershipExclusive != prior.ReviewedOwnershipExclusive {
			return false
		}
		delete(baseResources, current.Identity)
	}
	return len(baseResources) == 0
}

// CompletesOwnershipTakeover accepts the exact UID-preserving CAS movement
// caused by applying reviewed forward fields with the declarative manager.
// The adapter separately proves those desired bytes match the immutable
// target; this predicate only permits generation/RV movement on scoped
// resources and absent forward-only resources.
func (observation Observation) CompletesOwnershipTakeover(base Observation, manager string, plan OwnershipAdoptionPlan) bool {
	if !observation.Present || observation.Primary != base.Primary || observation.UID != base.UID ||
		observation.Generation < base.Generation || !resourceVersionPattern.MatchString(observation.ResourceVersion) ||
		len(observation.Resources) < len(base.Resources) || manager == "" {
		return false
	}
	baseResources := make(map[ResourceIdentity]ResourceObservation, len(base.Resources))
	for _, resource := range base.Resources {
		baseResources[resource.Identity] = resource
	}
	currentResources := make(map[ResourceIdentity]ResourceObservation, len(observation.Resources))
	for _, current := range observation.Resources {
		currentResources[current.Identity] = current
		prior, exists := baseResources[current.Identity]
		if !exists {
			if current.Present && !isForwardDependencyResource(current.Identity) {
				return false
			}
			continue
		}
		if current.Present != prior.Present || current.UID != prior.UID || current.Generation < prior.Generation ||
			current.RetainOnRollback != prior.RetainOnRollback || !resourceVersionPattern.MatchString(current.ResourceVersion) {
			return false
		}
		delete(baseResources, current.Identity)
	}
	if len(baseResources) != 0 {
		return false
	}
	for _, scope := range plan.Resources {
		current, exists := currentResources[scope.Identity]
		if !exists || !receiptContainsString(current.FieldManagers, manager) || !current.ReviewedOwnershipApplied || !current.ReviewedOwnershipExclusive {
			return false
		}
	}
	return true
}

// isForwardDependencyResource is intentionally narrow: an adopting workload
// may materialize its explicit ServiceAccount even when a historical LKG
// predates that dependency. No other new live resource is accepted by the
// ownership CAS predicates.
func isForwardDependencyResource(identity ResourceIdentity) bool {
	return identity.APIVersion == "v1" && identity.Kind == "ServiceAccount"
}

// SameSpecIdentity permits status-only resourceVersion movement while binding
// an unhealthy predecessor to the same UID, generation, desired object bytes,
// and managed-field ownership. The caller still uses the newest RV as the
// immediate server-side-apply CAS precondition.
func (observation Observation) SameSpecIdentity(other Observation) bool {
	if observation.Present != other.Present || observation.Primary != other.Primary || observation.UID != other.UID ||
		observation.Generation != other.Generation || observation.ImageRef != other.ImageRef ||
		observation.ConfigSHA != other.ConfigSHA || observation.ManifestSHA != other.ManifestSHA ||
		observation.OCIRevision != other.OCIRevision || observation.TemplateDigest != other.TemplateDigest ||
		!equalStrings(observation.FieldManagers, other.FieldManagers) || len(observation.Resources) != len(other.Resources) {
		return false
	}
	for index := range observation.Resources {
		left, right := observation.Resources[index], other.Resources[index]
		if left.Identity != right.Identity || left.Present != right.Present || left.UID != right.UID ||
			left.RetainOnRollback != right.RetainOnRollback || left.Generation != right.Generation ||
			left.ObjectDigest != right.ObjectDigest || !equalStrings(left.FieldManagers, right.FieldManagers) ||
			left.ReviewedOwnershipApplied != right.ReviewedOwnershipApplied || left.ReviewedOwnershipExclusive != right.ReviewedOwnershipExclusive {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
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

func (observation Observation) validateResourceCAS() error {
	if len(observation.Resources) == 0 || len(observation.Resources) > 32 {
		return errors.New("component resource observations are invalid")
	}
	previous := ""
	primaryFound := false
	for _, resource := range observation.Resources {
		key := resource.Identity.key()
		if key == "" || (previous != "" && previous >= key) {
			return errors.New("component resource observations are not strictly ordered")
		}
		previous = key
		if resource.Present {
			if resource.UID == "" || !resourceVersionPattern.MatchString(resource.ResourceVersion) ||
				resource.Generation < 0 || !digestPattern.MatchString(resource.ObjectDigest) ||
				!sort.StringsAreSorted(resource.FieldManagers) {
				return errors.New("component resource observation CAS is invalid")
			}
		} else if resource.UID != "" || resource.ResourceVersion != "" || resource.Generation != 0 ||
			resource.ObjectDigest != "" || len(resource.FieldManagers) != 0 {
			return errors.New("absent component resource carries observed state")
		}
		if resource.Identity == observation.Primary && resource.Present == observation.Present &&
			(!observation.Present || (resource.UID == observation.UID && resource.ResourceVersion == observation.ResourceVersion)) {
			primaryFound = true
		}
	}
	if !primaryFound {
		return errors.New("primary workload is missing from component resource observations")
	}
	return nil
}

func releaseByID(plan Plan, componentID string) (PlanRelease, error) {
	for _, release := range plan.Releases {
		if release.ComponentID == componentID {
			return release, nil
		}
	}
	return PlanRelease{}, fmt.Errorf("component %q is not in release plan", componentID)
}

// BindManifestCAS adds Kubernetes UID/resourceVersion preconditions immediately
// before a server-side apply. It never changes desired spec content.
func BindManifestCAS(manifest []byte, observation Observation) ([]byte, error) {
	if observation.validateResourceCAS() != nil {
		return nil, errors.New("manifest CAS observation is invalid")
	}
	set, err := DecodeResourceSet(bytes.NewReader(manifest))
	if err != nil {
		return nil, err
	}
	observed := make(map[string]ResourceObservation, len(observation.Resources))
	for _, resource := range observation.Resources {
		observed[resource.Identity.key()] = resource
	}
	for _, item := range set.Items {
		identity, identityErr := resourceIdentity(item)
		if identityErr != nil {
			return nil, identityErr
		}
		resource, exists := observed[identity.key()]
		if !exists {
			return nil, errors.New("manifest resource is missing from CAS observation")
		}
		if !resource.Present {
			continue
		}
		metadata, metadataErr := objectField(item, "metadata")
		if metadataErr != nil {
			return nil, metadataErr
		}
		metadata["uid"] = resource.UID
		metadata["resourceVersion"] = resource.ResourceVersion
	}
	return CanonicalJSON(set)
}

func digestOf(value []byte) string {
	digest := sha256.Sum256(value)
	return fmt.Sprintf("sha256:%x", digest)
}

func sealResult(result ExecutionResult) ExecutionResult {
	copy := result
	copy.ReceiptDigest = ""
	unsigned, err := CanonicalJSON(copy)
	if err != nil {
		panic(err)
	}
	result.ReceiptDigest = digestOf(unsigned)
	return result
}
