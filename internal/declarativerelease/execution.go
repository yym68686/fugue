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
	APIVersion          string         `json:"apiVersion"`
	Kind                string         `json:"kind"`
	Component           string         `json:"component"`
	ConfigSHA           string         `json:"configSha"`
	ReleasePlanDigest   string         `json:"releasePlanDigest"`
	IntentDigest        string         `json:"intentDigest"`
	ArtifactDigest      string         `json:"artifactDigest"`
	Forward             TargetIdentity `json:"forward"`
	LKG                 TargetIdentity `json:"lkg"`
	Prewrite            Observation    `json:"prewrite"`
	AlreadyConverged    bool           `json:"alreadyConverged"`
	DegradedPredecessor bool           `json:"degradedPredecessor,omitempty"`
	DegradedRoute       bool           `json:"degradedRoute,omitempty"`
	PreparedAt          string         `json:"preparedAt"`
	PlanDigest          string         `json:"planDigest"`
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
	Apply(context.Context, PlanRelease, TargetIdentity, []byte) error
	Delete(context.Context, PlanRelease, []byte, Observation) error
	DeleteCreated(context.Context, PlanRelease, []byte, Observation, Observation) error
	WaitHealthy(context.Context, PlanRelease, TargetIdentity, []byte) (Observation, error)
	ValidateEmergencyRollbackDrift(context.Context, PlanRelease, []byte, Observation) (Observation, error)
	Converged(context.Context, PlanRelease, []byte) error
	VerifyOwnershipConverged(context.Context, PlanRelease, []byte) error
}

var ErrDegradedPredecessorHealth = errors.New("declarative predecessor health is degraded")

// ErrPublicRouteHealth identifies only an independent public route probe
// failure. It lets an exact degraded-predecessor recovery preserve an already
// degraded route while still requiring the replacement workload and every
// other reviewed resource to converge.
var ErrPublicRouteHealth = errors.New("independent public route health is degraded")

type prewritePredecessorHealthWaitKey struct{}
type preservedRouteHealthWaitKey struct{}

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

func withPreservedRouteHealthWait(ctx context.Context) context.Context {
	return context.WithValue(ctx, preservedRouteHealthWaitKey{}, true)
}

func IsPreservedRouteHealthWait(ctx context.Context) bool {
	marked, _ := ctx.Value(preservedRouteHealthWaitKey{}).(bool)
	return marked
}

func DecodeExecutionPlan(reader io.Reader, releasePlan Plan, forwardManifest, lkgManifest []byte) (ExecutionPlan, error) {
	plan, err := decodeExecutionPlan(reader, releasePlan, forwardManifest, lkgManifest)
	if err != nil {
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

// DecodeRecordedExecutionPlan validates a plan that is already bound to a
// verified terminal receipt and stored as durable LKG evidence. The transient
// 15-minute prewrite window applies only before the first mutation; recorded
// plans remain cryptographically bound and usable by the continuous monitor.
func DecodeRecordedExecutionPlan(reader io.Reader, releasePlan Plan, forwardManifest, lkgManifest []byte) (ExecutionPlan, error) {
	return decodeExecutionPlan(reader, releasePlan, forwardManifest, lkgManifest)
}

func decodeExecutionPlan(reader io.Reader, releasePlan Plan, forwardManifest, lkgManifest []byte) (ExecutionPlan, error) {
	var plan ExecutionPlan
	if err := decodeStrict(reader, &plan); err != nil {
		return ExecutionPlan{}, fmt.Errorf("decode execution plan: %w", err)
	}
	if err := plan.Validate(releasePlan, forwardManifest, lkgManifest); err != nil {
		return ExecutionPlan{}, err
	}
	if _, err := time.Parse(time.RFC3339Nano, plan.PreparedAt); err != nil {
		return ExecutionPlan{}, errors.New("execution plan preparedAt is invalid")
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
	degradedRoute := false
	controlledEdgeRecovery := release.ExpectedPreviousPresent && release.SupersedesFailedConfigSHA != "" &&
		release.Transition != nil && release.Transition.Type == "edge-group-ab" && release.Transition.EdgeGroupAB != nil
	if controlledEdgeRecovery {
		// Edge group recovery owns its serving-health contract: the Front and
		// inactive slot may be intentionally unready while Guardian restores the
		// authority/candidate ledger. Bind only the immutable workload CAS here;
		// the edge transition performs the readiness and activation checks before
		// committing traffic.
		prewrite, err = prepareControlledEdgeRecoveryPredecessor(ctx, cluster, release, lkg, rendered.Forward)
		degradedPredecessor = err == nil
		degradedRoute = true
	} else if release.RetrySameLKG && release.ExpectedPreviousPresent {
		prewrite, err = cluster.Observe(ctx, release, forward, rendered.Forward)
		if err == nil && prewrite.Matches(forward, release, false) {
			prewrite, err = cluster.WaitHealthy(ctx, release, forward, rendered.Forward)
			if err == nil {
				err = cluster.Converged(ctx, release, rendered.Forward)
				alreadyConverged = err == nil
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
		if release.ExpectedPreviousPresent && lkgObserveErr == nil && !lkgMatched &&
			prewrite.MatchesSupersededFailedAtom(release) {
			prewrite, err = prepareOwnedDegradedPredecessor(ctx, cluster, release, rendered.Forward,
				errors.New("live workload is the exact superseded failed atom"))
			if err == nil && !prewrite.MatchesSupersededFailedAtom(release) {
				err = errors.New("superseded failed atom identity changed during validation")
			}
			degradedPredecessor = err == nil
		}
		if release.ExpectedPreviousPresent && errors.Is(lkgObserveErr, ErrDegradedPredecessorHealth) {
			degradedRoute = errors.Is(lkgObserveErr, ErrPublicRouteHealth)
			prewrite, err = prepareDegradedPredecessor(ctx, cluster, release, lkg, rendered.Forward, rendered.LKG)
			degradedPredecessor = err == nil
		} else if release.ExpectedPreviousPresent && lkgObserveErr != nil {
			var healthyLKG Observation
			healthyLKG, err = cluster.WaitHealthy(WithPrewritePredecessorHealthWait(ctx), release, lkg, rendered.LKG)
			if errors.Is(err, ErrDegradedPredecessorHealth) {
				degradedRoute = errors.Is(err, ErrPublicRouteHealth)
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
						degradedRoute = errors.Is(err, ErrPublicRouteHealth)
						prewrite, err = prepareDegradedPredecessor(ctx, cluster, release, lkg, rendered.Forward, rendered.LKG)
						degradedPredecessor = err == nil
					}
				}
				if err == nil && !degradedPredecessor {
					predecessorWitness, witnessErr := PredecessorConvergenceManifest(rendered.LKG)
					err = witnessErr
					if err == nil {
						err = cluster.Converged(ctx, release, predecessorWitness)
					}
					if err != nil && release.SupersedesFailedConfigSHA != "" {
						prewrite, err = prepareOwnedDegradedPredecessor(ctx, cluster, release, rendered.Forward, err)
						degradedPredecessor = err == nil
					}
					if err == nil && !degradedPredecessor {
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
	prewrite, err = bindForwardOnlyPrewriteCAS(ctx, cluster, release, prewrite, rendered.LKG, rendered.Forward)
	if err != nil {
		return ExecutionPlan{}, fmt.Errorf("bind forward-only resource CAS: %w", err)
	}
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
	plan := ExecutionPlan{
		APIVersion: ExecutionPlanAPIVersion, Kind: ExecutionPlanKind,
		Component: componentID, ConfigSHA: releasePlan.HeadSHA,
		ReleasePlanDigest: releasePlan.PlanDigest, IntentDigest: release.IntentDigest,
		ArtifactDigest: artifact.ReceiptDigest, Forward: forward, LKG: lkg,
		Prewrite: prewrite, AlreadyConverged: alreadyConverged, DegradedPredecessor: degradedPredecessor,
		DegradedRoute: degradedPredecessor && degradedRoute,
		PreparedAt:    now.UTC().Format(time.RFC3339Nano),
	}
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		return ExecutionPlan{}, err
	}
	plan.PlanDigest = digestOf(unsigned)
	return plan, nil
}

// bindForwardOnlyPrewriteCAS extends a verified predecessor observation with
// exact absent witnesses for resources introduced by the forward manifest.
// It never turns an existing object into a forward-owned resource: every
// forward-only identity must still be absent, while every predecessor object
// must retain the same UID, generation, desired bytes, and managed-field
// ownership. ResourceVersions may advance due to status-only writes; the
// returned fresh CAS becomes the sole precondition used by dry-run and execute.
func bindForwardOnlyPrewriteCAS(ctx context.Context, cluster Cluster, release PlanRelease, prewrite Observation, lkgManifest, forwardManifest []byte) (Observation, error) {
	forwardIdentities, err := ResourceSetIdentities(forwardManifest)
	if err != nil {
		return Observation{}, err
	}
	prewriteByIdentity := make(map[ResourceIdentity]ResourceObservation, len(prewrite.Resources))
	for _, resource := range prewrite.Resources {
		prewriteByIdentity[resource.Identity] = resource
	}
	missing := make([]ResourceIdentity, 0)
	for _, identity := range forwardIdentities {
		if _, exists := prewriteByIdentity[identity]; exists {
			continue
		}
		missing = append(missing, identity)
	}
	if len(missing) == 0 {
		return prewrite, nil
	}
	lkgIdentities, err := ResourceSetIdentities(lkgManifest)
	if err != nil {
		return Observation{}, err
	}
	lkgSet := make(map[ResourceIdentity]struct{}, len(lkgIdentities))
	for _, identity := range lkgIdentities {
		lkgSet[identity] = struct{}{}
	}
	for _, identity := range missing {
		if _, predecessor := lkgSet[identity]; predecessor {
			return Observation{}, errors.New("predecessor resource is missing from verified observation")
		}
	}
	fresh, err := cluster.ObserveCAS(ctx, release, forwardManifest)
	if err != nil {
		return Observation{}, fmt.Errorf("observe forward resource set: %w", err)
	}
	if err := fresh.validateResourceCAS(); err != nil {
		return Observation{}, err
	}
	if fresh.Present != prewrite.Present || fresh.Primary != prewrite.Primary || fresh.UID != prewrite.UID || fresh.Generation != prewrite.Generation {
		return Observation{}, errors.New("primary workload identity changed while binding forward resources")
	}
	freshByIdentity := make(map[ResourceIdentity]ResourceObservation, len(fresh.Resources))
	for _, resource := range fresh.Resources {
		freshByIdentity[resource.Identity] = resource
	}
	for _, identity := range forwardIdentities {
		current, exists := freshByIdentity[identity]
		if !exists {
			return Observation{}, errors.New("forward resource is missing from fresh CAS observation")
		}
		prior, predecessor := prewriteByIdentity[identity]
		if !predecessor {
			if current.Present {
				return Observation{}, errors.New("forward-only resource already exists outside the declared predecessor")
			}
			continue
		}
		if !sameResourceSpecIdentity(prior, current) {
			return Observation{}, errors.New("predecessor resource changed while binding forward resources")
		}
	}
	bound := prewrite
	bound.ResourceVersion = fresh.ResourceVersion
	bound.Resources = append([]ResourceObservation(nil), fresh.Resources...)
	return bound, nil
}

func sameResourceSpecIdentity(left, right ResourceObservation) bool {
	return left.Identity == right.Identity && left.Present == right.Present && left.UID == right.UID &&
		left.RetainOnRollback == right.RetainOnRollback && left.Generation == right.Generation &&
		left.ObjectDigest == right.ObjectDigest && equalStrings(left.FieldManagers, right.FieldManagers) &&
		left.ReviewedOwnershipApplied == right.ReviewedOwnershipApplied &&
		left.ReviewedOwnershipExclusive == right.ReviewedOwnershipExclusive
}

func prepareDegradedPredecessor(ctx context.Context, cluster Cluster, release PlanRelease, lkg TargetIdentity, forwardManifest, lkgManifest []byte) (Observation, error) {
	if !release.ExpectedPreviousPresent || !lkg.Present {
		return Observation{}, errors.New("degraded predecessor recovery is not authorized")
	}
	if verifyErr := cluster.VerifyTarget(ctx, lkg); verifyErr != nil {
		return Observation{}, fmt.Errorf("verify degraded predecessor artifact: %w", verifyErr)
	}
	witness, err := rollbackConvergenceWitness(lkgManifest, release)
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

// prepareControlledEdgeRecoveryPredecessor binds only the immutable workload
// CAS for an edge-group recovery. Edge transitions have a separate serving
// authority contract, so generic manifest convergence can reject a valid
// recovery while the old Front or standby slot is intentionally unready.
// The transition still receives two stable CAS observations and verifies the
// exact declared LKG artifact before it can mutate anything.
func prepareControlledEdgeRecoveryPredecessor(ctx context.Context, cluster Cluster, release PlanRelease, lkg TargetIdentity, forwardManifest []byte) (Observation, error) {
	if !release.ExpectedPreviousPresent || !lkg.Present {
		return Observation{}, errors.New("controlled edge recovery is not authorized")
	}
	if verifyErr := cluster.VerifyTarget(ctx, lkg); verifyErr != nil {
		return Observation{}, fmt.Errorf("verify controlled edge LKG artifact: %w", verifyErr)
	}
	first, err := cluster.ObserveCAS(ctx, release, forwardManifest)
	if err != nil {
		return Observation{}, err
	}
	if err := first.ValidateDegradedPredecessor(release); err != nil {
		return Observation{}, err
	}
	second, err := cluster.ObserveCAS(ctx, release, forwardManifest)
	if err != nil {
		return Observation{}, err
	}
	if err := second.ValidateDegradedPredecessor(release); err != nil {
		return Observation{}, err
	}
	if !second.SameSpecIdentity(first) {
		return Observation{}, errors.New("controlled edge recovery predecessor identity changed during validation")
	}
	return second, nil
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
	var current Observation
	if prepared.DegradedPredecessor {
		controlledEdgeRecovery := release.ExpectedPreviousPresent && release.SupersedesFailedConfigSHA != "" &&
			release.Transition != nil && release.Transition.Type == "edge-group-ab" && release.Transition.EdgeGroupAB != nil
		if prepared.Prewrite.ImageRef == "" {
			current, err = cluster.ObserveCAS(ctx, release, observationManifest)
		} else {
			current, err = cluster.ObserveDegraded(ctx, release, observationManifest)
		}
		if err == nil && !current.SameSpecIdentity(prepared.Prewrite) {
			err = fmt.Errorf("degraded predecessor identity changed: %s", specIdentityMismatch(prepared.Prewrite, current))
		}
		if err == nil && !controlledEdgeRecovery {
			var witness []byte
			if prepared.Prewrite.ImageRef == "" {
				witness, err = rollbackConvergenceWitness(lkgManifest, release)
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
	if prepared.AlreadyConverged {
		ownershipErr := cluster.VerifyOwnershipConverged(ctx, release, forwardManifest)
		if ownershipErr != nil {
			forwardCAS, bindErr := BindManifestCAS(forwardManifest, current)
			if bindErr != nil {
				result.Reason = "ownership-convergence-cas-manifest-invalid"
				return sealResult(result)
			}
			result.ForwardApplyCount = 1
			_ = cluster.Apply(ctx, release, prepared.Forward, forwardCAS)
		}
		forwardObservation, healthErr := cluster.WaitHealthy(ctx, release, prepared.Forward, forwardManifest)
		convergedErr := errors.Join(cluster.Converged(ctx, release, forwardManifest), cluster.VerifyOwnershipConverged(ctx, release, forwardManifest))
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
	result.ForwardApplyCount = 1
	applyErr := cluster.Apply(ctx, release, prepared.Forward, forwardCAS)
	healthCtx := ctx
	if prepared.DegradedRoute {
		healthCtx = withPreservedRouteHealthWait(ctx)
	}
	forwardObservation, healthErr, convergedErr := observeForwardResult(healthCtx, cluster, release, prepared.Forward, forwardManifest, applyErr)
	preservedRoute := applyErr == nil && prepared.DegradedRoute && errors.Is(healthErr, ErrPublicRouteHealth)
	if (healthErr == nil || preservedRoute) && convergedErr == nil && forwardObservation.Matches(prepared.Forward, release, false) {
		result.Status = "verified"
		if preservedRoute {
			result.Reason = "forward-verified-with-preserved-route-degradation"
		} else if applyErr != nil {
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
		} else {
			observed, observeErr = cluster.Observe(ctx, release, prepared.LKG, observationManifest)
			unchanged = observeErr == nil && observed.SameCAS(prepared.Prewrite)
		}
		if unchanged {
			result.Status = "failed-no-write"
			result.Reason = "forward-apply-rejected-before-commit"
			result.FailureClass = "forward_apply"
			result.FailureDetail = boundedFailureDetail(applyErr.Error())
			result.Final = observed
			return sealResult(result)
		}
	}
	rollbackBase := forwardObservation
	result.FailureClass = forwardFailureClass(applyErr, healthErr, convergedErr, forwardObservation, prepared.Forward, release)
	result.FailureDetail = forwardFailureDetail(applyErr, healthErr, convergedErr)
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
		lkgConvergedErr = errors.Join(cluster.Converged(ctx, release, lkgManifest), cluster.VerifyOwnershipConverged(ctx, release, lkgManifest))
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
	result.FailureDetail = combinedFailureDetail(
		result.FailureDetail,
		lkgFailureDetail(lkgHealthErr, lkgConvergedErr, lkgObservation, prepared.LKG, release),
	)
	result.Final = lkgObservation
	return sealResult(result)
}

func specIdentityMismatch(expected, actual Observation) string {
	if expected.Present != actual.Present || expected.Primary != actual.Primary || expected.UID != actual.UID || expected.Generation != actual.Generation ||
		expected.ImageRef != actual.ImageRef || expected.ConfigSHA != actual.ConfigSHA || expected.ManifestSHA != actual.ManifestSHA || expected.OCIRevision != actual.OCIRevision ||
		expected.TemplateDigest != actual.TemplateDigest || !equalStrings(expected.FieldManagers, actual.FieldManagers) {
		return fmt.Sprintf("workload expected=%+v actual=%+v", expected, actual)
	}
	if len(expected.Resources) != len(actual.Resources) {
		return fmt.Sprintf("resource count expected=%d actual=%d", len(expected.Resources), len(actual.Resources))
	}
	for index := range expected.Resources {
		left, right := expected.Resources[index], actual.Resources[index]
		if !sameResourceSpecIdentity(left, right) {
			return fmt.Sprintf("resource %s expected=%+v actual=%+v", left.Identity.key(), left, right)
		}
	}
	return "resource identity differs"
}

func rollbackConvergenceWitness(lkgManifest []byte, release PlanRelease) ([]byte, error) {
	witnessManifest := lkgManifest
	if len(release.RuntimeResourcesFromForward) > 0 {
		var err error
		witnessManifest, err = RuntimeResourcesRollbackWitness(lkgManifest, release.RuntimeResourcesFromForward)
		if err != nil {
			return nil, fmt.Errorf("prepare runtime resource rollback witness: %w", err)
		}
	}
	return PredecessorConvergenceManifest(witnessManifest)
}

func observeForwardResult(ctx context.Context, cluster Cluster, release PlanRelease, target TargetIdentity, manifest []byte, applyErr error) (Observation, error, error) {
	if applyErr != nil && release.Transition != nil && release.Transition.Type == "edge-group-ab" {
		observed, observeErr := cluster.ObserveCAS(ctx, release, manifest)
		if observeErr != nil {
			return observed, errors.Join(applyErr, fmt.Errorf("observe failed edge group transition: %w", observeErr)), nil
		}
		// The transition only returns nil after Front, active Worker, standby
		// Worker, and serving authority converge. Until then the primary Front
		// can intentionally remain at the LKG while the inactive slot runs the
		// new image. A generic target health check would compare that old Front
		// with the new OCI revision and replace the actual transition failure
		// with a false provenance mismatch.
		return observed, applyErr, nil
	}
	observed, healthErr := cluster.WaitHealthy(ctx, release, target, manifest)
	convergedErr := errors.Join(cluster.Converged(ctx, release, manifest), cluster.VerifyOwnershipConverged(ctx, release, manifest))
	return observed, healthErr, convergedErr
}

func lkgFailureDetail(healthErr, convergedErr error, observed Observation, target TargetIdentity, release PlanRelease) string {
	switch {
	case healthErr != nil:
		return boundedFailureDetail("LKG health: " + healthErr.Error())
	case convergedErr != nil:
		return boundedFailureDetail("LKG convergence: " + convergedErr.Error())
	case !observed.Matches(target, release, true):
		return boundedFailureDetail("LKG identity: " + observationMismatchClass(observed, target, release, true))
	default:
		return "LKG terminal proof is unavailable"
	}
}

func observationMismatchClass(observed Observation, target TargetIdentity, release PlanRelease, allowLegacyManager bool) string {
	if observed.Present != target.Present {
		return "presence mismatch"
	}
	if !target.Present {
		for _, resource := range observed.Resources {
			if resource.Present && !resource.RetainOnRollback {
				return "unexpected retained resource"
			}
		}
		return "absent target mismatch"
	}
	imageMatches := observed.ImageRef == target.ImageRef
	if !imageMatches && allowLegacyManager {
		separator := strings.LastIndex(target.ImageRef, "@")
		imageMatches = separator > 0 && observed.ImageID == target.ImageRef[separator+1:]
	}
	switch {
	case !imageMatches:
		return "image mismatch"
	case observed.ConfigSHA != target.ConfigSHA:
		return "config source mismatch"
	case observed.ManifestSHA != target.ManifestSHA:
		return "manifest source mismatch"
	case observed.OCIRevision != target.OCIRevision:
		return "OCI revision mismatch"
	case observed.ObservedGeneration != observed.Generation:
		return "generation is not observed"
	}
	activeDesired := observed.Desired - int32(release.Workload.PreservedUnavailable)
	switch {
	case activeDesired < 1:
		return "active desired replicas are invalid"
	case observed.Updated != activeDesired:
		return "updated replicas mismatch"
	case observed.Ready != activeDesired:
		return "ready replicas mismatch"
	case observed.Available != activeDesired:
		return "available replicas mismatch"
	case observed.Unavailable != int32(release.Workload.PreservedUnavailable):
		return "unavailable replicas mismatch"
	}
	for _, manager := range observed.FieldManagers {
		if manager == release.Workload.FieldManager {
			return "unknown identity mismatch"
		}
	}
	if allowLegacyManager {
		return "unknown identity mismatch"
	}
	return "declarative field manager is absent"
}

func boundedFailureDetail(detail string) string {
	detail = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, detail)
	if len(detail) > 512 {
		return detail[:512]
	}
	return detail
}

func combinedFailureDetail(forward, rollback string) string {
	forward = boundedFailureDetail(forward)
	rollback = boundedFailureDetail(rollback)
	if forward == "" {
		return rollback
	}
	if rollback == "" {
		return forward
	}
	const labelBytes = len("forward: ; rollback: ")
	const totalBytes = 512
	partBytes := (totalBytes - labelBytes) / 2
	if len(forward) > partBytes {
		forward = forward[:partBytes]
	}
	if len(rollback) > partBytes {
		rollback = rollback[:partBytes]
	}
	return "forward: " + forward + "; rollback: " + rollback
}

func forwardFailureDetail(applyErr, healthErr, convergedErr error) string {
	var detail string
	if applyErr != nil {
		detail = applyErr.Error()
	} else if healthErr != nil {
		detail = healthErr.Error()
	} else if convergedErr != nil {
		detail = convergedErr.Error()
	}
	return boundedFailureDetail(detail)
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

// ReconcileExecution// ReconcileExecution is the read-only terminal path used when the executor
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
	if plan.DegradedPredecessor {
		failedAtomSuccessor := shaPattern.MatchString(release.SupersedesFailedConfigSHA)
		if !release.ExpectedPreviousPresent || plan.AlreadyConverged ||
			(!release.RetrySameLKG && !failedAtomSuccessor) {
			return errors.New("degraded predecessor execution is not authorized")
		}
		if err := plan.Prewrite.ValidateDegradedPredecessor(release); err != nil {
			return err
		}
	} else if err := plan.Prewrite.ValidateMustBeStable(); err != nil {
		return err
	}
	if plan.DegradedRoute && (!plan.DegradedPredecessor || !releaseHasHealthProbe(release, "public-route-http")) {
		return errors.New("degraded route recovery is not authorized")
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

func releaseHasHealthProbe(release PlanRelease, probeType string) bool {
	for _, probe := range release.Health {
		if probe.Type == probeType {
			return true
		}
	}
	return false
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

// MatchesSupersededFailedAtom identifies only the immutable workload produced
// by the immediately preceding failed production atom. It does not authorize
// recovery by itself; callers must still use the degraded-predecessor CAS and
// ownership checks before mutating the workload.
func (observation Observation) MatchesSupersededFailedAtom(release PlanRelease) bool {
	failed := release.SupersedesFailedConfigSHA
	imagePrefix := release.Artifact.Repository + "@"
	return shaPattern.MatchString(failed) && observation.Present &&
		observation.ConfigSHA == failed && observation.ManifestSHA == failed && observation.OCIRevision == failed &&
		strings.HasPrefix(observation.ImageRef, imagePrefix) &&
		digestPattern.MatchString(strings.TrimPrefix(observation.ImageRef, imagePrefix))
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

// SameSpecIdentity// SameSpecIdentity permits status-only resourceVersion movement while binding
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
