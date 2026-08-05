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
	Identity         ResourceIdentity `json:"identity"`
	Present          bool             `json:"present"`
	RetainOnRollback bool             `json:"retainOnRollback"`
	UID              string           `json:"uid,omitempty"`
	ResourceVersion  string           `json:"resourceVersion,omitempty"`
	Generation       int64            `json:"generation,omitempty"`
	ObjectDigest     string           `json:"objectDigest,omitempty"`
	FieldManagers    []string         `json:"fieldManagers,omitempty"`
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
	ForwardApplyCount   int         `json:"forwardApplyCount"`
	LKGApplyCount       int         `json:"lkgApplyCount"`
	Final               Observation `json:"final"`
	ReceiptDigest       string      `json:"receiptDigest"`
}

type Cluster interface {
	Observe(context.Context, PlanRelease, TargetIdentity, []byte) (Observation, error)
	ObserveCAS(context.Context, PlanRelease, []byte) (Observation, error)
	VerifyTarget(context.Context, TargetIdentity) error
	DryRunApply(context.Context, PlanRelease, []byte) error
	Apply(context.Context, PlanRelease, TargetIdentity, []byte) error
	Delete(context.Context, PlanRelease, []byte, Observation) error
	DeleteCreated(context.Context, PlanRelease, []byte, Observation, Observation) error
	WaitHealthy(context.Context, PlanRelease, TargetIdentity, []byte) (Observation, error)
	Converged(context.Context, PlanRelease, []byte) error
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
		} else {
			prewrite, err = prepareDegradedPredecessor(ctx, cluster, release, lkg, rendered.Forward, rendered.LKG)
			degradedPredecessor = err == nil
		}
	} else {
		var lkgObserveErr error
		prewrite, lkgObserveErr = cluster.Observe(ctx, release, lkg, rendered.Forward)
		lkgMatched := lkgObserveErr == nil && prewrite.Matches(lkg, release, true)
		lkgHealthVerified := false
		if release.ExpectedPreviousPresent && lkgObserveErr != nil {
			var healthyLKG Observation
			healthyLKG, err = cluster.WaitHealthy(ctx, release, lkg, rendered.LKG)
			if err == nil && healthyLKG.Matches(lkg, release, true) {
				prewrite, err = cluster.Observe(ctx, release, lkg, rendered.Forward)
				lkgMatched = err == nil && prewrite.Matches(lkg, release, true)
				lkgHealthVerified = lkgMatched
			}
		}
		if lkgMatched {
			if release.ExpectedPreviousPresent {
				if !lkgHealthVerified {
					_, err = cluster.WaitHealthy(ctx, release, lkg, rendered.LKG)
				}
				if err == nil {
					var predecessorWitness []byte
					predecessorWitness, err = PredecessorConvergenceManifest(rendered.LKG)
					if err == nil {
						err = cluster.Converged(ctx, release, predecessorWitness)
					}
					if err == nil {
						var freshLKG Observation
						freshLKG, err = cluster.Observe(ctx, release, lkg, rendered.Forward)
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
		if err := prewrite.ValidateDegradedPredecessor(); err != nil {
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
	forwardDryRun, err := BindManifestCAS(rendered.Forward, prewrite)
	if err != nil {
		return ExecutionPlan{}, err
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
		PreparedAt: now.UTC().Format(time.RFC3339Nano),
	}
	unsigned, err := CanonicalJSON(plan)
	if err != nil {
		return ExecutionPlan{}, err
	}
	plan.PlanDigest = digestOf(unsigned)
	return plan, nil
}

func prepareDegradedPredecessor(ctx context.Context, cluster Cluster, release PlanRelease, lkg TargetIdentity, forwardManifest, lkgManifest []byte) (Observation, error) {
	if !release.RetrySameLKG || !release.ExpectedPreviousPresent || !lkg.Present {
		return Observation{}, errors.New("degraded predecessor recovery is not authorized")
	}
	if err := cluster.VerifyTarget(ctx, lkg); err != nil {
		return Observation{}, fmt.Errorf("verify degraded predecessor artifact: %w", err)
	}
	witness, err := PredecessorConvergenceManifest(lkgManifest)
	if err != nil {
		return Observation{}, err
	}
	first, err := cluster.ObserveCAS(ctx, release, forwardManifest)
	if err != nil {
		return Observation{}, err
	}
	if err := first.ValidateDegradedPredecessor(); err != nil {
		return Observation{}, err
	}
	if err := cluster.Converged(ctx, release, witness); err != nil {
		return Observation{}, fmt.Errorf("degraded predecessor manifest drift: %w", err)
	}
	second, err := cluster.ObserveCAS(ctx, release, forwardManifest)
	if err != nil {
		return Observation{}, err
	}
	if err := second.ValidateDegradedPredecessor(); err != nil {
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
	currentTarget := prepared.LKG
	if prepared.AlreadyConverged {
		currentTarget = prepared.Forward
	}
	var current Observation
	if prepared.DegradedPredecessor {
		current, err = cluster.ObserveCAS(ctx, release, observationManifest)
		if err == nil && !current.SameSpecIdentity(prepared.Prewrite) {
			err = errors.New("degraded predecessor identity changed")
		}
		if err == nil {
			var witness []byte
			witness, err = PredecessorConvergenceManifest(lkgManifest)
			if err == nil {
				err = cluster.Converged(ctx, release, witness)
			}
		}
	} else {
		current, err = cluster.Observe(ctx, release, currentTarget, observationManifest)
		if err == nil && !current.SameCAS(prepared.Prewrite) {
			err = errors.New("prewrite CAS changed")
		}
	}
	if err != nil {
		result.Reason = "prewrite-cas-drift"
		return sealResult(result)
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
			observed, observeErr = cluster.ObserveCAS(ctx, release, observationManifest)
			unchanged = observeErr == nil && observed.SameSpecIdentity(prepared.Prewrite)
		} else {
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
	if plan.DegradedPredecessor {
		if !release.RetrySameLKG || !release.ExpectedPreviousPresent || plan.AlreadyConverged {
			return errors.New("degraded predecessor execution is not authorized")
		}
		if err := plan.Prewrite.ValidateDegradedPredecessor(); err != nil {
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

func (observation Observation) ValidateDegradedPredecessor() error {
	if !observation.Present || observation.UID == "" || !resourceVersionPattern.MatchString(observation.ResourceVersion) || observation.Generation < 1 {
		return errors.New("degraded predecessor CAS is invalid")
	}
	if observation.ObservedGeneration != 0 || observation.Desired != 0 || observation.Updated != 0 || observation.Ready != 0 ||
		observation.Available != 0 || observation.Unavailable != 0 || observation.ImageRef != "" || observation.ImageID != "" ||
		observation.ConfigSHA != "" || observation.ManifestSHA != "" || observation.OCIRevision != "" || observation.TemplateDigest != "" ||
		observation.HealthDigest != "" || len(observation.FieldManagers) != 0 {
		return errors.New("degraded predecessor observation carries unverified health state")
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
	if observation.Present != other.Present || observation.Primary != other.Primary ||
		observation.UID != other.UID || observation.ResourceVersion != other.ResourceVersion ||
		observation.Generation != other.Generation || observation.TemplateDigest != other.TemplateDigest ||
		observation.ImageRef != other.ImageRef || observation.ConfigSHA != other.ConfigSHA ||
		observation.ManifestSHA != other.ManifestSHA || observation.OCIRevision != other.OCIRevision ||
		len(observation.Resources) != len(other.Resources) {
		return false
	}
	for index := range observation.Resources {
		left, right := observation.Resources[index], other.Resources[index]
		if left.Identity != right.Identity || left.Present != right.Present || left.UID != right.UID ||
			left.RetainOnRollback != right.RetainOnRollback || left.ResourceVersion != right.ResourceVersion || left.Generation != right.Generation ||
			left.ObjectDigest != right.ObjectDigest || !equalStrings(left.FieldManagers, right.FieldManagers) {
			return false
		}
	}
	return true
}

// SameSpecIdentity permits status-only resourceVersion movement while binding
// an unhealthy predecessor to the same UID, generation, desired object bytes,
// and managed-field ownership. The caller still uses the newest RV as the
// immediate server-side-apply CAS precondition.
func (observation Observation) SameSpecIdentity(other Observation) bool {
	if observation.Present != other.Present || observation.Primary != other.Primary || observation.UID != other.UID ||
		observation.Generation != other.Generation || len(observation.Resources) != len(other.Resources) {
		return false
	}
	for index := range observation.Resources {
		left, right := observation.Resources[index], other.Resources[index]
		if left.Identity != right.Identity || left.Present != right.Present || left.UID != right.UID ||
			left.RetainOnRollback != right.RetainOnRollback || left.Generation != right.Generation ||
			left.ObjectDigest != right.ObjectDigest || !equalStrings(left.FieldManagers, right.FieldManagers) {
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
