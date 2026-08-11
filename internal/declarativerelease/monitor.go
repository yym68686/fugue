package declarativerelease

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	MonitorRecordAPIVersion = "release.fugue.dev/v1"
	MonitorRecordKind       = "DeclarativeComponentMonitorRecord"
	MonitorStateAPIVersion  = "release.fugue.dev/v1"
	MonitorStateKind        = "DeclarativeComponentMonitorState"
	MonitorFailureThreshold = 3
)

// MonitorRecord is the durable, immutable binding between one verified
// production target and its exact LKG. The Kubernetes store persists the
// canonical release files alongside this digest-only envelope.
type MonitorRecord struct {
	APIVersion            string `json:"apiVersion"`
	Kind                  string `json:"kind"`
	Component             string `json:"component"`
	ConfigSHA             string `json:"configSha"`
	ReleasePlanDigest     string `json:"releasePlanDigest"`
	ArtifactDigest        string `json:"artifactDigest"`
	ExecutionPlanDigest   string `json:"executionPlanDigest"`
	TerminalReceiptDigest string `json:"terminalReceiptDigest"`
	ForwardManifestDigest string `json:"forwardManifestDigest"`
	LKGManifestDigest     string `json:"lkgManifestDigest"`
	RecordDigest          string `json:"recordDigest"`
}

type MonitorState struct {
	APIVersion          string `json:"apiVersion"`
	Kind                string `json:"kind"`
	Component           string `json:"component"`
	RecordDigest        string `json:"recordDigest"`
	ConfigSHA           string `json:"configSha"`
	ConsecutiveFailures int    `json:"consecutiveFailures"`
	LastReason          string `json:"lastReason,omitempty"`
	LastCheckedAt       string `json:"lastCheckedAt"`
	LastHealthyAt       string `json:"lastHealthyAt,omitempty"`
	RollbackStatus      string `json:"rollbackStatus,omitempty"`
}

func NewMonitorRecord(plan Plan, artifact ArtifactReceipt, prepared ExecutionPlan, terminal ExecutionResult, forwardManifest, lkgManifest []byte) (MonitorRecord, error) {
	if err := plan.ValidateBound(); err != nil {
		return MonitorRecord{}, err
	}
	if prepared.Validate(plan, forwardManifest, lkgManifest) != nil || terminal.Status != "verified" ||
		terminal.Component != prepared.Component || terminal.ConfigSHA != prepared.ConfigSHA ||
		terminal.ExecutionPlanDigest != prepared.PlanDigest || terminal.ReceiptDigest == "" ||
		terminal.Final.Matches(prepared.Forward, mustReleaseByID(plan, prepared.Component), false) == false {
		return MonitorRecord{}, errors.New("verified terminal result is not bound to the immutable execution plan")
	}
	if _, err := DecodeArtifactReceipt(bytes.NewReader(mustCanonical(artifact))); err != nil ||
		artifact.Component != prepared.Component || artifact.ConfigSHA != prepared.ConfigSHA || artifact.ReceiptDigest != prepared.ArtifactDigest {
		return MonitorRecord{}, errors.New("monitor artifact receipt is not bound to the immutable execution plan")
	}
	record := MonitorRecord{
		APIVersion: MonitorRecordAPIVersion, Kind: MonitorRecordKind,
		Component: prepared.Component, ConfigSHA: prepared.ConfigSHA,
		ReleasePlanDigest: plan.PlanDigest, ArtifactDigest: artifact.ReceiptDigest,
		ExecutionPlanDigest: prepared.PlanDigest, TerminalReceiptDigest: terminal.ReceiptDigest,
		ForwardManifestDigest: digestOf(forwardManifest), LKGManifestDigest: digestOf(lkgManifest),
	}
	unsigned, err := CanonicalJSON(record)
	if err != nil {
		return MonitorRecord{}, err
	}
	record.RecordDigest = digestOf(unsigned)
	return record, nil
}

func (record MonitorRecord) Validate(plan Plan, artifact ArtifactReceipt, prepared ExecutionPlan, terminal ExecutionResult, forwardManifest, lkgManifest []byte) error {
	if record.APIVersion != MonitorRecordAPIVersion || record.Kind != MonitorRecordKind ||
		!componentIDPattern.MatchString(record.Component) || !shaPattern.MatchString(record.ConfigSHA) ||
		!digestPattern.MatchString(record.ReleasePlanDigest) || !digestPattern.MatchString(record.ArtifactDigest) ||
		!digestPattern.MatchString(record.ExecutionPlanDigest) || !digestPattern.MatchString(record.TerminalReceiptDigest) ||
		!digestPattern.MatchString(record.ForwardManifestDigest) || !digestPattern.MatchString(record.LKGManifestDigest) ||
		!digestPattern.MatchString(record.RecordDigest) {
		return errors.New("monitor record identity is invalid")
	}
	copy := record
	copy.RecordDigest = ""
	unsigned, err := CanonicalJSON(copy)
	if err != nil || digestOf(unsigned) != record.RecordDigest {
		return errors.New("monitor record digest is invalid")
	}
	want, err := NewMonitorRecord(plan, artifact, prepared, terminal, forwardManifest, lkgManifest)
	if err != nil {
		return err
	}
	if want != record {
		return errors.New("monitor record does not match its canonical release files")
	}
	return nil
}

func NewMonitorState(record MonitorRecord, previous MonitorState, healthy bool, reason string, now time.Time) (MonitorState, bool, error) {
	return NewMonitorObservationState(record, previous, healthy, !healthy, reason, now)
}

// NewMonitorObservationState records one scheduled observation. A public-route
// failure without component-local evidence remains visible, but it does not
// advance the component rollback threshold. This prevents one shared route
// symptom from rolling back API, Control, and Worker independently.
func NewMonitorObservationState(record MonitorRecord, previous MonitorState, healthy, rollbackEligible bool, reason string, now time.Time) (MonitorState, bool, error) {
	if !digestPattern.MatchString(record.RecordDigest) || !componentIDPattern.MatchString(record.Component) || !shaPattern.MatchString(record.ConfigSHA) {
		return MonitorState{}, false, errors.New("monitor record identity is invalid")
	}
	now = now.UTC()
	if now.IsZero() {
		return MonitorState{}, false, errors.New("monitor check time is invalid")
	}
	state := MonitorState{
		APIVersion: MonitorStateAPIVersion, Kind: MonitorStateKind, Component: record.Component,
		RecordDigest: record.RecordDigest, ConfigSHA: record.ConfigSHA,
		LastCheckedAt: now.Format(time.RFC3339Nano),
	}
	if previous.RecordDigest == record.RecordDigest && previous.Component == record.Component && previous.ConfigSHA == record.ConfigSHA {
		state.ConsecutiveFailures = previous.ConsecutiveFailures
		state.LastHealthyAt = previous.LastHealthyAt
		state.RollbackStatus = previous.RollbackStatus
	}
	if healthy {
		state.ConsecutiveFailures = 0
		state.LastReason = ""
		state.LastHealthyAt = state.LastCheckedAt
		state.RollbackStatus = ""
		return state, false, nil
	}
	if !rollbackEligible {
		state.ConsecutiveFailures = 0
		state.LastReason = sanitizeMonitorReason(reason)
		return state, false, nil
	}
	if state.ConsecutiveFailures < MonitorFailureThreshold {
		state.ConsecutiveFailures++
	}
	state.LastReason = sanitizeMonitorReason(reason)
	return state, state.ConsecutiveFailures >= MonitorFailureThreshold, nil
}

func (state MonitorState) Validate() error {
	if state.APIVersion != MonitorStateAPIVersion || state.Kind != MonitorStateKind ||
		!componentIDPattern.MatchString(state.Component) || !digestPattern.MatchString(state.RecordDigest) ||
		!shaPattern.MatchString(state.ConfigSHA) || state.ConsecutiveFailures < 0 || state.ConsecutiveFailures > MonitorFailureThreshold ||
		state.LastCheckedAt == "" {
		return errors.New("monitor state is invalid")
	}
	if _, err := time.Parse(time.RFC3339Nano, state.LastCheckedAt); err != nil {
		return errors.New("monitor state check time is invalid")
	}
	if state.LastHealthyAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, state.LastHealthyAt); err != nil {
			return errors.New("monitor state healthy time is invalid")
		}
	}
	if len(state.LastReason) > 512 || strings.ContainsAny(state.LastReason, "\r\n\x00") {
		return errors.New("monitor state reason is invalid")
	}
	if state.RollbackStatus != "" && state.RollbackStatus != "lkg-restored" {
		return errors.New("monitor rollback status is invalid")
	}
	return nil
}

func sanitizeMonitorReason(reason string) string {
	reason = strings.TrimSpace(strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, reason))
	if len(reason) > 512 {
		reason = reason[:512]
	}
	if reason == "" {
		reason = "component health verification failed"
	}
	return reason
}

// RestoreMonitoredLKG performs one component-scoped rollback from a verified
// forward record. It reuses the existing resource CAS, component transition,
// health, ownership, and LKG semantics; it cannot address another component.
func RestoreMonitoredLKG(ctx context.Context, cluster Cluster, plan Plan, prepared ExecutionPlan, forwardManifest, lkgManifest []byte, healthRelease PlanRelease) ExecutionResult {
	result := ExecutionResult{
		APIVersion: ExecutionPlanAPIVersion, Kind: ExecutionResultKind,
		Component: prepared.Component, ConfigSHA: prepared.ConfigSHA,
		ExecutionPlanDigest: prepared.PlanDigest, Status: "recovery-required", Reason: "continuous-rollback-not-started",
	}
	release, err := releaseByID(plan, prepared.Component)
	if err != nil || prepared.Validate(plan, forwardManifest, lkgManifest) != nil || healthRelease.ComponentID != release.ComponentID ||
		healthRelease.Workload != release.Workload || healthRelease.Concurrency != release.Concurrency {
		result.Reason = "continuous-rollback-plan-invalid"
		return sealResult(result)
	}
	current, err := cluster.Observe(ctx, healthRelease, prepared.Forward, forwardManifest)
	if err != nil || !current.Matches(prepared.Forward, healthRelease, false) {
		// A reviewed emergency write can make the live image unverifiable as the
		// recorded forward target while leaving the component/resource identity
		// and every other declared field intact. Recapture Kubernetes CAS without
		// trusting the drifted image, then require the adapter to prove that every
		// mismatch is both inside the exact emergency allowlist and owned by a
		// reviewed short-lived Update manager. This is the only path that may
		// proceed without a full forward identity match.
		cas, casErr := cluster.ObserveCAS(ctx, healthRelease, forwardManifest)
		if casErr == nil {
			cas, casErr = cluster.ValidateEmergencyRollbackDrift(ctx, healthRelease, forwardManifest, cas)
		}
		if casErr == nil {
			current = cas
			err = nil
		} else {
			if prepared.LKG.Present {
				lkg, lkgErr := cluster.WaitHealthy(ctx, healthRelease, prepared.LKG, lkgManifest)
				if lkgErr == nil && lkg.Matches(prepared.LKG, healthRelease, true) && cluster.Converged(ctx, healthRelease, lkgManifest) == nil {
					result.Status = "compensated"
					result.Reason = "continuous-rollback-lkg-already-restored"
					result.Final = lkg
					return sealResult(result)
				}
			}
			result.Reason = "continuous-rollback-forward-identity-drift"
			return sealResult(result)
		}
	}
	result.LKGApplyCount = 1
	var rollbackErr error
	if prepared.LKG.Present {
		lkgCAS, bindErr := BindManifestCAS(lkgManifest, current)
		if bindErr != nil {
			result.Reason = "continuous-rollback-cas-invalid"
			return sealResult(result)
		}
		rollbackErr = cluster.Apply(ctx, healthRelease, prepared.LKG, lkgCAS)
		rollbackErr = errors.Join(rollbackErr, cluster.DeleteCreated(ctx, healthRelease, forwardManifest, prepared.Prewrite, current))
	} else {
		rollbackErr = cluster.Delete(ctx, healthRelease, forwardManifest, current)
	}
	var final Observation
	var healthErr, convergedErr error
	if prepared.LKG.Present {
		final, healthErr = cluster.WaitHealthy(ctx, healthRelease, prepared.LKG, lkgManifest)
		convergedErr = errors.Join(cluster.Converged(ctx, healthRelease, lkgManifest), cluster.VerifyOwnershipConverged(ctx, healthRelease, lkgManifest))
	} else {
		final, healthErr = cluster.Observe(ctx, healthRelease, prepared.LKG, forwardManifest)
	}
	result.Final = final
	if healthErr == nil && convergedErr == nil && final.Matches(prepared.LKG, healthRelease, true) {
		result.Status = "compensated"
		if rollbackErr != nil {
			result.Reason = "continuous-rollback-lkg-commit-unknown-reconciled"
		} else {
			result.Reason = "continuous-health-threshold-lkg-restored"
		}
		return sealResult(result)
	}
	result.Reason = "continuous-rollback-lkg-unproven"
	result.FailureDetail = forwardFailureDetail(healthErr, convergedErr)
	return sealResult(result)
}

func mustReleaseByID(plan Plan, componentID string) PlanRelease {
	release, err := releaseByID(plan, componentID)
	if err != nil {
		panic(fmt.Sprintf("validated plan lost component %q", componentID))
	}
	return release
}
