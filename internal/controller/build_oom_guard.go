package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

type buildOOMHistory struct {
	Version     int    `json:"version"`
	OperationID string `json:"operation_id,omitempty"`
	sourceimport.BuilderOOMObservation
}

// Resource policies are evaluated separately; changing a ceiling must not erase
// an OOM fact for unchanged code/configuration.
func buildOOMInputFingerprint(source model.AppSource, spec model.AppSpec) string {
	return buildInputFingerprint(source, spec, sourceimport.BuilderPodPolicy{})
}

func buildOOMHistoryKey(app model.App, fingerprint string) string {
	return "builder-oom-history/v1/" + app.TenantID + "/" + app.ID + "/" + fingerprint
}

func decodeBuildOOMHistory(raw []byte) (buildOOMHistory, error) {
	history := buildOOMHistory{Version: 1}
	if len(raw) == 0 {
		return history, nil
	}
	if err := json.Unmarshal(raw, &history); err != nil {
		return history, err
	}
	if history.Version != 1 || history.MemoryLimitBytes < 0 || history.WorkloadProfile != "" && history.WorkloadProfile != "light" && history.WorkloadProfile != "heavy" {
		return history, errors.New("invalid builder OOM history")
	}
	return history, nil
}

func (s *Service) readBuildOOMHistory(ctx context.Context, app model.App, fingerprint string) (buildOOMHistory, error) {
	if fingerprint == "" {
		return buildOOMHistory{}, errors.New("build OOM admission requires resolved inputs")
	}
	raw, err := s.Store.ReadObservationCheckpoint(ctx, buildOOMHistoryKey(app, fingerprint))
	if err != nil {
		return buildOOMHistory{}, err
	}
	if len(raw) > 0 {
		return decodeBuildOOMHistory(raw)
	}
	// Seed once from retained, structured pre-upgrade evidence. Never infer an
	// OOM from an operation's final placement error or exit 137 alone.
	history := buildOOMHistory{Version: 1}
	ops, err := s.Store.ListOperationsWithDesiredSourceByApp(app.TenantID, false, app.ID)
	if err != nil {
		return history, err
	}
	matching := map[string]bool{}
	for _, op := range ops {
		if op.DesiredSource != nil && op.DesiredSpec != nil && buildOOMInputFingerprint(*op.DesiredSource, *op.DesiredSpec) == fingerprint {
			matching[op.ID] = true
		}
	}
	for operationID := range matching {
		// Per-operation retention is below this limit. Reading per operation
		// avoids losing an older, higher OOM behind an app-wide evidence page.
		evidence, err := s.Store.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: app.TenantID, AppID: app.ID, OperationID: operationID, Types: []string{model.OperationEvidenceTypeBuildAttempt}, Limit: 1000})
		if err != nil {
			return history, err
		}
		for _, e := range evidence {
			data, err := json.Marshal(e.Payload["build_attempt"])
			if err != nil {
				return history, err
			}
			var fact model.BuilderAttemptEvidence
			if json.Unmarshal(data, &fact) != nil || fact.Outcome != "failed" || fact.MemoryLimitBytes <= 0 {
				continue
			}
			confirmed := false
			for _, code := range fact.Causes {
				if code == "memory_limit_exceeded" {
					confirmed = true
				}
			}
			if confirmed && fact.MemoryLimitBytes > history.MemoryLimitBytes {
				history.OperationID = e.OperationID
				history.BuilderOOMObservation = sourceimport.BuilderOOMObservation{MemoryLimitBytes: fact.MemoryLimitBytes, Attempt: fact.Attempt, ObservedAt: e.ObservedAt}
			}
		}
	}
	if err := s.writeBuildOOMHistory(ctx, app, fingerprint, history); err != nil {
		return history, err
	}
	// A concurrent writer may already have published a larger limit.
	raw, err = s.Store.ReadObservationCheckpoint(ctx, buildOOMHistoryKey(app, fingerprint))
	if err != nil {
		return history, err
	}
	return decodeBuildOOMHistory(raw)
}

func (s *Service) writeBuildOOMHistory(ctx context.Context, app model.App, fingerprint string, next buildOOMHistory) error {
	if fingerprint == "" {
		return errors.New("cannot persist OOM without resolved build input")
	}
	for attempt := 0; attempt < 8; attempt++ {
		written, err := s.Store.UpdateObservationCheckpoint(ctx, buildOOMHistoryKey(app, fingerprint), func(raw []byte) ([]byte, error) {
			previous, err := decodeBuildOOMHistory(raw)
			if err != nil {
				return nil, err
			}
			if len(raw) > 0 && previous.MemoryLimitBytes >= next.MemoryLimitBytes {
				if previous.WorkloadProfile == "" && next.WorkloadProfile != "" {
					previous.WorkloadProfile = next.WorkloadProfile
					return json.Marshal(previous)
				}
				return nil, nil
			}
			if next.WorkloadProfile == "" {
				next.WorkloadProfile = previous.WorkloadProfile
			}
			return json.Marshal(next)
		})
		if err != nil || written {
			return err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return errors.New("builder OOM history is concurrently updating")
}

func (s *Service) resolveBuildOOMProfile(ctx context.Context, app model.App, fingerprint, profile string) (buildOOMHistory, error) {
	history, err := s.readBuildOOMHistory(ctx, app, fingerprint)
	if err != nil || history.MemoryLimitBytes == 0 || history.WorkloadProfile != "" {
		return history, err
	}
	if profile != "light" && profile != "heavy" {
		return history, errors.New("invalid executed builder profile")
	}
	history.WorkloadProfile = profile
	if err := s.writeBuildOOMHistory(ctx, app, fingerprint, history); err != nil {
		return history, err
	}
	return history, nil
}

func (s *Service) recordBuildOOM(ctx context.Context, app model.App, opID, fingerprint string, fact sourceimport.BuilderOOMObservation) error {
	if fact.MemoryLimitBytes <= 0 {
		return errors.New("invalid executed OOM memory limit")
	}
	return s.writeBuildOOMHistory(ctx, app, fingerprint, buildOOMHistory{1, opID, fact})
}

func (s *Service) githubBuildOOMAdmission(ctx context.Context, app model.App, source model.AppSource, spec model.AppSpec, policy sourceimport.BuilderPodPolicy) (bool, error) {
	fingerprint := buildOOMInputFingerprint(source, spec)
	history, err := s.readBuildOOMHistory(ctx, app, fingerprint)
	if err != nil {
		return false, err
	}
	if history.MemoryLimitBytes == 0 {
		return true, nil
	}
	strategy := source.BuildStrategy
	switch history.WorkloadProfile {
	case "heavy":
		strategy = model.AppBuildStrategyDockerfile
	case "light":
		strategy = model.AppBuildStrategyStaticSite
	default:
		if strings.TrimSpace(strategy) == "" || strings.EqualFold(strings.TrimSpace(strategy), model.AppBuildStrategyAuto) {
			// Legacy evidence lacks the resolved profile. Allow one operation
			// to resolve it from source, then persist it before Pod admission.
			// The execution guard still enforces the historical memory limit.
			return true, nil
		}
	}
	inspect := s.inspectBuilderOOMPlacement
	if inspect == nil {
		inspect = sourceimport.InspectBuilderPlacementAfterOOM
	}
	checkCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	stateful := spec.Workspace != nil || spec.PersistentStorage != nil || spec.Postgres != nil
	labels := s.importBuildPlacementNodeSelector(ctx, app, model.Operation{DesiredSpec: &spec, DesiredSource: &source})
	namespace := s.Config.KubectlNamespace
	if namespace == "" {
		namespace = "fugue-system"
	}
	report, err := inspect(checkCtx, namespace, policy, strategy, stateful, labels, history.MemoryLimitBytes)
	var blocked *sourceimport.BuilderMemoryAdmissionError
	if err != nil && !errors.As(err, &blocked) {
		return false, err
	}
	var available int64
	for _, node := range report.Nodes {
		if node.Eligible && node.Allocation.MemoryBytes > history.MemoryLimitBytes {
			return true, nil
		}
		if node.Allocation.MemoryBytes > available {
			available = node.Allocation.MemoryBytes
		}
	}
	message := (&sourceimport.BuilderMemoryAdmissionError{PreviousOOMBytes: history.MemoryLimitBytes, AllocatedBytes: available}).Error()
	now := s.gitHubSyncNow()
	_, err = s.Store.RecordAppSourceSyncCheck(ctx, app, &model.AppSourceSyncStatus{Provider: model.AppSourceSyncProviderGitHub, Phase: model.AppSourceSyncPhaseDegraded, LastCheckedAt: &now, LastSuccessAt: &now, LastErrorAt: &now, LastErrorCode: "builder_memory_not_increased", LastErrorMessage: message})
	if err != nil {
		return false, fmt.Errorf("record build admission block: %w", err)
	}
	return false, nil
}
