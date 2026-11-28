package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/buildadmission"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"fugue/internal/store"
)

type builderAdmissionStore struct{ db *store.Store }

func (s builderAdmissionStore) Operations(app model.App) ([]model.Operation, error) {
	return s.db.ListOperationsWithDesiredSourceByApp(app.TenantID, false, app.ID)
}

func (s builderAdmissionStore) Attempts(app model.App, operationID string) ([]model.OperationEvidence, error) {
	return s.db.ListOperationEvidence(model.OperationEvidenceFilter{TenantID: app.TenantID, AppID: app.ID,
		OperationID: operationID, Types: []string{model.OperationEvidenceTypeBuildAttempt}, Limit: 1000})
}

func (s *Service) buildAdmission() *buildadmission.Service {
	return buildadmission.New(s.Store.BuilderAdmissionCheckpoints(), builderAdmissionStore{db: s.Store})
}

func (s *Service) effectiveImportBuilderPolicy(ceiling int64) sourceimport.BuilderPodPolicy {
	policy := sourceimport.BuilderPodPolicy{}
	if importer, ok := s.importer.(*sourceimport.Importer); ok {
		policy = importer.BuilderPolicy
	}
	return sourceimport.EffectiveBuilderPolicy(policy, ceiling)
}

func (s *Service) githubBuildOOMAdmission(ctx context.Context, app model.App, source model.AppSource, spec model.AppSpec, policy sourceimport.BuilderPodPolicy) (bool, error) {
	fingerprint := buildadmission.OOMInputFingerprint(source, spec)
	history, err := s.buildAdmission().ReadOOMHistory(ctx, app, fingerprint)
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
