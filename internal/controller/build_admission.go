package controller

import (
	"context"
	"fmt"

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
	inspect := s.inspectBuilderOOMPlacement
	if inspect == nil {
		inspect = sourceimport.InspectBuilderPlacementAfterOOM
	}
	rejection, err := s.buildAdmission().EvaluatePlacement(ctx, app, source, spec,
		func(checkCtx context.Context, strategy string, stateful bool, previous int64) (model.BuilderPlacementInspection, error) {
			labels := s.importBuildPlacementNodeSelector(ctx, app, model.Operation{DesiredSpec: &spec, DesiredSource: &source})
			namespace := s.Config.KubectlNamespace
			if namespace == "" {
				namespace = "fugue-system"
			}
			return inspect(checkCtx, namespace, policy, strategy, stateful, labels, previous)
		})
	if err != nil {
		return false, err
	}
	if rejection == nil {
		return true, nil
	}
	now := s.gitHubSyncNow()
	_, err = s.Store.RecordAppSourceSyncCheck(ctx, app, &model.AppSourceSyncStatus{Provider: model.AppSourceSyncProviderGitHub, Phase: model.AppSourceSyncPhaseDegraded, LastCheckedAt: &now, LastSuccessAt: &now, LastErrorAt: &now, LastErrorCode: "builder_memory_not_increased", LastErrorMessage: rejection.Error()})
	if err != nil {
		return false, fmt.Errorf("record build admission block: %w", err)
	}
	return false, nil
}
