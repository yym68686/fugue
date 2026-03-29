package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"fugue/internal/model"
	"fugue/internal/store"
)

const builderJobLabelSelector = "app.kubernetes.io/managed-by=fugue,app.kubernetes.io/component=builder"

type builderJobClient interface {
	listJobsBySelector(ctx context.Context, namespace, selector string) ([]kubeJobInfo, error)
	deleteJob(ctx context.Context, namespace, name string) error
}

func (s *Service) cleanupZombieBuildJobs(ctx context.Context) error {
	client, err := s.kubeClient()
	if err != nil {
		if strings.Contains(err.Error(), "kubernetes service host/port is not available") || strings.Contains(err.Error(), "resolve kubernetes namespace") {
			return nil
		}
		return fmt.Errorf("create kubernetes client for build job cleanup: %w", err)
	}
	return s.cleanupZombieBuildJobsWithClient(ctx, client)
}

func (s *Service) cleanupZombieBuildJobsWithClient(ctx context.Context, client builderJobClient) error {
	jobs, err := client.listJobsBySelector(ctx, "", builderJobLabelSelector)
	if err != nil {
		return fmt.Errorf("list builder jobs: %w", err)
	}

	var cleanupErr error
	for _, job := range jobs {
		jobName := strings.TrimSpace(job.Metadata.Name)
		if jobName == "" || job.Status.Active <= 0 {
			continue
		}

		shouldDelete, reason, err := s.shouldDeleteBuilderJob(job)
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("inspect builder job %s: %w", jobName, err))
			continue
		}
		if !shouldDelete {
			continue
		}
		if err := client.deleteJob(ctx, "", jobName); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("delete builder job %s: %w", jobName, err))
			continue
		}
		if s.Logger != nil {
			s.Logger.Printf("deleted zombie build job %s: %s", jobName, reason)
		}
	}
	return cleanupErr
}

func (s *Service) shouldDeleteBuilderJob(job kubeJobInfo) (bool, string, error) {
	opID := strings.TrimSpace(job.Metadata.Labels["fugue.pro/operation-id"])
	if opID == "" {
		return true, "job is missing fugue.pro/operation-id label", nil
	}

	op, err := s.Store.GetOperation(opID)
	switch {
	case err == nil:
	case errors.Is(err, store.ErrNotFound):
		return true, fmt.Sprintf("operation %s no longer exists", opID), nil
	default:
		return false, "", err
	}

	if op.Type == model.OperationTypeImport && op.ExecutionMode == model.ExecutionModeManaged && op.Status == model.OperationStatusRunning {
		return false, "", nil
	}
	return true, fmt.Sprintf("operation %s is %s/%s", opID, op.Type, op.Status), nil
}
