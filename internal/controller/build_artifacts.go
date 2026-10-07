package controller

import (
	"context"
	"fmt"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
	"time"
)

func (s *Service) recordBuildArtifact(ctx context.Context, app model.App, op model.Operation, job, ref string, complete bool, destination importImageDestination) error {
	if !s.imageStoreDistributedMode() {
		return nil
	}
	a := model.BuildArtifact{TenantID: app.TenantID, AppID: app.ID, OperationID: op.ID, JobName: job, ImageRef: ref, CacheEndpoint: destination.CacheEndpoint, ClusterNodeName: destination.Target.ClusterNodeName}
	var err error
	if !complete {
		_, err = s.Store.SaveBuildArtifact(a)
		return err
	}
	if destination.CacheEndpoint == "" {
		destination, err = s.completedBuilderImageDestination(ctx, app, op, sourceimport.GitHubImportResult{BuildJobName: job, ImageRef: ref, DestinationImageRef: cacheEndpointImageRef(s.builderRegistryPushBase, ref)})
		if err != nil {
			return fmt.Errorf("locate artifact receipt: %w", err)
		}
	}
	digest, _, err := s.resolveImportedImageDigestFromDestinationCache(ctx, ref, destination)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	a.Digest = digest
	a.CacheEndpoint = destination.CacheEndpoint
	a.ClusterNodeName = destination.Target.ClusterNodeName
	a.VerifiedAt = &now
	_, err = s.Store.SaveBuildArtifact(a)
	return err
}

// A failed deployment does not negate a verified build. Receipts are consumed
// independently of operation status and restore only the exact physical digest.
func (s *Service) reconcileBuildArtifacts(ctx context.Context) error {
	if !s.imageStoreDistributedMode() {
		return nil
	}
	artifacts, err := s.Store.ListBuildArtifacts()
	if err != nil {
		return err
	}
	for _, a := range artifacts {
		if err := ctx.Err(); err != nil {
			return err
		}
		if a.VerifiedAt != nil {
			continue
		}
		// Retry while the current build Job can still prove its UID/owner/node.
		// Expired jobs are never reconstructed from their name or mutable tag alone.
		if time.Since(a.RegisteredAt) > 7*24*time.Hour {
			continue
		}
		app, err := s.Store.GetAppMetadata(a.AppID)
		if err != nil {
			continue
		}
		op, err := s.Store.GetOperation(a.OperationID)
		if err != nil {
			continue
		}
		if app.TenantID != a.TenantID || op.AppID != a.AppID || op.TenantID != a.TenantID {
			continue
		}
		destination := importImageDestination{}
		if a.JobName == "image-copy" {
			runtime, found := s.runtimeForClusterNode(ctx, a.ClusterNodeName)
			if !found {
				continue
			}
			destination = s.importImageDestinationForRuntime(runtime)
			if destination.CacheEndpoint != a.CacheEndpoint {
				continue
			}
		}
		_ = s.recordBuildArtifact(ctx, app, op, a.JobName, a.ImageRef, true, destination)
	}
	return nil
}
