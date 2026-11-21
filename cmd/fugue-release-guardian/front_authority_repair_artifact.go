package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

const (
	frontRepairActionsMetadataMax  = 1 << 20
	frontRepairActionsArchiveMax   = 8 << 20
	frontRepairActionsManifestMax  = 4 << 10
	frontRepairActionsWorkflowPath = ".github/workflows/ci.yml"
	frontRepairActionsBuildStep    = "Build and validate standalone Front repair artifacts"
	frontRepairActionsUploadStep   = "Upload immutable Front repair tool inventory"
)

type frontRepairActionsPolicy struct {
	RepositoryID uint64
	WorkflowID   uint64
	SourceCommit string
}

type frontRepairActionsRepository struct {
	ID uint64 `json:"id"`
}

type frontRepairActionsRun struct {
	ID             uint64                       `json:"id"`
	WorkflowID     uint64                       `json:"workflow_id"`
	RunAttempt     uint32                       `json:"run_attempt"`
	Path           string                       `json:"path"`
	Event          string                       `json:"event"`
	HeadBranch     string                       `json:"head_branch"`
	HeadSHA        string                       `json:"head_sha"`
	Status         string                       `json:"status"`
	RunStartedAt   time.Time                    `json:"run_started_at"`
	Repository     frontRepairActionsRepository `json:"repository"`
	HeadRepository frontRepairActionsRepository `json:"head_repository"`
}

type frontRepairActionsStep struct {
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Conclusion  string    `json:"conclusion"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
}

type frontRepairActionsJob struct {
	ID          uint64                   `json:"id"`
	RunID       uint64                   `json:"run_id"`
	RunAttempt  uint32                   `json:"run_attempt"`
	HeadBranch  string                   `json:"head_branch"`
	HeadSHA     string                   `json:"head_sha"`
	Name        string                   `json:"name"`
	Status      string                   `json:"status"`
	Conclusion  string                   `json:"conclusion"`
	StartedAt   time.Time                `json:"started_at"`
	CompletedAt time.Time                `json:"completed_at"`
	Steps       []frontRepairActionsStep `json:"steps"`
}

type frontRepairActionsArtifactRun struct {
	ID               uint64 `json:"id"`
	RepositoryID     uint64 `json:"repository_id"`
	HeadRepositoryID uint64 `json:"head_repository_id"`
	HeadBranch       string `json:"head_branch"`
	HeadSHA          string `json:"head_sha"`
}

type frontRepairActionsArtifact struct {
	ID          uint64                        `json:"id"`
	Name        string                        `json:"name"`
	Digest      string                        `json:"digest"`
	Size        int64                         `json:"size_in_bytes"`
	Expired     *bool                         `json:"expired"`
	CreatedAt   time.Time                     `json:"created_at"`
	ExpiresAt   time.Time                     `json:"expires_at"`
	WorkflowRun frontRepairActionsArtifactRun `json:"workflow_run"`
}

type frontRepairVerifiedArtifact struct {
	identity frontRepairArtifactIdentity
	tools    frontRepairToolSet
}

// Called by the protected launcher with API response bodies authenticated
// for the pinned repository and requested run attempt, producer job, and
// artifact. Repository policy is not derived from those response bodies or
// the signed approval. This verifier performs no network or Kubernetes writes.
func readFrontRepairActionsArtifact(policy frontRepairActionsPolicy, expected frontRepairArtifactIdentity,
	runResponse, jobResponse, artifactResponse, archive io.Reader, now time.Time) (frontRepairVerifiedArtifact, error) {
	var run frontRepairActionsRun
	var job frontRepairActionsJob
	var artifact frontRepairActionsArtifact
	if err := decodeFrontRepairActionsMetadata(runResponse, &run); err != nil {
		return frontRepairVerifiedArtifact{}, fmt.Errorf("Front repair workflow attempt metadata: %w", err)
	}
	if err := decodeFrontRepairActionsMetadata(jobResponse, &job); err != nil {
		return frontRepairVerifiedArtifact{}, fmt.Errorf("Front repair producer job metadata: %w", err)
	}
	if err := decodeFrontRepairActionsMetadata(artifactResponse, &artifact); err != nil {
		return frontRepairVerifiedArtifact{}, fmt.Errorf("Front repair artifact metadata: %w", err)
	}
	if err := validateFrontRepairActionsMetadata(policy, expected, run, job, artifact, now); err != nil {
		return frontRepairVerifiedArtifact{}, err
	}
	if archive == nil {
		return frontRepairVerifiedArtifact{}, errors.New("Front repair artifact archive is missing")
	}
	data, err := io.ReadAll(io.LimitReader(archive, artifact.Size+1))
	if err != nil || int64(len(data)) != artifact.Size || shaDigest(data) != expected.ArchiveDigest {
		return frontRepairVerifiedArtifact{}, errors.New("Front repair archive differs from the authenticated byte count or digest")
	}
	tools, err := decodeFrontRepairActionsArchive(data, policy.SourceCommit)
	if err != nil {
		return frontRepairVerifiedArtifact{}, err
	}
	return frontRepairVerifiedArtifact{identity: expected, tools: tools}, nil
}

func decodeFrontRepairActionsMetadata(src io.Reader, dst any) error {
	if src == nil {
		return errors.New("missing metadata document")
	}
	raw, err := io.ReadAll(io.LimitReader(src, frontRepairActionsMetadataMax+1))
	if err != nil || len(raw) == 0 || len(raw) > frontRepairActionsMetadataMax {
		return errors.New("metadata is invalid or exceeds its bound")
	}
	// This is GitHub's full API response; fields not relevant to the binding
	// need not be projected and serialized by the caller.
	if err := json.Unmarshal(raw, dst); err != nil {
		return errors.New("invalid Actions metadata JSON")
	}
	return nil
}

func validateFrontRepairActionsMetadata(policy frontRepairActionsPolicy, expected frontRepairArtifactIdentity,
	run frontRepairActionsRun, job frontRepairActionsJob, artifact frontRepairActionsArtifact, now time.Time) error {
	if !expected.valid() || !exactSourceSHA(policy.SourceCommit) || policy.RepositoryID == 0 || policy.WorkflowID == 0 ||
		expected.RepositoryID != policy.RepositoryID || now.IsZero() ||
		run.ID != expected.WorkflowRunID || run.RunAttempt != expected.RunAttempt || run.WorkflowID != policy.WorkflowID ||
		run.Repository.ID != policy.RepositoryID || run.HeadRepository.ID != policy.RepositoryID ||
		run.Path != frontRepairActionsWorkflowPath || run.Event != "push" || run.HeadBranch != "main" ||
		run.HeadSHA != policy.SourceCommit || run.Status != "completed" || run.RunStartedAt.IsZero() || run.RunStartedAt.After(now) {
		return errors.New("Front repair artifact did not originate in the pinned main workflow attempt")
	}
	if job.ID != expected.ProducerJobID || job.RunID != run.ID || job.RunAttempt != run.RunAttempt ||
		job.HeadSHA != run.HeadSHA || job.HeadBranch != run.HeadBranch || job.Name != "prepush" ||
		job.Status != "completed" || job.Conclusion != "success" ||
		job.StartedAt.IsZero() || job.StartedAt.Before(run.RunStartedAt) ||
		job.CompletedAt.IsZero() || job.CompletedAt.Before(job.StartedAt) || job.CompletedAt.After(now) {
		return errors.New("Front repair artifact producer did not pass the protected prepush job for this attempt")
	}
	build, buildSeen := frontRepairActionsBuildStep, false
	upload, uploadSeen := frontRepairActionsUploadStep, false
	var built, uploaded frontRepairActionsStep
	for _, step := range job.Steps {
		switch step.Name {
		case build:
			if buildSeen || !validFrontRepairActionsStep(job, step) {
				return errors.New("Front repair artifact build step is duplicated or unsuccessful")
			}
			built, buildSeen = step, true
		case upload:
			if uploadSeen || !validFrontRepairActionsStep(job, step) {
				return errors.New("Front repair artifact upload step is duplicated or unsuccessful")
			}
			uploaded, uploadSeen = step, true
		}
	}
	if !buildSeen || !uploadSeen || uploaded.StartedAt.Before(built.CompletedAt) {
		return errors.New("Front repair artifact build and upload are not both complete in order")
	}
	wantName := fmt.Sprintf("front-repair-tools-%s-%d-%d", policy.SourceCommit, run.ID, run.RunAttempt)
	wireRun := artifact.WorkflowRun
	if artifact.ID != expected.ArtifactID || artifact.Name != wantName || artifact.Digest != expected.ArchiveDigest ||
		artifact.Size <= 0 || artifact.Size > frontRepairActionsArchiveMax || artifact.Expired == nil || *artifact.Expired ||
		artifact.CreatedAt.IsZero() || artifact.CreatedAt.Before(uploaded.StartedAt) || artifact.CreatedAt.After(uploaded.CompletedAt) ||
		!artifact.ExpiresAt.After(now) || wireRun.ID != run.ID ||
		wireRun.RepositoryID != policy.RepositoryID || wireRun.HeadRepositoryID != policy.RepositoryID ||
		wireRun.HeadBranch != run.HeadBranch || wireRun.HeadSHA != run.HeadSHA {
		return errors.New("Front repair artifact metadata does not bind the signed immutable upload from this attempt")
	}
	return nil
}

func validFrontRepairActionsStep(job frontRepairActionsJob, step frontRepairActionsStep) bool {
	return step.Status == "completed" && step.Conclusion == "success" && !step.StartedAt.IsZero() &&
		!step.StartedAt.Before(job.StartedAt) && !step.CompletedAt.Before(step.StartedAt) && !step.CompletedAt.After(job.CompletedAt)
}

func decodeFrontRepairActionsArchive(raw []byte, source string) (frontRepairToolSet, error) {
	if len(raw) == 0 || len(raw) > frontRepairActionsArchiveMax || !exactSourceSHA(source) {
		return frontRepairToolSet{}, errors.New("Front repair archive size or revision is invalid")
	}
	archive, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil || len(archive.File) != 3 {
		return frontRepairToolSet{}, errors.New("Front repair archive must contain one manifest and exactly two executables")
	}
	binaries := make(map[string][]byte, 2)
	var manifest frontRepairToolManifest
	hasManifest := false
	for _, f := range archive.File {
		var arch string
		limit := uint64(frontRepairToolMaxBytes)
		switch f.Name {
		case "manifest.json":
			limit = frontRepairActionsManifestMax
			if hasManifest {
				return frontRepairToolSet{}, errors.New("Front repair archive has a duplicate manifest")
			}
		case frontRepairToolArtifactPrefix + "amd64":
			arch = "amd64"
		case frontRepairToolArtifactPrefix + "arm64":
			arch = "arm64"
		default:
			return frontRepairToolSet{}, errors.New("Front repair archive has an unexpected entry")
		}
		if !f.Mode().IsRegular() || f.UncompressedSize64 == 0 || f.UncompressedSize64 > limit || binaries[arch] != nil {
			return frontRepairToolSet{}, errors.New("Front repair archive has a linked, oversized or duplicate entry")
		}
		entry, err := f.Open()
		if err != nil {
			return frontRepairToolSet{}, errors.New("Front repair archive entry cannot be opened")
		}
		data, readErr := io.ReadAll(io.LimitReader(entry, int64(limit)+1))
		closeErr := entry.Close()
		if readErr != nil || closeErr != nil || uint64(len(data)) != f.UncompressedSize64 {
			return frontRepairToolSet{}, errors.New("Front repair archive entry integrity check failed")
		}
		if arch == "" {
			hasManifest = true
			if decodeStrictJSON(data, &manifest) != nil {
				return frontRepairToolSet{}, errors.New("Front repair archive manifest is invalid")
			}
		} else {
			binaries[arch] = data
		}
	}
	if !hasManifest || len(binaries) != 2 || len(manifest.Tools) != 2 || manifest.SourceCommit != source {
		return frontRepairToolSet{}, errors.New("Front repair archive omits a required member or has the wrong source")
	}
	return loadFrontRepairToolSet(manifest, binaries)
}
