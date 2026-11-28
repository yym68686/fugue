package buildadmission

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/buildadmission/contract"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

type permanentBuildFailure struct {
	OperationID string    `json:"operation_id"`
	Resource    string    `json:"resource"`
	LimitBytes  int64     `json:"limit_bytes"`
	ObservedAt  time.Time `json:"observed_at"`
}

type FailureLedger struct {
	Version  int                              `json:"version"`
	Failures map[string]permanentBuildFailure `json:"failures"`
}

func buildFailureLedgerKey(app model.App) string {
	return contract.Failure(app.TenantID, app.ID)
}

func decodeBuildFailureLedger(raw []byte) (FailureLedger, error) {
	ledger := FailureLedger{Version: 1, Failures: map[string]permanentBuildFailure{}}
	if len(raw) == 0 {
		return ledger, nil
	}
	if err := json.Unmarshal(raw, &ledger); err != nil {
		return ledger, err
	}
	if ledger.Version != 1 || ledger.Failures == nil {
		return ledger, errors.New("unsupported permanent build failure ledger")
	}
	return ledger, nil
}

func InputFingerprint(source model.AppSource, spec model.AppSpec, policy sourceimport.BuilderPodPolicy) string {
	if source.CommitSHA == "" && source.ArchiveSHA256 == "" {
		return ""
	}
	// Keep only inputs, excluding credentials and mutable output metadata.
	input := model.AppSource{
		Type: source.Type, RepoURL: source.RepoURL, CommitSHA: source.CommitSHA,
		ArchiveSHA256: source.ArchiveSHA256, SourceDir: source.SourceDir,
		BuildStrategy: source.BuildStrategy, DockerfilePath: source.DockerfilePath,
		BuildContextDir: source.BuildContextDir, ComposeService: source.ComposeService,
	}
	if input.BuildStrategy == "" {
		input.BuildStrategy = model.AppBuildStrategyAuto
	}
	// Environment/file contents are hashed in memory, never persisted as facts.
	data, _ := json.Marshal(struct {
		Version  int
		Source   model.AppSource
		Env      map[string]string
		Files    []model.AppFile
		Stateful bool
		Policy   sourceimport.BuilderPodPolicy
	}{1, input, spec.Env, spec.Files, spec.Workspace != nil || spec.PersistentStorage != nil || spec.Postgres != nil, policy})
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data))
}

func (s *Service) ReadFailureLedger(ctx context.Context, app model.App) (FailureLedger, error) {
	raw, err := s.checkpoint.Read(ctx, buildFailureLedgerKey(app))
	if err != nil {
		return FailureLedger{}, err
	}
	return decodeBuildFailureLedger(raw)
}

func (s *Service) RecordPermanentFailure(ctx context.Context, app model.App, opID, fingerprint string, failure *sourceimport.BuilderResourceFailure) error {
	if fingerprint == "" || failure == nil {
		return nil
	}
	written, err := s.checkpoint.Update(ctx, buildFailureLedgerKey(app), func(raw []byte) ([]byte, error) {
		ledger, err := decodeBuildFailureLedger(raw)
		if err != nil {
			return nil, err
		}
		if _, exists := ledger.Failures[fingerprint]; exists {
			return nil, nil
		}
		if len(ledger.Failures) >= 4096 {
			return nil, errors.New("permanent build failure ledger requires archival; existing guards retained")
		}
		ledger.Failures[fingerprint] = permanentBuildFailure{opID, failure.Resource, failure.LimitBytes, time.Now().UTC()}
		return json.Marshal(ledger)
	})
	if err != nil {
		return err
	}
	if !written {
		return errors.New("permanent build failure ledger is concurrently updating")
	}
	return nil
}

func CachedRejection(ledger FailureLedger, fingerprint string) error {
	if previous, ok := ledger.Failures[fingerprint]; ok && fingerprint != "" {
		return &sourceimport.BuilderResourceFailure{Resource: previous.Resource, LimitBytes: previous.LimitBytes, Preflight: true}
	}
	return nil
}

// A changed input/policy can resume even after the old automatic retry budget
// was exhausted. Ordinary transient failures retain their existing backoff.
func PolicyChangedSinceFailure(ledger FailureLedger, op model.Operation, fingerprint string) bool {
	if strings.TrimSpace(op.ID) == "" || fingerprint == "" {
		return false
	}
	for prior, failure := range ledger.Failures {
		if failure.OperationID == op.ID && prior != fingerprint {
			return true
		}
	}
	return false
}
