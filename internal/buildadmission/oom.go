package buildadmission

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"fugue/internal/buildadmission/contract"
	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

type OOMHistory struct {
	Version     int    `json:"version"`
	OperationID string `json:"operation_id,omitempty"`
	sourceimport.BuilderOOMObservation
}

// Resource policies are evaluated separately; changing a ceiling must not erase
// an OOM fact for unchanged code/configuration.
func OOMInputFingerprint(source model.AppSource, spec model.AppSpec) string {
	return InputFingerprint(source, spec, sourceimport.BuilderPodPolicy{})
}

func oomHistoryKey(app model.App, fingerprint string) string {
	return contract.OOM(app.TenantID, app.ID, fingerprint)
}

func DecodeOOMHistory(raw []byte) (OOMHistory, error) {
	history := OOMHistory{Version: 1}
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

func (s *Service) ReadOOMHistory(ctx context.Context, app model.App, fingerprint string) (OOMHistory, error) {
	if fingerprint == "" {
		return OOMHistory{}, errors.New("build OOM admission requires resolved inputs")
	}
	raw, err := s.checkpoint.Read(ctx, oomHistoryKey(app, fingerprint))
	if err != nil {
		return OOMHistory{}, err
	}
	if len(raw) > 0 {
		return DecodeOOMHistory(raw)
	}
	// Seed once from retained, structured pre-upgrade evidence. Never infer an
	// OOM from an operation's final placement error or exit 137 alone.
	history := OOMHistory{Version: 1}
	ops, err := s.evidence.Operations(app)
	if err != nil {
		return history, err
	}
	matching := map[string]bool{}
	for _, op := range ops {
		if op.DesiredSource != nil && op.DesiredSpec != nil && OOMInputFingerprint(*op.DesiredSource, *op.DesiredSpec) == fingerprint {
			matching[op.ID] = true
		}
	}
	for operationID := range matching {
		// Per-operation retention is below this limit. Reading per operation
		// avoids losing an older, higher OOM behind an app-wide evidence page.
		evidence, err := s.evidence.Attempts(app, operationID)
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
	raw, err = s.checkpoint.Read(ctx, oomHistoryKey(app, fingerprint))
	if err != nil {
		return history, err
	}
	return DecodeOOMHistory(raw)
}

func (s *Service) writeBuildOOMHistory(ctx context.Context, app model.App, fingerprint string, next OOMHistory) error {
	if fingerprint == "" {
		return errors.New("cannot persist OOM without resolved build input")
	}
	if ctx == nil {
		return errors.New("cannot persist OOM without a context")
	}
	writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	delay := 10 * time.Millisecond
	for {
		written, err := s.checkpoint.Update(writeCtx, oomHistoryKey(app, fingerprint), func(raw []byte) ([]byte, error) {
			previous, err := DecodeOOMHistory(raw)
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
		timer := time.NewTimer(delay)
		select {
		case <-writeCtx.Done():
			timer.Stop()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errors.New("builder OOM history is concurrently updating")
		case <-timer.C:
		}
		if delay < 80*time.Millisecond {
			delay *= 2
		}
	}
}

func (s *Service) ResolveOOMProfile(ctx context.Context, app model.App, fingerprint, profile string) (OOMHistory, error) {
	history, err := s.ReadOOMHistory(ctx, app, fingerprint)
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

func (s *Service) RecordOOM(ctx context.Context, app model.App, opID, fingerprint string, fact sourceimport.BuilderOOMObservation) error {
	if fact.MemoryLimitBytes <= 0 {
		return errors.New("invalid executed OOM memory limit")
	}
	return s.writeBuildOOMHistory(ctx, app, fingerprint, OOMHistory{1, opID, fact})
}
