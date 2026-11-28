package buildadmission

import (
	"context"
	"errors"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/sourceimport"
)

type InspectPlacement func(ctx context.Context, strategy string, stateful bool, previousOOMBytes int64) (model.BuilderPlacementInspection, error)

// EvaluatePlacement checks whether an automatic build may be queued after an
// OOM. A nil rejection and nil error permit it. A rejection is an expected
// domain result; infrastructure errors leave the source-sync status unchanged.
func (s *Service) EvaluatePlacement(ctx context.Context, app model.App, source model.AppSource, spec model.AppSpec, inspect InspectPlacement) (*sourceimport.BuilderMemoryAdmissionError, error) {
	history, err := s.ReadOOMHistory(ctx, app, OOMInputFingerprint(source, spec))
	if err != nil || history.MemoryLimitBytes == 0 {
		return nil, err
	}
	strategy := source.BuildStrategy
	switch history.WorkloadProfile {
	case "heavy":
		strategy = model.AppBuildStrategyDockerfile
	case "light":
		strategy = model.AppBuildStrategyStaticSite
	default:
		if strings.TrimSpace(strategy) == "" || strings.EqualFold(strings.TrimSpace(strategy), model.AppBuildStrategyAuto) {
			// One operation can resolve a profile missing from legacy evidence.
			// Its execution guard still enforces the historical memory limit.
			return nil, nil
		}
	}
	checkCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	stateful := spec.Workspace != nil || spec.PersistentStorage != nil || spec.Postgres != nil
	report, err := inspect(checkCtx, strategy, stateful, history.MemoryLimitBytes)
	var blocked *sourceimport.BuilderMemoryAdmissionError
	if err != nil && !errors.As(err, &blocked) {
		return nil, err
	}
	var available int64
	for _, node := range report.Nodes {
		if node.Eligible && node.Allocation.MemoryBytes > history.MemoryLimitBytes {
			return nil, nil
		}
		if node.Allocation.MemoryBytes > available {
			available = node.Allocation.MemoryBytes
		}
	}
	return &sourceimport.BuilderMemoryAdmissionError{PreviousOOMBytes: history.MemoryLimitBytes, AllocatedBytes: available}, nil
}
