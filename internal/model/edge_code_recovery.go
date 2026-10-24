package model

import (
	"errors"
	"strings"
)

type EdgeCodeRecoveryTarget struct {
	Slot              string `json:"slot"`
	FrontGeneration   uint64 `json:"front_generation"`
	BundleGeneration  string `json:"bundle_generation"`
	WorkerSourceSHA   string `json:"worker_source_sha"`
	WorkerImageDigest string `json:"worker_image_digest"`
}

type EdgeCodeRecoveryPlan struct {
	Schema                string                   `json:"schema"`
	AuthorityRecordDigest string                   `json:"authority_record_digest"`
	Observed              EdgeCodeRecoveryTarget   `json:"observed"`
	Stages                []EdgeCodeRecoveryTarget `json:"stages"`
	ExecutionAuthorized   bool                     `json:"execution_authorized"`
}

func (t EdgeCodeRecoveryTarget) Valid() bool {
	return (t.Slot == "a" || t.Slot == "b") && t.FrontGeneration > 0 && t.FrontGeneration < ^uint64(0) && len(t.BundleGeneration) > 0 && len(t.BundleGeneration) <= 512 && !strings.ContainsAny(t.BundleGeneration, " \t\r\n\x00") && len(t.WorkerSourceSHA) == 40 && strings.Trim(t.WorkerSourceSHA, "0123456789abcdef") == "" && recoveryCodeDigest(t.WorkerImageDigest)
}

func recoveryCodeDigest(value string) bool {
	return strings.HasPrefix(value, "sha256:") && len(value) == 71 && strings.Trim(value[7:], "0123456789abcdef") == ""
}

// PlanEdgeCodeRecovery is a pure projection, not a serving or mutation grant.
// Replanning after a completed bridge naturally leaves only the current stage.
// No new fence, image or bundle is invented to bridge an unavailable history.
func PlanEdgeCodeRecovery(record string, observed, current EdgeCodeRecoveryTarget, previous *EdgeCodeRecoveryTarget) (EdgeCodeRecoveryPlan, error) {
	p := EdgeCodeRecoveryPlan{Schema: "fugue.edge-code-recovery-plan/v1", AuthorityRecordDigest: record, Observed: observed, Stages: []EdgeCodeRecoveryTarget{}}
	if !recoveryCodeDigest(record) || !observed.Valid() || !current.Valid() {
		return p, errors.New("code recovery identity unavailable")
	}
	if observed == current {
		return p, nil
	}
	if observed.FrontGeneration >= current.FrontGeneration {
		return p, errors.New("member does not precede current code authority")
	}
	if observed.Slot == current.Slot {
		if previous == nil || !previous.Valid() || previous.Slot == current.Slot || previous.FrontGeneration <= observed.FrontGeneration || previous.FrontGeneration >= current.FrontGeneration {
			return p, errors.New("same-slot recovery lacks an intervening retained authority")
		}
		p.Stages = append(p.Stages, *previous)
	}
	p.Stages = append(p.Stages, current)
	return p, nil
}
