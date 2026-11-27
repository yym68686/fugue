package participation

import (
	"errors"
	"time"

	"fugue/internal/productioncontrol"
	kubejson "sigs.k8s.io/json"
)

const (
	MaxReplayBytes = 16 << 20
	ReplaySchema   = "fugue.participation-snapshot-replay/v1"
)

// ReplayInput carries a caller-supplied observation for comparing decisions.
// Binding it to an authenticated topology does not authenticate the snapshot,
// its reader baseline, DNS results, clock, or claimed absence of a transport.
type ReplayInput struct {
	Schema       string                 `json:"schema"`
	Cluster      string                 `json:"cluster"`
	Topology     ReplayVersion          `json:"topology"`
	ObservedAt   time.Time              `json:"observedAt"`
	Observations *ParticipationSnapshot `json:"observations"`
}

type ReplayVersion struct {
	Generation int64  `json:"generation"`
	Digest     string `json:"digest"`
}

type ReplayReport struct {
	Kind              string                    `json:"kind"`
	ExecutionEvidence bool                      `json:"executionEvidence"`
	Cluster           string                    `json:"cluster"`
	Topology          productioncontrol.Version `json:"topology"`
	TopologyIntent    string                    `json:"topologyIntent"`
	TopologyApproval  string                    `json:"topologyApproval"`
	ObservationDigest string                    `json:"observationDigest"`
	ObservedAt        time.Time                 `json:"observedAt"`
	Preview           ParticipationPreview      `json:"preview"`
}

// ReplayParticipation has no provider clients or writes. The result format is
// distinct from independently observed or signed execution evidence.
func ReplayParticipation(cluster, node string, head productioncontrol.VerifiedParticipationTopology, raw []byte, now time.Time) (ReplayReport, error) {
	var in ReplayInput
	if len(raw) == 0 || len(raw) > MaxReplayBytes {
		return ReplayReport{}, errors.New("participation replay exceeds the snapshot size boundary")
	}
	strict, err := kubejson.UnmarshalStrict(raw, &in)
	if err != nil || len(strict) != 0 || in.Schema != ReplaySchema || in.Observations == nil {
		return ReplayReport{}, errors.New("invalid participation replay envelope")
	}
	if cluster == "" || in.Cluster != cluster || head.Version.Generation != head.Policy.Generation ||
		head.Version.Generation < 1 || head.Version.Digest == "" ||
		in.Topology != (ReplayVersion{Generation: head.Version.Generation, Digest: head.Version.Digest}) ||
		head.IntentDigest == "" || head.ApprovalDigest == "" || head.RecordedAt.IsZero() {
		return ReplayReport{}, errors.New("participation replay does not bind the current authenticated topology")
	}
	if in.ObservedAt.IsZero() || in.ObservedAt.Before(head.RecordedAt) || in.ObservedAt.After(now) {
		return ReplayReport{}, errors.New("participation replay has inconsistent observation time")
	}
	preview, err := PreviewParticipation(head.Policy, node, *in.Observations, in.ObservedAt)
	if err != nil {
		return ReplayReport{}, err
	}
	return ReplayReport{Kind: "untrusted_snapshot_replay", ExecutionEvidence: false, Cluster: cluster,
		Topology: head.Version, TopologyIntent: head.IntentDigest, TopologyApproval: head.ApprovalDigest,
		ObservationDigest: digest(raw), ObservedAt: in.ObservedAt.UTC(), Preview: preview}, nil
}
