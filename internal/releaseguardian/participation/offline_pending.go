package participation

import (
	"context"
	"errors"

	"fugue/internal/productioncontrol"
	"k8s.io/client-go/rest"
)

type offlinePendingJournal interface {
	ReadIntent(context.Context, string) (productioncontrol.Intent, []byte, error)
	ReadExecution(context.Context, productioncontrol.Scope, string) (productioncontrol.Claim, *productioncontrol.ExecutionResult, error)
	ReadResource(context.Context, productioncontrol.Scope) (productioncontrol.ResourceState, error)
	ReadParticipationTopology(context.Context, string) (productioncontrol.VerifiedParticipationTopology, error)
}

type PendingOfflineHoldReadback struct {
	IntentDigest   string                    `json:"intentDigest"`
	ApprovalDigest string                    `json:"approvalDigest"`
	Fence          int64                     `json:"fence"`
	LastVerified   productioncontrol.Version `json:"lastVerified"`
	Topology       productioncontrol.Version `json:"topology"`
	Provider       OfflineHoldReadback       `json:"provider"`
}

// ObservePendingOfflineHold combines a provider observation with the same
// durable pending claim and approved topology before and after the reads.
// An expired owner approval does not hide an already dispatched claim.
// This is not a settlement or redispatch entry point.
func ObservePendingOfflineHold(ctx context.Context, j *productioncontrol.Journal, config *rest.Config, cluster, intentDigest string) (PendingOfflineHoldReadback, error) {
	if j == nil {
		return PendingOfflineHoldReadback{}, errors.New("offline hold readback requires a production journal")
	}
	return observePendingOfflineHold(ctx, j, cluster, intentDigest, func(ctx context.Context, topology productioncontrol.VerifiedParticipationTopology, s productioncontrol.Submission) (OfflineHoldReadback, error) {
		o, err := NewBoundKubernetesObserver(config, topology.Policy)
		if err != nil {
			return OfflineHoldReadback{}, err
		}
		return o.ObserveOfflineHoldReadback(ctx, topology, s)
	})
}

func observePendingOfflineHold(ctx context.Context, j offlinePendingJournal, cluster, intentDigest string, observe func(context.Context, productioncontrol.VerifiedParticipationTopology, productioncontrol.Submission) (OfflineHoldReadback, error)) (PendingOfflineHoldReadback, error) {
	intent, raw, err := j.ReadIntent(ctx, intentDigest)
	if err != nil {
		return PendingOfflineHoldReadback{}, err
	}
	s := productioncontrol.Submission{Intent: intent, Payload: raw}
	p, err := decodeHold(s)
	actual, digestErr := intent.Digest()
	if err != nil || digestErr != nil || !exactHoldPhase(p.Phase) || intent.ClusterID != cluster || actual != intentDigest {
		return PendingOfflineHoldReadback{}, errors.New("pending hold does not identify this journal cluster and offline hold or rollback intent")
	}
	scope := HoldResource(cluster, p.To.Node)
	predecessor := productioncontrol.Version{Generation: intent.Generation, Digest: intent.CurrentDigest}
	readPending := func() (productioncontrol.Claim, productioncontrol.VerifiedParticipationTopology, error) {
		claim, terminal, err := j.ReadExecution(ctx, scope, intentDigest)
		if err != nil || terminal != nil || claim.Scope != scope || claim.IntentDigest != intentDigest ||
			claim.Fence < 1 || !canonicalDigest(claim.ApprovalDigest) {
			return productioncontrol.Claim{}, productioncontrol.VerifiedParticipationTopology{}, errors.New("offline hold has no readable unresolved claim")
		}
		head, err := j.ReadResource(ctx, scope)
		if err != nil || head.Scope != scope || head.Pending != intentDigest || head.LastFence != claim.Fence ||
			head.LastVerified != predecessor {
			return productioncontrol.Claim{}, productioncontrol.VerifiedParticipationTopology{}, errors.New("pending offline hold no longer owns the exact journal fence")
		}
		topology, err := j.ReadParticipationTopology(ctx, cluster)
		if err != nil || topology.Version != p.Topology || topology.ApprovalDigest != p.TopologyApproval ||
			!canonicalDigest(topology.IntentDigest) {
			return productioncontrol.Claim{}, productioncontrol.VerifiedParticipationTopology{}, errors.New("pending offline hold no longer matches the approved topology")
		}
		return claim, topology, nil
	}
	claim, topology, err := readPending()
	if err != nil {
		return PendingOfflineHoldReadback{}, err
	}
	provider, err := observe(ctx, topology, s)
	if err != nil {
		return PendingOfflineHoldReadback{}, err
	}
	latestClaim, latestTopology, err := readPending()
	if err != nil {
		return PendingOfflineHoldReadback{}, err
	}
	if latestClaim != claim || latestTopology.IntentDigest != topology.IntentDigest {
		return PendingOfflineHoldReadback{}, errors.New("offline hold claim or topology changed during provider readback")
	}
	return PendingOfflineHoldReadback{
		IntentDigest: claim.IntentDigest, ApprovalDigest: claim.ApprovalDigest, Fence: claim.Fence,
		LastVerified: predecessor, Topology: topology.Version, Provider: provider,
	}, nil
}
