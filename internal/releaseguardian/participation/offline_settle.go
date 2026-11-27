package participation

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"math"

	"fugue/internal/productioncontrol"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/rest"
)

// OfflineHoldCheckpoint is the stable, provider-observed data the independent
// signer commits to. Authority is separate: a stored freeze is not evidence
// that all endpoints enforce it or that in-flight requests have drained.
type OfflineHoldCheckpoint struct {
	Scope            productioncontrol.Scope   `json:"scope"`
	Topology         productioncontrol.Version `json:"topology"`
	TopologyIntent   string                    `json:"topologyIntent"`
	TopologyApproval string                    `json:"topologyApproval"`
	NamespaceUID     string                    `json:"namespaceUid"`
	Node             ObjectVersion             `json:"node"`
	OwnedDigest      string                    `json:"ownedDigest"`
	Freeze           FreezeObservation         `json:"freeze"`
	PendingIntent    string                    `json:"pendingIntent,omitempty"`
	Approval         string                    `json:"approval,omitempty"`
	Fence            int64                     `json:"fence,omitempty"`
}

func (c OfflineHoldCheckpoint) Digest() (string, error) {
	if !canonicalDigest(c.OwnedDigest) || !canonicalDigest(c.TopologyIntent) || !canonicalDigest(c.TopologyApproval) ||
		c.Topology.Generation < 1 || !canonicalDigest(c.Topology.Digest) || c.NamespaceUID == "" ||
		len(validation.IsDNS1123Subdomain(c.Node.Name)) != 0 || c.Node.UID == "" || c.Node.ResourceVersion == "" ||
		c.Freeze.Node != c.Node.Name || !c.Freeze.CurrentTypeChecked ||
		c.Scope != HoldResource(c.Scope.ClusterID, c.Node.Name) || c.Scope.ClusterID == "" ||
		c.Freeze.Policy.UID == "" || c.Freeze.Policy.ResourceVersion == "" ||
		c.Freeze.Binding.UID == "" || c.Freeze.Binding.ResourceVersion == "" ||
		(c.PendingIntent == "" && (c.Fence != 0 || c.Approval != "")) ||
		(c.PendingIntent != "" && (!canonicalDigest(c.PendingIntent) || c.Fence < 1 || !canonicalDigest(c.Approval))) {
		return "", errors.New("invalid offline hold checkpoint")
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return digest(append([]byte("fugue.participation.offline-checkpoint.v1\x00"), raw...)), nil
}

// ObserveOfflineHoldCheckpoint requires the closed (not exact-open) guard.
// Both the Node and freeze must remain identical around the observation.
// If intentDigest is nonempty, the same unresolved claim must remain open.
func ObserveOfflineHoldCheckpoint(ctx context.Context, j *productioncontrol.Journal, config *rest.Config, cluster, node, intentDigest string) (OfflineHoldCheckpoint, error) {
	if j == nil {
		return OfflineHoldCheckpoint{}, errors.New("offline hold checkpoint requires the production journal")
	}
	var c OfflineHoldCheckpoint
	observe := func(topo productioncontrol.VerifiedParticipationTopology) error {
		o, err := NewBoundKubernetesObserver(config, topo.Policy)
		if err != nil {
			return err
		}
		c, err = o.observeOfflineHoldCheckpoint(ctx, topo, cluster, node)
		return err
	}
	if intentDigest != "" {
		pending, err := observePendingOfflineHold(ctx, j, cluster, intentDigest, func(_ context.Context, topo productioncontrol.VerifiedParticipationTopology, s productioncontrol.Submission) (OfflineHoldReadback, error) {
			p, err := decodeHold(s)
			if err != nil || p.To.Node != node {
				return OfflineHoldReadback{}, errors.New("offline hold is not for the configured Node")
			}
			if err := observe(topo); err != nil {
				return OfflineHoldReadback{}, err
			}
			if c.Node.ResourceVersion == p.ResourceVersion && c.OwnedDigest != s.Intent.CurrentDigest {
				return OfflineHoldReadback{}, errors.New("closed freeze read contradicts the approved predecessor")
			}
			return OfflineHoldReadback{Node: c.Node, ObservedDigest: c.OwnedDigest}, nil
		})
		if err != nil {
			return OfflineHoldCheckpoint{}, err
		}
		c.PendingIntent, c.Fence, c.Approval = pending.IntentDigest, pending.Fence, pending.ApprovalDigest
		return c, nil
	}
	topo, err := j.ReadParticipationTopology(ctx, cluster)
	if err != nil {
		return OfflineHoldCheckpoint{}, err
	}
	if err := observe(topo); err != nil {
		return OfflineHoldCheckpoint{}, err
	}
	latest, err := j.ReadParticipationTopology(ctx, cluster)
	if err != nil || latest.Version != topo.Version || latest.IntentDigest != topo.IntentDigest || latest.ApprovalDigest != topo.ApprovalDigest {
		return OfflineHoldCheckpoint{}, errors.New("approved participation topology changed during baseline observation")
	}
	return c, nil
}

func (o *KubernetesObserver) observeOfflineHoldCheckpoint(ctx context.Context, topo productioncontrol.VerifiedParticipationTopology, cluster, node string) (OfflineHoldCheckpoint, error) {
	member, err := enrolledMember(topo.Policy, node)
	if err != nil {
		return OfflineHoldCheckpoint{}, err
	}
	if err := o.VerifyNamespace(ctx); err != nil {
		return OfflineHoldCheckpoint{}, err
	}
	freeze, err := o.ObserveNodeFreeze(ctx, topo.Policy, node)
	if err != nil || !freeze.CurrentTypeChecked {
		return OfflineHoldCheckpoint{}, errors.New("offline hold checkpoint requires the exact closed, type-checked Node freeze")
	}
	read := func() (ObjectVersion, string, error) {
		n, err := o.core.Nodes().Get(ctx, node, metav1.GetOptions{})
		if err != nil || !exactLiveObject(n, "", node) || string(n.UID) != member.NodeUID ||
			n.Labels[participationGroup] != member.Group || n.Labels[participationRole] != "true" {
			return ObjectVersion{}, "", errors.New("offline hold checkpoint could not read the enrolled physical Node")
		}
		return describeObject(n), HoldStateDigest(node, member.NodeUID, member.Group, n.Annotations), nil
	}
	n, version, err := read()
	if err != nil {
		return OfflineHoldCheckpoint{}, err
	}
	check, verified, err := read()
	if err != nil || check != n || verified != version {
		return OfflineHoldCheckpoint{}, errors.New("offline hold Node changed during checkpoint")
	}
	latest, err := o.ObserveNodeFreeze(ctx, topo.Policy, node)
	if err != nil || latest != freeze {
		return OfflineHoldCheckpoint{}, errors.New("closed Node freeze changed during checkpoint")
	}
	if err := o.VerifyNamespace(ctx); err != nil {
		return OfflineHoldCheckpoint{}, err
	}
	return OfflineHoldCheckpoint{
		Scope: HoldResource(cluster, node), Topology: topo.Version, TopologyIntent: topo.IntentDigest,
		TopologyApproval: topo.ApprovalDigest, NamespaceUID: o.namespaceUID, Node: n, OwnedDigest: version, Freeze: freeze,
	}, nil
}

// NewOfflineHoldSettlement deliberately has only a read provider client.
// It cannot configure RBAC, install the freeze or issue a new write.
func NewOfflineHoldSettlement(j *productioncontrol.Journal, config *rest.Config, cluster, node string, source productioncontrol.WitnessReader, key ed25519.PublicKey) (*productioncontrol.ExecutionJournal, error) {
	if config == nil || len(validation.IsDNS1123Subdomain(node)) != 0 {
		return nil, errors.New("offline hold settlement requires the explicit Node and Kubernetes reader")
	}
	scope := HoldResource(cluster, node)
	return productioncontrol.NewObservedExecutionJournal(j, source, map[string]productioncontrol.TrustedWitness{
		"provider-observer": {Scope: scope, PublicKey: key},
	}, func(ctx context.Context, w productioncontrol.ExecutionWitness) error {
		c, err := ObserveOfflineHoldCheckpoint(ctx, j, config, cluster, node, w.IntentDigest)
		if err != nil {
			return err
		}
		if w.Kind == productioncontrol.TerminalWitness {
			intent, _, err := j.ReadIntent(ctx, c.PendingIntent)
			if err != nil {
				return err
			}
			if err := verifyOfflineTerminalVersion(w, c, intent); err != nil {
				return err
			}
		}
		d, err := c.Digest()
		if err != nil || w.ObservationDigest != d || w.VerifiedDigest != c.OwnedDigest {
			return errors.New("offline hold witness does not match the fresh closed checkpoint")
		}
		return nil
	})
}

func verifyOfflineTerminalVersion(w productioncontrol.ExecutionWitness, c OfflineHoldCheckpoint, i productioncontrol.Intent) error {
	if w.IntentDigest != c.PendingIntent || w.ApprovalDigest != c.Approval || w.Fence != c.Fence {
		return errors.New("offline hold witness is for a different pending claim")
	}
	switch {
	case w.Outcome == productioncontrol.Applied && i.Generation < math.MaxInt64 && w.Generation == i.Generation+1 && c.OwnedDigest == i.TargetDigest:
		return nil
	case w.Outcome == productioncontrol.NotApplied && w.Generation == i.Generation && c.OwnedDigest == i.CurrentDigest:
		return nil
	}
	return errors.New("terminal witness does not verify an applied hold or positively unchanged predecessor; rollback needs a separate operation")
}
