package participation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

type offlineDispatchJournal interface {
	offlinePendingJournal
	ReserveExecution(context.Context, string, string, string) (productioncontrol.Claim, bool, error)
	VerifyOpenClaim(context.Context, productioncontrol.Claim) error
}

type offlineDispatchProvider interface {
	Check(context.Context, productioncontrol.VerifiedParticipationTopology, productioncontrol.Submission, productioncontrol.ResourceState) (*corev1.Node, string, error)
	Write(context.Context, productioncontrol.Submission, *corev1.Node) (*corev1.Node, error)
}

type OfflineHoldDispatch struct {
	Claim                   productioncontrol.Claim `json:"claim"`
	Reserved                bool                    `json:"reserved"`
	ProviderResponseVersion string                  `json:"providerResponseVersion,omitempty"`
}

// DispatchOfflineHold runs explicitly, never from a repository event.
// It can consume one claim but cannot install a guard, approve, settle,
// retry a write, or modify a topology. A worker crash is an unresolved
// claim recovered by observation, not a dispatch that can be taken over.
func DispatchOfflineHold(ctx context.Context, j *productioncontrol.Journal, reader, writerConfig *rest.Config, dns *DNSObserver,
	cluster, actor, writer, intentDigest, approvalDigest string, observerKey ed25519.PublicKey, authority []byte) (OfflineHoldDispatch, error) {
	if j == nil || reader == nil || writerConfig == nil || reader == writerConfig || writer == "" {
		return OfflineHoldDispatch{}, errors.New("offline hold requires its journal, reader and dedicated writer")
	}
	if err := j.VerifyIndependentObserverKey(observerKey); err != nil {
		return OfflineHoldDispatch{}, err
	}
	provider := &kubeOfflineDispatch{readConfig: reader, writeConfig: writerConfig, dns: dns, writer: writer}
	return dispatchOfflineHold(ctx, j, provider, cluster, actor, intentDigest, approvalDigest, bytes.Clone(observerKey), bytes.Clone(authority), time.Now)
}

func dispatchOfflineHold(ctx context.Context, j offlineDispatchJournal, provider offlineDispatchProvider, cluster, actor, intentDigest, approvalDigest string,
	observerKey ed25519.PublicKey, authority []byte, now func() time.Time) (OfflineHoldDispatch, error) {
	var result OfflineHoldDispatch
	i, raw, err := j.ReadIntent(ctx, intentDigest)
	if err != nil {
		return result, err
	}
	s := productioncontrol.Submission{Intent: i, Payload: raw}
	p, err := decodeHold(s)
	actual, digestErr := i.Digest()
	if err != nil || digestErr != nil || actual != intentDigest || i.ClusterID != cluster || p.Phase != ParticipationOffline ||
		!canonicalDigest(approvalDigest) {
		return result, errors.New("dispatch requires an exact offline hold in this cluster and its approval")
	}
	scope := HoldResource(cluster, p.To.Node)
	if claim, _, err := j.ReadExecution(ctx, scope, intentDigest); err == nil {
		if claim.Scope != scope || claim.IntentDigest != intentDigest || claim.ApprovalDigest != approvalDigest || claim.Fence < 1 {
			return result, productioncontrol.ErrStaleFence
		}
		return OfflineHoldDispatch{Claim: claim}, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	check := func(claim *productioncontrol.Claim) (*corev1.Node, string, error) {
		topology, err := j.ReadParticipationTopology(ctx, cluster)
		if err != nil || topology.Version != p.Topology || topology.ApprovalDigest != p.TopologyApproval {
			return nil, "", errors.New("offline hold no longer matches the approved topology")
		}
		head, err := j.ReadResource(ctx, scope)
		if err != nil || head.Scope != scope || head.LastVerified != (productioncontrol.Version{Generation: i.Generation, Digest: i.CurrentDigest}) {
			return nil, "", productioncontrol.ErrResourceDrift
		}
		if claim == nil {
			if head.Pending != "" {
				return nil, "", productioncontrol.ErrResourceBusy
			}
		} else if head.Pending != claim.IntentDigest || head.LastFence != claim.Fence {
			return nil, "", productioncontrol.ErrStaleFence
		}
		head.Pending = "" // Only our already-matched claim is ignored for the pure planner.
		live, boundary, err := provider.Check(ctx, topology, s, head)
		if err != nil {
			return nil, "", err
		}
		if err := verifyOfflineHoldAuthority(authority, observerKey, boundary, now()); err != nil {
			return nil, "", err
		}
		return live, boundary, nil
	}
	if _, _, err := check(nil); err != nil {
		return result, err
	}
	claim, created, err := j.ReserveExecution(ctx, actor, intentDigest, approvalDigest)
	result.Claim, result.Reserved = claim, created
	if err != nil || !created {
		return result, err
	}
	// This is deliberately after durable reservation. Any error or crash
	// from here on leaves a pending claim; no path refunds the permit.
	live, boundary, err := check(&claim)
	if err == nil {
		err = j.VerifyOpenClaim(ctx, claim)
	}
	if err == nil {
		err = verifyOfflineHoldAuthority(authority, observerKey, boundary, now())
	}
	if err != nil {
		return result, fmt.Errorf("offline hold reserved; resolve its pending claim before any further writes: %w", err)
	}
	n, err := provider.Write(ctx, s, live)
	if err != nil {
		return result, err
	}
	result.ProviderResponseVersion = n.ResourceVersion
	return result, nil
}

type kubeOfflineDispatch struct {
	readConfig, writeConfig *rest.Config
	dns                     *DNSObserver
	writer                  string
	client                  rest.Interface
}

func (k *kubeOfflineDispatch) Check(ctx context.Context, topology productioncontrol.VerifiedParticipationTopology, s productioncontrol.Submission, head productioncontrol.ResourceState) (*corev1.Node, string, error) {
	o, err := NewBoundKubernetesObserver(k.readConfig, topology.Policy)
	if err != nil {
		return nil, "", err
	}
	writeConfig, err := bindKubernetesConfig(k.writeConfig, topology.Policy)
	if err != nil {
		return nil, "", err
	}
	writer, err := offlineHoldClient(writeConfig)
	if err != nil {
		return nil, "", err
	}
	if err := verifyOfflineWriterIdentity(ctx, writer, k.writer); err != nil {
		return nil, "", err
	}
	if err := o.VerifyNamespace(ctx); err != nil {
		return nil, "", err
	}
	node, err := HoldTarget(s)
	if err != nil {
		return nil, "", err
	}
	var shadow KubernetesShadow
	if k.dns == nil {
		shadow, err = o.ShadowKubernetes(ctx, topology.Policy, node, time.Now)
	} else {
		shadow, err = o.ShadowWithDNS(ctx, topology.Policy, node, k.dns, time.Now)
	}
	if err != nil {
		return nil, "", err
	}
	if err := PreflightHold(s, topology, head, shadow, time.Now()); err != nil {
		return nil, "", err
	}
	guard, err := o.ObserveOfflineHoldGuard(ctx, topology, s, k.writer)
	if err != nil {
		return nil, "", err
	}
	boundary, err := OfflineHoldAuthorityBoundary(topology, s, k.writer, guard)
	if err != nil {
		return nil, "", err
	}
	if err := o.VerifyNamespace(ctx); err != nil {
		return nil, "", err
	}
	live, err := o.core.Nodes().Get(ctx, node, metav1.GetOptions{})
	if err != nil {
		return nil, "", errors.New("cannot re-read the predecessor Node immediately before offline hold dispatch")
	}
	if _, _, err := offlineHoldPatch(s, live); err != nil {
		return nil, "", err
	}
	k.client = writer
	return live, boundary, nil
}

func (k *kubeOfflineDispatch) Write(ctx context.Context, s productioncontrol.Submission, live *corev1.Node) (*corev1.Node, error) {
	return writeOfflineHoldOnce(ctx, k.client, s, live)
}
