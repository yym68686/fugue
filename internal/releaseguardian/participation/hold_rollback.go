package participation

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"time"

	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

// DraftOfflineHoldRollback returns a separately reviewable operation, never
// a permission to remove a hold. The current closed checkpoint has to be
// the independently settled result of the original operation. The target
// is solely the original approved predecessor, including absent fields.
func DraftOfflineHoldRollback(ctx context.Context, j *productioncontrol.Journal, config *rest.Config, cluster, originalDigest, request string) (productioncontrol.Submission, error) {
	if j == nil {
		return productioncontrol.Submission{}, errors.New("rollback draft requires the production journal")
	}
	original, _, err := rollbackSource(ctx, j, cluster, originalDigest, nil)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	p, err := decodeHold(original)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	checkpoint, err := ObserveOfflineHoldCheckpoint(ctx, j, config, cluster, p.To.Node, "")
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	final, head, err := rollbackSource(ctx, j, cluster, originalDigest, nil)
	if err != nil || final.Intent != original.Intent {
		return productioncontrol.Submission{}, errors.New("settled original hold changed during rollback observation")
	}
	return draftOfflineHoldRollback(original, head, checkpoint, request)
}

func rollbackSource(ctx context.Context, j offlinePendingJournal, cluster, originalDigest string, pending *productioncontrol.Claim) (productioncontrol.Submission, productioncontrol.ResourceState, error) {
	i, raw, err := j.ReadIntent(ctx, originalDigest)
	if err != nil {
		return productioncontrol.Submission{}, productioncontrol.ResourceState{}, err
	}
	s := productioncontrol.Submission{Intent: i, Payload: raw}
	p, err := decodeHold(s)
	actual, digestErr := i.Digest()
	if err != nil || digestErr != nil || actual != originalDigest || i.ClusterID != cluster || p.Phase != ParticipationOffline ||
		i.Generation >= math.MaxInt64-1 {
		return productioncontrol.Submission{}, productioncontrol.ResourceState{}, errors.New("rollback requires an exact original offline intent")
	}
	scope := HoldResource(cluster, p.To.Node)
	head, err := j.ReadResource(ctx, scope)
	if err != nil || head.Scope != scope || head.LastFence < 1 ||
		head.LastVerified != (productioncontrol.Version{Generation: i.Generation + 1, Digest: i.TargetDigest}) {
		return productioncontrol.Submission{}, productioncontrol.ResourceState{}, errors.New("rollback source is not the current settled hold")
	}
	originalFence := head.LastFence
	if pending == nil {
		if head.Pending != "" || head.LastIntent != actual {
			return productioncontrol.Submission{}, productioncontrol.ResourceState{}, productioncontrol.ErrResourceDrift
		}
	} else {
		if pending.Scope != scope || head.Pending != pending.IntentDigest || head.LastIntent != pending.IntentDigest ||
			head.LastFence != pending.Fence || pending.Fence <= 1 {
			return productioncontrol.Submission{}, productioncontrol.ResourceState{}, productioncontrol.ErrStaleFence
		}
		originalFence--
	}
	claim, result, err := j.ReadExecution(ctx, scope, actual)
	if err != nil || claim.Scope != scope || claim.IntentDigest != actual || claim.Fence != originalFence ||
		result == nil || result.Outcome != productioncontrol.Applied || result.Verified != head.LastVerified ||
		!canonicalDigest(result.EvidenceDigest) {
		return productioncontrol.Submission{}, productioncontrol.ResourceState{}, errors.New("rollback requires the original independently verified applied result")
	}
	return s, head, nil
}

func checkOfflineHoldRollbackSource(ctx context.Context, j offlinePendingJournal, s productioncontrol.Submission, p holdIntentPayload, pending *productioncontrol.Claim) error {
	original, _, err := rollbackSource(ctx, j, s.Intent.ClusterID, p.Reverses, pending)
	if err != nil {
		return err
	}
	origin, err := decodeHold(original)
	if err != nil || p.Topology != origin.Topology || p.TopologyApproval != origin.TopologyApproval ||
		p.ResourceVersion == origin.ResourceVersion || s.Intent.Generation != original.Intent.Generation+1 ||
		s.Intent.CurrentDigest != original.Intent.TargetDigest || s.Intent.TargetDigest != original.Intent.CurrentDigest {
		return errors.New("rollback is not the exact inverse of the verified offline change")
	}
	return nil
}

func draftOfflineHoldRollback(original productioncontrol.Submission, head productioncontrol.ResourceState, c OfflineHoldCheckpoint, request string) (productioncontrol.Submission, error) {
	p, err := decodeHold(original)
	if err != nil || p.Phase != ParticipationOffline {
		return productioncontrol.Submission{}, errors.New("rollback requires an original offline hold")
	}
	i := original.Intent
	originalDigest, err := i.Digest()
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	proof, err := c.Digest()
	if err != nil || c.Scope != HoldResource(i.ClusterID, p.To.Node) || c.Node.UID != p.To.UID || c.Node.ResourceVersion == p.ResourceVersion ||
		c.Topology != p.Topology || c.TopologyApproval != p.TopologyApproval || c.PendingIntent != "" ||
		c.OwnedDigest != i.TargetDigest || head.Scope != c.Scope || head.Pending != "" || head.LastIntent != originalDigest ||
		i.Generation >= math.MaxInt64-1 || head.LastVerified != (productioncontrol.Version{Generation: i.Generation + 1, Digest: i.TargetDigest}) {
		return productioncontrol.Submission{}, errors.New("rollback needs the exact currently verified Node and closed checkpoint in the original topology")
	}
	return encodeHoldRollback(i.ClusterID, request, p, originalDigest, c.Node.ResourceVersion, head.LastVerified, proof)
}

func encodeHoldRollback(cluster, request string, original holdIntentPayload, reverse, resourceVersion string, current productioncontrol.Version, proof string) (productioncontrol.Submission, error) {
	p := original
	p.Phase, p.Reverses, p.ResourceVersion, p.ShadowReportDigest = rollbackPhase, reverse, resourceVersion, proof
	p.From, p.To = original.To, original.From
	raw, err := json.Marshal(p)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	i, err := productioncontrol.NewIntent(cluster, HoldResource(cluster, p.To.Node).ResourceID, request,
		productioncontrol.NodeParticipation, current.Generation, current.Digest, p.To.digest(), current.Digest, raw)
	if err != nil {
		return productioncontrol.Submission{}, err
	}
	s := productioncontrol.Submission{Intent: i, Payload: raw}
	_, err = decodeHold(s)
	return s, err
}

// OfflineHoldRollbackBoundary reports current Kubernetes inputs for the
// independent observer. It is not positive network or serving evidence.
type OfflineHoldRollbackBoundary struct {
	Digest        string            `json:"digest"`
	Guard         FreezeObservation `json:"guard"`
	Member        MemberObservation `json:"member"`
	Readers       ReaderObservation `json:"readers"`
	ReadySince    int64             `json:"readySinceUnix"`
	StableSeconds int               `json:"stableSeconds"`
}

// ObserveOfflineHoldRollbackBoundary binds the admission revision and
// live Kubernetes inventory around reader checks. The separate witness
// must verify the actual running Front/Worker and approved network probes
// for this boundary, with the serving authority fenced through guard
// closure and provider request drainage.
func (o *KubernetesObserver) ObserveOfflineHoldRollbackBoundary(ctx context.Context, t productioncontrol.VerifiedParticipationTopology, s productioncontrol.Submission, writer string, now time.Time) (OfflineHoldRollbackBoundary, error) {
	var boundary OfflineHoldRollbackBoundary
	p, err := decodeHold(s)
	if err != nil || p.Phase != rollbackPhase {
		return boundary, errors.New("readmission observation requires an exact offline hold rollback")
	}
	m, err := enrolledMember(t.Policy, p.To.Node)
	if err != nil || m.StableSeconds < 120 || m.StableSeconds > 3600 {
		return boundary, errors.New("rollback stability requirements are not bound to the approved member")
	}
	if err := o.VerifyNamespace(ctx); err != nil {
		return boundary, err
	}
	guard, err := o.ObserveOfflineHoldGuard(ctx, t, s, writer)
	if err != nil {
		return boundary, err
	}
	guardDigest, err := OfflineHoldAuthorityBoundary(t, s, writer, guard)
	if err != nil {
		return boundary, err
	}
	before, err := o.readMemberObjects(ctx, t.Policy.Namespace, m)
	if err != nil {
		return boundary, err
	}
	readers, err := o.ObserveReaderRollouts(ctx, t.Policy, m.Node)
	if err != nil {
		return boundary, err
	}
	after, err := o.readMemberObjects(ctx, t.Policy.Namespace, m)
	if err != nil || !reflect.DeepEqual(before.report, after.report) {
		return boundary, errors.New("participation inventory changed during rollback observation")
	}
	after.snapshot.ValidatedReaderBaseline = readers.Baseline
	if service := after.report.Service; service != nil {
		// Select the serving rather than retained topology branch, without
		// asserting that this Service has approved or healthy transport.
		after.snapshot.Transport = &ParticipationTransport{Node: m.Node, Group: m.Group, ExternalIPs: []string{service.Address}}
	}
	if err := verifyParticipationInventory(m, t.Policy.Namespace, after.snapshot); err != nil {
		return boundary, err
	}
	if _, _, err := offlineHoldPatch(s, &after.snapshot.Node); err != nil {
		return boundary, err
	}
	readySince := rollbackNodeReadySince(&after.snapshot.Node)
	if readySince.IsZero() || now.Sub(readySince) < time.Duration(m.StableSeconds)*time.Second {
		return boundary, errors.New("rollback Node has not been Ready for the approved stability window")
	}
	latest, err := o.ObserveOfflineHoldGuard(ctx, t, s, writer)
	if err != nil || latest != guard {
		return boundary, errors.New("admission guard changed during rollback observation")
	}
	if err := o.VerifyNamespace(ctx); err != nil {
		return boundary, err
	}
	boundary = OfflineHoldRollbackBoundary{
		Guard: guard, Member: after.report, Readers: readers, ReadySince: readySince.Unix(), StableSeconds: m.StableSeconds,
	}
	raw, err := json.Marshal(struct {
		GuardDigest string                      `json:"guardDigest"`
		Live        OfflineHoldRollbackBoundary `json:"live"`
	}{guardDigest, boundary})
	if err != nil {
		return OfflineHoldRollbackBoundary{}, err
	}
	boundary.Digest = digest(append([]byte("fugue.participation.offline-rollback-boundary.v1\x00"), raw...))
	return boundary, nil
}

func rollbackNodeReadySince(n *corev1.Node) time.Time {
	if n != nil {
		for _, c := range n.Status.Conditions {
			if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
				return c.LastTransitionTime.Time
			}
		}
	}
	return time.Time{}
}
