package releaseguardian

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"fugue/internal/productioncontrol"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type NativePointerState string

const (
	NativePointerUnconfirmed NativePointerState = "unconfirmed"
	NativePointerCandidate   NativePointerState = "candidate"
	NativePointerRollback    NativePointerState = "rollback_pointer"
	NativePointerDiverged    NativePointerState = "diverged"
)

// NativePublicationObservation reports what the publication pointer currently
// says about a claim. Neither a missing pointer, a candidate pointer, nor a
// rollback pointer proves that the workload is settled or that a delayed write
// has been fenced. Result, when present, is the already-verified journal entry.
type NativePublicationObservation struct {
	Claim           productioncontrol.Claim
	Result          *productioncontrol.ExecutionResult
	Pointer         NativePointerState
	Desired         *DesiredRelease
	ResourceVersion string
}

// ObservePublication is safe after a timeout or restart. It only reads the
// existing claim and pointer, even if no provider write was attempted.
func (p *NativePublisher) ObservePublication(ctx context.Context, intentDigest string) (NativePublicationObservation, error) {
	if p == nil || p.store == nil || p.journal == nil {
		return NativePublicationObservation{}, errors.New("native Guardian publication observer is not configured")
	}
	intent, payload, err := p.readReleaseIntent(ctx, intentDigest)
	if err != nil {
		return NativePublicationObservation{}, err
	}
	scope := productioncontrol.Scope{ClusterID: intent.ClusterID, ResourceID: intent.ResourceID}
	claim, result, err := p.journal.Execution(ctx, scope, intentDigest)
	if err != nil {
		return NativePublicationObservation{}, err
	}
	observation := NativePublicationObservation{Claim: claim, Result: result, Pointer: NativePointerUnconfirmed}
	if result != nil {
		return observation, nil
	}
	key := payload.ProposedRecord.Key()
	target, configured := p.store.targets[key]
	if !configured || target.Namespace != payload.Namespace {
		return observation, errors.New("native Guardian provider target no longer matches the persisted claim")
	}
	object, err := p.store.client.CoreV1().ConfigMaps(target.Namespace).Get(ctx, desiredName(key), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return observation, nil
	}
	if err != nil {
		return observation, fmt.Errorf("observe native Guardian DesiredRelease: %w", err)
	}
	var desired DesiredRelease
	if err := decodeStrict([]byte(object.Data["desired.json"]), &desired); err != nil || desired.Validate() != nil || desired.Key() != key {
		return observation, errors.New("native Guardian DesiredRelease pointer is invalid")
	}
	observation.Desired, observation.ResourceVersion = &desired, object.ResourceVersion
	exactClaim := object.Annotations[nativeIntentAnnotation] == claim.IntentDigest &&
		object.Annotations[nativeFenceAnnotation] == strconv.FormatInt(claim.Fence, 10)
	switch {
	case intent.Generation < math.MaxInt64 && desired.Generation == intent.Generation+1 && desired.RecordDigest == intent.TargetDigest && exactClaim:
		observation.Pointer = NativePointerCandidate
	case intent.Generation < math.MaxInt64-1 && desired.Generation == intent.Generation+2 && desired.RecordDigest == intent.RollbackDigest && exactClaim:
		observation.Pointer = NativePointerRollback
	case desired.Generation == intent.Generation && desired.RecordDigest == intent.CurrentDigest:
		// The pointer is still the predecessor, which leaves execution unknown.
	default:
		observation.Pointer = NativePointerDiverged
	}
	return observation, nil
}
