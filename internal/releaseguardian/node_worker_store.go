package releaseguardian

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const nodeWorkerJournalKind = "node-worker-journal"
const nodeWorkerProjectionKind = "node-worker-projection"
const nativeWorkerClusterAnnotation = "fugue.pro/native-cluster"
const nativeWorkerApprovalAnnotation = "fugue.pro/native-approval-digest"
const nativeWorkerClusterExpression = "^[a-z0-9][a-z0-9_-]{0,95}$"

var nativeWorkerClusterPattern = regexp.MustCompile(nativeWorkerClusterExpression)

var ErrNodeWorkerGroupMode = errors.New("group has node-local Worker state; legacy whole-group writes are disabled")

func NodeWorkerModeName(group string) string { return "fugue-authority-node-mode-" + group }

func NodeWorkerProjectionName(group, node string) string {
	sum := sha256.Sum256([]byte(group + "\n" + node))
	return fmt.Sprintf("fugue-node-worker-projection-%x", sum[:16])
}

// NodeWorkerMode is deliberately fail-closed on any object occupying the
// reserved name. The marker survives a completed journal and conservatively
// disables the old single-slot group model until there is a separate migration.
func (store *AuthorityStore) NodeWorkerMode(ctx context.Context, group string) (bool, error) {
	if !groupPattern.MatchString(group) {
		return false, errors.New("invalid node Worker mode group")
	}
	_, err := store.client.CoreV1().ConfigMaps(store.namespace).Get(ctx, NodeWorkerModeName(group), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

// NodeWorkerFence is the exact recoverable, immutable journal. It uses
// the same ConfigMap name as ordinary promotion and Front repair, so those
// paths cannot claim a second mutation even if the Guardian restarts.
type NodeWorkerFence struct {
	Journal   edgegroupfront.NodeWorkerJournal
	UID       types.UID
	RV        string
	Claim     productioncontrol.Claim
	ClaimedAt time.Time
}

// CheckNodeWorkerShadowFence observes whether a new handoff can claim the
// shared group fence. A shadow does not acquire a Lease or resume a live claim.
func (store *AuthorityStore) CheckNodeWorkerShadowFence(ctx context.Context, group string) error {
	if err := store.RequireNoActivatedTransition(ctx, group); err != nil {
		return err
	}
	_, err := store.client.CoreV1().ConfigMaps(store.namespace).Get(ctx, FrontRepairFenceName(group), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return errors.Join(errors.New("node Worker group mutation fence is not proven vacant"), err)
}

// ClaimNodeWorker must be invoked by the Guardian under the group mutation
// Lease, after proving the independent release and physical node bindings.
func (store *AuthorityStore) ClaimNodeWorker(ctx context.Context, j edgegroupfront.NodeWorkerJournal) (NodeWorkerFence, error) {
	return store.claimNodeWorker(ctx, j, productioncontrol.Claim{})
}

// ClaimNativeNodeWorker binds the existing provider fence to a durable
// execution. The caller revalidates the pending claim under the group Lease.
func (store *AuthorityStore) ClaimNativeNodeWorker(ctx context.Context, j edgegroupfront.NodeWorkerJournal, claim productioncontrol.Claim) (NodeWorkerFence, error) {
	if !validNativeWorkerClaim(store.namespace, j.Recipient.Group, claim) {
		return NodeWorkerFence{}, errors.New("native node Worker fence requires the exact PostgreSQL claim")
	}
	return store.claimNodeWorker(ctx, j, claim)
}

func validNativeWorkerClaim(namespace, group string, claim productioncontrol.Claim) bool {
	return claim.ResourceID != "" && claim.ResourceID == productioncontrol.FrontWorkerResource(namespace, group) &&
		nativeWorkerClusterPattern.MatchString(claim.ClusterID) && claim.Fence > 0 &&
		digestPattern.MatchString(claim.IntentDigest) && digestPattern.MatchString(claim.ApprovalDigest)
}

func (store *AuthorityStore) claimNodeWorker(ctx context.Context, j edgegroupfront.NodeWorkerJournal, claim productioncontrol.Claim) (NodeWorkerFence, error) {
	group := j.Recipient.Group
	raw, err := json.Marshal(j)
	if err != nil || !groupPattern.MatchString(group) {
		return NodeWorkerFence{}, errors.New("invalid node Worker group or journal")
	}
	if _, err := edgegroupfront.ReadNodeWorkerJournal(raw); err != nil {
		return NodeWorkerFence{}, err
	}
	if err := store.RequireNoActivatedTransition(ctx, group); err != nil {
		return NodeWorkerFence{}, err
	}
	if _, retired, err := store.ReadNodeWorkerResult(ctx, j); err != nil || retired {
		return NodeWorkerFence{}, errors.Join(errors.New("terminal node Worker journal cannot be claimed again"), err)
	}
	if claim.Fence > 0 {
		configs := store.client.CoreV1().ConfigMaps(store.namespace)
		for _, name := range []string{NodeWorkerArmName(j.Digest), NodeWorkerWitnessName(j.Digest)} {
			if _, err := configs.Get(ctx, name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
				if err != nil {
					return NodeWorkerFence{}, err
				}
				return NodeWorkerFence{}, errors.New("native node Worker journal already has a provider input")
			}
		}
	}
	immutable := true
	object := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: FrontRepairFenceName(group), Namespace: store.namespace,
		Labels: map[string]string{"fugue.pro/group": group, "fugue.pro/authority-kind": nodeWorkerJournalKind,
			"fugue.pro/authority-store": "true", "app.kubernetes.io/managed-by": "fugue-release-guardian"},
	}, Immutable: &immutable, Data: map[string]string{"journal.json": string(raw)}}
	if claim != (productioncontrol.Claim{}) {
		object.Annotations = map[string]string{nativeWorkerClusterAnnotation: claim.ClusterID, nativeIntentAnnotation: claim.IntentDigest,
			nativeWorkerApprovalAnnotation: claim.ApprovalDigest, nativeFenceAnnotation: strconv.FormatInt(claim.Fence, 10)}
	}
	configs := store.client.CoreV1().ConfigMaps(store.namespace)
	out, err := configs.Create(ctx, object, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		// Idempotent if Create succeeded but its reply was lost. A different
		// journal, repair, or ordinary group transition never gets adopted.
		out, err = configs.Get(ctx, object.Name, metav1.GetOptions{})
	}
	if err != nil {
		return NodeWorkerFence{}, err
	}
	fence, err := decodeNodeWorkerFence(out, group)
	if err != nil || fence.Journal.Digest != j.Digest || fence.Claim != claim {
		return NodeWorkerFence{}, errors.Join(errors.New("node Worker mutation fence already has another owner"), err)
	}
	return fence, nil
}

func (store *AuthorityStore) LoadNodeWorker(ctx context.Context, group string) (NodeWorkerFence, bool, error) {
	if !groupPattern.MatchString(group) {
		return NodeWorkerFence{}, false, errors.New("invalid node Worker group")
	}
	object, err := store.client.CoreV1().ConfigMaps(store.namespace).Get(ctx, FrontRepairFenceName(group), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return NodeWorkerFence{}, false, nil
	}
	if err != nil {
		return NodeWorkerFence{}, false, err
	}
	if object.Labels["fugue.pro/authority-kind"] != nodeWorkerJournalKind {
		return NodeWorkerFence{}, false, nil
	}
	fence, err := decodeNodeWorkerFence(object, group)
	return fence, err == nil, err
}

func decodeNodeWorkerFence(object *corev1.ConfigMap, group string) (NodeWorkerFence, error) {
	if object == nil || object.Name != FrontRepairFenceName(group) || object.UID == "" || object.ResourceVersion == "" ||
		object.Immutable == nil || !*object.Immutable || object.DeletionTimestamp != nil || len(object.Data) != 1 || len(object.BinaryData) != 0 ||
		object.Labels["fugue.pro/group"] != group || object.Labels["fugue.pro/authority-kind"] != nodeWorkerJournalKind ||
		object.Labels["app.kubernetes.io/managed-by"] != "fugue-release-guardian" {
		return NodeWorkerFence{}, errors.New("node Worker mutation fence has invalid metadata")
	}
	j, err := edgegroupfront.ReadNodeWorkerJournal([]byte(object.Data["journal.json"]))
	if err != nil || j.Recipient.Group != group {
		return NodeWorkerFence{}, errors.Join(errors.New("node Worker mutation fence has invalid journal scope"), err)
	}
	var claim productioncontrol.Claim
	for _, name := range []string{nativeWorkerClusterAnnotation, nativeIntentAnnotation, nativeWorkerApprovalAnnotation, nativeFenceAnnotation} {
		if _, exists := object.Annotations[name]; !exists {
			continue
		}
		n := object.Annotations
		sequence, err := strconv.ParseInt(n[nativeFenceAnnotation], 10, 64)
		claim = productioncontrol.Claim{
			Scope: productioncontrol.Scope{ClusterID: n[nativeWorkerClusterAnnotation], ResourceID: productioncontrol.FrontWorkerResource(object.Namespace, group)},
			Fence: sequence, IntentDigest: n[nativeIntentAnnotation], ApprovalDigest: n[nativeWorkerApprovalAnnotation],
		}
		if err != nil || n[nativeFenceAnnotation] != strconv.FormatInt(sequence, 10) || !validNativeWorkerClaim(object.Namespace, group, claim) {
			return NodeWorkerFence{}, errors.New("native node Worker journal has an incomplete execution claim")
		}
		break
	}
	if claim != (productioncontrol.Claim{}) && object.CreationTimestamp.IsZero() {
		return NodeWorkerFence{}, errors.New("native node Worker fence has no Kubernetes creation timestamp")
	}
	return NodeWorkerFence{
		Journal: j, UID: object.UID, RV: object.ResourceVersion, Claim: claim, ClaimedAt: object.CreationTimestamp.Time.UTC(),
	}, nil
}

// EnsureNodeWorkerProjection publishes the exact immutable claim to the
// target Front's read-only volume. This mirror must stay mutable: kubelet
// stops watching immutable ConfigMaps, even when their names are reused.
// The Guardian calls this only while holding the group Lease and checks the
// mounted digest and recipient before every native write.
func (store *AuthorityStore) EnsureNodeWorkerProjection(ctx context.Context, fence NodeWorkerFence) error {
	j := fence.Journal
	if err := store.requireNodeWorkerFence(ctx, fence); err != nil {
		return err
	}
	raw, err := json.Marshal(j)
	if err != nil {
		return err
	}
	configs := store.client.CoreV1().ConfigMaps(store.namespace)
	name := NodeWorkerProjectionName(j.Recipient.Group, j.Recipient.Node)
	current, err := configs.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		labels := authorityLabels(j.Recipient.Group)
		labels["fugue.pro/authority-kind"] = nodeWorkerProjectionKind
		_, err := configs.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: store.namespace, Labels: labels, Annotations: map[string]string{"fugue.io/edge-node": j.Recipient.Node},
		}, Data: map[string]string{"journal.json": string(raw)}}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	if err := validateNodeWorkerProjection(current, name, j.Recipient.Group, j.Recipient.Node); err != nil {
		return err
	}
	if present, ok := current.Data["journal.json"]; ok {
		if present == string(raw) {
			return nil
		}
		return errors.New("node Worker projection still has a different unretired journal")
	}
	next := current.DeepCopy()
	next.Data = map[string]string{"journal.json": string(raw)}
	_, err = configs.Update(ctx, next, metav1.UpdateOptions{})
	return err
}

func validateNodeWorkerProjection(cm *corev1.ConfigMap, name, group, node string) error {
	if cm == nil || cm.Name != name || cm.UID == "" || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil ||
		cm.Immutable != nil && *cm.Immutable || len(cm.BinaryData) != 0 ||
		cm.Labels["fugue.pro/group"] != group || cm.Labels["fugue.pro/authority-kind"] != nodeWorkerProjectionKind ||
		cm.Labels["app.kubernetes.io/managed-by"] != "fugue-release-guardian" || cm.Annotations["fugue.io/edge-node"] != node {
		return errors.New("node Worker projection metadata is invalid")
	}
	if _, hasJournal := cm.Data["journal.json"]; len(cm.Data) > 1 || len(cm.Data) == 1 && !hasJournal {
		return errors.New("node Worker projection has unexpected keys")
	}
	return nil
}

func (store *AuthorityStore) requireNodeWorkerFence(ctx context.Context, fence NodeWorkerFence) error {
	current, found, err := store.LoadNodeWorker(ctx, fence.Journal.Recipient.Group)
	if err != nil || !found || current != fence {
		return errors.Join(errors.New("node Worker fence is not the exact current claim"), err)
	}
	return nil
}

// FinalizeNodeWorker must run under the group Lease. Archive the exact claim
// and preserve the node-mode fence before releasing the transient claim. A
// lost reply can safely resume, and a late deletion cannot erase the next
// pilot's claim because both UID and resourceVersion are preconditions.
func (store *AuthorityStore) FinalizeNodeWorker(ctx context.Context, fence NodeWorkerFence) error {
	if err := store.requireNodeWorkerFence(ctx, fence); err != nil {
		return err
	}
	j := fence.Journal
	result, found, err := store.ReadNodeWorkerResult(ctx, j)
	if err != nil || !found || !result.MatchesExecution(fence.Claim) {
		return errors.Join(errors.New("node Worker cannot retire without its immutable result"), err)
	}
	group, node := j.Recipient.Group, j.Recipient.Node
	if result.Outcome != "expired_without_cutover" {
		if err := store.createImmutable(ctx, NodeWorkerModeName(group), group, "node-worker-mode", "mode.json",
			map[string]string{"schema": "edge-node-worker-mode/v1", "group": group}); err != nil {
			return err
		}
	}
	archive := NodeWorkerArchiveName(j.Digest)
	if err := store.createImmutable(ctx, archive, group, "node-worker-archive", "journal.json", j); err != nil {
		return err
	}
	var archived edgegroupfront.NodeWorkerJournal
	if found, err := store.readNodeWorkerInput(ctx, j, archive, "node-worker-archive", "journal.json", &archived); err != nil || !found || archived != j {
		return errors.Join(errors.New("archived node Worker journal readback is not exact"), err)
	}
	if result.Outcome != "expired_without_cutover" {
		// Commit the lookup before releasing the fence. Recovery cannot see
		// a settled group while its exact committed head is still missing.
		if err := store.advanceNodeWorkerIndex(ctx, j, result); err != nil {
			return err
		}
	}
	configs := store.client.CoreV1().ConfigMaps(store.namespace)
	name := NodeWorkerProjectionName(group, node)
	projection, err := configs.Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		if err := validateNodeWorkerProjection(projection, name, group, node); err != nil {
			return err
		}
		if present, projected := projection.Data["journal.json"]; projected {
			raw, marshalErr := json.Marshal(j)
			if marshalErr != nil || present != string(raw) {
				return errors.Join(errors.New("refusing to retire a different node Worker projection"), marshalErr)
			}
			next := projection.DeepCopy()
			next.Data = nil
			if _, err := configs.Update(ctx, next, metav1.UpdateOptions{}); err != nil {
				return err
			}
		}
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	return configs.Delete(ctx, FrontRepairFenceName(group), metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &fence.UID, ResourceVersion: &fence.RV},
	})
}
