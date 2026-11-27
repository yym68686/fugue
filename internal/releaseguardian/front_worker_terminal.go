package releaseguardian

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type FrontWorkerTerminalClosure struct {
	Claim      productioncontrol.Claim       `json:"claim"`
	Policy     string                        `json:"policyDigest"`
	Journal    string                        `json:"journalDigest"`
	Receipt    FrontWorkerLeaseObservation   `json:"receipt"`
	Archive    FrontWorkerLeaseObservation   `json:"archive"`
	Projection FrontWorkerLeaseObservation   `json:"projection"`
	Arm        *FrontWorkerLeaseObservation  `json:"arm,omitempty"`
	Witness    *FrontWorkerLeaseObservation  `json:"witness,omitempty"`
	Index      *FrontWorkerLeaseObservation  `json:"index,omitempty"`
	Leases     []FrontWorkerLeaseObservation `json:"leases"`
}

// ObserveFrontWorkerTerminalClosure reads independently persisted Kubernetes
// facts. The immutable terminal receipt stops a repeat claim, and the
// cleared projection and released Leases close the in-flight write path.
func (store *AuthorityStore) ObserveFrontWorkerTerminalClosure(ctx context.Context, config FrontWorkerCutover,
	j edgegroupfront.NodeWorkerJournal, claim productioncontrol.Claim, now time.Time) (FrontWorkerTerminalClosure, NodeWorkerResult, error) {
	var closure FrontWorkerTerminalClosure
	var terminal NodeWorkerResult
	if config.Namespace != store.namespace || config.Group != j.Recipient.Group ||
		config.Phase != KubeCutoverNative || !validNativeWorkerClaim(store.namespace, config.Group, claim) {
		return closure, terminal, errors.New("Front/Worker closure is outside the enrolled Kubernetes scope")
	}
	if err := store.CheckNodeWorkerShadowFence(ctx, config.Group); err != nil {
		return closure, terminal, err
	}
	policies, err := ObserveFrontWorkerCutover(ctx, store.client, config)
	if err != nil {
		return closure, terminal, err
	}
	policyDigest, err := FrontWorkerPolicyDigest(config, policies)
	if err != nil {
		return closure, terminal, err
	}
	configs := store.client.CoreV1().ConfigMaps(store.namespace)
	receipt, err := configs.Get(ctx, NodeWorkerResultName(j.Digest), metav1.GetOptions{})
	if err != nil {
		return closure, terminal, err
	}
	if !isTerminalWorkerRecord(receipt, store.namespace, j, "node-worker-result", "result.json") ||
		receipt.CreationTimestamp.IsZero() ||
		decodeStrict([]byte(receipt.Data["result.json"]), &terminal) != nil || !terminal.MatchesJournal(j) || !terminal.MatchesExecution(claim) {
		return closure, NodeWorkerResult{}, errors.New("Front/Worker has no immutable terminal receipt for this execution")
	}
	archive, err := configs.Get(ctx, NodeWorkerArchiveName(j.Digest), metav1.GetOptions{})
	if err != nil {
		return closure, terminal, err
	}
	archived, err := ReadArchivedNodeWorkerJournal([]byte(archive.Data["journal.json"]))
	// Archiving occurs at finalization, after any required drain. Allow
	// outstanding Front commands to exceed their bounded execution window.
	if err != nil || archived != j || !isTerminalWorkerRecord(archive, store.namespace, j, "node-worker-archive", "journal.json") ||
		archive.CreationTimestamp.IsZero() || archive.CreationTimestamp.Before(&receipt.CreationTimestamp) ||
		now.Before(archive.CreationTimestamp.Time.Add(30*time.Second)) {
		return closure, terminal, errors.New("Front/Worker archive does not prove a mature finalization of this journal")
	}
	if terminal.Outcome == "verified" {
		if j.ProbeServiceUID != "" && terminal.DrainStart == nil {
			return closure, terminal, errors.New("Front/Worker cutover with a pinned probe lacks its durable drain start")
		}
		armRecord, err := configs.Get(ctx, NodeWorkerArmName(j.Digest), metav1.GetOptions{})
		if err != nil {
			return closure, terminal, err
		}
		witnessRecord, err := configs.Get(ctx, NodeWorkerWitnessName(j.Digest), metav1.GetOptions{})
		if err != nil {
			return closure, terminal, err
		}
		var arm NodeWorkerArm
		var witness NodeWorkerWitness
		if !isTerminalWorkerRecord(armRecord, store.namespace, j, "node-worker-arm", "arm.json") ||
			!isTerminalWorkerRecord(witnessRecord, store.namespace, j, "node-worker-witness", "witness.json") ||
			decodeStrict([]byte(armRecord.Data["arm.json"]), &arm) != nil || arm.Validate(j, now) != nil ||
			decodeStrict([]byte(witnessRecord.Data["witness.json"]), &witness) != nil ||
			!reflect.DeepEqual(witness, *terminal.Witness) || witness.Validate(j, arm, terminal.Activation, now) != nil {
			return closure, terminal, errors.New("Front/Worker immutable cutover proof disagrees with the terminal receipt")
		}
		armRef, witnessRef := workerTerminalRef(armRecord), workerTerminalRef(witnessRecord)
		closure.Arm, closure.Witness = &armRef, &witnessRef
	}
	projectionName := NodeWorkerProjectionName(config.Group, j.Recipient.Node)
	projectionRef := FrontWorkerLeaseObservation{Name: projectionName}
	projection, err := configs.Get(ctx, projectionName, metav1.GetOptions{})
	if err == nil {
		if err := validateNodeWorkerProjection(projection, projectionName, config.Group, j.Recipient.Node); err != nil || len(projection.Data) != 0 {
			return closure, terminal, errors.New("Front/Worker journal remains projected to the active Front")
		}
		projectionRef = workerTerminalRef(projection)
	} else if !apierrors.IsNotFound(err) || terminal.Outcome != "expired_without_cutover" {
		return closure, terminal, fmt.Errorf("read closed Front/Worker projection: %w", err)
	}
	if terminal.Outcome != "expired_without_cutover" {
		indexMap, err := configs.Get(ctx, NodeWorkerIndexName(config.Group), metav1.GetOptions{})
		if err != nil {
			return closure, terminal, err
		}
		index, err := ReadNodeWorkerIndex(indexMap, config.Group)
		if err != nil || index.Nodes[j.NodeUID] != (NodeWorkerIndexHead{JournalDigest: j.Digest, Generation: terminal.Activation.Generation}) {
			return closure, terminal, errors.New("Front/Worker result is not the published terminal version for the Node")
		}
		indexRef := workerTerminalRef(indexMap)
		closure.Index = &indexRef
	}
	leases, err := observeReleasedFrontWorkerLeases(ctx, store.client, config)
	if err != nil {
		return closure, terminal, err
	}
	if err := store.CheckNodeWorkerShadowFence(ctx, config.Group); err != nil {
		return closure, terminal, err
	}
	closure.Claim, closure.Policy, closure.Journal = claim, policyDigest, j.Digest
	closure.Receipt, closure.Archive = workerTerminalRef(receipt), workerTerminalRef(archive)
	closure.Projection, closure.Leases = projectionRef, leases
	return closure, terminal, nil
}

func isTerminalWorkerRecord(object *corev1.ConfigMap, namespace string, j edgegroupfront.NodeWorkerJournal, kind, key string) bool {
	return object.Namespace == namespace && object.UID != "" && object.ResourceVersion != "" && object.DeletionTimestamp == nil &&
		object.Immutable != nil && *object.Immutable && len(object.Data) == 1 && len(object.BinaryData) == 0 &&
		len(object.Data[key]) > 0 && len(object.Data[key]) <= 16<<10 &&
		object.Labels["fugue.pro/group"] == j.Recipient.Group && object.Labels["fugue.pro/authority-kind"] == kind &&
		object.Labels["fugue.pro/authority-store"] == "true" && object.Labels["app.kubernetes.io/managed-by"] == "fugue-release-guardian"
}

func workerTerminalRef(object *corev1.ConfigMap) FrontWorkerLeaseObservation {
	return FrontWorkerLeaseObservation{Name: object.Name, UID: string(object.UID), Revision: object.ResourceVersion}
}
