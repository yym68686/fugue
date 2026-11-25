package releaseguardian

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"fugue/internal/edgegroupfront"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const nodeWorkerDrainKind = "node-worker-drain"

func NodeWorkerDrainName(group, node string) string {
	sum := sha256.Sum256([]byte(group + "\n" + node))
	return fmt.Sprintf("fugue-node-worker-drain-%x", sum[:16])
}

// ProjectNodeWorkerDrain mirrors the immutable cutover checkpoint. Callers
// must hold the group lease and obtain a new serving witness immediately
// before calling this method. Time alone does not renew the serving witness.
func (store *AuthorityStore) ProjectNodeWorkerDrain(ctx context.Context, fence NodeWorkerFence, healthyAt time.Time) (edgegroupfront.WorkerDrainGrant, error) {
	j := fence.Journal
	empty := edgegroupfront.WorkerDrainGrant{}
	if err := store.requireNodeWorkerFence(ctx, fence); err != nil {
		return empty, err
	}
	result, found, err := store.ReadNodeWorkerResult(ctx, j)
	if err != nil || !found || result.Outcome != "verified" || result.DrainStart == nil {
		return empty, errors.Join(errors.New("bounded Worker drain lacks a durable verified cutover"), err)
	}
	group, node := j.Recipient.Group, j.Recipient.Node
	grant := edgegroupfront.WorkerDrainGrant{
		Schema: edgegroupfront.WorkerDrainSchema, FrontUID: j.Recipient.PodUID, Node: node,
		Previous: j.Expected, Serving: result.Activation, Start: result.DrainStart.UTC(),
		Deadline: result.DrainStart.Add(edgegroupfront.WorkerDrainLimit).UTC(), HealthyAt: healthyAt.UTC(),
	}
	if err := grant.ValidateFor(j.Recipient.PodUID, node, group); err != nil {
		return empty, err
	}
	raw, err := json.Marshal(grant)
	if err != nil {
		return empty, err
	}
	configs := store.client.CoreV1().ConfigMaps(store.namespace)
	name := NodeWorkerDrainName(group, node)
	current, err := configs.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		labels := authorityLabels(group)
		labels["fugue.pro/authority-kind"] = nodeWorkerDrainKind
		_, err = configs.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: store.namespace, Labels: labels,
			Annotations: map[string]string{"fugue.io/edge-node": node},
		}, Data: map[string]string{"grant.json": string(raw)}}, metav1.CreateOptions{})
		return grant, err
	}
	if err != nil {
		return empty, err
	}
	if current.UID == "" || current.ResourceVersion == "" || current.DeletionTimestamp != nil ||
		current.Immutable != nil && *current.Immutable || len(current.BinaryData) != 0 || len(current.Data) != 1 ||
		current.Labels["fugue.pro/group"] != group || current.Labels["fugue.pro/authority-kind"] != nodeWorkerDrainKind ||
		current.Labels["fugue.pro/authority-store"] != "true" ||
		current.Labels["app.kubernetes.io/managed-by"] != "fugue-release-guardian" ||
		current.Annotations["fugue.io/edge-node"] != node {
		return empty, errors.New("bounded Worker drain projection has unexpected metadata")
	}
	var old edgegroupfront.WorkerDrainGrant
	if err := decodeStrict([]byte(current.Data["grant.json"]), &old); err != nil {
		return empty, err
	}
	if err := old.ValidateFor(old.FrontUID, node, group); err != nil {
		return empty, err
	}
	if old.Serving.Generation > grant.Serving.Generation || old.HealthyAt.After(grant.HealthyAt) {
		return empty, errors.New("bounded Worker drain projection would regress")
	}
	if old.Serving.Generation == grant.Serving.Generation {
		old.HealthyAt = grant.HealthyAt
		if old != grant {
			return empty, errors.New("bounded Worker drain cutover changed at the same generation")
		}
	}
	if current.Data["grant.json"] == string(raw) {
		return grant, nil
	}
	// Keep the read UID/resourceVersion so an update cannot overwrite a
	// concurrently created or replaced projection even if a reply was lost.
	next := current.DeepCopy()
	next.Data["grant.json"] = string(raw)
	_, err = configs.Update(ctx, next, metav1.UpdateOptions{})
	return grant, err
}
