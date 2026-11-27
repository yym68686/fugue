package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

const (
	canonicalDrainBudget = 10 * time.Minute
	canonicalDrainLabel  = "fugue.io/canonical-drain-pending"
	// Reserve one normal observation/retry interval to start graceful shutdown.
	canonicalDrainStopReserve = time.Minute
	// The kubelet can extend an overrunning preStop by two seconds. Leave
	// room for that and for rounding the requested grace to a whole second.
	canonicalDrainKubeletBuffer = 4 * time.Second
)

func canonicalDrainDue(handoff safeCanonicalHandoff, now time.Time) bool {
	return handoff.DrainDeadline != nil && !now.Before(handoff.DrainDeadline.Add(-canonicalDrainStopReserve))
}

// A canceled/superseded deploy may have left the durable handoff behind.
// Discover only armed canonical Deployments instead of waiting for the slower
// full ManagedApp scan. Active operations still own their own reconciliation.
func (s *Service) reconcileCanonicalDrainDeadlines(ctx context.Context) error {
	client, err := s.kubeClient()
	if err != nil {
		return err
	}
	var deployments kubeObjectList
	query := url.Values{"labelSelector": {canonicalDrainLabel + "=true"}}
	if _, err := client.doMetadataList(ctx, "/apis/apps/v1/deployments?"+query.Encode(), &deployments); err != nil {
		return err
	}
	if deployments.Metadata.Continue != "" {
		return fmt.Errorf("canonical drain checkpoint inventory is incomplete")
	}
	var firstErr error
	for _, deployment := range deployments.Items {
		meta := objectMapField(deployment, "metadata")
		labels := objectStringMapValue(meta["labels"])
		annotations := objectStringMapValue(meta["annotations"])
		var handoff safeCanonicalHandoff
		if json.Unmarshal([]byte(annotations[safeCanonicalHandoffAnnotation]), &handoff) != nil ||
			handoff.DeploymentUID != objectStringField(meta, "uid") || (!handoff.Stopping && !canonicalDrainDue(handoff, time.Now())) {
			continue
		}
		app := model.App{ID: labels[runtime.FugueLabelAppID], TenantID: labels[runtime.FugueLabelTenantID]}
		ns, name := runtime.NamespaceForTenant(app.TenantID), runtime.RuntimeAppResourceName(app)
		if app.ID == "" || app.TenantID == "" || labels[runtime.FugueLabelManagedBy] != runtime.FugueLabelManagedByValue ||
			objectStringField(meta, "namespace") != ns || objectStringField(meta, "name") != name {
			continue
		}
		managed, found, err := client.getManagedApp(ctx, ns, name)
		if err == nil && found && managed.Metadata.DeletionTimestamp == "" {
			identity := runtime.AppFromManagedApp(managed)
			if identity.ID == app.ID && identity.TenantID == app.TenantID {
				err = s.reconcileManagedAppObject(withSkipExistingCloudNativePGWrites(ctx), client, managed)
			}
		}
		if err != nil && !errors.Is(err, errSafeCanonicalHandoffPending) && firstErr == nil {
			firstErr = err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return firstErr
}

func (s *Service) canonicalDrainSource(ctx context.Context, client *kubeClient, app model.App, handoff safeCanonicalHandoff, expected string) (map[string]any, kubeDeployment, error) {
	var dep kubeDeployment
	namespace, name := runtime.NamespaceForTenant(app.TenantID), runtime.RuntimeAppResourceName(app)
	live, found, err := client.getRawObject(ctx, deploymentAPIPath(namespace, name))
	if err != nil || !found {
		return nil, dep, fmt.Errorf("canonical drain source Deployment unavailable")
	}
	meta := objectMapField(live, "metadata")
	annotations := objectStringMapValue(meta["annotations"])
	if objectStringField(meta, "uid") != handoff.DeploymentUID || objectStringField(meta, "deletionTimestamp") != "" ||
		!appWorkloadOwnerMatches(meta, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) ||
		annotations[safeCanonicalHandoffAnnotation] != expected || annotations[runtime.FugueAnnotationReleaseKey] != handoff.ReleaseKey {
		return nil, dep, fmt.Errorf("canonical drain source checkpoint changed")
	}
	raw, _ := json.Marshal(live)
	if err := json.Unmarshal(raw, &dep); err != nil || dep.Spec.Replicas == nil ||
		dep.Spec.Template.Metadata.Annotations[runtime.FugueAnnotationReleaseKey] != handoff.ReleaseKey ||
		!s.migrationImageRefsEquivalent(app, deploymentPrimaryContainerImage(dep), handoff.Image) {
		return nil, dep, fmt.Errorf("canonical drain source executable changed")
	}
	return live, dep, nil
}

func (s *Service) writeCanonicalDrainState(ctx context.Context, client *kubeClient, app model.App, handoff safeCanonicalHandoff, expected string, stop bool) (string, error) {
	live, dep, err := s.canonicalDrainSource(ctx, client, app, handoff, expected)
	if err != nil {
		return expected, err
	}
	if *dep.Spec.Replicas <= 0 || *dep.Spec.Replicas > 32 {
		return expected, fmt.Errorf("canonical drain source has unexpected replica intent")
	}
	encoded, err := json.Marshal(handoff)
	if err != nil {
		return expected, err
	}
	patch := map[string]any{"metadata": map[string]any{
		"annotations": map[string]any{safeCanonicalHandoffAnnotation: string(encoded)}, "labels": map[string]any{canonicalDrainLabel: "true"},
	}}
	if stop {
		patch["spec"] = map[string]any{"replicas": 0}
	}
	if err := client.patchAppWorkloadObject(ctx, deploymentAPIPath(runtime.NamespaceForTenant(app.TenantID), runtime.RuntimeAppResourceName(app)), live, patch); err != nil {
		return expected, err
	}
	return string(encoded), nil
}

// Only stop the canonical source, not the independent revision carrying live
// traffic. Scale intent and the stop receipt are written together, so a
// restart cannot recreate the old version while its Pods are terminating.
func (s *Service) stopWithdrawnCanonicalPods(ctx context.Context, client *kubeClient, app model.App, handoff safeCanonicalHandoff, expected string, quiet bool) error {
	if handoff.DrainDeadline == nil {
		return fmt.Errorf("canonical drain has no verified withdrawal deadline")
	}
	namespace, name := runtime.NamespaceForTenant(app.TenantID), runtime.RuntimeAppResourceName(app)
	_, dep, err := s.canonicalDrainSource(ctx, client, app, handoff, expected)
	if err != nil {
		return err
	}
	if handoff.Stopping && *dep.Spec.Replicas != 0 {
		return fmt.Errorf("canonical drain stop replica intent changed")
	}
	inv, err := fetchReleaseDrainInventory(ctx, client, namespace, false)
	if err != nil {
		return err
	}
	pods, err := canonicalStoppingPods(inv, app, name, handoff)
	if err != nil {
		return err
	}
	if !handoff.Stopping {
		// A busy cohort can enter Kubernetes termination within its last minute
		// only if its native sidecar can keep the hook alive. Without that hook,
		// do not interrupt connections early.
		if !quiet && time.Until(*handoff.DrainDeadline) > canonicalDrainKubeletBuffer+time.Second {
			for _, pod := range pods {
				if objectStringField(objectMapField(pod, "metadata"), "deletionTimestamp") != "" {
					continue
				}
				full, err := hydrateReleaseDrainPod(ctx, client, namespace, pod)
				if err != nil {
					return err
				}
				if !canonicalPodHasDrainHook(full) {
					return fmt.Errorf("canonical Pod has no connection-aware preStop before deadline")
				}
			}
		}
		handoff.Stopping = true
		if _, err := s.writeCanonicalDrainState(ctx, client, app, handoff, expected, true); err != nil {
			return err
		}
	} else if len(pods) == 0 && dep.Status.ObservedGeneration >= dep.Metadata.Generation && dep.Status.Replicas == 0 {
		return nil
	}
	// A normal scale-down races this request. A UID-preconditioned DELETE can
	// initiate graceful termination or shorten an existing deletion, never
	// extend it. Do not force-remove the API object on an unreachable node.
	capAt := handoff.DrainDeadline.Add(-canonicalDrainKubeletBuffer)
	var stopErr error
	for _, pod := range pods {
		meta := objectMapField(pod, "metadata")
		if deleting := objectStringField(meta, "deletionTimestamp"); deleting != "" {
			at, err := time.Parse(time.RFC3339, deleting)
			if err != nil {
				return fmt.Errorf("canonical drain Pod deletion deadline is invalid")
			}
			if !at.After(capAt) || !at.After(time.Now().Add(time.Second)) {
				continue
			}
		}
		seconds := int64(time.Until(capAt) / time.Second)
		if seconds < 1 {
			seconds = 1
		}
		path := "/api/v1/namespaces/" + url.PathEscape(namespace) + "/pods/" + url.PathEscape(objectStringField(meta, "name"))
		options := map[string]any{
			"apiVersion": "v1", "kind": "DeleteOptions", "gracePeriodSeconds": seconds,
			"preconditions": map[string]any{"uid": objectStringField(meta, "uid")},
		}
		if _, err := client.doRequest(ctx, http.MethodDelete, path, "application/json", options, nil); normalizeDeleteNotFound(err) != nil && stopErr == nil {
			stopErr = err
		}
	}
	if stopErr != nil {
		return fmt.Errorf("cap canonical Pod termination: %w", stopErr)
	}
	return fmt.Errorf("%w: stage=canonical_drain_terminating pod_count=%d drain_deadline=%s", errSafeCanonicalHandoffPending, len(pods), handoff.DrainDeadline.Format(time.RFC3339))
}

func canonicalPodHasDrainHook(pod map[string]any) bool {
	apps := mapSlice(nestedObjectValue(pod, "spec", "containers"))
	if len(apps) == 0 {
		return false
	}
	hook := objectMapValue(nestedObjectValue(apps[0], "lifecycle", "preStop", "httpGet"))
	port, numeric := hook["port"].(float64)
	if !numeric || port < 1 || port > 65535 || port != float64(int(port)) || hook["path"] != "/drain/prestop" ||
		objectStringField(hook, "host") != "" || (hook["scheme"] != nil && hook["scheme"] != "HTTP") {
		return false
	}
	for _, agent := range mapSlice(nestedObjectValue(pod, "spec", "initContainers")) {
		if agent["name"] == "fugue-drain-agent" && agent["restartPolicy"] == "Always" {
			for _, p := range mapSlice(agent["ports"]) {
				if p["name"] == "drain-agent" && p["containerPort"] == port {
					for _, env := range mapSlice(agent["env"]) {
						if env["name"] == "FUGUE_DRAIN_FAIL_CLOSED" && env["value"] == "true" {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

func canonicalStoppingPods(inv *releaseDrainInventory, app model.App, name string, handoff safeCanonicalHandoff) ([]map[string]any, error) {
	if inv.err != nil {
		return nil, inv.err
	}
	labels := map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}
	owned := map[string]string{}
	for _, rs := range inv.replicasByDeployment[handoff.DeploymentUID] {
		meta := objectMapField(rs, "metadata")
		rsName, uid := objectStringField(meta, "name"), objectStringField(meta, "uid")
		if rsName == "" || owned[rsName] != "" || !appWorkloadOwnerMatches(meta, labels) || !appWorkloadControlledBy(meta, "Deployment", name, handoff.DeploymentUID) || uid == "" {
			return nil, fmt.Errorf("canonical drain ReplicaSet ownership differs")
		}
		owned[rsName] = uid
	}
	pods := inv.podsForRelease("", name, true, owned)
	for _, pod := range pods {
		meta := objectMapField(pod, "metadata")
		ownedPod := false
		for rsName, uid := range owned {
			if appWorkloadControlledBy(meta, "ReplicaSet", rsName, uid) {
				ownedPod = true
				break
			}
		}
		if !ownedPod || !appWorkloadOwnerMatches(meta, labels) || objectStringField(meta, "uid") == "" ||
			objectStringMapValue(meta["annotations"])[runtime.FugueAnnotationReleaseKey] != handoff.ReleaseKey {
			return nil, fmt.Errorf("canonical drain Pod identity differs")
		}
	}
	return pods, nil
}
