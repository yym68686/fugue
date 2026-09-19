package controller

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"strings"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// Only repair an incompatible placement before any container has started.
// A running/previously serving RWO workload must use the fenced migration
// workflow; a failed mount must never become permission for a second writer.
func (s *Service) unstartedStoragePlacementRecovery(ctx context.Context, client *kubeClient, namespace string, managed runtime.ManagedAppObject, deployment kubeDeployment, current, desired model.App, before, after runtime.SchedulingConstraints) (bool, error) {
	oldNode, newNode := before.NodeSelector[kubeHostnameLabelKey], after.NodeSelector[kubeHostnameLabelKey]
	if desired.Spec.Replicas <= 0 || current.Spec.Replicas != desired.Spec.Replicas || oldNode == "" || newNode == "" || oldNode == newNode || (desired.Spec.PersistentStorage == nil && desired.Spec.Workspace == nil) {
		return false, nil
	}
	if deployment.Metadata.UID == "" || deployment.Metadata.Generation <= 0 || deployment.Status.ObservedGeneration < deployment.Metadata.Generation || deployment.Status.ReadyReplicas > 0 || deployment.Status.AvailableReplicas > 0 || managed.Status.ReadyReplicas > 0 || managed.Status.CurrentReleaseKey != "" || managed.Status.CurrentReleaseReadyAt != "" || managed.Status.CurrentReleaseStartedAt != "" {
		return false, nil
	}
	if !managedAppSpecsEqualExceptReplicas(current.Spec, desired.Spec) || !reflect.DeepEqual(current.Route, desired.Route) || !reflect.DeepEqual(model.AppOriginSource(current), model.AppOriginSource(desired)) || !reflect.DeepEqual(model.AppBuildSource(current), model.AppBuildSource(desired)) || !reflect.DeepEqual(current.Bindings, desired.Bindings) || !reflect.DeepEqual(current.BackingServices, desired.BackingServices) {
		return false, nil
	}
	ready, err := liveManagedAppHasReadyEndpoint(ctx, client, namespace, current)
	if err != nil {
		return false, err
	}
	if ready {
		return false, nil
	}
	pods, err := client.listPodsBySelector(ctx, namespace, managedAppPodLabelSelector(current))
	if err != nil {
		return false, err
	}
	if len(pods) == 0 {
		return false, nil
	}
	for _, pod := range pods {
		if pod.ObservedUID == "" || !podWaitingForVolumes(pod) || !podHasDeploymentTemplateIdentity(pod, deployment) || pod.Spec.NodeName != oldNode {
			return false, nil
		}
		ownerOK := false
		for _, owner := range pod.ObservedOwnerReferences {
			if owner.Kind != "ReplicaSet" || owner.APIVersion != "apps/v1" || !owner.Controller || owner.UID == "" {
				continue
			}
			var rs struct {
				Metadata struct {
					UID             string `json:"uid"`
					OwnerReferences []struct {
						APIVersion string `json:"apiVersion"`
						Kind       string `json:"kind"`
						UID        string `json:"uid"`
						Controller *bool  `json:"controller"`
					} `json:"ownerReferences"`
				} `json:"metadata"`
			}
			if _, err := client.doJSON(ctx, "GET", "/apis/apps/v1/namespaces/"+url.PathEscape(namespace)+"/replicasets/"+url.PathEscape(owner.Name), nil, &rs); err != nil {
				return false, fmt.Errorf("verify unstarted storage workload owner: %w", err)
			}
			if rs.Metadata.UID != owner.UID {
				continue
			}
			for _, parent := range rs.Metadata.OwnerReferences {
				if parent.APIVersion == "apps/v1" && parent.Kind == "Deployment" && parent.UID == deployment.Metadata.UID && parent.Controller != nil && *parent.Controller {
					ownerOK = true
				}
			}
		}
		if !ownerOK {
			return false, nil
		}
	}
	objects := s.Renderer.BuildManagedAppChildObjects(desired, after, nil)
	storage, err := s.appStoragePlacement(ctx, client, desired, objects)
	if err != nil {
		return false, err
	}
	if storage == nil {
		return false, nil
	}
	for _, claim := range storage.claims {
		if len(claim.attachedNodes) > 0 {
			return false, nil
		}
	}
	old, found, err := client.getNode(ctx, oldNode)
	if err != nil {
		return false, err
	}
	if !found || storage.rejection(old) == "" {
		return false, nil
	}
	if err := s.validateAppStoragePlacement(ctx, client, desired, after, objects); err != nil {
		return false, err
	}
	if s.Logger != nil {
		s.Logger.Printf("repairing unstarted storage placement app=%s from=%s to=%s after owner and attachment preflight", strings.TrimSpace(desired.ID), oldNode, newNode)
	}
	return true, nil
}
