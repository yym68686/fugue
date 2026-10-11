package sshcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"fugue/internal/managedssh"
	"fugue/internal/model"
	"fugue/internal/sshidentity"
	"golang.org/x/crypto/ssh"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

func (runtime *KubernetesRuntime) RetirementPolicy(ctx context.Context, binding model.ManagedSSHPublicBinding, active managedssh.ServingSlot) ([]EdgeConsumerInstance, time.Duration, error) {
	options, err := runtime.options()
	if err != nil {
		return nil, 0, err
	}
	if len(options.RequiredEdges) == 0 || len(options.RequiredEdges) > 64 {
		return nil, 0, errors.New("SSH retirement requires configured serving edge consumers")
	}
	retention := time.Duration(options.RollbackRetentionSeconds) * time.Second
	if retention == 0 {
		retention = 24 * time.Hour
	}
	if retention < time.Hour || retention > 30*24*time.Hour {
		return nil, 0, managedssh.ErrConfiguration
	}
	consumers := []EdgeConsumerInstance{}
	seen := map[string]bool{}
	identity := active.Artifact.Configuration.Identity
	if binding.AppID != identity.AppID || binding.TenantID != identity.TenantID || binding.IdentityID != identity.ID {
		return nil, 0, managedssh.ErrIdentityChange
	}
	publicKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(identity.PublicKey))
	if err != nil {
		return nil, 0, managedssh.ErrIdentityChange
	}
	for _, edge := range options.RequiredEdges {
		if edge.EdgeID == "" || edge.EdgeGroupID == "" || edge.Namespace == "" || len(edge.PodLabels) == 0 || seen[edge.EdgeID] {
			return nil, 0, managedssh.ErrConfiguration
		}
		seen[edge.EdgeID] = true
		if binding.EdgeGroupID != "" && edge.EdgeGroupID != binding.EdgeGroupID {
			continue
		}
		pods, err := runtime.client.CoreV1().Pods(edge.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(edge.PodLabels).String(), FieldSelector: "spec.nodeName=" + edge.EdgeID, Limit: 3})
		if err != nil {
			return nil, 0, err
		}
		if pods.Continue != "" || len(pods.Items) != 1 {
			return nil, 0, errors.New("SSH edge instance set is missing or ambiguous")
		}
		pod := pods.Items[0]
		if string(pod.UID) == "" || pod.Spec.NodeName != edge.EdgeID || pod.DeletionTimestamp != nil || !pod.Spec.HostNetwork || len(pod.Status.ContainerStatuses) != 1 || !pod.Status.ContainerStatuses[0].Ready {
			return nil, 0, errors.New("SSH edge instance is not positively serving")
		}
		declaredInstance := false
		for _, container := range pod.Spec.Containers {
			for _, variable := range container.Env {
				if variable.Name == "FUGUE_SSH_FRONT_INSTANCE_UID" && variable.ValueFrom != nil && variable.ValueFrom.FieldRef != nil && variable.ValueFrom.FieldRef.FieldPath == "metadata.uid" {
					declaredInstance = true
				}
			}
		}
		if !declaredInstance {
			return nil, 0, errors.New("SSH edge lacks independently bound instance acknowledgement")
		}
		address := net.ParseIP(edge.PublicAddress)
		if address == nil || address.IsUnspecified() || address.IsLoopback() {
			return nil, 0, managedssh.ErrConfiguration
		}
		node, err := runtime.client.CoreV1().Nodes().Get(ctx, edge.EdgeID, metav1.GetOptions{})
		if err != nil {
			return nil, 0, err
		}
		addressBound := false
		for _, assigned := range node.Status.Addresses {
			if (assigned.Type == "InternalIP" || assigned.Type == "ExternalIP") && address.Equal(net.ParseIP(assigned.Address)) {
				addressBound = true
			}
		}
		if !addressBound {
			return nil, 0, errors.New("SSH public probe address is not bound to its declared edge node")
		}
		observation, err := sshidentity.Probe(ctx, net.JoinHostPort(edge.PublicAddress, strconv.Itoa(binding.PublicPort)), publicKey, 5*time.Second)
		if err != nil || !observation.IdentityMatched || !observation.KeyExchangeVerified {
			return nil, 0, managedssh.ErrCandidateUnverified
		}
		consumers = append(consumers, EdgeConsumerInstance{EdgeID: edge.EdgeID, EdgeGroupID: edge.EdgeGroupID, InstanceUID: string(pod.UID)})
	}
	if len(consumers) == 0 {
		return nil, 0, errors.New("SSH binding has no configured edge consumers")
	}
	return consumers, retention, nil
}

func (runtime *KubernetesRuntime) StopDrainedSlot(ctx context.Context, slot managedssh.ServingSlot) (bool, error) {
	if slot.RetiringAt == nil || !slot.Artifact.Configuration.Draining {
		return false, managedssh.ErrConfiguration
	}
	deployment, err := runtime.client.AppsV1().Deployments(slot.Namespace).Get(ctx, slot.ID, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	if !ownedSlotLabels(deployment.Labels, slot) || deployment.DeletionTimestamp != nil || deployment.Spec.Replicas == nil || len(deployment.Spec.Template.Spec.Containers) != 1 || deployment.Spec.Template.Spec.Containers[0].Image != slot.Release.Image {
		return false, managedssh.ErrUnauthorized
	}
	pods, err := runtime.client.CoreV1().Pods(slot.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.Set{"fugue.pro/ssh-instance": slot.ID}.String(), Limit: 3})
	if err != nil {
		return false, err
	}
	if pods.Continue != "" {
		return false, managedssh.ErrUnavailable
	}
	if *deployment.Spec.Replicas == 0 {
		return len(pods.Items) == 0, nil
	}
	if len(pods.Items) != 1 || *deployment.Spec.Replicas != 1 {
		return false, managedssh.ErrUnavailable
	}
	pod := pods.Items[0]
	if pod.DeletionTimestamp != nil || !ownedSlotLabels(pod.Labels, slot) || net.ParseIP(pod.Status.PodIP) == nil || pod.Spec.HostNetwork {
		return false, managedssh.ErrUnavailable
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(pod.Status.PodIP, "7837")+"/healthz", nil)
	if err != nil {
		return false, err
	}
	response, err := runtime.httpClient.Do(request)
	if err != nil {
		return false, managedssh.ErrUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return false, managedssh.ErrUnavailable
	}
	var status managedssh.Status
	if err := json.NewDecoder(io.LimitReader(response.Body, 65536)).Decode(&status); err != nil {
		return false, err
	}
	if !status.Draining || status.Digest != slot.Artifact.Digest || status.IdentityID != slot.Artifact.Configuration.Identity.ID || !status.ValidUntil.After(time.Now()) || status.ActiveConnections != 0 {
		return false, nil
	}
	patch, _ := json.Marshal([]map[string]any{{"op": "test", "path": "/metadata/uid", "value": string(deployment.UID)}, {"op": "test", "path": "/metadata/resourceVersion", "value": deployment.ResourceVersion}, {"op": "replace", "path": "/spec/replicas", "value": 0}})
	_, err = runtime.client.AppsV1().Deployments(slot.Namespace).Patch(ctx, slot.ID, types.JSONPatchType, patch, metav1.PatchOptions{FieldManager: "fugue-managed-ssh"})
	return false, safeResourceError("Deployment", slot.ID, err)
}
