package api

import (
	"context"
	"encoding/json"
	"errors"

	"fugue/internal/kubeauth"
	corev1 "k8s.io/api/core/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
)

const clusterPodInventoryPath = "/api/v1/pods"

// The node inventory needs real Pod specs and cannot use PartialObjectMetadata.
// Change only the representation: keep the same full, current LIST and allow
// JSON negotiation with servers that cannot provide Kubernetes Protobuf.
func (c *clusterNodeClient) readClusterPodInventory(ctx context.Context, out *clusterNodePodList) error {
	ctx = kubeauth.WithObservationSite(ctx, kubeauth.ObserveNodePodInventory)
	return c.getKubernetesCoreResource(ctx, clusterPodInventoryPath, "Pod inventory", func(raw []byte, contentType string) error {
		return decodeClusterPodInventory(raw, contentType, out)
	})
}

func decodeClusterPodInventory(raw []byte, contentType string, out *clusterNodePodList) error {
	if contentType != kuberuntime.ContentTypeProtobuf {
		if err := json.Unmarshal(raw, out); err != nil {
			return err
		}
		if out.Metadata.Continue != "" {
			return errors.New("incomplete Kubernetes Pod inventory")
		}
		return nil
	}
	message, err := kubernetesCoreProtobufMessage(raw, "PodList")
	if err != nil {
		return err
	}
	var pods corev1.PodList
	if err := pods.Unmarshal(message); err != nil {
		return err
	}
	if pods.Continue != "" {
		return errors.New("incomplete Kubernetes Pod inventory")
	}
	out.Items = make([]clusterNodePod, len(pods.Items))
	for i := range pods.Items {
		src, dst := &pods.Items[i], &out.Items[i]
		dst.Metadata.Name = src.Name
		dst.Metadata.Namespace = src.Namespace
		dst.Metadata.Labels = src.Labels
		dst.Spec.NodeName = src.Spec.NodeName
		dst.Spec.InitContainers = projectClusterPodContainers(src.Spec.InitContainers)
		dst.Spec.Containers = projectClusterPodContainers(src.Spec.Containers)
		dst.Status.Phase = string(src.Status.Phase)
	}
	return nil
}

func projectClusterPodContainers(src []corev1.Container) []clusterNodeContainer {
	if len(src) == 0 {
		return nil
	}
	out := make([]clusterNodeContainer, len(src))
	for i := range src {
		out[i].Name = src[i].Name
		if src[i].RestartPolicy != nil {
			out[i].RestartPolicy = string(*src[i].RestartPolicy)
		}
		for _, name := range [...]corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory, corev1.ResourceEphemeralStorage} {
			if value, ok := src[i].Resources.Requests[name]; ok {
				if out[i].Resources.Requests == nil {
					out[i].Resources.Requests = make(map[string]string, 3)
				}
				out[i].Resources.Requests[string(name)] = value.String()
			}
		}
	}
	return out
}
