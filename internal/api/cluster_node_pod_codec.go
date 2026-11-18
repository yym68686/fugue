package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"

	"fugue/internal/kubeauth"
	corev1 "k8s.io/api/core/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
)

const clusterPodInventoryPath = "/api/v1/pods"

// The node inventory needs real Pod specs and cannot use PartialObjectMetadata.
// Change only the representation: keep the same full, current LIST and allow
// JSON negotiation with servers that cannot provide Kubernetes Protobuf.
func (c *clusterNodeClient) readClusterPodInventory(ctx context.Context, out *clusterNodePodList) error {
	ctx = kubeauth.WithObservationSite(ctx, kubeauth.ObserveNodePodInventory)
	err := c.readClusterPodInventoryWithAccept(ctx, kuberuntime.ContentTypeProtobuf+","+kuberuntime.ContentTypeJSON, out)
	var status *kubernetesHTTPStatusError
	if errors.As(err, &status) && status.status == http.StatusNotAcceptable && ctx.Err() == nil {
		return c.readClusterPodInventoryWithAccept(ctx, kuberuntime.ContentTypeJSON, out)
	}
	return err
}

func (c *clusterNodeClient) readClusterPodInventoryWithAccept(ctx context.Context, accept string, out *clusterNodePodList) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+clusterPodInventoryPath, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", accept)
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("kubernetes Pod inventory request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		// Kubernetes can negotiate a binary Status here as well. The status
		// code is sufficient for fallback and authorization; do not log a body.
		return &kubernetesHTTPStatusError{status: resp.StatusCode, detail: fmt.Errorf("kubernetes Pod inventory failed: status=%d", resp.StatusCode)}
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read kubernetes Pod inventory: %w", err)
	}
	contentType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err := decodeClusterPodInventory(raw, contentType, out); err != nil {
		return fmt.Errorf("decode kubernetes Pod inventory: %w", err)
	}
	return nil
}

var clusterPodProtobuf = protobuf.NewSerializer(nil, nil)

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
	var envelope kuberuntime.Unknown
	// Decode the Kubernetes framing (not a raw CRI protobuf message). The
	// Unknown branch needs no global scheme and retains the actual wire kind.
	_, gvk, err := clusterPodProtobuf.Decode(raw, nil, &envelope)
	if err != nil {
		return err
	}
	if gvk == nil || *gvk != corev1.SchemeGroupVersion.WithKind("PodList") || envelope.ContentEncoding != "" || envelope.Raw == nil {
		return errors.New("unexpected Kubernetes PodList envelope")
	}
	var pods corev1.PodList
	if err := pods.Unmarshal(envelope.Raw); err != nil {
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
