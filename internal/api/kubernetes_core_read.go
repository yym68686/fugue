package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apimachinery/pkg/types"
)

var clusterCoreProtobuf = protobuf.NewSerializer(nil, nil)

func kubernetesCoreProtobufMessage(raw []byte, kind string) ([]byte, error) {
	return kubernetesProtobufMessage(raw, corev1.SchemeGroupVersion.WithKind(kind))
}

func kubernetesProtobufMessage(raw []byte, expected schema.GroupVersionKind) ([]byte, error) {
	var envelope kuberuntime.Unknown
	// Decode the Kubernetes framing, retaining the actual wire kind.
	_, gvk, err := clusterCoreProtobuf.Decode(raw, nil, &envelope)
	if err != nil {
		return nil, err
	}
	if gvk == nil || *gvk != expected || envelope.ContentEncoding != "" || envelope.Raw == nil {
		return nil, fmt.Errorf("unexpected Kubernetes %s envelope", expected.Kind)
	}
	return envelope.Raw, nil
}

func (c *clusterNodeClient) getKubernetesCoreResource(ctx context.Context, path, subject string, decode func([]byte, string) error) error {
	return c.getKubernetesResource(ctx, path, subject, kuberuntime.ContentTypeProtobuf+","+kuberuntime.ContentTypeJSON, decode)
}

func (c *clusterNodeClient) getKubernetesResource(ctx context.Context, path, subject, accept string, decode func([]byte, string) error) error {
	err := c.getKubernetesCoreResourceWithAccept(ctx, path, subject, accept, decode)
	if kubernetesReadStatus(err, http.StatusNotAcceptable) && ctx.Err() == nil {
		return c.getKubernetesCoreResourceWithAccept(ctx, path, subject, kuberuntime.ContentTypeJSON, decode)
	}
	return err
}

const kubernetesMetadataAccept = kuberuntime.ContentTypeProtobuf + ";as=PartialObjectMetadata;g=meta.k8s.io;v=v1," +
	kuberuntime.ContentTypeJSON + ";as=PartialObjectMetadata;g=meta.k8s.io;v=v1;q=0.9," + kuberuntime.ContentTypeJSON + ";q=0.8"

type kubernetesObjectRevision struct {
	UID             types.UID `json:"uid"`
	ResourceVersion string    `json:"resourceVersion"`
}

func (r kubernetesObjectRevision) matches(original metav1.Object) bool {
	return r.UID != "" && r.ResourceVersion != "" && r.UID == original.GetUID() && r.ResourceVersion == original.GetResourceVersion()
}

// Use this only when the caller needs to recheck an already validated object's
// UID and resourceVersion. The GET remains fresh and targets the exact resource.
func (c *clusterNodeClient) getKubernetesObjectRevision(ctx context.Context, path, kind string, out *kubernetesObjectRevision) error {
	const partialKind = "PartialObjectMetadata"
	return c.getKubernetesResource(ctx, path, kind+" revision", kubernetesMetadataAccept, func(raw []byte, contentType string) error {
		if contentType == kuberuntime.ContentTypeProtobuf {
			message, err := kubernetesProtobufMessage(raw, metav1.SchemeGroupVersion.WithKind(partialKind))
			if err != nil {
				return err
			}
			var metadata metav1.PartialObjectMetadata
			if err := metadata.Unmarshal(message); err != nil {
				return err
			}
			*out = kubernetesObjectRevision{UID: metadata.UID, ResourceVersion: metadata.ResourceVersion}
			return nil
		}
		var response struct {
			Kind     string                   `json:"kind"`
			Metadata kubernetesObjectRevision `json:"metadata"`
		}
		if err := json.Unmarshal(raw, &response); err != nil {
			return err
		}
		if response.Kind != "" && response.Kind != kind && response.Kind != partialKind {
			return errors.New("unexpected Kubernetes object kind")
		}
		*out = response.Metadata
		return nil
	})
}

func kubernetesReadStatus(err error, code int) bool {
	var status *kubernetesHTTPStatusError
	return errors.As(err, &status) && status.status == code
}

func (c *clusterNodeClient) getKubernetesCoreResourceWithAccept(ctx context.Context, path, subject, accept string, decode func([]byte, string) error) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.bearerToken)
	req.Header.Set("Accept", accept)
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("kubernetes %s request: %w", subject, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		// A negotiated error may be a binary Status. Do not expose the body.
		return &kubernetesHTTPStatusError{status: resp.StatusCode, detail: fmt.Errorf("kubernetes %s failed: status=%d", subject, resp.StatusCode)}
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read kubernetes %s: %w", subject, err)
	}
	contentType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err := decode(raw, contentType); err != nil {
		return fmt.Errorf("decode kubernetes %s: %w", subject, err)
	}
	return nil
}

// Read fresh Node status without changing selectors, resourceVersion or RBAC.
func (c *clusterNodeClient) getKubernetesNode(ctx context.Context, path string, node *corev1.Node) error {
	return c.getKubernetesCoreResource(ctx, path, "Node", func(raw []byte, contentType string) error {
		if contentType != kuberuntime.ContentTypeProtobuf {
			return json.Unmarshal(raw, node)
		}
		message, err := kubernetesCoreProtobufMessage(raw, "Node")
		if err != nil {
			return err
		}
		return node.Unmarshal(message)
	})
}

// Capacity consumers need only this sample, not per-Pod, network, filesystem
// or volume stats. Keep the same kubelet source and let callers validate its
// original timestamps against the Node identity on both sides of the read.
func (c *clusterNodeClient) getNodeCapacitySummary(ctx context.Context, nodeName string) (*kubeNodeSummaryNode, error) {
	var sample struct {
		Node kubeNodeSummaryNode `json:"node"`
	}
	path := "/api/v1/nodes/" + url.PathEscape(strings.TrimSpace(nodeName)) + "/proxy/stats/summary"
	err := c.doJSON(ctx, http.MethodGet, path+"?only_cpu_and_memory=true", &sample)
	if ctx.Err() == nil && (kubernetesReadStatus(err, http.StatusBadRequest) || kubernetesReadStatus(err, http.StatusNotAcceptable)) {
		// An older proxy may explicitly reject the query. Do not retry a
		// permission failure, timeout, invalid sample, or server error.
		err = c.doJSON(ctx, http.MethodGet, path, &sample)
	}
	if err != nil {
		return nil, err
	}
	return &sample.Node, nil
}
