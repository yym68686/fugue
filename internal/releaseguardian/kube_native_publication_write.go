package releaseguardian

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// The typed client can replay a write after Retry-After. Under a durable
// claim an indeterminate response must instead leave the claim pending.
type nativeConfigMapWriter struct{ client rest.Interface }

func (w nativeConfigMapWriter) Create(ctx context.Context, cm *corev1.ConfigMap, opts metav1.CreateOptions) (*corev1.ConfigMap, error) {
	observed := &corev1.ConfigMap{}
	err := w.client.Post().Namespace(cm.Namespace).Resource("configmaps").
		VersionedParams(&opts, scheme.ParameterCodec).MaxRetries(0).Body(cm).Do(ctx).Into(observed)
	return observed, err
}

func (w nativeConfigMapWriter) Update(ctx context.Context, cm *corev1.ConfigMap, opts metav1.UpdateOptions) (*corev1.ConfigMap, error) {
	observed := &corev1.ConfigMap{}
	err := w.client.Put().Namespace(cm.Namespace).Resource("configmaps").Name(cm.Name).
		VersionedParams(&opts, scheme.ParameterCodec).MaxRetries(0).Body(cm).Do(ctx).Into(observed)
	return observed, err
}
