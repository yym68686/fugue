package api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
)

const (
	appImageRegistryGCRequestedAtAnnotation   = "fugue.pro/registry-gc-requested-at"
	appImageRegistryGCRequestReasonAnnotation = "fugue.pro/registry-gc-request-reason"
)

func (s *Server) requestAppImageRegistryGarbageCollect(ctx context.Context, reason string) error {
	if s != nil && strings.TrimSpace(s.imageStoreMode) == "distributed" {
		return nil
	}
	if s.requestRegistryGC != nil {
		return s.requestRegistryGC(ctx, reason)
	}
	namespace, err := s.controlPlaneNamespaceForRegistryGC()
	if err != nil {
		return err
	}
	leaseName := strings.TrimSpace(s.registryGCLeaseName)
	if leaseName == "" {
		return fmt.Errorf("registry GC lease name is not configured")
	}

	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		return fmt.Errorf("registry GC request is not available: %w", err)
	}
	defer client.closeIdleConnections()

	apiPath := "/apis/coordination.k8s.io/v1/namespaces/" + url.PathEscape(namespace) + "/leases/" + url.PathEscape(leaseName)
	for attempt := 0; attempt < 4; attempt++ {
		var lease coordinationv1.Lease
		if err := client.doJSON(ctx, http.MethodGet, apiPath, &lease); err != nil {
			return fmt.Errorf("read registry GC lease: %w", err)
		}
		if lease.Annotations == nil {
			lease.Annotations = make(map[string]string)
		}
		lease.Annotations[appImageRegistryGCRequestedAtAnnotation] = time.Now().UTC().Format(time.RFC3339Nano)
		lease.Annotations[appImageRegistryGCRequestReasonAnnotation] = truncateAppImageGCReason(reason, 240)
		if err := client.doJSONWithBody(ctx, http.MethodPut, apiPath, lease, nil); err != nil {
			if strings.Contains(err.Error(), "status=409") {
				continue
			}
			return fmt.Errorf("update registry GC lease: %w", err)
		}
		return nil
	}
	return fmt.Errorf("update registry GC lease after retries")
}

func (s *Server) controlPlaneNamespaceForRegistryGC() (string, error) {
	if namespace := strings.TrimSpace(s.controlPlaneNamespace); namespace != "" {
		return namespace, nil
	}
	namespace, err := kubeNamespace()
	if err != nil {
		return "", fmt.Errorf("resolve control plane namespace: %w", err)
	}
	return namespace, nil
}

func truncateAppImageGCReason(value string, limit int) string {
	value = strings.TrimSpace(value)
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[:limit]
}
