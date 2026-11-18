package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"fugue/internal/model"
	"fugue/internal/runtime"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
)

// The cold-migration marker identifies both the fenced source and the restored
// target. Only the exact completed, serving target can use the image migration
// path. Keep the marker and the recovery record intact for source retirement.
func (c *kubeClient) verifyImageMigrationColdTarget(ctx context.Context, ns, serviceID string, postgres model.AppPostgresSpec, cluster kubeCloudNativePGCluster, primaryJSON []byte) error {
	marker := coldStateName(serviceID)
	if serviceID == "" || cluster.Metadata.Labels[runtime.FugueLabelBackingServiceID] != serviceID ||
		cluster.Metadata.Annotations[coldMigrationAnnotation] != marker {
		return fmt.Errorf("database cold migration marker does not belong to this service")
	}
	st, _, err := loadColdState(ctx, c, ns, serviceID)
	if err != nil {
		return fmt.Errorf("cannot verify database cold migration record: %w", err)
	}
	endpoint := model.PostgresEndpointName(postgres)
	if st == nil || st.Phase != "complete" || st.TargetName != postgres.ServiceName || st.TargetName != cluster.Metadata.Name ||
		st.TargetUID == "" || st.TargetUID != cluster.Metadata.UID || st.SystemID == "" || st.SystemID != cluster.Status.SystemID ||
		st.SourceName == "" || st.SourceName == st.TargetName || st.SourceUID == "" || st.SourceUID == st.TargetUID ||
		endpoint == "" || st.Endpoint != endpoint || cluster.Metadata.DeletionTimestamp != "" {
		return fmt.Errorf("database cold migration record does not prove this Cluster is the completed target")
	}
	source, found, err := c.getCloudNativePGCluster(ctx, ns, st.SourceName)
	if err != nil {
		return fmt.Errorf("cannot verify retained cold migration source: %w", err)
	}
	if found && (source.Metadata.UID != st.SourceUID || source.Metadata.Annotations[coldMigrationAnnotation] != marker ||
		source.Metadata.Annotations["cnpg.io/fencedInstances"] != `["*"]`) {
		return fmt.Errorf("retained cold migration source is not the exact fenced Cluster")
	}

	// The completed record proves historical cutover. The Service and its
	// EndpointSlices must still exclusively route to this observed primary.
	var svc corev1.Service
	svcPath := "/api/v1/namespaces/" + url.PathEscape(ns) + "/services/" + url.PathEscape(endpoint)
	if _, err := c.doJSON(ctx, http.MethodGet, svcPath, nil, &svc); err != nil {
		return fmt.Errorf("cannot verify the cold migration stable Service: %w", err)
	}
	if svc.UID == "" || svc.Name != endpoint || svc.Namespace != ns || svc.DeletionTimestamp != nil ||
		svc.Spec.PublishNotReadyAddresses || len(svc.Spec.Selector) != 2 ||
		svc.Spec.Selector[cloudNativePGClusterLabel] != st.TargetName || svc.Spec.Selector[cloudNativePGInstanceRoleLabel] != "primary" {
		return fmt.Errorf("cold migration stable Service does not exclusively select the completed target primary")
	}
	for _, owner := range svc.OwnerReferences {
		if string(owner.UID) == st.SourceUID {
			return fmt.Errorf("cold migration stable Service is still owned by the source")
		}
	}
	var primary corev1.Pod
	if err := json.Unmarshal(primaryJSON, &primary); err != nil {
		return fmt.Errorf("cannot decode cold migration primary identity: %w", err)
	}
	if primary.Status.PodIP == "" {
		return fmt.Errorf("cold migration primary Pod IP is unavailable")
	}
	ips := map[string]bool{primary.Status.PodIP: true}
	for _, ip := range primary.Status.PodIPs {
		if ip.IP != "" {
			ips[ip.IP] = true
		}
	}
	var slices discoveryv1.EndpointSliceList
	slicePath := "/apis/discovery.k8s.io/v1/namespaces/" + url.PathEscape(ns) + "/endpointslices?labelSelector=" +
		url.QueryEscape(discoveryv1.LabelServiceName+"="+endpoint)
	if _, err := c.doJSON(ctx, http.MethodGet, slicePath, nil, &slices); err != nil {
		return fmt.Errorf("cannot verify the cold migration stable EndpointSlices: %w", err)
	}
	if slices.Continue != "" {
		return fmt.Errorf("cold migration stable EndpointSlice inventory is incomplete")
	}
	ready := false
	for _, slice := range slices.Items {
		owned := false
		for _, owner := range slice.OwnerReferences {
			owned = owned || (owner.Kind == "Service" && owner.APIVersion == "v1" && owner.Name == svc.Name && owner.UID == svc.UID)
		}
		if !owned || slice.Namespace != ns || slice.Labels[discoveryv1.LabelServiceName] != endpoint || slice.DeletionTimestamp != nil {
			return fmt.Errorf("cold migration stable EndpointSlice does not belong to the verified Service")
		}
		for _, ep := range slice.Endpoints {
			ref := ep.TargetRef
			if ref == nil || ref.Kind != "Pod" || ref.Namespace != ns || ref.Name != primary.Name || ref.UID != primary.UID ||
				ep.Conditions.Ready == nil || !*ep.Conditions.Ready || (ep.Conditions.Serving != nil && !*ep.Conditions.Serving) ||
				(ep.Conditions.Terminating != nil && *ep.Conditions.Terminating) || len(ep.Addresses) == 0 {
				return fmt.Errorf("cold migration stable endpoint does not exclusively serve the Ready target primary")
			}
			for _, addr := range ep.Addresses {
				if !ips[addr] {
					return fmt.Errorf("cold migration stable endpoint address is not on the target primary")
				}
			}
			ready = true
		}
	}
	if !ready {
		return fmt.Errorf("cold migration stable endpoint has no Ready target primary")
	}
	return nil
}
