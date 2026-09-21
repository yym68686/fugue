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

func (s *Service) bindSafeRolloutWorkload(ctx context.Context, client *kubeClient, op model.Operation, state *safeRolloutState, objects []map[string]any) error {
	app, release := state.CandidateApp, state.Candidate
	if op.ID == "" || op.AppID != app.ID || op.TenantID != app.TenantID || release.AppID != app.ID || release.TenantID != app.TenantID {
		return fmt.Errorf("revision operation ownership differs")
	}
	var expectedDeployment, expectedService map[string]any
	for _, obj := range objects {
		name := objectStringField(objectMapField(obj, "metadata"), "name")
		if obj["kind"] == "Deployment" && name == release.DeploymentName {
			expectedDeployment = obj
		}
		if obj["kind"] == "Service" && name == release.ServiceName {
			expectedService = obj
		}
	}
	if expectedDeployment == nil || expectedService == nil {
		return fmt.Errorf("revision has no complete desired resource pair")
	}
	ns := runtime.NamespaceForTenant(app.TenantID)
	depPath := deploymentAPIPath(ns, release.DeploymentName)
	svcPath := "/api/v1/namespaces/" + url.PathEscape(ns) + "/services/" + url.PathEscape(release.ServiceName)
	read := func() (model.AppReleaseWorkload, error) {
		dep, found, err := client.getRawObject(ctx, depPath)
		if err != nil || !found {
			return model.AppReleaseWorkload{}, fmt.Errorf("read revision Deployment identity")
		}
		svc, found, err := client.getRawObject(ctx, svcPath)
		if err != nil || !found {
			return model.AppReleaseWorkload{}, fmt.Errorf("read revision Service identity")
		}
		return s.safeRolloutWorkloadIdentity(op, app, release, dep, svc, expectedDeployment, expectedService)
	}
	first, err := read()
	if err != nil {
		return err
	}
	second, err := read()
	if err != nil {
		return err
	}
	if first != second {
		return fmt.Errorf("revision resources changed during identity capture")
	}
	bound, err := s.Store.BindAppReleaseWorkload(ctx, release, first)
	if err != nil {
		return fmt.Errorf("persist immutable revision identity: %w", err)
	}
	state.Candidate = bound
	return nil
}

func (s *Service) safeRolloutWorkloadIdentity(op model.Operation, app model.App, release model.AppRelease, dep, svc, expectedDeployment, expectedService map[string]any) (model.AppReleaseWorkload, error) {
	var out model.AppReleaseWorkload
	ns := runtime.NamespaceForTenant(app.TenantID)
	for _, pair := range []struct {
		object map[string]any
		name   string
	}{{dep, release.DeploymentName}, {svc, release.ServiceName}} {
		m := objectMapField(pair.object, "metadata")
		labels := objectStringMapValue(m["labels"])
		if objectStringField(m, "name") != pair.name || objectStringField(m, "namespace") != ns || objectStringField(m, "uid") == "" || objectStringField(m, "deletionTimestamp") != "" ||
			!appWorkloadOwnerMatches(m, map[string]string{runtime.FugueLabelAppID: app.ID, runtime.FugueLabelTenantID: app.TenantID}) || labels[runtime.FugueLabelAppReleaseID] != release.ID {
			return out, fmt.Errorf("revision resource ownership or lifecycle differs")
		}
	}
	dm, sm := objectMapField(dep, "metadata"), objectMapField(svc, "metadata")
	key := objectStringMapValue(dm["annotations"])[runtime.FugueAnnotationReleaseKey]
	expectedKey := objectStringMapValue(objectMapField(expectedDeployment, "metadata")["annotations"])[runtime.FugueAnnotationReleaseKey]
	generation, ok := dm["generation"].(float64)
	if !ok || generation < 1 || generation != float64(int64(generation)) || key == "" || key != expectedKey ||
		objectStringMapValue(nestedObjectValue(dep, "spec", "template", "metadata", "annotations"))[runtime.FugueAnnotationReleaseKey] != key ||
		!revisionAppliedFieldsMatch("spec.template", normalizeKubeValue(nestedObjectValue(dep, "spec", "template")), normalizeKubeValue(nestedObjectValue(expectedDeployment, "spec", "template"))) {
		return out, fmt.Errorf("revision Deployment does not match applied executable identity")
	}
	selector := objectStringMapValue(nestedObjectValue(svc, "spec", "selector"))
	if !reflect.DeepEqual(selector, objectStringMapValue(nestedObjectValue(expectedService, "spec", "selector"))) ||
		selector[runtime.FugueLabelAppReleaseID] != release.ID || selector[runtime.FugueLabelAppWorkload] != release.DeploymentName ||
		!revisionAppliedFieldsMatch("spec.ports", normalizeKubeValue(nestedObjectValue(svc, "spec", "ports")), normalizeKubeValue(nestedObjectValue(expectedService, "spec", "ports"))) {
		return out, fmt.Errorf("revision Service does not select the applied workload")
	}
	return model.AppReleaseWorkload{OperationID: op.ID, Namespace: ns, DeploymentName: release.DeploymentName, DeploymentUID: objectStringField(dm, "uid"), DeploymentGeneration: int64(generation),
		ServiceName: release.ServiceName, ServiceUID: objectStringField(sm, "uid"), ReleaseKey: key, RuntimeID: release.RuntimeID, ImageRef: release.ResolvedImageRef}, nil
}

// Kubernetes adds defaults inside list entries too. Compare all fields we
// applied recursively, preserving exact list membership and element order.
func revisionAppliedFieldsMatch(path string, actual, desired any) bool {
	switch want := desired.(type) {
	case map[string]any:
		got, ok := actual.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range want {
			if !revisionAppliedFieldsMatch(path+"."+key, got[key], value) {
				return false
			}
		}
		return true
	case []any:
		got, ok := actual.([]any)
		if !ok || len(got) != len(want) {
			return false
		}
		for i, value := range want {
			if !revisionAppliedFieldsMatch(path, got[i], value) {
				return false
			}
		}
		return true
	default:
		for _, suffix := range []string{"resources.requests.cpu", "resources.requests.memory", "resources.limits.cpu", "resources.limits.memory"} {
			if strings.HasSuffix(path, suffix) && kubeQuantityValueEqual(actual, desired) {
				return true
			}
		}
		return reflect.DeepEqual(actual, desired)
	}
}
