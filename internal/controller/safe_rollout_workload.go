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
	first, err := s.captureSafeRolloutWorkload(ctx, client, op, state.CandidateApp, state.Candidate, objects)
	if err != nil {
		return err
	}
	bound, err := s.Store.BindAppReleaseWorkload(ctx, state.Candidate, first)
	if err != nil {
		return fmt.Errorf("persist immutable revision identity: %w", err)
	}
	state.Candidate = bound
	return nil
}

func (s *Service) captureSafeRolloutWorkload(ctx context.Context, client *kubeClient, op model.Operation, app model.App, release model.AppRelease, objects []map[string]any) (model.AppReleaseWorkload, error) {
	if op.ID == "" || op.AppID != app.ID || op.TenantID != app.TenantID || release.AppID != app.ID || release.TenantID != app.TenantID {
		return model.AppReleaseWorkload{}, fmt.Errorf("revision operation ownership differs")
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
		return model.AppReleaseWorkload{}, fmt.Errorf("revision has no complete desired resource pair")
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
		return model.AppReleaseWorkload{}, err
	}
	second, err := read()
	if err != nil {
		return model.AppReleaseWorkload{}, err
	}
	if first != second {
		return model.AppReleaseWorkload{}, fmt.Errorf("revision resources changed during identity capture")
	}
	return first, nil
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
		if !historicalWorkloadCreatedDuringOperation(m, op) {
			return out, fmt.Errorf("historical resource was not created during its source operation")
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
	if op.Status == model.OperationStatusCompleted || op.Status == model.OperationStatusFailed {
		for _, field := range []string{"containers", "initContainers"} {
			for _, actual := range mapSlice(nestedObjectValue(dep, "spec", "template", "spec", field)) {
				if actual["name"] != "fugue-drain-agent" {
					continue
				}
				for _, desired := range mapSlice(nestedObjectValue(expectedDeployment, "spec", "template", "spec", field)) {
					if desired["name"] != actual["name"] {
						continue
					}
					// These execution fields have no Kubernetes default. Extra
					// arguments or environment sources cannot hide in a subset match.
					for _, key := range []string{"command", "args", "envFrom", "workingDir"} {
						if !reflect.DeepEqual(normalizeKubeValue(actual[key]), normalizeKubeValue(desired[key])) {
							return out, fmt.Errorf("historical drain helper execution differs")
						}
					}
				}
			}
		}
	}
	selector := objectStringMapValue(nestedObjectValue(svc, "spec", "selector"))
	if !revisionServiceSelectorMatches(selector, expectedService, expectedDeployment) ||
		selector[runtime.FugueLabelAppReleaseID] != release.ID || selector[runtime.FugueLabelAppWorkload] != release.DeploymentName ||
		!revisionAppliedFieldsMatch("spec.ports", normalizeKubeValue(nestedObjectValue(svc, "spec", "ports")), normalizeKubeValue(nestedObjectValue(expectedService, "spec", "ports"))) {
		return out, fmt.Errorf("revision Service does not select the applied workload")
	}
	return model.AppReleaseWorkload{OperationID: op.ID, Namespace: ns, DeploymentName: release.DeploymentName, DeploymentUID: objectStringField(dm, "uid"), DeploymentGeneration: int64(generation),
		ServiceName: release.ServiceName, ServiceUID: objectStringField(sm, "uid"), ReleaseKey: key, RuntimeID: release.RuntimeID, ImageRef: release.ResolvedImageRef}, nil
}

// Accept the two known renderer schemas, not an arbitrary subset of Pod
// labels. Older revision Services included presentation/project labels; both
// schemas must retain the exact tenant, workload and immutable release identity.
func revisionServiceSelectorMatches(actual map[string]string, service, deployment map[string]any) bool {
	expected := objectStringMapValue(nestedObjectValue(service, "spec", "selector"))
	if reflect.DeepEqual(actual, expected) {
		return true
	}
	legacy := make(map[string]string, len(expected)+3)
	for key, value := range expected {
		legacy[key] = value
	}
	labels := objectStringMapValue(nestedObjectValue(deployment, "spec", "template", "metadata", "labels"))
	for _, key := range []string{runtime.FugueLabelName, runtime.FugueLabelProjectID, runtime.FugueLabelOwnerAppID} {
		if value, ok := labels[key]; ok {
			legacy[key] = value
		}
	}
	return reflect.DeepEqual(actual, legacy)
}

// Kubernetes adds defaults inside list entries too. Compare all fields we
// applied recursively, preserving exact list membership and element order.
func revisionAppliedFieldsMatch(path string, actual, desired any) bool {
	switch want := desired.(type) {
	case map[string]any:
		// Optional empty maps (such as resource limits) are omitted by the API.
		if actual == nil && len(want) == 0 {
			return true
		}
		got, ok := actual.(map[string]any)
		if !ok {
			return false
		}
		// EnvVar.Value has an empty-string default and the Kubernetes API omits
		// it on serialization. Accept only a named literal EnvVar with no source;
		// an injected valueFrom must never masquerade as an empty literal.
		if strings.HasSuffix(path, ".env") && want["name"] != nil && want["value"] == "" && want["valueFrom"] == nil && got["value"] == nil {
			if got["valueFrom"] != nil {
				return false
			}
			copy := make(map[string]any, len(got)+1)
			for key, value := range got {
				copy[key] = value
			}
			copy["value"] = ""
			got = copy
		}
		for key, value := range want {
			if !revisionAppliedFieldsMatch(path+"."+key, got[key], value) {
				return false
			}
		}
		return true
	case []any:
		// Kubernetes serializes optional empty lists as absent (for example
		// volumes and volumeMounts). Both contain no members; a nonempty or
		// differently typed observed value must still fail the identity check.
		if actual == nil && len(want) == 0 {
			return true
		}
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
		// The API omits the default zero delay when serializing probes even
		// when the submitted manifest explicitly contains zero.
		if actual == nil && desired == float64(0) &&
			(strings.HasSuffix(path, ".readinessProbe.initialDelaySeconds") || strings.HasSuffix(path, ".livenessProbe.initialDelaySeconds") || strings.HasSuffix(path, ".startupProbe.initialDelaySeconds")) {
			return true
		}
		for _, suffix := range []string{"resources.requests.cpu", "resources.requests.memory", "resources.limits.cpu", "resources.limits.memory"} {
			if strings.HasSuffix(path, suffix) && kubeQuantityValueEqual(actual, desired) {
				return true
			}
		}
		return reflect.DeepEqual(actual, desired)
	}
}
