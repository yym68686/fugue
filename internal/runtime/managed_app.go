package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	ManagedAppAPIGroup   = "fugue.pro"
	ManagedAppAPIVersion = ManagedAppAPIGroup + "/v1alpha1"
	ManagedAppKind       = "ManagedApp"
	ManagedAppPlural     = "managedapps"

	ManagedAppPhasePending     = "Pending"
	ManagedAppPhaseProgressing = "Progressing"
	ManagedAppPhaseReady       = "Ready"
	ManagedAppPhaseDisabled    = "Disabled"
	ManagedAppPhaseDeleting    = "Deleting"
	ManagedAppPhaseError       = "Error"
)

type OwnerReference struct {
	APIVersion         string
	Kind               string
	Name               string
	UID                string
	Controller         bool
	BlockOwnerDeletion bool
}

type ManagedAppObject struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	Metadata   ManagedAppMeta   `json:"metadata"`
	Spec       ManagedAppSpec   `json:"spec"`
	Status     ManagedAppStatus `json:"status,omitempty"`
}

type ManagedAppMeta struct {
	Name              string            `json:"name,omitempty"`
	Namespace         string            `json:"namespace,omitempty"`
	UID               string            `json:"uid,omitempty"`
	ResourceVersion   string            `json:"resourceVersion,omitempty"`
	Generation        int64             `json:"generation,omitempty"`
	DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
}

type ManagedAppSpec struct {
	AppID           string                 `json:"appID"`
	TenantID        string                 `json:"tenantID"`
	ProjectID       string                 `json:"projectID,omitempty"`
	Name            string                 `json:"name"`
	AppSpec         model.AppSpec          `json:"appSpec"`
	Bindings        []model.ServiceBinding `json:"bindings,omitempty"`
	BackingServices []model.BackingService `json:"backingServices,omitempty"`
	Scheduling      SchedulingConstraints  `json:"scheduling,omitempty"`
}

type ManagedAppStatus struct {
	Phase                   string                        `json:"phase,omitempty"`
	Message                 string                        `json:"message,omitempty"`
	ReadyReplicas           int                           `json:"readyReplicas,omitempty"`
	DesiredReplicas         int                           `json:"desiredReplicas,omitempty"`
	ObservedGeneration      int64                         `json:"observedGeneration,omitempty"`
	LastAppliedSpecHash     string                        `json:"lastAppliedSpecHash,omitempty"`
	LastAppliedTime         string                        `json:"lastAppliedTime,omitempty"`
	CurrentReleaseKey       string                        `json:"currentReleaseKey,omitempty"`
	CurrentReleaseStartedAt string                        `json:"currentReleaseStartedAt,omitempty"`
	CurrentReleaseReadyAt   string                        `json:"currentReleaseReadyAt,omitempty"`
	PendingReleaseKey       string                        `json:"pendingReleaseKey,omitempty"`
	PendingReleaseStartedAt string                        `json:"pendingReleaseStartedAt,omitempty"`
	BackingServices         []ManagedBackingServiceStatus `json:"backingServices,omitempty"`
	Conditions              []ManagedAppCondition         `json:"conditions,omitempty"`
}

type ManagedBackingServiceStatus struct {
	ServiceID               string `json:"serviceID,omitempty"`
	RuntimeKey              string `json:"runtimeKey,omitempty"`
	CurrentRuntimeStartedAt string `json:"currentRuntimeStartedAt,omitempty"`
	CurrentRuntimeReadyAt   string `json:"currentRuntimeReadyAt,omitempty"`
}

type ManagedAppCondition struct {
	Type               string `json:"type,omitempty"`
	Status             string `json:"status,omitempty"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

func ManagedAppResourceName(app model.App) string {
	if id := strings.TrimSpace(app.ID); id != "" {
		name := model.Slugify(strings.ReplaceAll(id, "_", "-"))
		if len(name) > 63 {
			return name[:63]
		}
		if name != "" {
			return name
		}
	}
	name := sanitizeName(app.Name)
	if name == "" {
		return "app"
	}
	return name
}

func RuntimeResourceName(name string) string {
	return sanitizeName(name)
}

func BuildManagedAppObject(app model.App, scheduling SchedulingConstraints) map[string]any {
	labels := appLabels(app)
	labels[FugueLabelManagedApp] = ManagedAppResourceName(app)

	managed := ManagedAppObject{
		APIVersion: ManagedAppAPIVersion,
		Kind:       ManagedAppKind,
		Metadata: ManagedAppMeta{
			Name:      ManagedAppResourceName(app),
			Namespace: NamespaceForTenant(app.TenantID),
			Labels:    labels,
		},
		Spec: ManagedAppSpec{
			AppID:           strings.TrimSpace(app.ID),
			TenantID:        strings.TrimSpace(app.TenantID),
			ProjectID:       strings.TrimSpace(app.ProjectID),
			Name:            strings.TrimSpace(app.Name),
			AppSpec:         cloneManagedAppSpec(app.Spec),
			Bindings:        cloneManagedServiceBindings(app.Bindings),
			BackingServices: cloneManagedBackingServices(app.BackingServices),
			Scheduling:      cloneSchedulingConstraints(scheduling),
		},
	}
	return mustManagedAppMap(managed)
}

func BuildManagedAppStateObjects(app model.App, scheduling SchedulingConstraints) []map[string]any {
	return []map[string]any{
		buildNamespaceObject(NamespaceForTenant(app.TenantID)),
		BuildManagedAppObject(app, scheduling),
	}
}

func BuildManagedAppChildObjects(app model.App, scheduling SchedulingConstraints, ownerRef *OwnerReference) []map[string]any {
	objects := buildAppObjectsWithOwner(app, scheduling, ownerRef)
	if len(objects) == 0 {
		return nil
	}
	return objects[1:]
}

func ManagedAppObjectFromMap(obj map[string]any) (ManagedAppObject, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return ManagedAppObject{}, fmt.Errorf("marshal managed app object: %w", err)
	}
	var managed ManagedAppObject
	if err := json.Unmarshal(data, &managed); err != nil {
		return ManagedAppObject{}, fmt.Errorf("decode managed app object: %w", err)
	}
	return managed, nil
}

func ManagedAppOwnerReference(managed ManagedAppObject) *OwnerReference {
	if strings.TrimSpace(managed.Metadata.Name) == "" || strings.TrimSpace(managed.Metadata.UID) == "" {
		return nil
	}
	return &OwnerReference{
		APIVersion:         ManagedAppAPIVersion,
		Kind:               ManagedAppKind,
		Name:               managed.Metadata.Name,
		UID:                managed.Metadata.UID,
		Controller:         true,
		BlockOwnerDeletion: true,
	}
}

func AppFromManagedApp(managed ManagedAppObject) model.App {
	return model.App{
		ID:              strings.TrimSpace(managed.Spec.AppID),
		TenantID:        strings.TrimSpace(managed.Spec.TenantID),
		ProjectID:       strings.TrimSpace(managed.Spec.ProjectID),
		Name:            strings.TrimSpace(managed.Spec.Name),
		Spec:            cloneManagedAppSpec(managed.Spec.AppSpec),
		Bindings:        cloneManagedServiceBindings(managed.Spec.Bindings),
		BackingServices: cloneManagedBackingServices(managed.Spec.BackingServices),
	}
}

func OverlayAppStatusFromManagedApp(app model.App, managed ManagedAppObject) model.App {
	out := app
	status := managed.Status
	if strings.TrimSpace(status.Phase) == "" &&
		status.ReadyReplicas == 0 &&
		strings.TrimSpace(status.Message) == "" &&
		strings.TrimSpace(status.CurrentReleaseStartedAt) == "" &&
		strings.TrimSpace(status.CurrentReleaseReadyAt) == "" &&
		len(status.BackingServices) == 0 {
		return out
	}

	switch strings.TrimSpace(status.Phase) {
	case ManagedAppPhaseReady:
		out.Status.Phase = "deployed"
	case ManagedAppPhaseDisabled:
		out.Status.Phase = "disabled"
	case ManagedAppPhaseDeleting:
		out.Status.Phase = "deleting"
	case ManagedAppPhaseError:
		out.Status.Phase = "failed"
	case ManagedAppPhasePending, ManagedAppPhaseProgressing:
		out.Status.Phase = "deploying"
	}

	out.Status.CurrentRuntimeID = out.Spec.RuntimeID
	out.Status.CurrentReplicas = status.ReadyReplicas
	if startedAt, ok := parseManagedAppStatusTime(status.CurrentReleaseStartedAt); ok {
		out.Status.CurrentReleaseStartedAt = &startedAt
	} else if strings.TrimSpace(status.CurrentReleaseStartedAt) == "" {
		out.Status.CurrentReleaseStartedAt = nil
	}
	if readyAt, ok := parseManagedAppStatusTime(status.CurrentReleaseReadyAt); ok {
		out.Status.CurrentReleaseReadyAt = &readyAt
	} else if strings.TrimSpace(status.CurrentReleaseReadyAt) == "" {
		out.Status.CurrentReleaseReadyAt = nil
	}
	if strings.TrimSpace(status.Message) != "" {
		out.Status.LastMessage = strings.TrimSpace(status.Message)
	}
	if updatedAt, ok := parseManagedAppStatusTime(status.LastAppliedTime); ok {
		out.Status.UpdatedAt = updatedAt
	}
	overlayManagedBackingServiceStatus(&out, status.BackingServices)
	return out
}

func ManagedAppSpecHash(spec ManagedAppSpec) string {
	payload, err := json.Marshal(spec)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func cloneManagedAppSpec(spec model.AppSpec) model.AppSpec {
	out := spec
	if len(spec.Command) > 0 {
		out.Command = append([]string(nil), spec.Command...)
	}
	if len(spec.Args) > 0 {
		out.Args = append([]string(nil), spec.Args...)
	}
	if len(spec.Ports) > 0 {
		out.Ports = append([]int(nil), spec.Ports...)
	}
	if len(spec.Env) > 0 {
		out.Env = make(map[string]string, len(spec.Env))
		for key, value := range spec.Env {
			out.Env[key] = value
		}
	}
	if len(spec.Files) > 0 {
		out.Files = append([]model.AppFile(nil), spec.Files...)
	}
	if spec.Workspace != nil {
		workspace := *spec.Workspace
		out.Workspace = &workspace
	}
	if spec.Failover != nil {
		failover := *spec.Failover
		out.Failover = &failover
	}
	if spec.Postgres != nil {
		postgres := *spec.Postgres
		out.Postgres = &postgres
	}
	return out
}

func cloneManagedServiceBindings(bindings []model.ServiceBinding) []model.ServiceBinding {
	if len(bindings) == 0 {
		return nil
	}
	out := make([]model.ServiceBinding, len(bindings))
	for index, binding := range bindings {
		out[index] = binding
		if len(binding.Env) == 0 {
			continue
		}
		out[index].Env = make(map[string]string, len(binding.Env))
		for key, value := range binding.Env {
			out[index].Env[key] = value
		}
	}
	return out
}

func cloneManagedBackingServices(services []model.BackingService) []model.BackingService {
	if len(services) == 0 {
		return nil
	}
	out := make([]model.BackingService, len(services))
	for index, service := range services {
		out[index] = service
		out[index].CurrentRuntimeStartedAt = nil
		out[index].CurrentRuntimeReadyAt = nil
		if service.Spec.Postgres == nil {
			continue
		}
		postgres := *service.Spec.Postgres
		out[index].Spec.Postgres = &postgres
	}
	return out
}

func cloneSchedulingConstraints(in SchedulingConstraints) SchedulingConstraints {
	out := SchedulingConstraints{}
	if len(in.NodeSelector) > 0 {
		out.NodeSelector = make(map[string]string, len(in.NodeSelector))
		for key, value := range in.NodeSelector {
			out.NodeSelector[key] = value
		}
	}
	if len(in.Tolerations) > 0 {
		out.Tolerations = append([]Toleration(nil), in.Tolerations...)
	}
	return out
}

func overlayManagedBackingServiceStatus(app *model.App, statuses []ManagedBackingServiceStatus) {
	if app == nil || len(app.BackingServices) == 0 || len(statuses) == 0 {
		return
	}
	statusByID := make(map[string]ManagedBackingServiceStatus, len(statuses))
	for _, status := range statuses {
		if id := strings.TrimSpace(status.ServiceID); id != "" {
			statusByID[id] = status
		}
	}
	for index := range app.BackingServices {
		status, ok := statusByID[strings.TrimSpace(app.BackingServices[index].ID)]
		if !ok {
			continue
		}
		if startedAt, ok := parseManagedAppStatusTime(status.CurrentRuntimeStartedAt); ok {
			app.BackingServices[index].CurrentRuntimeStartedAt = &startedAt
		} else if strings.TrimSpace(status.CurrentRuntimeStartedAt) == "" {
			app.BackingServices[index].CurrentRuntimeStartedAt = nil
		}
		if readyAt, ok := parseManagedAppStatusTime(status.CurrentRuntimeReadyAt); ok {
			app.BackingServices[index].CurrentRuntimeReadyAt = &readyAt
		} else if strings.TrimSpace(status.CurrentRuntimeReadyAt) == "" {
			app.BackingServices[index].CurrentRuntimeReadyAt = nil
		}
	}
}

func parseManagedAppStatusTime(value string) (time.Time, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, trimmed)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func ownerReferenceMaps(ownerRef *OwnerReference) []map[string]any {
	if ownerRef == nil {
		return nil
	}
	return []map[string]any{
		{
			"apiVersion":         ownerRef.APIVersion,
			"kind":               ownerRef.Kind,
			"name":               ownerRef.Name,
			"uid":                ownerRef.UID,
			"controller":         ownerRef.Controller,
			"blockOwnerDeletion": ownerRef.BlockOwnerDeletion,
		},
	}
}

func attachOwnerReference(objects []map[string]any, ownerRef *OwnerReference) {
	if ownerRef == nil {
		return
	}
	references := ownerReferenceMaps(ownerRef)
	for _, obj := range objects {
		kind, _ := obj["kind"].(string)
		if kind == "Namespace" {
			continue
		}
		metadata, _ := obj["metadata"].(map[string]any)
		if metadata == nil {
			continue
		}
		metadata["ownerReferences"] = references
	}
}

func mustManagedAppMap(managed ManagedAppObject) map[string]any {
	payload, err := json.Marshal(managed)
	if err != nil {
		panic(err)
	}
	var out map[string]any
	if err := json.Unmarshal(payload, &out); err != nil {
		panic(err)
	}
	return out
}
