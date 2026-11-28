package managedappstatus

import (
	"maps"
	"slices"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

type AppSnapshot struct {
	Managed     runtime.ManagedAppObject
	Found       bool
	Ok          bool
	ClusterID   string
	Evidence    Evidence
	RefreshedAt time.Time
	ExpiresAt   time.Time
	Sequence    Sequence
}

type InventoryInput struct {
	Items       map[string]runtime.ManagedAppObject
	Evidence    map[string]Evidence
	Ok          bool
	ClusterID   string
	RefreshedAt time.Time
	ExpiresAt   time.Time
	Sequence    Sequence
}

// InventorySnapshot is immutable. A lookup detaches just the selected entry.
// Callers needing the entire inventory explicitly request a detached map.
type InventorySnapshot struct {
	items       map[string]runtime.ManagedAppObject
	evidence    map[string]Evidence
	Ok          bool
	ClusterID   string
	RefreshedAt time.Time
	ExpiresAt   time.Time
	Sequence    Sequence
}

func NewInventory(input InventoryInput) InventorySnapshot {
	var evidence map[string]Evidence
	if input.Evidence != nil {
		evidence = make(map[string]Evidence, len(input.Evidence))
		for key, value := range input.Evidence {
			evidence[key] = value.clone()
		}
	}
	return InventorySnapshot{
		items: copyItems(input.Items), evidence: evidence,
		Ok: input.Ok, ClusterID: input.ClusterID,
		RefreshedAt: input.RefreshedAt, ExpiresAt: input.ExpiresAt, Sequence: input.Sequence,
	}
}

func (s InventorySnapshot) Len() int {
	return len(s.items)
}

func (s InventorySnapshot) Has(key string) bool {
	_, found := s.items[key]
	return found
}

func (s InventorySnapshot) EvidenceFor(key string) Evidence {
	return s.evidence[key].clone()
}

func (s InventorySnapshot) App(key string) AppSnapshot {
	return s.app(key).clone()
}

func (s InventorySnapshot) app(key string) AppSnapshot {
	managed, found := s.items[key]
	return AppSnapshot{
		Managed: managed, Found: found, Ok: s.Ok, ClusterID: s.ClusterID,
		Evidence: s.evidence[key], RefreshedAt: s.RefreshedAt, ExpiresAt: s.ExpiresAt, Sequence: s.Sequence,
	}
}

func (s InventorySnapshot) CopyItems() map[string]runtime.ManagedAppObject {
	return copyItems(s.items)
}

func copyItems(items map[string]runtime.ManagedAppObject) map[string]runtime.ManagedAppObject {
	out := make(map[string]runtime.ManagedAppObject, len(items))
	for key, item := range items {
		out[key] = copyManagedApp(item)
	}
	return out
}

func (s AppSnapshot) clone() AppSnapshot {
	s.Managed = copyManagedApp(s.Managed)
	s.Evidence = s.Evidence.clone()
	return s
}

type Sequence struct {
	RefreshStarted   uint64
	ManagedAppsRead  uint64
	KubeSnapshotRead uint64
	DurableAppsRead  uint64
	RefreshCompleted uint64
}

// Evidence contains only authoritative results from the same
// Kubernetes snapshot as the ManagedApp observation. Nil fields mean that a
// resource is not applicable (for example a background app has no Service),
// while a non-nil false is an authoritative absence.
type Evidence struct {
	AppObservationKey            string
	NamespacePresent             *bool
	ServicePresent               *bool
	EndpointPresent              *bool
	EndpointReady                *bool
	PhysicalReplicas             *int
	ServingReplicas              *int
	PhysicalDesiredReplicas      *int
	ImagePresent                 *bool
	ImageRef                     string
	CurrentRuntimeID             string
	InvariantViolations          []string
	EvidenceSources              []string
	ManagedGeneration            int64
	ManagedObservedGeneration    int64
	ManagedImageDigest           string
	ManagedDesiredReplicas       int
	ManagedReadyReplicas         int
	DeploymentGeneration         int64
	DeploymentObservedGeneration int64
	DeploymentImageDigest        string
	DeploymentReplicas           int
	DeploymentUpdatedReplicas    int
	DeploymentReadyReplicas      int
	DeploymentAvailableReplicas  int
	ImageLocationStatus          string
	ImageLocationSource          string
	ImageLocationObservedAt      time.Time
	ServingReleaseID             string
	ServingReleaseReady          bool
	ServingReleaseCreatedAt      time.Time
	ServingReleaseReadyAt        *time.Time
}

func (e Evidence) clone() Evidence {
	e.NamespacePresent = copyPointer(e.NamespacePresent)
	e.ServicePresent = copyPointer(e.ServicePresent)
	e.EndpointPresent = copyPointer(e.EndpointPresent)
	e.EndpointReady = copyPointer(e.EndpointReady)
	e.PhysicalReplicas = copyPointer(e.PhysicalReplicas)
	e.ServingReplicas = copyPointer(e.ServingReplicas)
	e.PhysicalDesiredReplicas = copyPointer(e.PhysicalDesiredReplicas)
	e.ImagePresent = copyPointer(e.ImagePresent)
	e.InvariantViolations = slices.Clone(e.InvariantViolations)
	e.EvidenceSources = slices.Clone(e.EvidenceSources)
	e.ServingReleaseReadyAt = copyPointer(e.ServingReleaseReadyAt)
	return e
}

func copyManagedApp(app runtime.ManagedAppObject) runtime.ManagedAppObject {
	app.Metadata.Labels = maps.Clone(app.Metadata.Labels)
	app.Metadata.Annotations = maps.Clone(app.Metadata.Annotations)
	if app.Spec.Route != nil {
		route := *app.Spec.Route
		route.DNSRouting = model.CloneAppStaticEdgeRouting(route.DNSRouting)
		app.Spec.Route = &route
	}
	app.Spec.Source = model.CloneAppSource(app.Spec.Source)
	app.Spec.AppSpec = app.Spec.AppSpec.DeepCopy()
	app.Spec.Bindings = slices.Clone(app.Spec.Bindings)
	for i := range app.Spec.Bindings {
		app.Spec.Bindings[i].Env = maps.Clone(app.Spec.Bindings[i].Env)
	}
	app.Spec.BackingServices = slices.Clone(app.Spec.BackingServices)
	for i := range app.Spec.BackingServices {
		svc := &app.Spec.BackingServices[i]
		svc.Spec.Postgres = model.CloneAppPostgresSpec(svc.Spec.Postgres)
		svc.RuntimeStatus = copyPointer(svc.RuntimeStatus)
		svc.CurrentResourceUsage = copyResourceUsage(svc.CurrentResourceUsage)
		svc.CurrentRuntimeStartedAt = copyPointer(svc.CurrentRuntimeStartedAt)
		svc.CurrentRuntimeReadyAt = copyPointer(svc.CurrentRuntimeReadyAt)
	}
	app.Spec.Scheduling.NodeSelector = maps.Clone(app.Spec.Scheduling.NodeSelector)
	app.Spec.Scheduling.Tolerations = slices.Clone(app.Spec.Scheduling.Tolerations)
	for i := range app.Spec.Scheduling.Tolerations {
		tol := &app.Spec.Scheduling.Tolerations[i]
		tol.TolerationSeconds = copyPointer(tol.TolerationSeconds)
	}
	app.Status.BackingServices = slices.Clone(app.Status.BackingServices)
	app.Status.Conditions = slices.Clone(app.Status.Conditions)
	return app
}

func copyResourceUsage(usage *model.ResourceUsage) *model.ResourceUsage {
	if usage == nil {
		return nil
	}
	return &model.ResourceUsage{
		CPUMilliCores:                  copyPointer(usage.CPUMilliCores),
		MemoryBytes:                    copyPointer(usage.MemoryBytes),
		EphemeralStorageBytes:          copyPointer(usage.EphemeralStorageBytes),
		PersistentStorageUsedBytes:     copyPointer(usage.PersistentStorageUsedBytes),
		PersistentStorageCapacityBytes: copyPointer(usage.PersistentStorageCapacityBytes),
	}
}

func copyPointer[T any](in *T) *T {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}
