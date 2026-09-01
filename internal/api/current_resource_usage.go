package api

import (
	"context"
	"math"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

const (
	resourceUsageSampleInterval  = 5 * time.Minute
	resourceUsageSampleRetention = 168 * time.Hour

	// Resource requests are applied to each replica, so right-sizing samples must
	// use the same unit. Keep the version in the internal target kind so aggregate
	// samples written by older releases are never mixed with per-replica samples.
	rightSizingSampleTargetKindAppV1            = "right_sizing_app_per_replica_v1"
	rightSizingSampleTargetKindBackingServiceV1 = "right_sizing_backing_service_per_replica_v1"
)

type currentResourceUsageOverlay struct {
	apps                     map[string]model.ResourceUsage
	services                 map[string]model.ResourceUsage
	rightSizingApps          map[string]model.ResourceUsage
	rightSizingServices      map[string]model.ResourceUsage
	appPersistentVolumes     map[string][]persistentVolumeObservation
	servicePersistentVolumes map[string][]persistentVolumeObservation
}

type resourceUsageAccumulator struct {
	cpuMilliCores         int64
	hasCPU                bool
	memoryBytes           int64
	hasMemory             bool
	ephemeralStorageBytes int64
	hasEphemeralStorage   bool
	persistentVolumes     map[string]persistentVolumeUsage
}

type rightSizingResourceUsageAccumulator struct {
	cpuMilliCores         int64
	hasCPU                bool
	memoryBytes           int64
	hasMemory             bool
	ephemeralStorageBytes int64
	hasEphemeralStorage   bool
}

type persistentVolumeUsage struct {
	usedBytes         int64
	hasUsedBytes      bool
	availableBytes    int64
	hasAvailableBytes bool
	capacityBytes     int64
	hasCapacityBytes  bool
}

type persistentVolumeObservation struct {
	ClaimKey       string
	UsedBytes      *int64
	AvailableBytes *int64
	CapacityBytes  *int64
}

func (s *Server) overlayCurrentResourceUsageOnApps(ctx context.Context, apps []model.App) []model.App {
	if len(apps) == 0 {
		return apps
	}

	overlay := s.currentResourceUsageOverlay(ctx, apps, collectAppBackingServices(apps))
	if len(overlay.apps) == 0 && len(overlay.services) == 0 {
		return apps
	}

	out := make([]model.App, 0, len(apps))
	for _, app := range apps {
		out = append(out, applyCurrentResourceUsageToApp(app, overlay))
	}
	return out
}

func (s *Server) overlayCurrentResourceUsageOnApp(ctx context.Context, app model.App) model.App {
	return firstAppOrDefault(s.overlayCurrentResourceUsageOnApps(ctx, []model.App{app}), app)
}

func (s *Server) overlayCurrentResourceUsageOnServices(ctx context.Context, services []model.BackingService) []model.BackingService {
	if len(services) == 0 {
		return services
	}

	overlay := s.currentResourceUsageOverlay(ctx, nil, services)
	if len(overlay.services) == 0 {
		return services
	}

	out := make([]model.BackingService, 0, len(services))
	for _, service := range services {
		out = append(out, applyCurrentResourceUsageToService(service, overlay))
	}
	return out
}

func (s *Server) currentResourceUsageOverlay(ctx context.Context, apps []model.App, services []model.BackingService) currentResourceUsageOverlay {
	overlay := currentResourceUsageOverlay{
		apps:                     map[string]model.ResourceUsage{},
		services:                 map[string]model.ResourceUsage{},
		rightSizingApps:          map[string]model.ResourceUsage{},
		rightSizingServices:      map[string]model.ResourceUsage{},
		appPersistentVolumes:     map[string][]persistentVolumeObservation{},
		servicePersistentVolumes: map[string][]persistentVolumeObservation{},
	}
	if len(apps) == 0 && len(services) == 0 {
		return overlay
	}

	snapshots, err := s.loadClusterNodeInventory(ctx)
	if err != nil {
		if s.log != nil {
			s.log.Printf("current resource usage overlay inventory error: %v", err)
		}
		return overlay
	}
	policies, err := s.loadPersistentVolumeUsagePolicies(ctx, snapshots)
	if err != nil {
		if s.log != nil {
			s.log.Printf("current persistent volume usage overlay error: %v", err)
		}
		// Fail closed: kubelet reports filesystem-wide usage for directory-backed
		// local-path volumes, so an unclassified value must never reach clients.
		policies = persistentVolumeUsagePolicies{
			strict:  true,
			byClaim: map[string]persistentVolumeUsagePolicy{},
		}
	}
	return buildCurrentResourceUsageOverlayWithPolicies(snapshots, apps, services, policies)
}

func buildCurrentResourceUsageOverlay(snapshots []clusterNodeSnapshot, apps []model.App, services []model.BackingService) currentResourceUsageOverlay {
	return buildCurrentResourceUsageOverlayWithPolicies(
		snapshots,
		apps,
		services,
		persistentVolumeUsagePolicies{},
	)
}

func buildCurrentResourceUsageOverlayWithPolicies(
	snapshots []clusterNodeSnapshot,
	apps []model.App,
	services []model.BackingService,
	policies persistentVolumeUsagePolicies,
) currentResourceUsageOverlay {
	overlay := currentResourceUsageOverlay{
		apps:                     map[string]model.ResourceUsage{},
		services:                 map[string]model.ResourceUsage{},
		rightSizingApps:          map[string]model.ResourceUsage{},
		rightSizingServices:      map[string]model.ResourceUsage{},
		appPersistentVolumes:     map[string][]persistentVolumeObservation{},
		servicePersistentVolumes: map[string][]persistentVolumeObservation{},
	}
	if len(snapshots) == 0 {
		return overlay
	}

	resolver := newClusterWorkloadResolver(apps, services)
	claimOwners := persistentVolumeClaimOwners(snapshots, resolver)
	accumulators := map[string]*resourceUsageAccumulator{}
	rightSizingAccumulators := map[string]*rightSizingResourceUsageAccumulator{}

	for _, snapshot := range snapshots {
		if len(snapshot.pods) == 0 || snapshot.summary == nil || len(snapshot.summary.Pods) == 0 {
			continue
		}

		usageByPod := kubeNodeSummaryPodUsageIndex(snapshot.summary)
		if len(usageByPod) == 0 {
			continue
		}

		for _, pod := range snapshot.pods {
			workload, ok := resolver.resolvePod(pod)
			if !ok {
				continue
			}
			key := clusterWorkloadUsageKey(workload)
			if key == "\x00" {
				continue
			}

			usage, ok := usageByPod[clusterNamespacedResourceKey(pod.Metadata.Namespace, pod.Metadata.Name)]
			if !ok {
				continue
			}

			accumulator, ok := accumulators[key]
			if !ok {
				accumulator = &resourceUsageAccumulator{}
				accumulators[key] = accumulator
			}
			accumulator.addPodUsage(usage, key, claimOwners, policies)

			if strings.EqualFold(strings.TrimSpace(pod.Status.Phase), "Running") {
				rightSizingAccumulator, ok := rightSizingAccumulators[key]
				if !ok {
					rightSizingAccumulator = &rightSizingResourceUsageAccumulator{}
					rightSizingAccumulators[key] = rightSizingAccumulator
				}
				rightSizingAccumulator.addPodUsage(pod, usage)
			}
		}
	}

	for key, accumulator := range accumulators {
		usage, ok := accumulator.resourceUsage()
		if !ok {
			continue
		}

		parts := strings.SplitN(key, "\x00", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
			continue
		}

		switch parts[0] {
		case model.ClusterNodeWorkloadKindApp:
			overlay.apps[parts[1]] = usage
			overlay.appPersistentVolumes[parts[1]] = accumulator.persistentVolumeObservations()
		case model.ClusterNodeWorkloadKindBackingService:
			overlay.services[parts[1]] = usage
			overlay.servicePersistentVolumes[parts[1]] = accumulator.persistentVolumeObservations()
		}
	}
	for key, accumulator := range rightSizingAccumulators {
		usage, ok := accumulator.resourceUsage()
		if !ok {
			continue
		}
		parts := strings.SplitN(key, "\x00", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
			continue
		}
		switch parts[0] {
		case model.ClusterNodeWorkloadKindApp:
			overlay.rightSizingApps[parts[1]] = usage
		case model.ClusterNodeWorkloadKindBackingService:
			overlay.rightSizingServices[parts[1]] = usage
		}
	}

	return overlay
}

func (s *Server) startResourceUsageSamplingLoop(ctx context.Context) {
	if s == nil || ctx == nil || s.store == nil {
		return
	}
	go func() {
		timer := time.NewTimer(15 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				if err := s.recordCurrentResourceUsageSamples(ctx); err != nil && s.log != nil {
					s.log.Printf("resource usage sampling failed: %v", err)
				}
				timer.Reset(resourceUsageSampleInterval)
			}
		}
	}()
}

func (s *Server) recordCurrentResourceUsageSamples(ctx context.Context) error {
	apps, err := s.store.ListApps("", true)
	if err != nil {
		return err
	}
	services, err := s.store.ListBackingServices("", true)
	if err != nil {
		return err
	}
	if len(apps) == 0 && len(services) == 0 {
		return nil
	}
	snapshots, err := s.loadClusterNodeInventory(ctx)
	if err != nil {
		return err
	}
	policies, err := s.loadPersistentVolumeUsagePolicies(ctx, snapshots)
	if err != nil {
		if s.log != nil {
			s.log.Printf("resource usage sampling persistent volume overlay error: %v", err)
		}
		policies = persistentVolumeUsagePolicies{
			strict:  true,
			byClaim: map[string]persistentVolumeUsagePolicy{},
		}
	}
	overlay := buildCurrentResourceUsageOverlayWithPolicies(snapshots, apps, services, policies)
	now := time.Now().UTC()
	samples := buildResourceUsageSamples(now, apps, services, overlay)
	return s.store.RecordResourceUsageSamples(samples, now.Add(-resourceUsageSampleRetention))
}

func buildResourceUsageSamples(
	observedAt time.Time,
	apps []model.App,
	services []model.BackingService,
	overlay currentResourceUsageOverlay,
) []model.ResourceUsageSample {
	samples := make([]model.ResourceUsageSample, 0, len(overlay.rightSizingApps)+len(overlay.rightSizingServices))
	for _, app := range apps {
		id := strings.TrimSpace(app.ID)
		usage, ok := overlay.rightSizingApps[id]
		if !ok || !hasRightSizingResourceUsage(usage) {
			continue
		}
		samples = append(samples, resourceUsageSampleFromUsage(observedAt, rightSizingSampleTargetKind(model.ClusterNodeWorkloadKindApp), app.TenantID, app.ProjectID, app.ID, app.Name, "", usage))
	}
	for _, service := range services {
		id := strings.TrimSpace(service.ID)
		usage, ok := overlay.rightSizingServices[id]
		if !ok || !hasRightSizingResourceUsage(usage) {
			continue
		}
		samples = append(samples, resourceUsageSampleFromUsage(observedAt, rightSizingSampleTargetKind(model.ClusterNodeWorkloadKindBackingService), service.TenantID, service.ProjectID, service.ID, service.Name, service.Type, usage))
	}
	return samples
}

func rightSizingSampleTargetKind(targetKind string) string {
	switch strings.TrimSpace(targetKind) {
	case model.ClusterNodeWorkloadKindApp:
		return rightSizingSampleTargetKindAppV1
	case model.ClusterNodeWorkloadKindBackingService:
		return rightSizingSampleTargetKindBackingServiceV1
	default:
		return strings.TrimSpace(targetKind)
	}
}

func resourceUsageSampleFromUsage(observedAt time.Time, targetKind, tenantID, projectID, targetID, targetName, serviceType string, usage model.ResourceUsage) model.ResourceUsageSample {
	return model.ResourceUsageSample{
		ID:                    model.NewID("usage"),
		TenantID:              strings.TrimSpace(tenantID),
		ProjectID:             strings.TrimSpace(projectID),
		TargetKind:            strings.TrimSpace(targetKind),
		TargetID:              strings.TrimSpace(targetID),
		TargetName:            strings.TrimSpace(targetName),
		ServiceType:           strings.TrimSpace(serviceType),
		ObservedAt:            observedAt,
		CPUMilliCores:         cloneInt64Pointer(usage.CPUMilliCores),
		MemoryBytes:           cloneInt64Pointer(usage.MemoryBytes),
		EphemeralStorageBytes: cloneInt64Pointer(usage.EphemeralStorageBytes),
	}
}

func hasSampledResourceUsage(usage model.ResourceUsage) bool {
	return usage.CPUMilliCores != nil || usage.MemoryBytes != nil || usage.EphemeralStorageBytes != nil
}

func hasRightSizingResourceUsage(usage model.ResourceUsage) bool {
	return usage.CPUMilliCores != nil || usage.MemoryBytes != nil
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

func collectAppBackingServices(apps []model.App) []model.BackingService {
	if len(apps) == 0 {
		return nil
	}

	byID := make(map[string]model.BackingService)
	for _, app := range apps {
		for _, service := range app.BackingServices {
			id := strings.TrimSpace(service.ID)
			if id == "" {
				continue
			}
			byID[id] = service
		}
	}
	if len(byID) == 0 {
		return nil
	}

	out := make([]model.BackingService, 0, len(byID))
	for _, service := range byID {
		out = append(out, service)
	}
	return out
}

func applyCurrentResourceUsageToApp(app model.App, overlay currentResourceUsageOverlay) model.App {
	app.CurrentResourceUsage = currentResourceUsagePointer(overlay.apps[strings.TrimSpace(app.ID)])
	for index := range app.BackingServices {
		app.BackingServices[index] = applyCurrentResourceUsageToService(app.BackingServices[index], overlay)
	}
	return app
}

func applyCurrentResourceUsageToService(service model.BackingService, overlay currentResourceUsageOverlay) model.BackingService {
	service.CurrentResourceUsage = currentResourceUsagePointer(overlay.services[strings.TrimSpace(service.ID)])
	return service
}

func currentResourceUsagePointer(usage model.ResourceUsage) *model.ResourceUsage {
	if usage.CPUMilliCores == nil &&
		usage.MemoryBytes == nil &&
		usage.EphemeralStorageBytes == nil &&
		usage.PersistentStorageUsedBytes == nil &&
		usage.PersistentStorageCapacityBytes == nil {
		return nil
	}
	copied := usage
	return &copied
}

func firstAppOrDefault(apps []model.App, fallback model.App) model.App {
	if len(apps) == 0 {
		return fallback
	}
	return apps[0]
}

func kubeNodeSummaryPodUsageIndex(summary *kubeNodeSummary) map[string]kubeNodeSummaryPod {
	if summary == nil || len(summary.Pods) == 0 {
		return nil
	}

	index := make(map[string]kubeNodeSummaryPod, len(summary.Pods))
	for _, pod := range summary.Pods {
		key := clusterNamespacedResourceKey(pod.PodRef.Namespace, pod.PodRef.Name)
		if key == "" {
			continue
		}
		index[key] = pod
	}
	return index
}

func (a *resourceUsageAccumulator) addPodUsage(
	pod kubeNodeSummaryPod,
	workloadKey string,
	claimOwners map[string]string,
	policies persistentVolumeUsagePolicies,
) {
	if cpu := kubeSummaryCPUMilliUsage(pod.CPU); cpu != nil {
		a.cpuMilliCores += *cpu
		a.hasCPU = true
	}
	if memory := kubeSummaryMemoryUsage(pod.Memory); memory != nil {
		a.memoryBytes += *memory
		a.hasMemory = true
	}
	if storage := kubeSummaryFilesystemUsage(pod.EphemeralStorage); storage != nil {
		a.ephemeralStorageBytes += *storage
		a.hasEphemeralStorage = true
	}
	a.addPersistentVolumeUsage(pod, workloadKey, claimOwners, policies)
}

func (a *resourceUsageAccumulator) resourceUsage() (model.ResourceUsage, bool) {
	if a == nil {
		return model.ResourceUsage{}, false
	}

	usage := model.ResourceUsage{}
	if a.hasCPU {
		usage.CPUMilliCores = int64Pointer(a.cpuMilliCores)
	}
	if a.hasMemory {
		usage.MemoryBytes = int64Pointer(a.memoryBytes)
	}
	if a.hasEphemeralStorage {
		usage.EphemeralStorageBytes = int64Pointer(a.ephemeralStorageBytes)
	}
	for _, volume := range a.persistentVolumes {
		if volume.hasUsedBytes {
			if usage.PersistentStorageUsedBytes == nil {
				usage.PersistentStorageUsedBytes = int64Pointer(0)
			}
			*usage.PersistentStorageUsedBytes += volume.usedBytes
		}
		if volume.hasCapacityBytes {
			if usage.PersistentStorageCapacityBytes == nil {
				usage.PersistentStorageCapacityBytes = int64Pointer(0)
			}
			*usage.PersistentStorageCapacityBytes += volume.capacityBytes
		}
	}
	if usage.CPUMilliCores == nil &&
		usage.MemoryBytes == nil &&
		usage.EphemeralStorageBytes == nil &&
		usage.PersistentStorageUsedBytes == nil &&
		usage.PersistentStorageCapacityBytes == nil {
		return model.ResourceUsage{}, false
	}
	return usage, true
}

func (a *rightSizingResourceUsageAccumulator) addPodUsage(pod clusterNodePod, usage kubeNodeSummaryPod) {
	// One ResourceSpec configures one main container. Maxima preserve a safe
	// per-replica envelope without weighting an app by its replica count.
	cpu, memory := rightSizingMainContainerUsage(pod, usage)
	if value := kubeSummaryCPUMilliUsage(cpu); value != nil && (!a.hasCPU || *value > a.cpuMilliCores) {
		a.cpuMilliCores = *value
		a.hasCPU = true
	}
	if value := kubeSummaryMemoryUsage(memory); value != nil && (!a.hasMemory || *value > a.memoryBytes) {
		a.memoryBytes = *value
		a.hasMemory = true
	}
	if value := kubeSummaryFilesystemUsage(usage.EphemeralStorage); value != nil && (!a.hasEphemeralStorage || *value > a.ephemeralStorageBytes) {
		a.ephemeralStorageBytes = *value
		a.hasEphemeralStorage = true
	}
}

func (a *rightSizingResourceUsageAccumulator) resourceUsage() (model.ResourceUsage, bool) {
	if a == nil {
		return model.ResourceUsage{}, false
	}
	usage := model.ResourceUsage{}
	if a.hasCPU {
		usage.CPUMilliCores = int64Pointer(a.cpuMilliCores)
	}
	if a.hasMemory {
		usage.MemoryBytes = int64Pointer(a.memoryBytes)
	}
	if a.hasEphemeralStorage {
		usage.EphemeralStorageBytes = int64Pointer(a.ephemeralStorageBytes)
	}
	return usage, hasRightSizingResourceUsage(usage)
}

func rightSizingMainContainerUsage(pod clusterNodePod, usage kubeNodeSummaryPod) (kubeNodeSummaryCPU, kubeNodeSummaryMem) {
	if len(pod.Spec.Containers) == 0 || len(usage.Containers) == 0 {
		return usage.CPU, usage.Memory
	}
	mainContainerName := strings.TrimSpace(pod.Spec.Containers[0].Name)
	if mainContainerName == "" {
		return usage.CPU, usage.Memory
	}
	for _, container := range usage.Containers {
		if strings.TrimSpace(container.Name) == mainContainerName {
			return container.CPU, container.Memory
		}
	}
	return kubeNodeSummaryCPU{}, kubeNodeSummaryMem{}
}

func (a *resourceUsageAccumulator) persistentVolumeObservations() []persistentVolumeObservation {
	if a == nil {
		return nil
	}
	keys := make([]string, 0, len(a.persistentVolumes))
	for claimKey := range a.persistentVolumes {
		keys = append(keys, claimKey)
	}
	sort.Strings(keys)
	observations := make([]persistentVolumeObservation, 0, len(keys))
	for _, claimKey := range keys {
		volume := a.persistentVolumes[claimKey]
		observation := persistentVolumeObservation{ClaimKey: persistentVolumeClaimDisplayKey(claimKey)}
		if volume.hasUsedBytes {
			observation.UsedBytes = int64Pointer(volume.usedBytes)
		}
		if volume.hasAvailableBytes {
			observation.AvailableBytes = int64Pointer(volume.availableBytes)
		}
		if volume.hasCapacityBytes {
			observation.CapacityBytes = int64Pointer(volume.capacityBytes)
		}
		observations = append(observations, observation)
	}
	return observations
}

func persistentVolumeClaimDisplayKey(claimKey string) string {
	namespace, name, found := strings.Cut(claimKey, "\x00")
	if !found {
		return strings.TrimSpace(claimKey)
	}
	namespace = strings.TrimSpace(namespace)
	name = strings.TrimSpace(name)
	if namespace == "" {
		return name
	}
	if name == "" {
		return namespace
	}
	return namespace + "/" + name
}

func (a *resourceUsageAccumulator) addPersistentVolumeUsage(
	pod kubeNodeSummaryPod,
	workloadKey string,
	claimOwners map[string]string,
	policies persistentVolumeUsagePolicies,
) {
	if a == nil {
		return
	}
	for _, volume := range pod.Volumes {
		if volume.PVCRef == nil {
			continue
		}
		key := persistentVolumeClaimKey(pod, volume)
		if key == "" {
			continue
		}
		if owner, ok := claimOwners[key]; ok && owner != workloadKey {
			continue
		}

		usedBytes, availableBytes, capacityBytes := persistentVolumeUsageValues(key, volume, policies)
		if usedBytes == nil && availableBytes == nil && capacityBytes == nil {
			continue
		}
		if a.persistentVolumes == nil {
			a.persistentVolumes = make(map[string]persistentVolumeUsage)
		}
		current := a.persistentVolumes[key]
		if usedBytes != nil && (!current.hasUsedBytes || *usedBytes > current.usedBytes) {
			current.usedBytes = *usedBytes
			current.hasUsedBytes = true
		}
		if availableBytes != nil && (!current.hasAvailableBytes || *availableBytes < current.availableBytes) {
			current.availableBytes = *availableBytes
			current.hasAvailableBytes = true
		}
		if capacityBytes != nil && (!current.hasCapacityBytes || *capacityBytes < current.capacityBytes) {
			current.capacityBytes = *capacityBytes
			current.hasCapacityBytes = true
		}
		a.persistentVolumes[key] = current
	}
}

func persistentVolumeUsageValues(
	claimKey string,
	volume kubeNodeSummaryVolume,
	policies persistentVolumeUsagePolicies,
) (*int64, *int64, *int64) {
	if claimKey == "" {
		return nil, nil, nil
	}
	policy, ok := policies.byClaim[claimKey]
	if !ok {
		if policies.strict {
			return nil, nil, nil
		}
	} else if !policy.useKubelet {
		return cloneInt64Pointer(policy.usedBytes), nil, nil
	}
	return kubeSummaryFilesystemUsage(kubeNodeSummaryFS{
			AvailableBytes: volume.AvailableBytes,
			CapacityBytes:  volume.CapacityBytes,
			UsedBytes:      volume.UsedBytes,
		}),
		uint64PointerToInt64(volume.AvailableBytes),
		uint64PointerToInt64(volume.CapacityBytes)
}

func persistentVolumeClaimOwners(
	snapshots []clusterNodeSnapshot,
	resolver clusterWorkloadResolver,
) map[string]string {
	owners := make(map[string]string)
	for _, snapshot := range snapshots {
		if len(snapshot.pods) == 0 || snapshot.summary == nil || len(snapshot.summary.Pods) == 0 {
			continue
		}
		usageByPod := kubeNodeSummaryPodUsageIndex(snapshot.summary)
		for _, pod := range snapshot.pods {
			workload, ok := resolver.resolvePod(pod)
			if !ok {
				continue
			}
			workloadKey := clusterWorkloadUsageKey(workload)
			if workloadKey == "\x00" {
				continue
			}
			usage, ok := usageByPod[clusterNamespacedResourceKey(pod.Metadata.Namespace, pod.Metadata.Name)]
			if !ok {
				continue
			}
			for _, volume := range usage.Volumes {
				claimKey := persistentVolumeClaimKey(usage, volume)
				if claimKey == "" {
					continue
				}
				if current, exists := owners[claimKey]; !exists || persistentVolumeOwnerLess(workloadKey, current) {
					owners[claimKey] = workloadKey
				}
			}
		}
	}
	return owners
}

func clusterWorkloadUsageKey(workload model.ClusterNodeWorkload) string {
	return strings.TrimSpace(workload.Kind) + "\x00" + strings.TrimSpace(workload.ID)
}

func persistentVolumeClaimKey(pod kubeNodeSummaryPod, volume kubeNodeSummaryVolume) string {
	if volume.PVCRef == nil {
		return ""
	}
	claimName := strings.TrimSpace(volume.PVCRef.Name)
	if claimName == "" {
		return ""
	}
	namespace := strings.TrimSpace(volume.PVCRef.Namespace)
	if namespace == "" {
		namespace = strings.TrimSpace(pod.PodRef.Namespace)
	}
	return clusterNamespacedResourceKey(namespace, claimName)
}

func persistentVolumeOwnerLess(candidate, current string) bool {
	candidateRank := persistentVolumeOwnerRank(candidate)
	currentRank := persistentVolumeOwnerRank(current)
	if candidateRank != currentRank {
		return candidateRank < currentRank
	}
	return candidate < current
}

func persistentVolumeOwnerRank(workloadKey string) int {
	kind, _, _ := strings.Cut(workloadKey, "\x00")
	switch kind {
	case model.ClusterNodeWorkloadKindBackingService:
		return 0
	case model.ClusterNodeWorkloadKindApp:
		return 1
	default:
		return 2
	}
}

func kubeSummaryCPUMilliUsage(cpu kubeNodeSummaryCPU) *int64 {
	if cpu.UsageNanoCores == nil {
		return nil
	}
	value := int64(math.Round(float64(*cpu.UsageNanoCores) / 1_000_000))
	return &value
}

func kubeSummaryMemoryUsage(memory kubeNodeSummaryMem) *int64 {
	switch {
	case memory.WorkingSetBytes != nil:
		return uint64PointerToInt64(memory.WorkingSetBytes)
	case memory.UsageBytes != nil:
		return uint64PointerToInt64(memory.UsageBytes)
	default:
		return nil
	}
}

func kubeSummaryFilesystemUsage(fs kubeNodeSummaryFS) *int64 {
	if fs.UsedBytes != nil {
		return uint64PointerToInt64(fs.UsedBytes)
	}
	if fs.AvailableBytes == nil || fs.CapacityBytes == nil || *fs.AvailableBytes > *fs.CapacityBytes {
		return nil
	}
	value := int64(*fs.CapacityBytes - *fs.AvailableBytes)
	return &value
}

func int64Pointer(value int64) *int64 {
	copied := value
	return &copied
}
