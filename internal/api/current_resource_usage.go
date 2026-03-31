package api

import (
	"context"
	"math"
	"strings"

	"fugue/internal/model"
)

type currentResourceUsageOverlay struct {
	apps     map[string]model.ResourceUsage
	services map[string]model.ResourceUsage
}

type resourceUsageAccumulator struct {
	cpuMilliCores         int64
	hasCPU                bool
	memoryBytes           int64
	hasMemory             bool
	ephemeralStorageBytes int64
	hasEphemeralStorage   bool
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
		apps:     map[string]model.ResourceUsage{},
		services: map[string]model.ResourceUsage{},
	}
	if len(apps) == 0 && len(services) == 0 {
		return overlay
	}

	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		return overlay
	}

	snapshots, err := client.listClusterNodeInventory(ctx)
	if err != nil {
		if s.log != nil {
			s.log.Printf("current resource usage overlay inventory error: %v", err)
		}
		return overlay
	}
	return buildCurrentResourceUsageOverlay(snapshots, apps, services)
}

func buildCurrentResourceUsageOverlay(snapshots []clusterNodeSnapshot, apps []model.App, services []model.BackingService) currentResourceUsageOverlay {
	overlay := currentResourceUsageOverlay{
		apps:     map[string]model.ResourceUsage{},
		services: map[string]model.ResourceUsage{},
	}
	if len(snapshots) == 0 {
		return overlay
	}

	resolver := newClusterWorkloadResolver(apps, services)
	accumulators := map[string]*resourceUsageAccumulator{}

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
			key := strings.TrimSpace(workload.Kind) + "\x00" + strings.TrimSpace(workload.ID)
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
			accumulator.addPodUsage(usage)
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
		case model.ClusterNodeWorkloadKindBackingService:
			overlay.services[parts[1]] = usage
		}
	}

	return overlay
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
	if usage.CPUMilliCores == nil && usage.MemoryBytes == nil && usage.EphemeralStorageBytes == nil {
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

func (a *resourceUsageAccumulator) addPodUsage(pod kubeNodeSummaryPod) {
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
}

func (a *resourceUsageAccumulator) resourceUsage() (model.ResourceUsage, bool) {
	if a == nil || (!a.hasCPU && !a.hasMemory && !a.hasEphemeralStorage) {
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
	return usage, true
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
