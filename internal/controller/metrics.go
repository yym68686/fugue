package controller

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/observability"
)

func (s *Service) StartMetricsServer(ctx context.Context, bindAddr string) error {
	bindAddr = strings.TrimSpace(bindAddr)
	if bindAddr == "" {
		return nil
	}
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return err
	}
	if s.Logger == nil {
		s.Logger = log.Default()
	}
	server := &http.Server{
		Handler:           s.MetricsHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
			s.Logger.Printf("controller metrics shutdown error: %v", err)
		}
	}()
	go func() {
		s.Logger.Printf("fugue-controller metrics listening on %s", bindAddr)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.Logger.Printf("controller metrics server exited: %v", err)
		}
	}()
	return nil
}

func (s *Service) MetricsHandler() http.Handler {
	return http.HandlerFunc(s.handleMetrics)
}

func (s *Service) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	observability.WriteComponentRuntimeMetrics(w, "controller", s.metricsStartedAt)
	observability.WriteGaugeMetric(w, "fugue_controller_leader_election_enabled", "Whether controller leader election is enabled.", nil, boolGauge(s.Config.LeaderElectionEnabled))
	observability.WriteGaugeMetric(w, "fugue_controller_kubectl_apply_enabled", "Whether the controller applies Kubernetes resources.", nil, boolGauge(s.Config.KubectlApply))
	observability.WriteGaugeMetric(w, "fugue_controller_poll_interval_seconds", "Configured foreground controller poll interval.", nil, s.Config.PollInterval.Seconds())
	observability.WriteGaugeMetric(w, "fugue_controller_fallback_poll_interval_seconds", "Configured fallback controller poll interval.", nil, s.Config.FallbackPollInterval.Seconds())
	activeLoopRunning, activeLoopStartedAt, leaderActive, leaderIdentity := s.controllerHealthSnapshot()
	observability.WriteGaugeMetric(w, "fugue_controller_active_loop_running", "Whether this controller process is currently running the active reconciliation loop.", nil, boolGauge(activeLoopRunning))
	activeLoopStarted := float64(0)
	if !activeLoopStartedAt.IsZero() {
		activeLoopStarted = float64(activeLoopStartedAt.Unix())
	}
	observability.WriteGaugeMetric(w, "fugue_controller_active_loop_started_timestamp_seconds", "Unix timestamp for the current active controller loop start.", nil, activeLoopStarted)
	observability.WriteMetricHeader(w, "fugue_controller_leader_active", "Whether this controller identity currently holds leadership.", "gauge")
	observability.WriteMetricSample(w, "fugue_controller_leader_active", map[string]string{"identity": leaderIdentity}, boolGauge(leaderActive))
	s.writeImageTrackingMetrics(w)
	observability.WriteMetricHeader(w, "fugue_controller_workers_configured", "Configured controller worker slots by lane; zero means unbounded.", "gauge")
	observability.WriteMetricSample(w, "fugue_controller_workers_configured", map[string]string{"lane": "foreground_import"}, float64(s.Config.ForegroundImportWorkers))
	observability.WriteMetricSample(w, "fugue_controller_workers_configured", map[string]string{"lane": "foreground_activate"}, float64(s.Config.ForegroundActivateWorkers))
	observability.WriteMetricSample(w, "fugue_controller_workers_configured", map[string]string{"lane": "github_sync_import"}, float64(s.Config.GitHubSyncImportWorkers))
	observability.WriteMetricSample(w, "fugue_controller_workers_configured", map[string]string{"lane": "github_sync_activate"}, float64(s.Config.GitHubSyncActivateWorkers))

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	registry := registryMaintenanceStatus{}
	if s.readRegistryMaintenance != nil {
		registry = s.readRegistryMaintenance(ctx)
	} else {
		registry = s.readRegistryMaintenanceStatus(ctx)
	}
	observability.WriteMetricHeader(w, "fugue_registry_janitor_present", "Whether the configured registry maintenance CronJob exists and is not suspended.", "gauge")
	observability.WriteMetricSample(w, "fugue_registry_janitor_present", map[string]string{"job": "retention"}, boolGauge(registry.JanitorPresent))
	observability.WriteMetricSample(w, "fugue_registry_janitor_present", map[string]string{"job": "gc"}, boolGauge(registry.GCCronJobPresent))
	observability.WriteGaugeMetric(w, "fugue_registry_gc_running", "Whether protected registry garbage collection is currently running.", nil, boolGauge(registry.GCRunning))
	observability.WriteGaugeMetric(w, "fugue_registry_gc_requested", "Whether image deletion requested a registry garbage collection newer than the last successful run.", nil, boolGauge(registry.GCRequested))
	lastGCTimestamp := float64(0)
	if !registry.LastGCTimestamp.IsZero() {
		lastGCTimestamp = float64(registry.LastGCTimestamp.Unix())
	}
	observability.WriteGaugeMetric(w, "fugue_registry_gc_last_success_timestamp_seconds", "Unix timestamp of the last successful protected registry garbage collection.", nil, lastGCTimestamp)
	observability.WriteGaugeMetric(w, "fugue_registry_storage_used_bytes", "Bytes used on the dedicated registry storage filesystem at the last maintenance scan.", nil, float64(registry.StorageUsedBytes))
	observability.WriteGaugeMetric(w, "fugue_registry_storage_capacity_bytes", "Capacity of the dedicated registry storage filesystem at the last maintenance scan.", nil, float64(registry.StorageCapacityBytes))
	observability.WriteGaugeMetric(w, "fugue_registry_unreferenced_blob_bytes", "Registry blob bytes not reachable from any retained manifest or workload digest at the last maintenance scan.", nil, float64(registry.UnreferencedBlobBytes))
	observability.WriteGaugeMetric(w, "fugue_registry_unreferenced_blob_count", "Registry blobs not reachable from any retained manifest or workload digest at the last maintenance scan.", nil, float64(registry.UnreferencedBlobCount))
	observability.WriteGaugeMetric(w, "fugue_registry_protected_workload_digests", "Current workload image digests included in the registry GC keep set at the last maintenance scan.", nil, float64(registry.ProtectedDigestCount))
	s.writeImageCacheLocalPVMetrics(w)
}

func (s *Service) controllerHealthSnapshot() (bool, time.Time, bool, string) {
	if s == nil {
		return false, time.Time{}, false, ""
	}
	s.controllerHealthMu.RLock()
	defer s.controllerHealthMu.RUnlock()
	return s.activeLoopRunning, s.activeLoopStartedAt, s.leaderActive, strings.TrimSpace(s.leaderIdentity)
}

func (s *Service) observeImageTrackingDecision(check model.AppImageTrackingCheck) {
	if s == nil {
		return
	}
	decision := strings.TrimSpace(check.Decision)
	if decision == "" {
		decision = "unknown"
	}
	s.imageTrackingMetricsMu.Lock()
	defer s.imageTrackingMetricsMu.Unlock()
	if s.imageTrackingDecisionCounts == nil {
		s.imageTrackingDecisionCounts = map[string]int64{}
	}
	s.imageTrackingDecisionCounts[decision]++
	if !check.CheckedAt.IsZero() && (s.imageTrackingLastCheckAt.IsZero() || check.CheckedAt.After(s.imageTrackingLastCheckAt)) {
		s.imageTrackingLastCheckAt = check.CheckedAt
	}
	if decision == model.AppImageTrackingDecisionQueued && !check.CheckedAt.IsZero() {
		s.imageTrackingLastQueuedAt = check.CheckedAt
	}
	if strings.TrimSpace(check.ResolverError) != "" || decision == model.AppImageTrackingDecisionQueueError || decision == model.AppImageTrackingDecisionResolverError {
		if !check.CheckedAt.IsZero() {
			s.imageTrackingLastErrorAt = check.CheckedAt
		}
		s.imageTrackingLastError = firstNonEmptyImageTrackingMetric(check.ResolverError, check.SkipReason, decision)
	}
}

func (s *Service) markImageTrackingSyncStarted() {
	if s == nil {
		return
	}
	now := time.Now().UTC()
	s.imageTrackingMetricsMu.Lock()
	defer s.imageTrackingMetricsMu.Unlock()
	s.imageTrackingSyncRunning = true
	s.imageTrackingLastSyncStartedAt = now
}

func (s *Service) markImageTrackingSyncFinished(startedAt time.Time, syncErr error) {
	if s == nil {
		return
	}
	finishedAt := time.Now().UTC()
	duration := finishedAt.Sub(startedAt)
	if duration < 0 {
		duration = 0
	}
	s.imageTrackingMetricsMu.Lock()
	defer s.imageTrackingMetricsMu.Unlock()
	s.imageTrackingSyncRunning = false
	s.imageTrackingLastSyncFinishedAt = finishedAt
	s.imageTrackingLastSyncDuration = duration
	if syncErr != nil {
		s.imageTrackingLastSyncErrorAt = finishedAt
		s.imageTrackingLastSyncError = syncErr.Error()
	}
}

func (s *Service) imageTrackingMetricsSnapshot() (map[string]int64, time.Time, time.Time, time.Time, string) {
	if s == nil {
		return map[string]int64{}, time.Time{}, time.Time{}, time.Time{}, ""
	}
	s.imageTrackingMetricsMu.RLock()
	defer s.imageTrackingMetricsMu.RUnlock()
	counts := make(map[string]int64, len(s.imageTrackingDecisionCounts))
	for key, value := range s.imageTrackingDecisionCounts {
		counts[key] = value
	}
	return counts, s.imageTrackingLastCheckAt, s.imageTrackingLastQueuedAt, s.imageTrackingLastErrorAt, strings.TrimSpace(s.imageTrackingLastError)
}

func (s *Service) imageTrackingSyncMetricsSnapshot() (bool, time.Time, time.Time, time.Duration, time.Time, string) {
	if s == nil {
		return false, time.Time{}, time.Time{}, 0, time.Time{}, ""
	}
	s.imageTrackingMetricsMu.RLock()
	defer s.imageTrackingMetricsMu.RUnlock()
	return s.imageTrackingSyncRunning,
		s.imageTrackingLastSyncStartedAt,
		s.imageTrackingLastSyncFinishedAt,
		s.imageTrackingLastSyncDuration,
		s.imageTrackingLastSyncErrorAt,
		strings.TrimSpace(s.imageTrackingLastSyncError)
}

func (s *Service) writeImageTrackingMetrics(w http.ResponseWriter) {
	counts, lastCheckAt, lastQueuedAt, lastErrorAt, _ := s.imageTrackingMetricsSnapshot()
	syncRunning, syncStartedAt, syncFinishedAt, syncDuration, syncErrorAt, _ := s.imageTrackingSyncMetricsSnapshot()
	observability.WriteMetricHeader(w, "fugue_image_tracking_decisions_total", "Image tracking check decisions made by this controller process.", "counter")
	for _, decision := range knownImageTrackingMetricDecisions(counts) {
		observability.WriteMetricSample(w, "fugue_image_tracking_decisions_total", map[string]string{"decision": decision}, float64(counts[decision]))
	}
	lastCheck := float64(0)
	if !lastCheckAt.IsZero() {
		lastCheck = float64(lastCheckAt.Unix())
	}
	lastQueued := float64(0)
	if !lastQueuedAt.IsZero() {
		lastQueued = float64(lastQueuedAt.Unix())
	}
	lastErrorTs := float64(0)
	if !lastErrorAt.IsZero() {
		lastErrorTs = float64(lastErrorAt.Unix())
	}
	observability.WriteGaugeMetric(w, "fugue_image_tracking_last_check_timestamp_seconds", "Unix timestamp for the latest image tracking check handled by this controller process.", nil, lastCheck)
	observability.WriteGaugeMetric(w, "fugue_image_tracking_last_queued_timestamp_seconds", "Unix timestamp for the latest image tracking queued import handled by this controller process.", nil, lastQueued)
	observability.WriteGaugeMetric(w, "fugue_image_tracking_last_error_timestamp_seconds", "Unix timestamp for the latest image tracking resolver or queue error handled by this controller process.", nil, lastErrorTs)
	syncStarted := float64(0)
	if !syncStartedAt.IsZero() {
		syncStarted = float64(syncStartedAt.Unix())
	}
	syncFinished := float64(0)
	if !syncFinishedAt.IsZero() {
		syncFinished = float64(syncFinishedAt.Unix())
	}
	syncError := float64(0)
	if !syncErrorAt.IsZero() {
		syncError = float64(syncErrorAt.Unix())
	}
	observability.WriteGaugeMetric(w, "fugue_controller_image_tracking_sync_running", "Whether this controller process is currently running an image tracking sync.", nil, boolGauge(syncRunning))
	observability.WriteGaugeMetric(w, "fugue_controller_image_tracking_sync_started_timestamp_seconds", "Unix timestamp for the latest image tracking sync start.", nil, syncStarted)
	observability.WriteGaugeMetric(w, "fugue_controller_last_image_tracking_sync_timestamp_seconds", "Unix timestamp for the latest completed image tracking sync.", nil, syncFinished)
	observability.WriteGaugeMetric(w, "fugue_controller_image_tracking_sync_duration_seconds", "Duration of the latest completed image tracking sync.", nil, syncDuration.Seconds())
	observability.WriteGaugeMetric(w, "fugue_controller_image_tracking_sync_last_error_timestamp_seconds", "Unix timestamp for the latest image tracking sync-level error.", nil, syncError)
}

func knownImageTrackingMetricDecisions(counts map[string]int64) []string {
	decisions := []string{
		model.AppImageTrackingDecisionQueued,
		model.AppImageTrackingDecisionAlreadyDeployed,
		model.AppImageTrackingDecisionNoChange,
		model.AppImageTrackingDecisionReplicasZero,
		model.AppImageTrackingDecisionActiveOperation,
		model.AppImageTrackingDecisionRetrySuppressed,
		model.AppImageTrackingDecisionResolverError,
		model.AppImageTrackingDecisionQueueConflict,
		model.AppImageTrackingDecisionQueueError,
	}
	seen := map[string]struct{}{}
	for _, decision := range decisions {
		seen[decision] = struct{}{}
	}
	for decision := range counts {
		if _, ok := seen[decision]; !ok {
			decisions = append(decisions, decision)
		}
	}
	return decisions
}

func firstNonEmptyImageTrackingMetric(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func boolGauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func (s *Service) writeImageCacheLocalPVMetrics(w http.ResponseWriter) {
	if s == nil || s.Store == nil {
		return
	}
	now := time.Now().UTC()
	if nodes, err := s.Store.ListImageCacheNodeInventories(model.ImageCacheNodeInventoryFilter{}); err == nil {
		observability.WriteMetricHeader(w, "fugue_image_cache_inventory_age_seconds", "Age of the latest node-local image-cache inventory report.", "gauge")
		observability.WriteMetricHeader(w, "fugue_image_cache_manifest_count", "Number of manifests reported by each node-local image-cache inventory.", "gauge")
		for _, node := range nodes {
			labels := imageCacheMetricNodeLabels(node.NodeID, node.ClusterNodeName, node.RuntimeID)
			age := float64(0)
			if !node.ObservedAt.IsZero() {
				age = now.Sub(node.ObservedAt).Seconds()
			}
			observability.WriteMetricSample(w, "fugue_image_cache_inventory_age_seconds", labels, age)
			observability.WriteMetricSample(w, "fugue_image_cache_manifest_count", labels, float64(node.ManifestCount))
		}
	}
	if plans, err := s.Store.ListImageCachePrunePlans(model.ImageCachePrunePlanFilter{Limit: 200}); err == nil {
		observability.WriteMetricHeader(w, "fugue_image_cache_candidate_manifest_count", "Candidate manifest count in recent image-cache prune plans.", "gauge")
		observability.WriteMetricHeader(w, "fugue_image_cache_prune_planned_bytes", "Planned delete bytes in recent image-cache prune plans.", "gauge")
		observability.WriteMetricHeader(w, "fugue_image_cache_prune_skipped_count", "Protected manifest count in recent image-cache prune plans.", "gauge")
		for _, plan := range plans {
			labels := imageCacheMetricNodeLabels(plan.NodeID, plan.ClusterNodeName, plan.RuntimeID)
			labels["mode"] = plan.Mode
			labels["status"] = plan.Status
			observability.WriteMetricSample(w, "fugue_image_cache_candidate_manifest_count", labels, float64(plan.CandidateManifestCount))
			observability.WriteMetricSample(w, "fugue_image_cache_prune_planned_bytes", labels, float64(plan.PlannedDeleteBytes))
			observability.WriteMetricSample(w, "fugue_image_cache_prune_skipped_count", labels, float64(plan.ProtectedManifestCount))
		}
	}
	if inventories, err := s.Store.ListLocalPVInventories(model.LocalPVInventoryFilter{}); err == nil {
		observability.WriteMetricHeader(w, "fugue_localpv_inventory_age_seconds", "Age of the latest LVM LocalPV inventory report.", "gauge")
		observability.WriteMetricHeader(w, "fugue_localpv_backing_file_bytes", "LVM LocalPV backing file size reported by each node.", "gauge")
		observability.WriteMetricHeader(w, "fugue_localpv_active_lv_count", "Active LVM LV count reported by each node.", "gauge")
		observability.WriteMetricHeader(w, "fugue_localpv_bound_pv_count", "Bound Kubernetes PV count reported by each node.", "gauge")
		observability.WriteMetricHeader(w, "fugue_localpv_decommission_eligible", "Whether the latest LVM LocalPV inventory is eligible for explicit decommission.", "gauge")
		for _, inventory := range inventories {
			labels := imageCacheMetricNodeLabels(inventory.NodeID, inventory.ClusterNodeName, inventory.RuntimeID)
			labels["vg"] = inventory.VGName
			age := float64(0)
			if !inventory.ObservedAt.IsZero() {
				age = now.Sub(inventory.ObservedAt).Seconds()
			}
			observability.WriteMetricSample(w, "fugue_localpv_inventory_age_seconds", labels, age)
			observability.WriteMetricSample(w, "fugue_localpv_backing_file_bytes", labels, float64(inventory.ImageSizeBytes))
			observability.WriteMetricSample(w, "fugue_localpv_active_lv_count", labels, float64(inventory.ActiveLVCount))
			observability.WriteMetricSample(w, "fugue_localpv_bound_pv_count", labels, float64(inventory.BoundPVCount))
			observability.WriteMetricSample(w, "fugue_localpv_decommission_eligible", labels, boolGauge(inventory.SafeToDecommission))
		}
	}
}

func imageCacheMetricNodeLabels(nodeID, clusterNodeName, runtimeID string) map[string]string {
	return map[string]string{
		"node_id":           strings.TrimSpace(nodeID),
		"cluster_node_name": strings.TrimSpace(clusterNodeName),
		"runtime_id":        strings.TrimSpace(runtimeID),
	}
}
