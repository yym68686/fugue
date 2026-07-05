package controller

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
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
	s.writeOperationEvidenceMetrics(w)
	s.writeReleaseAttemptMetrics(w)
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

func (s *Service) writeOperationEvidenceMetrics(w http.ResponseWriter) {
	if s == nil || s.Store == nil {
		return
	}
	records, captures, rollouts, err := s.Store.CountOperationEvidenceMetricGroups()
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("operation evidence metrics unavailable: %v", err)
		}
		return
	}
	observability.WriteMetricHeader(w, "fugue_operation_evidence_records_total", "Operation evidence records retained by the control plane, grouped only by low-cardinality evidence type, severity, and confidence.", "gauge")
	for _, item := range records {
		observability.WriteMetricSample(w, "fugue_operation_evidence_records_total", map[string]string{
			"evidence_type": safeMetricLabelValue(item.Type, "unknown"),
			"severity":      safeMetricLabelValue(item.Severity, "unknown"),
			"confidence":    safeMetricLabelValue(item.Confidence, "unknown"),
		}, float64(item.Count))
	}
	observability.WriteMetricHeader(w, "fugue_rollout_failure_evidence_capture_total", "Rollout evidence capture outcomes retained by the control plane.", "gauge")
	for _, item := range captures {
		observability.WriteMetricSample(w, "fugue_rollout_failure_evidence_capture_total", map[string]string{
			"result": safeMetricLabelValue(item.Result, "unknown"),
		}, float64(item.Count))
	}
	observability.WriteMetricHeader(w, "fugue_rollout_failures_total", "Rollout failures with durable evidence retained by the control plane, grouped by low-cardinality reason and confidence.", "gauge")
	for _, item := range rollouts {
		observability.WriteMetricSample(w, "fugue_rollout_failures_total", map[string]string{
			"reason":     safeMetricLabelValue(item.Reason, "unknown"),
			"confidence": safeMetricLabelValue(item.Confidence, "unknown"),
		}, float64(item.Count))
	}
}

func (s *Service) writeReleaseAttemptMetrics(w http.ResponseWriter) {
	if s == nil || s.Store == nil {
		return
	}
	attempts, err := s.Store.CountReleaseAttemptMetricGroups()
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("release attempt metrics unavailable: %v", err)
		}
		return
	}
	observability.WriteMetricHeader(w, "fugue_release_attempts_total", "Release attempts retained by the control plane, grouped only by trigger type and status.", "gauge")
	for _, item := range attempts {
		observability.WriteMetricSample(w, "fugue_release_attempts_total", map[string]string{
			"trigger_type": safeMetricLabelValue(item.TriggerType, "unknown"),
			"status":       safeMetricLabelValue(item.Status, "unknown"),
		}, float64(item.Count))
	}
	s.writeReleaseDurationMetrics(w)
}

func (s *Service) writeReleaseDurationMetrics(w http.ResponseWriter) {
	attempts, err := s.Store.ListReleaseAttempts(model.ReleaseAttemptFilter{PlatformAdmin: true, Limit: 500})
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("release duration metrics unavailable: %v", err)
		}
		return
	}
	attemptDurations := map[string]durationMetricAggregate{}
	importToDeploy := map[string]durationMetricAggregate{}
	deployToReady := map[string]durationMetricAggregate{}
	for _, attempt := range attempts {
		status := safeMetricLabelValue(attempt.Status, "unknown")
		trigger := safeMetricLabelValue(attempt.TriggerType, "unknown")
		if attempt.FinishedAt != nil && !attempt.StartedAt.IsZero() {
			key := trigger + "\x00" + status
			attemptDurations[key] = attemptDurations[key].add(attempt.FinishedAt.Sub(attempt.StartedAt).Seconds(), map[string]string{"trigger_type": trigger, "status": status})
		}
		steps, err := s.Store.ListReleaseSteps(attempt.TenantID, true, attempt.ID)
		if err != nil {
			continue
		}
		if seconds, ok := releaseStepDurationSeconds(steps, model.ReleaseStepTypeImageImport, model.ReleaseStepTypeDeployQueued); ok {
			importToDeploy[status] = importToDeploy[status].add(seconds, map[string]string{"status": status})
		}
		if seconds, ok := releaseStepDurationSeconds(steps, model.ReleaseStepTypeDeployApply, model.ReleaseStepTypeFinalize); ok {
			deployToReady[status] = deployToReady[status].add(seconds, map[string]string{"status": status})
		}
	}
	writeAverageDurationMetric(w, "fugue_release_attempt_duration_seconds", "Average retained release attempt duration by trigger type and terminal status.", attemptDurations)
	writeAverageDurationMetric(w, "fugue_image_import_to_deploy_duration_seconds", "Average retained duration between image import start and deploy queue step by release status.", importToDeploy)
	writeAverageDurationMetric(w, "fugue_deploy_to_ready_duration_seconds", "Average retained duration between deploy apply and release finalization by release status.", deployToReady)
}

type durationMetricAggregate struct {
	Labels map[string]string
	Sum    float64
	Count  int
}

func (a durationMetricAggregate) add(seconds float64, labels map[string]string) durationMetricAggregate {
	if seconds < 0 {
		seconds = 0
	}
	a.Labels = labels
	a.Sum += seconds
	a.Count++
	return a
}

func writeAverageDurationMetric(w http.ResponseWriter, name, help string, values map[string]durationMetricAggregate) {
	observability.WriteMetricHeader(w, name, help, "gauge")
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := values[key]
		if value.Count == 0 {
			continue
		}
		observability.WriteMetricSample(w, name, value.Labels, value.Sum/float64(value.Count))
	}
}

func releaseStepDurationSeconds(steps []model.ReleaseStep, startType, finishType string) (float64, bool) {
	var started time.Time
	var finished time.Time
	for _, step := range steps {
		if started.IsZero() && step.Type == startType {
			started = step.StartedAt
		}
		if step.Type == finishType {
			if step.FinishedAt != nil {
				finished = *step.FinishedAt
			} else {
				finished = step.StartedAt
			}
		}
	}
	if started.IsZero() || finished.IsZero() || finished.Before(started) {
		return 0, false
	}
	return finished.Sub(started).Seconds(), true
}

func safeMetricLabelValue(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
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
		observability.WriteMetricHeader(w, "fugue_image_cache_unreferenced_blob_count", "Unreferenced image-cache blobs reported by each node-local inventory.", "gauge")
		observability.WriteMetricHeader(w, "fugue_image_cache_unreferenced_blob_bytes", "Unreferenced image-cache blob bytes reported by each node-local inventory.", "gauge")
		for _, node := range nodes {
			labels := imageCacheMetricNodeLabels(node.NodeID, node.ClusterNodeName, node.RuntimeID)
			age := float64(0)
			if !node.ObservedAt.IsZero() {
				age = now.Sub(node.ObservedAt).Seconds()
			}
			observability.WriteMetricSample(w, "fugue_image_cache_inventory_age_seconds", labels, age)
			observability.WriteMetricSample(w, "fugue_image_cache_manifest_count", labels, float64(node.ManifestCount))
			observability.WriteMetricSample(w, "fugue_image_cache_unreferenced_blob_count", labels, float64(node.UnreferencedBlobCount))
			observability.WriteMetricSample(w, "fugue_image_cache_unreferenced_blob_bytes", labels, float64(node.UnreferencedBlobBytes))
		}
	}
	if plans, err := s.Store.ListImageCachePrunePlans(model.ImageCachePrunePlanFilter{Limit: 200}); err == nil {
		observability.WriteMetricHeader(w, "fugue_image_cache_candidate_manifest_count", "Candidate manifest count in recent image-cache prune plans.", "gauge")
		observability.WriteMetricHeader(w, "fugue_image_cache_candidate_blob_count", "Candidate unreferenced blob count in recent image-cache prune plans.", "gauge")
		observability.WriteMetricHeader(w, "fugue_image_cache_candidate_blob_bytes", "Candidate unreferenced blob bytes in recent image-cache prune plans.", "gauge")
		observability.WriteMetricHeader(w, "fugue_image_cache_prune_planned_bytes", "Planned delete bytes in recent image-cache prune plans.", "gauge")
		observability.WriteMetricHeader(w, "fugue_image_cache_prune_skipped_count", "Protected manifest count in recent image-cache prune plans.", "gauge")
		observability.WriteMetricHeader(w, "fugue_image_cache_prune_skip_reason_count", "Protected manifest count in recent image-cache prune plans grouped by skip reason.", "gauge")
		for _, plan := range latestImageCachePrunePlansByMetricLabels(plans) {
			labels := imageCachePrunePlanMetricLabels(plan)
			observability.WriteMetricSample(w, "fugue_image_cache_candidate_manifest_count", labels, float64(plan.CandidateManifestCount))
			observability.WriteMetricSample(w, "fugue_image_cache_candidate_blob_count", labels, float64(plan.CandidateBlobCount))
			observability.WriteMetricSample(w, "fugue_image_cache_candidate_blob_bytes", labels, float64(plan.CandidateBlobBytes))
			observability.WriteMetricSample(w, "fugue_image_cache_prune_planned_bytes", labels, float64(plan.PlannedDeleteBytes))
			observability.WriteMetricSample(w, "fugue_image_cache_prune_skipped_count", labels, float64(plan.ProtectedManifestCount))
			for reason, count := range plan.ProtectionSummary {
				reasonLabels := cloneMetricLabels(labels)
				reasonLabels["reason"] = safeMetricLabelValue(reason, "unknown")
				observability.WriteMetricSample(w, "fugue_image_cache_prune_skip_reason_count", reasonLabels, float64(count))
			}
		}
	}
	s.writeImageCachePruneTaskResultMetrics(w)
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

func latestImageCachePrunePlansByMetricLabels(plans []model.ImageCachePrunePlan) []model.ImageCachePrunePlan {
	latest := map[string]model.ImageCachePrunePlan{}
	for _, plan := range plans {
		key := imageCachePrunePlanMetricKey(plan)
		current, ok := latest[key]
		if !ok || plan.CreatedAt.After(current.CreatedAt) {
			latest[key] = plan
		}
	}
	keys := make([]string, 0, len(latest))
	for key := range latest {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]model.ImageCachePrunePlan, 0, len(keys))
	for _, key := range keys {
		out = append(out, latest[key])
	}
	return out
}

func (s *Service) writeImageCachePruneTaskResultMetrics(w http.ResponseWriter) {
	if s == nil || s.Store == nil {
		return
	}
	tasks, err := s.Store.ListNodeUpdateTasks("", true, "", model.NodeUpdateTaskStatusCompleted)
	if err != nil {
		if s.Logger != nil {
			s.Logger.Printf("image-cache prune task result metrics unavailable: %v", err)
		}
		return
	}
	observability.WriteMetricHeader(w, "fugue_image_cache_prune_deleted_bytes", "Deleted bytes reported by completed node-local image-cache prune tasks.", "gauge")
	observability.WriteMetricHeader(w, "fugue_image_cache_prune_completed_task_count", "Completed node-local image-cache prune tasks that reported a result.", "gauge")
	for _, task := range latestImageCachePruneTasksByMetricLabels(tasks) {
		if task.Type != model.NodeUpdateTaskTypePruneImageCache {
			continue
		}
		if strings.TrimSpace(task.Payload["prune_reason"]) != "image-cache-orphan" {
			continue
		}
		labels := imageCacheMetricNodeLabels(task.MachineID, task.ClusterNodeName, task.RuntimeID)
		labels["dry_run"] = safeMetricLabelValue(task.Payload["dry_run"], "unknown")
		labels["allow_delete"] = safeMetricLabelValue(task.Payload["allow_delete"], "unknown")
		observability.WriteMetricSample(w, "fugue_image_cache_prune_completed_task_count", labels, 1)
		observability.WriteMetricSample(w, "fugue_image_cache_prune_deleted_bytes", labels, float64(parseMetricKeyValueInt64(task.ResultMessage, "deleted_bytes")))
	}
}

func latestImageCachePruneTasksByMetricLabels(tasks []model.NodeUpdateTask) []model.NodeUpdateTask {
	latest := map[string]model.NodeUpdateTask{}
	for _, task := range tasks {
		if task.Type != model.NodeUpdateTaskTypePruneImageCache {
			continue
		}
		labels := imageCacheMetricNodeLabels(task.MachineID, task.ClusterNodeName, task.RuntimeID)
		labels["dry_run"] = strings.TrimSpace(task.Payload["dry_run"])
		labels["allow_delete"] = strings.TrimSpace(task.Payload["allow_delete"])
		key := metricLabelKey(labels)
		current, ok := latest[key]
		if !ok || task.UpdatedAt.After(current.UpdatedAt) {
			latest[key] = task
		}
	}
	keys := make([]string, 0, len(latest))
	for key := range latest {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]model.NodeUpdateTask, 0, len(keys))
	for _, key := range keys {
		out = append(out, latest[key])
	}
	return out
}

func imageCachePrunePlanMetricLabels(plan model.ImageCachePrunePlan) map[string]string {
	labels := imageCacheMetricNodeLabels(plan.NodeID, plan.ClusterNodeName, plan.RuntimeID)
	labels["mode"] = strings.TrimSpace(plan.Mode)
	labels["status"] = strings.TrimSpace(plan.Status)
	return labels
}

func imageCachePrunePlanMetricKey(plan model.ImageCachePrunePlan) string {
	return metricLabelKey(imageCachePrunePlanMetricLabels(plan))
}

func metricLabelKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	for _, key := range keys {
		builder.WriteString(key)
		builder.WriteByte(0)
		builder.WriteString(labels[key])
		builder.WriteByte(0)
	}
	return builder.String()
}

func parseMetricKeyValueInt64(message, key string) int64 {
	key = strings.TrimSpace(key)
	if key == "" {
		return 0
	}
	for _, field := range strings.Fields(message) {
		name, value, ok := strings.Cut(field, "=")
		if !ok || strings.TrimSpace(name) != key {
			continue
		}
		parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if err == nil && parsed > 0 {
			return parsed
		}
		return 0
	}
	return 0
}

func imageCacheMetricNodeLabels(nodeID, clusterNodeName, runtimeID string) map[string]string {
	return map[string]string{
		"node_id":           strings.TrimSpace(nodeID),
		"cluster_node_name": strings.TrimSpace(clusterNodeName),
		"runtime_id":        strings.TrimSpace(runtimeID),
	}
}

func cloneMetricLabels(labels map[string]string) map[string]string {
	out := make(map[string]string, len(labels)+1)
	for key, value := range labels {
		out[key] = value
	}
	return out
}
