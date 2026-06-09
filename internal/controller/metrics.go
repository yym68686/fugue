package controller

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

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
}

func boolGauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
