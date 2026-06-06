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

func (s *Service) handleMetrics(w http.ResponseWriter, _ *http.Request) {
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
}

func boolGauge(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
