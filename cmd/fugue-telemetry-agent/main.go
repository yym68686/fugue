package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"fugue/internal/config"
	"fugue/internal/observability"
)

func main() {
	cfg := config.TelemetryAgentFromEnv()
	logger := log.Default()
	agent := telemetryAgent{
		cfg:    cfg.Observability.Normalize(),
		logger: logger,
	}
	if err := agent.cfg.Validate(); err != nil {
		logger.Printf("observability configuration degraded: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", agent.handleHealthz)
	mux.HandleFunc("GET /readyz", agent.handleReadyz)
	mux.HandleFunc("GET /metrics", agent.handleMetrics)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server := &http.Server{
		Addr:              cfg.BindAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Printf("shutdown error: %v", err)
		}
	}()

	logger.Printf("fugue-telemetry-agent listening on %s", cfg.BindAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatalf("listen and serve: %v", err)
	}
}

type telemetryAgent struct {
	cfg    observability.Config
	logger *log.Logger
}

func (a telemetryAgent) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeAgentJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a telemetryAgent) handleReadyz(w http.ResponseWriter, r *http.Request) {
	status := "ok"
	message := "observability exporters are configured"
	cfg := a.cfg.Normalize()
	if !cfg.Enabled {
		status = "skipped"
		message = "observability exporters are disabled"
	} else if err := cfg.Validate(); err != nil {
		status = "degraded"
		message = err.Error()
	} else if !cfg.HasExporters() {
		status = "degraded"
		message = "observability is enabled but no exporters are configured"
	}
	writeAgentJSON(w, http.StatusOK, map[string]any{
		"status": status,
		"checks": map[string]any{
			"observability": map[string]string{
				"status":  status,
				"message": message,
			},
		},
		"observability": cfg.Status(),
	})
}

func (a telemetryAgent) handleMetrics(w http.ResponseWriter, r *http.Request) {
	cfg := a.cfg.Normalize()
	statusValue := 0
	if cfg.Enabled && cfg.HasExporters() && cfg.Validate() == nil {
		statusValue = 1
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintln(w, "# HELP fugue_telemetry_agent_ready Whether the telemetry agent has valid configured exporters.")
	_, _ = fmt.Fprintln(w, "# TYPE fugue_telemetry_agent_ready gauge")
	_, _ = fmt.Fprintf(w, "fugue_telemetry_agent_ready %d\n", statusValue)
	_, _ = fmt.Fprintln(w, "# HELP fugue_telemetry_agent_retention_seconds Configured observability retention window in seconds.")
	_, _ = fmt.Fprintln(w, "# TYPE fugue_telemetry_agent_retention_seconds gauge")
	_, _ = fmt.Fprintf(w, "fugue_telemetry_agent_retention_seconds %.0f\n", cfg.Retention.Seconds())
	for _, exporter := range []string{"metrics", "logs", "analytics", "otlp"} {
		configured := 0
		if containsExporter(cfg.Exporters(), exporter) {
			configured = 1
		}
		_, _ = fmt.Fprintf(w, "fugue_telemetry_agent_exporter_configured{exporter=\"%s\"} %d\n", exporter, configured)
	}
}

func writeAgentJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func containsExporter(exporters []string, want string) bool {
	for _, exporter := range exporters {
		if strings.EqualFold(exporter, want) {
			return true
		}
	}
	return false
}
