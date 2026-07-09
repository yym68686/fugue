package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"fugue/internal/watchdog"
)

func main() {
	var timeout time.Duration
	var once bool
	flag.DurationVar(&timeout, "timeout", envDuration("FUGUE_WATCHDOG_TIMEOUT", 5*time.Second), "Probe timeout")
	flag.BoolVar(&once, "once", true, "Run one probe cycle and exit")
	flag.Parse()
	_ = once

	targets, err := watchdog.ParseProviderTargets(os.Getenv("FUGUE_WATCHDOG_PROVIDER_TARGETS_JSON"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse provider targets: %v\n", err)
		os.Exit(2)
	}
	cfg := watchdog.Config{
		APIURL:          os.Getenv("FUGUE_WATCHDOG_API_URL"),
		KubernetesAPI:   os.Getenv("FUGUE_WATCHDOG_KUBE_API_URL"),
		DBTCPAddr:       os.Getenv("FUGUE_WATCHDOG_DB_TCP_ADDR"),
		EtcdQuorumURL:   os.Getenv("FUGUE_WATCHDOG_ETCD_QUORUM_URL"),
		DNSServer:       os.Getenv("FUGUE_WATCHDOG_DNS_SERVER"),
		DNSName:         os.Getenv("FUGUE_WATCHDOG_DNS_NAME"),
		EdgeURLs:        splitCSV(os.Getenv("FUGUE_WATCHDOG_EDGE_URLS")),
		RunnerURL:       os.Getenv("FUGUE_WATCHDOG_GITHUB_RUNNER_URL"),
		Timeout:         timeout,
		ProviderMode:    firstNonEmpty(os.Getenv("FUGUE_WATCHDOG_PROVIDER_ACTION_MODE"), watchdog.ProviderActionModeObserve),
		ProviderTargets: targets,
	}
	report := watchdog.Run(context.Background(), cfg, watchdog.NoopProviderPowerClient{Mode: cfg.ProviderMode})
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintf(os.Stderr, "encode report: %v\n", err)
		os.Exit(2)
	}
	if !report.Pass {
		os.Exit(2)
	}
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
