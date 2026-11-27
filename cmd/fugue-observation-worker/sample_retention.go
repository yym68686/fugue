package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"fugue/internal/observability"
	"fugue/internal/store"
)

type sampleRetentionStore interface {
	RunObservationSampleRetentionPass(context.Context, string, store.ObservationRetentionPolicy) (store.ObservationSampleRetentionPass, error)
	ReadObservationSampleRetentionProgress(context.Context) ([]store.ObservationSampleRetentionProgress, error)
}

func readSampleRetentionPolicy(path string) (store.ObservationRetentionPolicy, error) {
	f, err := os.Open(path)
	if err != nil {
		return store.ObservationRetentionPolicy{}, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return store.ObservationRetentionPolicy{}, err
	}
	return store.ParseObservationRetentionPolicy(raw)
}

func sampleRetentionRetryDelay(policy store.ObservationRetentionPolicy, pass store.ObservationSampleRetentionPass, err error) time.Duration {
	if err == nil && (pass.State == "busy" || (pass.State == "completed" && pass.Deleted == int64(policy.BatchRows))) {
		// A saturated window has independent headroom. Do not process the
		// next batch in the same transaction or spin while contended.
		return 5 * time.Second
	}
	return time.Duration(policy.IntervalSeconds) * time.Second
}

func runSampleRetention(ctx context.Context, storage sampleRetentionStore, policyPath string, logger *log.Logger) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	targets := store.ObservationSampleRetentionTargets()
	var next [3]time.Time
	var policy store.ObservationRetentionPolicy
	havePolicy := false
	for ctx.Err() == nil {
		// Preserve the last validated policy during a transient bad update,
		// as the old timer did. A fresh process has no fallback to guess at.
		if current, err := readSampleRetentionPolicy(policyPath); err == nil {
			if !havePolicy || policy != current {
				next = [3]time.Time{}
			}
			policy, havePolicy = current, true
		}
		if havePolicy {
			for i, target := range targets {
				if ctx.Err() != nil {
					return
				}
				if time.Now().Before(next[i]) {
					continue
				}
				pass, err := storage.RunObservationSampleRetentionPass(ctx, target, policy)
				if err != nil && ctx.Err() == nil {
					logger.Printf("observation sample retention pass failed for %s; will retry", target)
				}
				next[i] = time.Now().Add(sampleRetentionRetryDelay(policy, pass, err))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func sampleRetentionReady(ctx context.Context, storage sampleRetentionStore, path string) bool {
	if _, err := readSampleRetentionPolicy(path); err != nil || ctx.Err() != nil {
		return false
	}
	_, err := storage.ReadObservationSampleRetentionProgress(ctx)
	return err == nil
}

func sampleRetentionMetrics(storage sampleRetentionStore, policyPath string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, policyErr := readSampleRetentionPolicy(policyPath)
		policyAvailable := 1.0
		if policyErr != nil {
			policyAvailable = 0
		}
		observability.WriteGaugeMetric(w, "fugue_observation_sample_retention_policy_valid", "Whether the current projected retention policy is available and valid.", nil, policyAvailable)
		const available = "fugue_observation_sample_retention_storage_available"
		const storageHelp = "Whether the worker can read durable sample retention progress and execute the bounded capability."
		progress, err := storage.ReadObservationSampleRetentionProgress(r.Context())
		if err != nil {
			observability.WriteGaugeMetric(w, available, storageHelp, nil, 0)
			return
		}
		observability.WriteGaugeMetric(w, available, storageHelp, nil, 1)
		const owner = "fugue_observation_sample_retention_writer_owner"
		const passes = "fugue_observation_sample_retention_completed_passes"
		const deleted = "fugue_observation_sample_retention_deleted_rows"
		const completed = "fugue_observation_sample_retention_completed_timestamp_seconds"
		observability.WriteMetricHeader(w, owner, "Durable sample retention writer.", "gauge")
		observability.WriteMetricHeader(w, passes, "Durably committed deletion batches, including empty batches.", "gauge")
		observability.WriteMetricHeader(w, deleted, "Rows deleted by durably committed batches.", "gauge")
		observability.WriteMetricHeader(w, completed, "Last committed sample retention batch timestamp.", "gauge")
		for _, target := range progress {
			labels := map[string]string{"target": target.Target}
			observability.WriteMetricSample(w, owner, map[string]string{"target": target.Target, "owner": target.Owner}, 1)
			observability.WriteMetricSample(w, passes, labels, float64(target.CompletedPasses))
			observability.WriteMetricSample(w, deleted, labels, float64(target.TotalDeleted))
			if !target.CompletedAt.IsZero() {
				observability.WriteMetricSample(w, completed, labels, float64(target.CompletedAt.Unix()))
			}
		}
	}
}
