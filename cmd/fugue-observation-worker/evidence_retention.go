package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"fugue/internal/observability"
	"fugue/internal/store"
)

type evidenceRetentionStore interface {
	RunQualityEvidenceRetentionPass(context.Context) (store.QualityEvidenceRetentionPass, error)
	ReadQualityEvidenceRetentionProgress(context.Context) (store.QualityEvidenceRetentionProgress, error)
}

func runQualityEvidenceRetention(ctx context.Context, storage evidenceRetentionStore, logger *log.Logger) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return
		}
		pass, err := storage.RunQualityEvidenceRetentionPass(ctx)
		if err != nil && ctx.Err() == nil {
			// Database errors may include connection details.
			logger.Print("quality evidence retention pass failed; will retry")
		}
		timer.Reset(evidenceRetentionRetryDelay(pass, err))
	}
}

func evidenceRetentionRetryDelay(pass store.QualityEvidenceRetentionPass, err error) time.Duration {
	if err == nil && (pass.State == "busy" || (pass.State == "completed" && pass.Deleted == store.QualityEvidenceRetentionBatchRows)) {
		// Drain bounded batches without holding a connection between attempts.
		return 10 * time.Second
	}
	return time.Minute
}

func evidenceRetentionMetrics(storage evidenceRetentionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		progress, err := storage.ReadQualityEvidenceRetentionProgress(r.Context())
		const available = "fugue_quality_evidence_retention_storage_available"
		if err != nil {
			observability.WriteGaugeMetric(w, available, "Whether observation maintenance can read its durable progress and execute its bounded capability.", nil, 0)
			return
		}
		observability.WriteGaugeMetric(w, available, "Whether observation maintenance can read its durable progress and execute its bounded capability.", nil, 1)
		observability.WriteGaugeMetric(w, "fugue_quality_evidence_retention_writer_owner", "Current durable expiry timer owner.", map[string]string{"owner": progress.Owner}, 1)
		observability.WriteGaugeMetric(w, "fugue_quality_evidence_retention_completed_passes", "Number of committed expiry batches from PostgreSQL.", nil, float64(progress.CompletedPasses))
		observability.WriteGaugeMetric(w, "fugue_quality_evidence_retention_deleted_objects", "Number of objects deleted by committed expiry batches.", nil, float64(progress.TotalDeleted))
		if !progress.CompletedAt.IsZero() {
			observability.WriteGaugeMetric(w, "fugue_quality_evidence_retention_completed_timestamp_seconds", "Time of the most recent committed expiry batch.", nil, float64(progress.CompletedAt.Unix()))
		}
	}
}
