package api

import (
	"io"
	"strings"

	"fugue/internal/edgequality"
	"fugue/internal/observability"
)

func normalizeEdgeTrafficClass(value string) string {
	return edgequality.NormalizeTrafficClass(value)
}

func writeRetiredEdgeQualityRankingMetrics(w io.Writer) {
	labels := map[string]string{"mode": "retired"}
	observability.WriteGaugeMetric(w, "fugue_edge_quality_ranking_active", "Whether scoped edge quality ranking actively changes DNS answers.", labels, 0)
	observability.WriteGaugeMetric(w, "fugue_edge_quality_ranking_shadow", "Whether scoped edge quality ranking only records shadow decisions.", labels, 0)
}

func truncateMetricLabel(value string, max int) string {
	value = strings.TrimSpace(value)
	if max <= 0 || len(value) <= max {
		return value
	}
	return value[:max]
}
