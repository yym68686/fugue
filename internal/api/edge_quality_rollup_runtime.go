package api

import (
	"context"
	"io"
	"strings"

	"fugue/internal/observability"
	"fugue/internal/qualityrollup"
)

func normalizeEdgeTrafficClass(value string) string {
	return qualityrollup.NormalizeTrafficClass(value)
}

func (s *Server) StartBackgroundEdgeQualityRollups(ctx context.Context) {
	if s != nil {
		s.qualityRollups.Run(ctx)
	}
}

func (s *Server) writeEdgeQualityRollupMetrics(w io.Writer) {
	labels := map[string]string{"mode": "retired"}
	observability.WriteGaugeMetric(w, "fugue_edge_quality_ranking_active", "Whether scoped edge quality ranking actively changes DNS answers.", labels, 0)
	observability.WriteGaugeMetric(w, "fugue_edge_quality_ranking_shadow", "Whether scoped edge quality ranking only records shadow decisions.", labels, 0)
	s.qualityRollups.WriteMetrics(w)
}

func truncateMetricLabel(value string, max int) string {
	value = strings.TrimSpace(value)
	if max <= 0 || len(value) <= max {
		return value
	}
	return value[:max]
}
