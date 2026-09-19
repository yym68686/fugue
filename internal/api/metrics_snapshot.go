package api

import (
	"context"
	"time"
)

func (s *Server) StartBackgroundMetrics(ctx context.Context) {
	s.metricsSnapshot.Run(ctx, 15*time.Second, s.collectMetrics)
}
func (s *Server) refreshMetrics(ctx context.Context) error {
	return s.metricsSnapshot.Refresh(ctx, s.collectMetrics)
}
