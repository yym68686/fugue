package api

import (
	"context"
	"time"

	"fugue/internal/hosteddns"
	"fugue/internal/store"
)

func (s *Server) reconcileHostedDNSFlattenRecords(ctx context.Context, now time.Time) (int, error) {
	if s == nil || s.store == nil {
		return 0, nil
	}
	owner, err := s.store.ActiveHostedDNSFlattenWriter(ctx)
	if err != nil || owner != store.HostedDNSFlattenAPI {
		return 0, err
	}
	return hosteddns.Reconcile(ctx, s.store, now)
}
