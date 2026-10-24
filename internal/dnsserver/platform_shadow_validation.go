package dnsserver

import (
	"context"
	"errors"
	"strings"
	"time"

	"fugue/internal/platformconsumer"
	"fugue/internal/platformcontrol"
)

func (s *Service) newIsolatedDNSShadowValidator() *Service {
	authority := platformcontrol.ConsumerAuthorityID(s.Config.EdgeGroupID)
	if s.PlatformTokenFile == "" || strings.TrimSpace(s.Config.CachePath) == "" || authority == "" || s.Config.PlatformScopeKey != "authority-cell:"+authority {
		return nil
	}
	cfg := s.Config
	cfg.CachePath += ".shadow-validation"
	// This object never starts listeners, ambient configuration, zone services or
	// serving synchronization. Only the existing signed shadow validator runs.
	return &Service{Config: cfg, HTTPClient: s.HTTPClient, Logger: s.Logger, PlatformTokenFile: s.PlatformTokenFile, shadowValidationParent: s}
}

func (s *Service) runIsolatedDNSShadow(ctx context.Context, worker *Service) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		s.syncIsolatedDNSShadowOnce(ctx, worker)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) syncIsolatedDNSShadowOnce(ctx context.Context, worker *Service) {
	err := worker.SyncPlatformShadowOnce(ctx)
	if ctx.Err() != nil {
		return
	}
	s.recordDNSSnapshotFailure(err)
	worker.mu.Lock()
	candidate := worker.platformCandidate
	worker.mu.Unlock()
	if err != nil {
		candidate.State = "failed"
		candidate.LastError = err.Error()
		if errors.Is(err, platformconsumer.ErrAssignmentChanged) || errors.Is(err, platformconsumer.ErrNoShadowAssignment) {
			candidate.State = "awaiting_release"
		}
	}
	s.mu.Lock()
	s.platformCandidate = candidate
	s.mu.Unlock()
	if err != nil {
		s.Logger.Printf("DNS isolated shadow validation: %v", err)
	}
}
