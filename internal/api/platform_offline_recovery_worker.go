package api

import (
	"context"
	"errors"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

// Recovery I/O has its own lifetime, election and budget. A slow or unreachable
// member never holds the global configuration producer's publication lock.
func (s *Server) runOfflineEdgeRecovery(ctx context.Context) {
	for ctx.Err() == nil {
		_, err := s.store.WithAdvisoryLock(ctx, platformproducer.Actor+"|offline-recovery", func() error {
			for ctx.Err() == nil {
				if err := s.reconcileActiveOfflineRecovery(ctx); err != nil && s.log != nil {
					s.log.Printf("offline Edge recovery unavailable; retaining holds: %v", err)
				}
				if !waitPlatformProducer(ctx, 30*time.Second) {
					break
				}
			}
			return nil
		})
		if err != nil && ctx.Err() == nil && s.log != nil {
			s.log.Printf("offline Edge recovery election unavailable: %v", err)
		}
		if !waitPlatformProducer(ctx, 30*time.Second) {
			return
		}
	}
}

func (s *Server) reconcileActiveOfflineRecovery(ctx context.Context) error {
	artifact, _, found, err := s.store.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
	if err != nil || !found {
		return err
	}
	policy, err := platformproducer.Decode(artifact)
	if err != nil || artifact.Status != model.PlatformArtifactStatusValidated || s.store.VerifyPlatformArtifactIntegrity(artifact) != nil || !platformsafety.EvaluateArtifactIntegrity(artifact, s.bundleKeyring()).Pass {
		return errors.New("signed recovery scheduling policy unavailable")
	}
	if policy.Mode == "paused" {
		return nil
	}
	return s.reconcileOfflineEdgeRecovery(ctx, policy)
}
