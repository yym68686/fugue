package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/config"
)

type leaseElector struct {
	client         *kubeClient
	leaseName      string
	leaseNamespace string
	identity       string
	leaseDuration  time.Duration
}

func newLeaseElector(client *kubeClient, cfg config.ControllerConfig) *leaseElector {
	return &leaseElector{
		client:         client,
		leaseName:      strings.TrimSpace(cfg.LeaderElectionLeaseName),
		leaseNamespace: client.effectiveNamespace(cfg.LeaderElectionLeaseNamespace),
		identity:       strings.TrimSpace(cfg.LeaderElectionIdentity),
		leaseDuration:  cfg.LeaderElectionLeaseDuration,
	}
}

func (e *leaseElector) TryAcquireOrRenew(ctx context.Context, now time.Time) (bool, error) {
	if e.client == nil {
		return false, fmt.Errorf("leader election client is not configured")
	}
	if e.leaseName == "" {
		return false, fmt.Errorf("leader election lease name is empty")
	}
	if e.identity == "" {
		return false, fmt.Errorf("leader election identity is empty")
	}
	if e.leaseDuration <= 0 {
		return false, fmt.Errorf("leader election lease duration must be greater than zero")
	}

	current, found, err := e.client.getLease(ctx, e.leaseNamespace, e.leaseName)
	if err != nil {
		return false, err
	}
	if !found {
		record := e.newLease(now)
		if err := e.client.createLease(ctx, e.leaseNamespace, record); err != nil {
			if errors.Is(err, errKubeConflict) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}

	if strings.TrimSpace(current.Spec.HolderIdentity) == e.identity {
		current.Spec.LeaseDurationSeconds = int(e.leaseDuration.Seconds())
		current.Spec.RenewTime = formatKubeTimestamp(now)
		if strings.TrimSpace(current.Spec.AcquireTime) == "" {
			current.Spec.AcquireTime = formatKubeTimestamp(now)
		}
		if err := e.client.updateLease(ctx, e.leaseNamespace, current); err != nil {
			if errors.Is(err, errKubeConflict) || errors.Is(err, errKubeNotFound) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	}

	if !leaseExpired(current, now) {
		return false, nil
	}

	current.Spec.HolderIdentity = e.identity
	current.Spec.LeaseDurationSeconds = int(e.leaseDuration.Seconds())
	current.Spec.AcquireTime = formatKubeTimestamp(now)
	current.Spec.RenewTime = formatKubeTimestamp(now)
	current.Spec.LeaseTransitions++
	if err := e.client.updateLease(ctx, e.leaseNamespace, current); err != nil {
		if errors.Is(err, errKubeConflict) || errors.Is(err, errKubeNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (e *leaseElector) newLease(now time.Time) kubeLease {
	var lease kubeLease
	lease.APIVersion = "coordination.k8s.io/v1"
	lease.Kind = "Lease"
	lease.Metadata.Name = e.leaseName
	lease.Metadata.Namespace = e.leaseNamespace
	lease.Spec.HolderIdentity = e.identity
	lease.Spec.LeaseDurationSeconds = int(e.leaseDuration.Seconds())
	lease.Spec.AcquireTime = formatKubeTimestamp(now)
	lease.Spec.RenewTime = formatKubeTimestamp(now)
	lease.Spec.LeaseTransitions = 1
	return lease
}

func leaseExpired(lease kubeLease, now time.Time) bool {
	renewTime := parseKubeTimestamp(lease.Spec.RenewTime)
	if renewTime.IsZero() {
		renewTime = parseKubeTimestamp(lease.Spec.AcquireTime)
	}
	if renewTime.IsZero() {
		return true
	}
	duration := time.Duration(lease.Spec.LeaseDurationSeconds) * time.Second
	if duration <= 0 {
		return true
	}
	return now.UTC().After(renewTime.Add(duration))
}

func (s *Service) runWithLeaderElection(ctx context.Context) error {
	client, err := newKubeClient(s.Config.KubectlNamespace)
	if err != nil {
		return fmt.Errorf("initialize kubernetes leader election client: %w", err)
	}
	elector := newLeaseElector(client, s.Config)
	s.Logger.Printf(
		"controller leader election enabled; lease=%s/%s identity=%s lease_duration=%s renew_deadline=%s retry_period=%s",
		elector.leaseNamespace,
		elector.leaseName,
		elector.identity,
		s.Config.LeaderElectionLeaseDuration,
		s.Config.LeaderElectionRenewDeadline,
		s.Config.LeaderElectionRetryPeriod,
	)

	ticker := time.NewTicker(s.Config.LeaderElectionRetryPeriod)
	defer ticker.Stop()

	var (
		isLeader         bool
		lastRenewSuccess time.Time
		leaderLoopCancel context.CancelFunc
		leaderLoopDoneCh chan error
	)

	stopLeaderLoop := func(reason string) {
		if leaderLoopCancel == nil {
			isLeader = false
			return
		}
		if reason != "" {
			s.Logger.Printf("controller leadership lost: %s", reason)
		}
		leaderLoopCancel()
		leaderLoopCancel = nil
		isLeader = false
	}

	startLeaderLoop := func() {
		if leaderLoopDoneCh != nil {
			return
		}
		leaderCtx, cancel := context.WithCancel(ctx)
		doneCh := make(chan error, 1)
		leaderLoopCancel = cancel
		leaderLoopDoneCh = doneCh
		isLeader = true
		s.Logger.Printf("controller leadership acquired")
		go func() {
			doneCh <- s.runLeaderLoop(leaderCtx, client)
		}()
	}

	renew := func(now time.Time) {
		held, err := elector.TryAcquireOrRenew(ctx, now)
		if err != nil {
			if isLeader && !lastRenewSuccess.IsZero() && now.Sub(lastRenewSuccess) < s.Config.LeaderElectionRenewDeadline {
				s.Logger.Printf("controller lease renew error; retaining leadership until renew deadline: %v", err)
				return
			}
			s.Logger.Printf("controller lease renew error: %v", err)
			if isLeader {
				stopLeaderLoop("renew deadline exceeded")
			}
			return
		}

		if held {
			lastRenewSuccess = now
			if !isLeader && leaderLoopDoneCh == nil {
				startLeaderLoop()
			}
			return
		}

		if isLeader {
			stopLeaderLoop("lease is held by another controller")
		}
	}

	renew(time.Now().UTC())

	for {
		select {
		case <-ctx.Done():
			stopLeaderLoop("shutdown requested")
			if leaderLoopDoneCh != nil {
				err := <-leaderLoopDoneCh
				leaderLoopDoneCh = nil
				if err != nil && !errors.Is(err, context.Canceled) {
					return err
				}
			}
			return ctx.Err()
		case err := <-leaderLoopDoneCh:
			leaderLoopDoneCh = nil
			leaderLoopCancel = nil
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
			if isLeader {
				return fmt.Errorf("controller leader loop exited unexpectedly")
			}
		case <-ticker.C:
			renew(time.Now().UTC())
		}
	}
}

func (s *Service) runLeaderLoop(ctx context.Context, client *kubeClient) error {
	if err := s.waitForLegacyControllerPods(ctx, client); err != nil {
		return err
	}
	return s.runActiveLoop(ctx)
}

func (s *Service) waitForLegacyControllerPods(ctx context.Context, client *kubeClient) error {
	labelSelector := strings.TrimSpace(s.Config.LegacyControllerLabelSelector)
	containerName := strings.TrimSpace(s.Config.LegacyControllerContainerName)
	if client == nil || labelSelector == "" || containerName == "" {
		return nil
	}

	checkInterval := s.Config.LegacyControllerCheckInterval
	if checkInterval <= 0 {
		checkInterval = 2 * time.Second
	}

	waitingLogged := false
	for {
		exists, err := client.podWithContainerExists(ctx, client.namespace, labelSelector, containerName)
		if err != nil {
			s.Logger.Printf("legacy controller pod check error: %v", err)
		} else if !exists {
			if waitingLogged {
				s.Logger.Printf("legacy controller pods drained; starting active controller loop")
			}
			return nil
		} else if !waitingLogged {
			waitingLogged = true
			s.Logger.Printf("waiting for legacy controller pods with container %q to exit before activating new controller", containerName)
		}

		if !sleepContext(ctx, checkInterval) {
			return ctx.Err()
		}
	}
}
