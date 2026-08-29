package controller

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

const (
	hostJournaldPolicyActorID          = "fugue-controller/host-journald-policy"
	hostJournaldPolicyFailureRetry     = 30 * time.Minute
	hostJournaldPolicyMaxTasksPerCheck = 1
)

var (
	hostJournaldRetentionPattern = regexp.MustCompile(`^[1-9][0-9]*(s|min|h|d|day|week|month|year)$`)
	hostJournaldSizePattern      = regexp.MustCompile(`^[1-9][0-9]*(K|M|G|T)$`)
)

func (s *Service) scheduleHostJournaldPolicyReconciliation(ctx context.Context) error {
	if s == nil || s.Store == nil || !s.Config.HostJournaldPolicyEnabled {
		return nil
	}
	payload, err := controllerHostJournaldPolicyPayload(
		s.Config.HostJournaldMaxRetentionSec,
		s.Config.HostJournaldSystemMaxUse,
	)
	if err != nil {
		return err
	}
	reconcileInterval := s.Config.HostJournaldPolicyReconcileInterval
	if reconcileInterval <= 0 {
		reconcileInterval = 24 * time.Hour
	}
	now := time.Now().UTC()

	tasks, err := s.Store.ListNodeUpdateTasks("", true, "", "")
	if err != nil {
		return err
	}
	latestByUpdater := map[string]model.NodeUpdateTask{}
	for _, task := range tasks {
		if task.Type != model.NodeUpdateTaskTypeReconcileHostJournaldPolicy {
			continue
		}
		if task.Status == model.NodeUpdateTaskStatusPending || task.Status == model.NodeUpdateTaskStatusRunning {
			// Only one host mutation may be in flight across the fleet.
			return nil
		}
		current, ok := latestByUpdater[task.NodeUpdaterID]
		if !ok || task.CreatedAt.After(current.CreatedAt) {
			latestByUpdater[task.NodeUpdaterID] = task
		}
	}

	updaters, err := s.Store.ListNodeUpdaters("", true)
	if err != nil {
		return err
	}
	sort.Slice(updaters, func(i, j int) bool {
		if updaters[i].ClusterNodeName != updaters[j].ClusterNodeName {
			return updaters[i].ClusterNodeName < updaters[j].ClusterNodeName
		}
		return updaters[i].ID < updaters[j].ID
	})
	created := 0
	for _, updater := range updaters {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !strings.EqualFold(strings.TrimSpace(updater.Status), model.NodeUpdaterStatusActive) {
			continue
		}
		supported, err := s.Store.NodeUpdaterTargetSupportsTask(
			updater.ID,
			updater.ClusterNodeName,
			updater.RuntimeID,
			model.NodeUpdateTaskTypeReconcileHostJournaldPolicy,
		)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrInvalidInput) {
				continue
			}
			return err
		}
		if !supported {
			continue
		}

		if latest, ok := latestByUpdater[updater.ID]; ok && latest.Payload["policy_hash"] == payload["policy_hash"] {
			terminalAt := latest.UpdatedAt
			if latest.CompletedAt != nil {
				terminalAt = latest.CompletedAt.UTC()
			}
			switch latest.Status {
			case model.NodeUpdateTaskStatusCompleted:
				if terminalAt.Add(reconcileInterval).After(now) {
					continue
				}
			case model.NodeUpdateTaskStatusFailed:
				if latest.RequestedByID == hostJournaldPolicyActorID && terminalAt.Add(hostJournaldPolicyFailureRetry).After(now) {
					continue
				}
			}
		}

		principal := model.Principal{
			ActorType: model.ActorTypeSystem,
			ActorID:   hostJournaldPolicyActorID,
			Scopes:    map[string]struct{}{"platform.admin": {}},
		}
		if _, err := s.Store.CreateNodeUpdateTask(
			principal,
			updater.ID,
			updater.ClusterNodeName,
			updater.RuntimeID,
			model.NodeUpdateTaskTypeReconcileHostJournaldPolicy,
			payload,
		); err != nil {
			if errors.Is(err, store.ErrInvalidInput) || errors.Is(err, store.ErrNotFound) {
				continue
			}
			return err
		}
		created++
		if created >= hostJournaldPolicyMaxTasksPerCheck {
			return nil
		}
	}
	return nil
}

func controllerHostJournaldPolicyPayload(maxRetentionSec, systemMaxUse string) (map[string]string, error) {
	maxRetentionSec = strings.TrimSpace(maxRetentionSec)
	systemMaxUse = strings.TrimSpace(systemMaxUse)
	if !hostJournaldRetentionPattern.MatchString(maxRetentionSec) {
		return nil, fmt.Errorf("invalid host journald MaxRetentionSec %q", maxRetentionSec)
	}
	if !hostJournaldSizePattern.MatchString(systemMaxUse) {
		return nil, fmt.Errorf("invalid host journald SystemMaxUse %q", systemMaxUse)
	}
	policyMaterial := fmt.Sprintf("fugue-host-journald-policy/v1\nMaxRetentionSec=%s\nSystemMaxUse=%s\n", maxRetentionSec, systemMaxUse)
	policyHash := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(policyMaterial)))
	return map[string]string{
		"policy_hash":       policyHash,
		"max_retention_sec": maxRetentionSec,
		"system_max_use":    systemMaxUse,
		"dry_run":           "false",
		"allow_delete":      "true",
		"allow_restart":     "true",
		"reason":            "scheduled-host-journald-policy",
	}, nil
}
