package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/store"
)

const gitHubSourceSyncMaxUserActionFailures = 3

type gitHubSourceSyncErrorClass struct {
	Code            string
	NeedsUserAction bool
}

func gitHubSourceSyncCheckReady(status *model.AppSourceSyncStatus, now time.Time) bool {
	if status == nil {
		return true
	}
	if strings.TrimSpace(status.Phase) == model.AppSourceSyncPhaseSuspended {
		return false
	}
	if status.NextCheckAt != nil && now.Before(*status.NextCheckAt) {
		return false
	}
	return true
}

func (s *Service) recordGitHubSourceSyncFailure(app model.App, source model.AppSource, checkErr error, now time.Time) {
	if checkErr == nil {
		return
	}

	previous := model.CloneAppSourceSyncStatus(app.Status.SourceSync)
	class := classifyGitHubSourceSyncError(checkErr)
	failures := 1
	if previous != nil && strings.TrimSpace(previous.Provider) == model.AppSourceSyncProviderGitHub {
		failures = previous.ConsecutiveFailures + 1
	}

	lastCheckedAt := now
	lastErrorAt := now
	status := &model.AppSourceSyncStatus{
		Provider:            model.AppSourceSyncProviderGitHub,
		Phase:               model.AppSourceSyncPhaseDegraded,
		ConsecutiveFailures: failures,
		LastCheckedAt:       &lastCheckedAt,
		LastErrorAt:         &lastErrorAt,
		LastErrorCode:       class.Code,
		LastErrorMessage:    sanitizeGitHubSourceSyncError(checkErr),
		NeedsUserAction:     class.NeedsUserAction,
	}

	if class.NeedsUserAction && failures >= gitHubSourceSyncMaxUserActionFailures {
		suspendedAt := now
		status.Phase = model.AppSourceSyncPhaseSuspended
		status.SuspendedAt = &suspendedAt
	} else {
		nextCheckAt := now.Add(gitHubSourceSyncRetryDelay(
			s.Config.GitHubSyncCheckRetryBaseDelay,
			s.Config.GitHubSyncCheckRetryMaxDelay,
			failures,
		))
		status.NextCheckAt = &nextCheckAt
	}

	if _, err := s.Store.UpdateAppSourceSyncStatus(app.ID, status); err != nil {
		if !errors.Is(err, store.ErrNotFound) && s.Logger != nil {
			s.Logger.Printf("github sync source status update failed for app=%s: %v", app.ID, err)
		}
		return
	}
	if s.Logger == nil {
		return
	}

	repo := strings.TrimSpace(source.RepoURL)
	branch := strings.TrimSpace(source.RepoBranch)
	if status.Phase == model.AppSourceSyncPhaseSuspended {
		if previous == nil || strings.TrimSpace(previous.Phase) != model.AppSourceSyncPhaseSuspended {
			s.Logger.Printf(
				"github sync suspended for app=%s repo=%s branch=%s code=%s failures=%d: %s",
				app.ID,
				repo,
				branch,
				status.LastErrorCode,
				status.ConsecutiveFailures,
				status.LastErrorMessage,
			)
		}
		return
	}

	nextCheckAt := ""
	if status.NextCheckAt != nil {
		nextCheckAt = status.NextCheckAt.Format(time.RFC3339)
	}
	s.Logger.Printf(
		"github sync check failed for app=%s repo=%s branch=%s code=%s failures=%d next_check_at=%s: %s",
		app.ID,
		repo,
		branch,
		status.LastErrorCode,
		status.ConsecutiveFailures,
		nextCheckAt,
		status.LastErrorMessage,
	)
}

func (s *Service) recordGitHubSourceSyncSuccess(app model.App, now time.Time) {
	previous := model.CloneAppSourceSyncStatus(app.Status.SourceSync)
	if previous == nil {
		return
	}
	if strings.TrimSpace(previous.Provider) != model.AppSourceSyncProviderGitHub {
		return
	}
	if strings.TrimSpace(previous.Phase) == model.AppSourceSyncPhaseOK &&
		previous.ConsecutiveFailures == 0 &&
		previous.LastErrorAt == nil &&
		previous.NextCheckAt == nil &&
		previous.SuspendedAt == nil {
		return
	}

	lastCheckedAt := now
	lastSuccessAt := now
	status := &model.AppSourceSyncStatus{
		Provider:      model.AppSourceSyncProviderGitHub,
		Phase:         model.AppSourceSyncPhaseOK,
		LastCheckedAt: &lastCheckedAt,
		LastSuccessAt: &lastSuccessAt,
	}
	if _, err := s.Store.UpdateAppSourceSyncStatus(app.ID, status); err != nil {
		if !errors.Is(err, store.ErrNotFound) && s.Logger != nil {
			s.Logger.Printf("github sync source status recovery update failed for app=%s: %v", app.ID, err)
		}
		return
	}
	if strings.TrimSpace(previous.Phase) != model.AppSourceSyncPhaseOK && s.Logger != nil {
		s.Logger.Printf("github sync source recovered for app=%s", app.ID)
	}
}

func gitHubSourceSyncRetryDelay(baseDelay, maxDelay time.Duration, consecutiveFailures int) time.Duration {
	if baseDelay <= 0 {
		baseDelay = 5 * time.Minute
	}
	if maxDelay <= 0 {
		maxDelay = 6 * time.Hour
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay
	}
	return gitHubTrackedCommitRetryDelay(baseDelay, maxDelay, consecutiveFailures)
}

func classifyGitHubSourceSyncError(err error) gitHubSourceSyncErrorClass {
	message := strings.ToLower(strings.TrimSpace(fmt.Sprint(err)))
	switch {
	case strings.Contains(message, "branch ") && strings.Contains(message, " was not found"):
		return gitHubSourceSyncErrorClass{Code: "branch_not_found", NeedsUserAction: true}
	case strings.Contains(message, "invalid github repository url"):
		return gitHubSourceSyncErrorClass{Code: "invalid_repo_url", NeedsUserAction: true}
	case strings.Contains(message, "could not read username") ||
		strings.Contains(message, "authentication failed") ||
		strings.Contains(message, "permission denied") ||
		strings.Contains(message, "support for password authentication was removed"):
		return gitHubSourceSyncErrorClass{Code: "auth_required", NeedsUserAction: true}
	case strings.Contains(message, "repository not found") ||
		strings.Contains(message, "not found"):
		return gitHubSourceSyncErrorClass{Code: "repo_not_found", NeedsUserAction: true}
	case strings.Contains(message, "rate limit"):
		return gitHubSourceSyncErrorClass{Code: "rate_limited"}
	case errors.Is(err, context.DeadlineExceeded) ||
		strings.Contains(message, "context deadline exceeded") ||
		strings.Contains(message, "operation timed out") ||
		strings.Contains(message, "i/o timeout"):
		return gitHubSourceSyncErrorClass{Code: "timeout"}
	case strings.Contains(message, "could not resolve host") ||
		strings.Contains(message, "connection refused") ||
		strings.Contains(message, "connection reset") ||
		strings.Contains(message, "temporary failure"):
		return gitHubSourceSyncErrorClass{Code: "network"}
	default:
		return gitHubSourceSyncErrorClass{Code: "unknown"}
	}
}

func sanitizeGitHubSourceSyncError(err error) string {
	message := strings.Join(strings.Fields(strings.TrimSpace(fmt.Sprint(err))), " ")
	if len(message) > 500 {
		return strings.TrimSpace(message[:500]) + "..."
	}
	return message
}
