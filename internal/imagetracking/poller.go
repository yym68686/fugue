package imagetracking

import (
	"context"
	"log"
	"time"

	"fugue/internal/model"
	"fugue/internal/runtime"
)

// PollRepository is the polling worker's persistence contract, not a handle to
// an application's state or to another domain's transaction.
type PollRepository interface {
	ListAppImageTrackings(model.AppImageTrackingFilter) ([]model.AppImageTracking, error)
	RecordAppImageTrackingCheck(string, string, string, string, string) (model.AppImageTracking, error)
	QueueAppImageTrackingImport(model.App, model.AppImageTracking, string, string) (model.Operation, error)
	RecordAppImageTrackingQueued(string, string, string, string, string) (model.AppImageTracking, error)
	CreateAppImageTrackingCheck(model.AppImageTrackingCheck) (model.AppImageTrackingCheck, error)
}

type PollOptions struct {
	Repository              PollRepository
	GetApp                  func(string) (model.App, error)
	ListReleases            func(context.Context, model.AppReleaseFilter) ([]model.AppRelease, error)
	Metrics                 *Metrics
	Logger                  *log.Logger
	Interval, Timeout       time.Duration
	LeaderIdentity          string
	LiveApply               bool
	Now                     func() time.Time
	ResolveDigest           func(context.Context, string) (string, error)
	ActiveOperationID       func(model.App) (string, error)
	RecentDeploys           func(model.App) ([]model.Operation, error)
	StartupRetrySuppression func(context.Context, model.App, string) (string, error)
	ListWorkloads           func(context.Context, string) (map[string]bool, map[string]bool, error)
	IsRetired               func(model.AppRelease) bool
	CandidateRevision       func(string) runtime.AppRevisionRenderOptions
	RecordRelease           func(model.App, model.AppImageTracking, model.Operation, string, string, string) *model.ReleaseAttempt
	NotFound, Conflict      error
}

type Poller struct {
	store                    PollRepository
	getApp                   func(string) (model.App, error)
	listReleases             func(context.Context, model.AppReleaseFilter) ([]model.AppRelease, error)
	metrics                  *Metrics
	logger                   *log.Logger
	interval, timeout        time.Duration
	leaderIdentity           string
	liveApply                bool
	now                      func() time.Time
	resolveDigest            func(context.Context, string) (string, error)
	activeOperationID        func(model.App) (string, error)
	recentDeploys            func(model.App) ([]model.Operation, error)
	startupRetrySuppression  func(context.Context, model.App, string) (string, error)
	listWorkloads            func(context.Context, string) (map[string]bool, map[string]bool, error)
	isRetired                func(model.AppRelease) bool
	candidateRevision        func(string) runtime.AppRevisionRenderOptions
	recordRelease            func(model.App, model.AppImageTracking, model.Operation, string, string, string) *model.ReleaseAttempt
	errNotFound, errConflict error
}

func NewPoller(o PollOptions) *Poller {
	return &Poller{
		store: o.Repository, getApp: o.GetApp, listReleases: o.ListReleases,
		metrics: o.Metrics, logger: o.Logger, interval: o.Interval,
		timeout: o.Timeout, leaderIdentity: o.LeaderIdentity, liveApply: o.LiveApply,
		now: o.Now, resolveDigest: o.ResolveDigest, activeOperationID: o.ActiveOperationID,
		recentDeploys: o.RecentDeploys, startupRetrySuppression: o.StartupRetrySuppression,
		listWorkloads: o.ListWorkloads, isRetired: o.IsRetired, candidateRevision: o.CandidateRevision,
		recordRelease: o.RecordRelease, errNotFound: o.NotFound, errConflict: o.Conflict,
	}
}
