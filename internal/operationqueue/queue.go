// Package operationqueue owns local arbitration of the Controller's durable
// operation queue. Store owns the atomic transitions; this package cannot write
// application state, start workloads, or bypass the persistent interlocks.
package operationqueue

import (
	"fmt"
	"sync"
	"time"

	"fugue/internal/model"
)

// Repository exposes queue transitions and the metadata used to plan them.
// False from a transition means the snapshot lost its claim or no longer
// exists. Reads do not grant write ownership.
type Repository interface {
	ListActiveOperations() ([]model.Operation, error)
	ListAppsMetadataByIDs([]string) ([]model.App, error)
	ListAppsMetadataByProjectIDs([]string) ([]model.App, error)
	TryClaimPendingOperation(string) (model.Operation, bool, error)
	FailBeforeClaim(string, string) (model.Operation, bool, error)
	CancelSupersededDeploy(string, string) (bool, error)
}

type Lane int

const (
	ForegroundImport Lane = iota
	ForegroundActivate
	GitHubSyncImport
	GitHubSyncActivate
	CPURequestRestore
)

func (lane Lane) String() string {
	switch lane {
	case ForegroundImport:
		return "foreground-import"
	case ForegroundActivate:
		return "foreground-activate"
	case GitHubSyncImport:
		return "github-sync-import"
	case GitHubSyncActivate:
		return "github-sync-activate"
	case CPURequestRestore:
		return "cpu-request-restore"
	default:
		return "unknown"
	}
}

type EventKind uint8

const (
	Blocking EventKind = iota
	Failed
	Claimed
)

type Event struct {
	Kind      EventKind
	Lane      Lane
	Operation model.Operation
	App       model.App
	Message   string
}

func notify(observe func(Event), event Event) {
	if observe != nil {
		observe(event)
	}
}

// Queue serializes claims with recovery registration. Its zero value is
// ready to use, and it must not be copied after use.
type Queue struct {
	claimMu   sync.Mutex
	executing sync.Map
}

// ReserveAttempt is used by recovery paths that reuse the normal worker
// lifecycle. The worker releases the reservation with FinishAttempt.
func (q *Queue) ReserveAttempt(op model.Operation) bool {
	q.claimMu.Lock()
	defer q.claimMu.Unlock()
	_, exists := q.executing.LoadOrStore(op.ID, ExecutionKey(op))
	return !exists
}

func (q *Queue) FinishAttempt(op model.Operation) {
	q.executing.CompareAndDelete(op.ID, ExecutionKey(op))
}

// ReserveRecovery holds a separate reservation for a read/probe recovery.
// Its release cannot clear a later attempt or another recovery reservation.
func (q *Queue) ReserveRecovery(id string) (release func(), acquired bool) {
	q.claimMu.Lock()
	defer q.claimMu.Unlock()
	owner := new(int)
	if _, exists := q.executing.LoadOrStore(id, owner); exists {
		return nil, false
	}
	return func() { q.executing.CompareAndDelete(id, owner) }, true
}

func (q *Queue) IsExecuting(id string) bool {
	_, exists := q.executing.Load(id)
	return exists
}

func ExecutionKey(op model.Operation) string {
	if op.StartedAt == nil {
		return op.ID + ":unstarted"
	}
	return op.StartedAt.UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano)
}

func (q *Queue) Claim(repo Repository, lane Lane, clock func() time.Time, observe func(Event)) (model.Operation, bool, error) {
	q.claimMu.Lock()
	defer q.claimMu.Unlock()
	for {
		now := time.Now().UTC()
		if clock != nil {
			now = clock().UTC()
		}
		activeOps, err := repo.ListActiveOperations()
		if err != nil {
			return model.Operation{}, false, fmt.Errorf("list active operations: %w", err)
		}
		if changed, err := cancelDeploysSupersededByDeletion(repo, activeOps); err != nil {
			return model.Operation{}, false, err
		} else if changed {
			continue
		}
		apps, err := loadAppsForPendingOperationClaim(repo, activeOps, lane)
		if err != nil {
			return model.Operation{}, false, fmt.Errorf("list apps: %w", err)
		}
		appsByID, composeAppsByProject := indexAppsForPendingOperations(apps)
		activeOpsByApp := indexActiveOperationsByApp(activeOps)

		restartScan := false
		for _, op := range activeOps {
			if op.Status != model.OperationStatusPending {
				continue
			}
			if !pendingOperationMatchesLane(op, lane) {
				continue
			}
			if !RetryReady(op, now) {
				continue
			}
			app, ok := appsByID[op.AppID]
			ready, terminalReason := pendingOperationReadyForClaim(op, app, ok, activeOpsByApp, composeAppsByProject)
			if terminalReason != "" {
				notify(observe, Event{Kind: Blocking, Lane: lane, Operation: op, App: app, Message: terminalReason})
				failed, written, failErr := repo.FailBeforeClaim(op.ID, terminalReason)
				if failErr != nil {
					return model.Operation{}, false, fmt.Errorf("fail blocked pending operation %s: %w", op.ID, failErr)
				}
				if written {
					notify(observe, Event{Kind: Failed, Lane: lane, Operation: failed, App: app, Message: terminalReason})
				}
				restartScan = true
				break
			}
			if !ready {
				continue
			}
			claimed, claimedOK, claimErr := repo.TryClaimPendingOperation(op.ID)
			if claimErr != nil {
				return model.Operation{}, false, fmt.Errorf("try claim operation %s: %w", op.ID, claimErr)
			}
			if !claimedOK {
				restartScan = true
				break
			}
			notify(observe, Event{Kind: Claimed, Lane: lane, Operation: claimed, App: app, Message: "operation claimed"})
			q.executing.Store(claimed.ID, ExecutionKey(claimed))
			return claimed, true, nil
		}
		if restartScan {
			continue
		}
		return model.Operation{}, false, nil
	}
}

// A deletion must not wait for the readiness of the workload being removed.
// CancelOperation already restricts running cancellation to deploy operations;
// database transitions and migrations retain their existing safety gates.
func cancelDeploysSupersededByDeletion(repo Repository, operations []model.Operation) (bool, error) {
	deletes := map[string]model.Operation{}
	for _, op := range operations {
		if op.Type == model.OperationTypeDelete {
			deletes[op.AppID] = op
		}
	}
	changed := false
	for _, op := range operations {
		deletion, ok := deletes[op.AppID]
		if !ok || op.Type != model.OperationTypeDeploy || (op.Status != model.OperationStatusPending && op.Status != model.OperationStatusRunning) || deletion.CreatedAt.Before(op.CreatedAt) {
			continue
		}
		canceled, err := repo.CancelSupersededDeploy(op.ID, "deployment superseded by app deletion")
		if err != nil {
			return changed, fmt.Errorf("cancel deploy before app deletion: %w", err)
		}
		changed = changed || canceled
	}
	return changed, nil
}
