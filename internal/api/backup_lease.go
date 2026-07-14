package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/kubelease"
	"fugue/internal/model"

	coordinationv1 "k8s.io/api/coordination/v1"
)

const controlPlaneBackupCoordinationTokenAnnotation = "fugue.pro/coordination-token"

type controlPlaneBackupCoordinationLease interface {
	TryAcquireOrRenew(ctx context.Context, now time.Time) (bool, error)
	Release(ctx context.Context, now time.Time) (bool, error)
	Close()
}

type controlPlaneBackupCoordinationLeaseFactory func(runID string) (controlPlaneBackupCoordinationLease, error)

type kubernetesControlPlaneBackupCoordinationLease struct {
	manager *kubelease.Manager
	client  *clusterNodeClient
}

func (l *kubernetesControlPlaneBackupCoordinationLease) TryAcquireOrRenew(ctx context.Context, now time.Time) (bool, error) {
	return l.manager.TryAcquireOrRenew(ctx, now)
}

func (l *kubernetesControlPlaneBackupCoordinationLease) Release(ctx context.Context, now time.Time) (bool, error) {
	return l.manager.Release(ctx, now)
}

func (l *kubernetesControlPlaneBackupCoordinationLease) Close() {
	if l != nil && l.client != nil {
		l.client.closeIdleConnections()
	}
}

func (s *Server) newKubernetesControlPlaneBackupCoordinationLease(runID string) (controlPlaneBackupCoordinationLease, error) {
	if s == nil {
		return nil, fmt.Errorf("control-plane backup coordination is not configured")
	}
	name := strings.TrimSpace(s.backupCoordination.LeaseName)
	if name == "" {
		return nil, fmt.Errorf("FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAME is empty")
	}
	namespace := strings.TrimSpace(s.backupCoordination.LeaseNamespace)
	if namespace == "" {
		return nil, fmt.Errorf("FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_NAMESPACE is empty")
	}
	duration := s.backupCoordination.LeaseDuration
	renewPeriod := s.backupCoordination.RenewPeriod
	if duration < time.Second {
		return nil, fmt.Errorf("FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_DURATION_SECONDS must be at least 1")
	}
	if renewPeriod <= 0 || renewPeriod >= duration {
		return nil, fmt.Errorf("FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_LEASE_RENEW_SECONDS must be greater than 0 and less than the lease duration")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("control-plane backup run id is empty")
	}
	token, err := newControlPlaneBackupCoordinationToken()
	if err != nil {
		return nil, err
	}
	clientFactory := s.newClusterNodeClient
	if clientFactory == nil {
		clientFactory = newClusterNodeClient
	}
	client, err := clientFactory()
	if err != nil {
		return nil, fmt.Errorf("initialize Kubernetes Lease client: %w", err)
	}
	leaseClient := clusterNodeCoordinationLeaseClient{client: client}
	return &kubernetesControlPlaneBackupCoordinationLease{
		client: client,
		manager: &kubelease.Manager{
			Client:             leaseClient,
			Namespace:          namespace,
			Name:               name,
			HolderIdentity:     "backup/" + runID,
			Duration:           duration,
			TokenAnnotationKey: controlPlaneBackupCoordinationTokenAnnotation,
			Token:              token,
			CanTakeOverExpired: func(holder string) bool {
				return s.canTakeOverExpiredBackupCoordinationHolder(runID, holder)
			},
		},
	}, nil
}

func (s *Server) canTakeOverExpiredBackupCoordinationHolder(requestedRunID, holder string) bool {
	if s == nil || s.store == nil {
		return false
	}
	holder = strings.TrimSpace(holder)
	if !strings.HasPrefix(holder, "backup/") {
		// In particular, release/* is never recoverable by a backup worker.
		return false
	}
	heldRunID := strings.TrimSpace(strings.TrimPrefix(holder, "backup/"))
	requestedRunID = strings.TrimSpace(requestedRunID)
	if heldRunID == "" || requestedRunID == "" {
		return false
	}
	heldRun, err := s.store.GetBackupRun(heldRunID, "", true)
	if err != nil || model.NormalizeBackupTargetType(heldRun.Target.Type) != model.BackupTargetControlPlaneDatabase {
		// A missing/unknown run or a holder that does not map to the shared
		// control-plane database target is not sufficient proof of safety.
		return false
	}
	if heldRunID == requestedRunID {
		// A replacement worker for the same still-pending run may recover a
		// dead execution token after the Kubernetes Lease has expired.
		return heldRun.Status == model.BackupRunStatusPending
	}
	// A different backup run may replace an expired holder only after the
	// exact old run is durably terminal. Pending/running/unknown are denied.
	return backupRunTerminalStatus(heldRun.Status)
}

func newControlPlaneBackupCoordinationToken() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate control-plane backup coordination token: %w", err)
	}
	return hex.EncodeToString(raw), nil
}

type clusterNodeCoordinationLeaseClient struct {
	client *clusterNodeClient
}

func (c clusterNodeCoordinationLeaseClient) Get(ctx context.Context, namespace, name string) (coordinationv1.Lease, bool, error) {
	var lease coordinationv1.Lease
	err := c.client.doJSON(ctx, http.MethodGet, coordinationLeaseAPIPath(namespace, name), &lease)
	if err != nil {
		if kubernetesRequestHasStatus(err, http.StatusNotFound) {
			return coordinationv1.Lease{}, false, nil
		}
		return coordinationv1.Lease{}, false, err
	}
	return lease, true, nil
}

func (c clusterNodeCoordinationLeaseClient) Create(ctx context.Context, namespace string, lease coordinationv1.Lease) error {
	apiPath := "/apis/coordination.k8s.io/v1/namespaces/" + url.PathEscape(strings.TrimSpace(namespace)) + "/leases"
	err := c.client.doJSONWithBody(ctx, http.MethodPost, apiPath, lease, nil)
	return normalizeCoordinationLeaseWriteError(err)
}

func (c clusterNodeCoordinationLeaseClient) Update(ctx context.Context, namespace string, lease coordinationv1.Lease) error {
	err := c.client.doJSONWithBody(ctx, http.MethodPut, coordinationLeaseAPIPath(namespace, lease.Name), lease, nil)
	return normalizeCoordinationLeaseWriteError(err)
}

func coordinationLeaseAPIPath(namespace, name string) string {
	return "/apis/coordination.k8s.io/v1/namespaces/" + url.PathEscape(strings.TrimSpace(namespace)) + "/leases/" + url.PathEscape(strings.TrimSpace(name))
}

func normalizeCoordinationLeaseWriteError(err error) error {
	if err == nil {
		return nil
	}
	if kubernetesRequestHasStatus(err, http.StatusConflict) {
		return kubelease.ErrConflict
	}
	if kubernetesRequestHasStatus(err, http.StatusNotFound) {
		return kubelease.ErrNotFound
	}
	return err
}

func kubernetesRequestHasStatus(err error, status int) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), fmt.Sprintf("status=%d", status))
}
