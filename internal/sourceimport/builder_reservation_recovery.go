package sourceimport

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"fugue/internal/kubeauth"
)

// A reservation is renewed every 30 seconds, including the interval before the
// Job is submitted. Allow multiple missed renewals before considering an absent
// workload orphaned; a fresh admission must not race the recovery sweep.
const builderReservationRecoveryGrace = 90 * time.Second

func (s *builderScheduler) reservationHolderFor(name string) string {
	if s.reservationHolder != "" {
		return s.reservationHolder
	}
	return name
}

// ReconcileBuilderReservations repairs interrupted/failed releases on startup
// and subsequent controller sweeps, including reservations from older versions.
func ReconcileBuilderReservations(ctx context.Context, config kubeauth.Config, client *http.Client) error {
	return (&builderScheduler{client: &builderKubeClient{
		client: client, namespace: config.Namespace, baseURL: config.BaseURL, bearerToken: config.Token,
	}}).reconcileReservations(ctx, time.Now())
}

func (s *builderScheduler) reconcileReservations(ctx context.Context, now time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	leases, err := s.client.listLeases(ctx, builderReservationLabelSelector)
	if err != nil {
		return err
	}
	var result error
	for _, lease := range leases {
		if ctx.Err() != nil {
			return errors.Join(result, ctx.Err())
		}
		lastRenewed := builderLeaseRenewedAt(lease)
		if lastRenewed.IsZero() || now.Sub(lastRenewed) < builderReservationRecoveryGrace {
			continue
		}
		stopped, err := s.client.reservationWorkloadStopped(ctx, lease)
		if err != nil {
			result = errors.Join(result, fmt.Errorf("inspect reservation %s: %w", lease.Metadata.Name, err))
			continue
		}
		if !stopped {
			continue
		}
		if err := s.client.deleteReservation(ctx, lease); err != nil {
			if !builderIsConflictError(err) { // Renewed/replaced since observation: preserve it.
				result = errors.Join(result, fmt.Errorf("release reservation %s: %w", lease.Metadata.Name, err))
			}
			continue
		}
		log.Printf("reclaimed orphan builder reservation name=%s node=%s", lease.Metadata.Name, lease.Metadata.Annotations[builderAnnotationNodeName])
	}
	return result
}

func (s *builderScheduler) releaseReservationWithRetry(ctx context.Context, name string, delay time.Duration) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if err := s.releaseReservation(ctx, name); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == 2 {
			break
		}
		timer := time.NewTimer(delay * time.Duration(attempt+1))
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(lastErr, ctx.Err())
		case <-timer.C:
		}
	}
	return lastErr
}

func (c *builderKubeClient) reservationWorkloadStopped(ctx context.Context, lease builderKubeLease) (bool, error) {
	name := strings.TrimSpace(lease.Metadata.Name)
	nodeName := strings.TrimSpace(lease.Metadata.Annotations[builderAnnotationNodeName])
	if name == "" || nodeName == "" {
		return false, nil
	}
	var job struct {
		Status struct {
			Conditions []struct {
				Type, Status string
			} `json:"conditions"`
		} `json:"status"`
	}
	status, err := c.doJSONStatus(ctx, http.MethodGet, "/apis/batch/v1/namespaces/"+c.namespace+"/jobs/"+url.PathEscape(name), nil, &job)
	if status != http.StatusNotFound {
		if err != nil {
			return false, err
		}
		terminal := false
		for _, condition := range job.Status.Conditions {
			if (condition.Type == "Complete" || condition.Type == "Failed") && condition.Status == "True" {
				terminal = true
			}
		}
		if !terminal {
			return false, nil
		}
	}
	// Include terminating/unknown/pending Pods. Job deletion alone is not proof
	// that its processes have stopped, and an unsuccessful list is never absence.
	query := url.Values{"labelSelector": {"job-name=" + name}}
	var pods builderKubePodList
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/namespaces/"+c.namespace+"/pods?"+query.Encode(), nil, &pods); err != nil {
		return false, err
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase != "Succeeded" && pod.Status.Phase != "Failed" {
			return false, nil
		}
	}
	// An unreachable node is unknown execution state, even after API objects
	// disappear. Keep its reservation until the node can confirm reconciliation.
	var node builderKubeNode
	if err := c.doJSON(ctx, http.MethodGet, "/api/v1/nodes/"+url.PathEscape(nodeName), nil, &node); err != nil {
		return false, err
	}
	return builderNodeConditionStatus(node.Status.Conditions, "Ready"), nil
}

func (c *builderKubeClient) deleteReservation(ctx context.Context, lease builderKubeLease) error {
	if lease.Metadata.UID == "" || lease.Metadata.ResourceVersion == "" {
		return fmt.Errorf("reservation %s has no deletion preconditions", lease.Metadata.Name)
	}
	body := map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": map[string]string{
		"uid": lease.Metadata.UID, "resourceVersion": lease.Metadata.ResourceVersion,
	}}
	status, err := c.doJSONStatus(ctx, http.MethodDelete, "/apis/coordination.k8s.io/v1/namespaces/"+c.namespace+"/leases/"+url.PathEscape(lease.Metadata.Name), body, nil)
	if status == http.StatusNotFound {
		return nil
	}
	return err
}
