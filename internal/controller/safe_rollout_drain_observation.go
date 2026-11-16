package controller

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"syscall"
	"time"
)

// Retain failures independently of successful polls: the generic HTTP ring can
// wrap before an administrator investigates a rare drain observation failure.
var podDrainObservations struct {
	sync.Mutex
	latest   map[string]podDrainObservation
	failures []podDrainObservation
	total    uint64
	failed   uint64
}

type podDrainObservation struct {
	Namespace         string    `json:"namespace"`
	Pod               string    `json:"pod"`
	Port              int       `json:"port"`
	StartedAt         time.Time `json:"started_at"`
	FinishedAt        time.Time `json:"finished_at"`
	ElapsedMS         int64     `json:"elapsed_ms"`
	HasDeadline       bool      `json:"has_deadline"`
	BudgetMS          int64     `json:"budget_ms"`
	RemainingMS       int64     `json:"remaining_ms"`
	Context           string    `json:"context"`
	Stage             string    `json:"stage"`
	Cause             string    `json:"cause,omitempty"`
	StatusCode        int       `json:"status_code"`
	ActiveConnections int       `json:"active_connections"`
}

func newPodDrainObservation(ctx context.Context, namespace string, pod releaseDrainPod) podDrainObservation {
	observation := podDrainObservation{Namespace: namespace, Pod: pod.Name, Port: pod.Port,
		StartedAt: time.Now().UTC(), BudgetMS: -1, RemainingMS: -1, ActiveConnections: -1, Stage: "request"}
	if deadline, ok := ctx.Deadline(); ok {
		observation.HasDeadline = true
		observation.BudgetMS = deadline.Sub(observation.StartedAt).Milliseconds()
	}
	return observation
}

func (o podDrainObservation) finish(ctx context.Context) podDrainObservation {
	o.FinishedAt = time.Now().UTC()
	o.ElapsedMS = o.FinishedAt.Sub(o.StartedAt).Milliseconds()
	o.Context = drainObservationContext(ctx)
	if deadline, ok := ctx.Deadline(); ok {
		o.RemainingMS = deadline.Sub(o.FinishedAt).Milliseconds()
	}
	return o
}

func drainObservationContext(ctx context.Context) string {
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(ctx.Err(), context.Canceled):
		return "canceled"
	default:
		return "active"
	}
}

// Error output is an allowlist of facts. A net/url error can contain credentials,
// the complete request URL and nonce; never interpolate its raw Error string.
type podDrainProxyError struct {
	observation podDrainObservation
	err         error
}

func (e *podDrainProxyError) Error() string {
	o := e.observation
	return fmt.Sprintf("Pod drain proxy observation failed: namespace=%q pod=%q port=%d stage=%s cause=%s status=%d elapsed_ms=%d has_deadline=%t budget_ms=%d remaining_ms=%d context=%s",
		o.Namespace, o.Pod, o.Port, o.Stage, o.Cause, o.StatusCode, o.ElapsedMS, o.HasDeadline, o.BudgetMS, o.RemainingMS, o.Context)
}

func (e *podDrainProxyError) Unwrap() error { return e.err }

func podDrainFailureCause(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "context_deadline"
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection_refused"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection_reset"
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof"
	case errors.Is(err, io.EOF):
		return "eof"
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "dns_error"
	}
	var unknownCA x509.UnknownAuthorityError
	var certificate x509.CertificateInvalidError
	var hostname x509.HostnameError
	if errors.As(err, &unknownCA) || errors.As(err, &certificate) || errors.As(err, &hostname) {
		return "tls_verification"
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return "transport_timeout"
	}
	return "transport_error"
}

func recordPodDrainObservation(observation podDrainObservation) {
	podDrainObservations.Lock()
	defer podDrainObservations.Unlock()
	podDrainObservations.total++
	if podDrainObservations.latest == nil {
		podDrainObservations.latest = make(map[string]podDrainObservation)
	}
	key := observation.Namespace + "/" + observation.Pod
	if _, exists := podDrainObservations.latest[key]; !exists && len(podDrainObservations.latest) >= 256 {
		oldest := ""
		for candidate, previous := range podDrainObservations.latest {
			if oldest == "" || previous.FinishedAt.Before(podDrainObservations.latest[oldest].FinishedAt) {
				oldest = candidate
			}
		}
		delete(podDrainObservations.latest, oldest)
	}
	podDrainObservations.latest[key] = observation
	if observation.Cause != "" {
		podDrainObservations.failed++
		if len(podDrainObservations.failures) == 64 {
			copy(podDrainObservations.failures, podDrainObservations.failures[1:])
			podDrainObservations.failures = podDrainObservations.failures[:63]
		}
		podDrainObservations.failures = append(podDrainObservations.failures, observation)
	}
}

// ReleaseDrainObservationSnapshot is read-only and served only on the existing
// administrator-local observation socket, not a new public API or listener.
func ReleaseDrainObservationSnapshot(ctx context.Context) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	podDrainObservations.Lock()
	latest := make([]podDrainObservation, 0, len(podDrainObservations.latest))
	for _, observation := range podDrainObservations.latest {
		latest = append(latest, observation)
	}
	failures := append([]podDrainObservation{}, podDrainObservations.failures...)
	total, failed := podDrainObservations.total, podDrainObservations.failed
	podDrainObservations.Unlock()
	sort.Slice(latest, func(i, j int) bool { return latest[i].FinishedAt.Before(latest[j].FinishedAt) })
	return map[string]any{"schema": "fugue.release-drain.observation.v1", "observed_at": time.Now().UTC(),
		"attempts_total": total, "failures_total": failed, "latest": latest, "recent_failures": failures,
		"latest_limit": 256, "failure_limit": 64, "note": "active_connections=-1 means unobserved; budget_ms/remaining_ms=-1 without a deadline; observations do not authorize retirement"}, nil
}
