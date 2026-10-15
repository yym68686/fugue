package observability

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const kubernetesLogRetryLimit = 5 * time.Minute

// A failed kubelet is shared by all of its container sources. Epochs prevent
// successful requests already in flight from closing a newly opened circuit.
// All fields are protected by cursorsMu, including the half-open probe lease.
type kubernetesLogNodeRetry struct {
	Failures    uint32
	NextAttempt time.Time
	Probe       bool
	Epoch       uint64
	Visited     time.Time
}

type kubernetesLogNodeRead struct {
	Node  string
	Epoch uint64
	Probe bool
}

func (c *kubernetesLogCollector) logNow() time.Time {
	if c.clock != nil {
		return c.clock().UTC()
	}
	return time.Now().UTC()
}

func logRetryDelay(poll time.Duration, failures uint32) time.Duration {
	delay := min(kubernetesLogRetryLimit, max(15*time.Second, poll))
	for n := uint32(1); n < failures && delay < kubernetesLogRetryLimit; n++ {
		delay = min(kubernetesLogRetryLimit, delay*2)
	}
	// Downward jitter keeps the five-minute ceiling, even at saturation.
	return time.Duration(float64(delay) * (0.8 + rand.Float64()*0.2))
}

func logNodeUnavailable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	// HTTP errors about a particular container (400/404), authorization (401/403)
	// and rate limiting (429) remain source-local. Gateway/internal failures and
	// transport errors cannot be fixed by opening another pod log on this node.
	if apierrors.IsServiceUnavailable(err) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) {
		return true
	}
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		value := status.Status()
		if value.Code == 502 || value.Code == 504 {
			return true
		}
		if value.Code == 500 {
			message := strings.ToLower(err.Error() + " " + value.Message)
			if value.Details != nil {
				for _, cause := range value.Details.Causes {
					message += " " + strings.ToLower(cause.Message)
				}
			}
			for _, marker := range []string{"error dialing backend", "proxy error", "no agent available", "failed to find session", "dial tcp", "connection refused", "no route to host"} {
				if strings.Contains(message, marker) {
					return true
				}
			}
		}
	}
	var network net.Error
	return errors.As(err, &network) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func (c *kubernetesLogCollector) beginLogNodeRead(node string, now time.Time) (kubernetesLogNodeRead, time.Time, bool) {
	ticket := kubernetesLogNodeRead{Node: node}
	if node == "" {
		return ticket, time.Time{}, true
	}
	c.cursorsMu.Lock()
	defer c.cursorsMu.Unlock()
	if c.nodeRetries == nil {
		c.nodeRetries = map[string]kubernetesLogNodeRetry{}
	}
	state := c.nodeRetries[node]
	state.Visited = now
	ticket.Epoch = state.Epoch
	allowed := true
	if state.Failures > 0 {
		allowed = !state.Probe && !now.Before(state.NextAttempt)
		if allowed {
			state.Probe, ticket.Probe = true, true
		}
	}
	c.nodeRetries[node] = state
	return ticket, state.NextAttempt, allowed
}

// Returns whether this failure should emit a warning. Other requests from the
// same failed epoch still count as errors, but do not multiply warnings/backoff.
func (c *kubernetesLogCollector) finishLogNodeRead(ticket kubernetesLogNodeRead, err error, now time.Time) bool {
	if ticket.Node == "" {
		return true
	}
	c.cursorsMu.Lock()
	defer c.cursorsMu.Unlock()
	state := c.nodeRetries[ticket.Node]
	if state.Epoch != ticket.Epoch {
		return false
	}
	if logNodeUnavailable(err) {
		state.Failures = min(state.Failures+1, uint32(32))
		state.Epoch++
		state.Probe = false
		state.NextAttempt = now.Add(logRetryDelay(c.pipeline.cfg.KubernetesLogPollInterval, state.Failures))
	} else if ticket.Probe {
		state.Probe = false
		if !errors.Is(err, context.Canceled) {
			state.Failures, state.NextAttempt = 0, time.Time{}
			state.Epoch++
			c.pipeline.logger.Printf("Kubernetes log node reachable again node=%s; resuming retained cursors", ticket.Node)
		}
	}
	state.Visited = now
	c.nodeRetries[ticket.Node] = state
	return true
}

func (c *kubernetesLogCollector) deferLogSource(cur *kubernetesLogCursor) {
	cur.Failures = min(cur.Failures+1, uint32(32))
	cur.NextAttempt = c.logNow().Add(logRetryDelay(c.pipeline.cfg.KubernetesLogPollInterval, cur.Failures))
}
