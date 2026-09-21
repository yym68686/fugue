package observability

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
	"sync/atomic"
	"time"
)

type kubernetesLogCursor struct {
	Time              time.Time
	Boundary          map[[32]byte]int
	Visited           time.Time
	DrainedThrough    time.Time
	Complete          bool
	NextAttempt       time.Time
	SourceUnavailable bool
	PendingAt         time.Time
}

func logTargetFinishedAt(t kubernetesLogTarget) time.Time {
	for _, s := range append(append(append([]corev1.ContainerStatus(nil), t.pod.Status.ContainerStatuses...), t.pod.Status.InitContainerStatuses...), t.pod.Status.EphemeralContainerStatuses...) {
		if s.Name != t.container {
			continue
		}
		state := s.State
		if t.previous {
			state = s.LastTerminationState
		}
		if state.Terminated != nil {
			return state.Terminated.FinishedAt.Time
		}
	}
	return time.Time{}
}

func logTargetKey(t kubernetesLogTarget) string {
	identity := string(t.pod.UID)
	for _, s := range append(append([]corev1.ContainerStatus(nil), t.pod.Status.ContainerStatuses...), t.pod.Status.InitContainerStatuses...) {
		if s.Name != t.container {
			continue
		}
		id := s.ContainerID
		if t.previous && s.LastTerminationState.Terminated != nil {
			id = s.LastTerminationState.Terminated.ContainerID
		}
		if id != "" {
			identity += "/" + id
		} else {
			restart := int(s.RestartCount)
			if t.previous {
				restart--
			}
			identity += "/restart-" + strconv.Itoa(restart)
		}
		break
	}
	return t.pod.Namespace + "/" + t.pod.Name + "/" + t.container + "/" + identity
}

// Read oldest outstanding records, never TailLines (which discards the front
// of a busy stream). The cursor advances only after queue admission, and an
// inclusive timestamp plus boundary occurrence counts preserves equal-time
// duplicate lines without replaying the entire history on every poll.
func (c *kubernetesLogCollector) collectCursorTarget(parent context.Context, target kubernetesLogTarget, budget *atomic.Int64) (budgetBlockedWithoutProgress bool) {
	return c.collectReservedCursorTarget(parent, target, budget, nil)
}

func takeLogBudget(budget *atomic.Int64) bool {
	if budget == nil {
		return false
	}
	for left := budget.Load(); left > 0; left = budget.Load() {
		if budget.CompareAndSwap(left, left-1) {
			return true
		}
	}
	return false
}

func (c *kubernetesLogCollector) collectReservedCursorTarget(parent context.Context, target kubernetesLogTarget, budget, shared *atomic.Int64) (budgetBlockedWithoutProgress bool) {
	if parent.Err() != nil {
		return false
	}
	if budget.Load() <= 0 {
		return true
	}
	c.cycleVisited.Add(1)
	key := logTargetKey(target)
	now := time.Now().UTC()
	c.cursorsMu.Lock()
	if c.cursors == nil {
		c.cursors = map[string]kubernetesLogCursor{}
	}
	cur, exists := c.cursors[key]
	c.cursorsMu.Unlock()
	if !exists {
		cur = kubernetesLogCursor{Time: now.Add(-5 * time.Minute), Boundary: map[[32]byte]int{}}
	}
	finished := logTargetFinishedAt(target)
	if cur.Complete || now.Before(cur.NextAttempt) || (!finished.IsZero() && finished.Before(cur.Time)) {
		cur.Visited = now
		if !finished.IsZero() && finished.Before(cur.Time) {
			cur.Complete = true
		}
		c.cursorsMu.Lock()
		c.cursors[key] = cur
		c.cursorsMu.Unlock()
		return
	}
	// Runtime retention is a finite bound, not permission to silently skip an
	// unbounded backlog. Make any gap visible before resetting the lower bound.
	if maxAge := c.pipeline.cfg.Retention; maxAge > 0 && now.Sub(cur.Time) > maxAge {
		cur.Time = now.Add(-maxAge)
		cur.Boundary = map[[32]byte]int{}
		if cur.DrainedThrough.Before(cur.Time) {
			c.pipeline.kubernetesLogCursorGaps.Add(1)
		}
	}
	original := cur.Time
	observation := logSourceObservation{Identity: key, Node: target.pod.Spec.NodeName, Previous: target.previous, ObservedAt: now, CursorBefore: cur.Time, Outcome: "drained", LineLimitBytes: c.pipeline.cfg.KubernetesLogMaxLineBytes}
	defer func() {
		observation.CursorAfter = cur.Time
		observation.PendingAt = cur.PendingAt
		observation.DrainedThrough = cur.DrainedThrough
		observation.TotalMillis = time.Since(now).Milliseconds()
		c.pipeline.observeLogSource(observation)
	}()
	boundary := cur.Boundary
	seen := map[[32]byte]int{}
	ctx, cancel := context.WithTimeout(parent, c.pipeline.cfg.KubernetesLogPollInterval)
	defer cancel()
	since := metav1.NewTime(cur.Time)
	stream, err := c.client.CoreV1().Pods(target.pod.Namespace).GetLogs(target.pod.Name, &corev1.PodLogOptions{Container: target.container, Timestamps: true, SinceTime: &since, Previous: target.previous}).Stream(ctx)
	observation.OpenMillis = time.Since(now).Milliseconds()
	if err != nil {
		observation.Outcome = "open_error"
		observation.ErrorClass = logReadErrorClass(err)
		if !isBenignKubernetesLogReadError(err) && parent.Err() == nil {
			c.pipeline.kubernetesLogErrors.Add(1)
			c.pipeline.recordError(fmt.Errorf("read Kubernetes cursor logs: %w", err))
		}
		return
	}
	defer stream.Close()
	attrs := kubernetesLogAttributes(target.pod, target.container)
	source := "kubernetes://" + target.pod.Namespace + "/" + target.pod.Name + "/" + target.container
	scanner := bufio.NewScanner(stream)
	// Incoming OTLP request limits are unrelated to a container log record.
	// Keep source reads independently bounded, including its CRI timestamp.
	scanner.Buffer(make([]byte, 0, min(64<<10, c.pipeline.cfg.KubernetesLogMaxLineBytes+64)), c.pipeline.cfg.KubernetesLogMaxLineBytes+64)
	n := 0
	truncated := false
	invalid := false
	for scanner.Scan() {
		observation.MaxLineBytes = max(observation.MaxLineBytes, len(scanner.Bytes()))
		ts, msg := splitKubernetesLogLine(scanner.Text())
		if ts.IsZero() {
			observation.Outcome = "source_unavailable"
			// Kubelet can return HTTP 200 with an untimestamped unavailable-log
			// message after container GC. Count the loss once and retry with backoff.
			if !cur.SourceUnavailable {
				c.pipeline.kubernetesLogCursorGaps.Add(1)
			}
			cur.SourceUnavailable = true
			cur.NextAttempt = now.Add(5 * time.Minute)
			invalid = true
			break
		}
		if ts.Before(original) {
			continue
		}
		hash := sha256.Sum256([]byte(msg))
		if ts.Equal(original) {
			seen[hash]++
			if seen[hash] <= boundary[hash] {
				continue
			}
		}
		if n >= int(c.pipeline.cfg.KubernetesLogTailLines) {
			observation.Outcome = "source_budget"
			truncated = true
			cur.PendingAt = ts
			break
		}
		if !takeLogBudget(budget) && !takeLogBudget(shared) {
			budgetBlockedWithoutProgress = n == 0
			observation.Outcome = "cycle_budget"
			truncated = true
			cur.PendingAt = ts
			break
		}
		if !c.pipeline.IngestLogLineWithAttributes(ctx, source, msg, attrs, ts) {
			observation.Outcome = "queue_rejected"
			budget.Add(1)
			truncated = true
			cur.PendingAt = ts
			break
		}
		cur.PendingAt = time.Time{}
		n++
		observation.Lines = n
		c.pipeline.kubernetesLogLines.Add(1)
		if kubernetesLogPriorityMessage(msg) {
			c.pipeline.kubernetesPriorityLines.Add(1)
		}
		if ts.After(cur.Time) {
			cur.Time = ts
			cur.Boundary = map[[32]byte]int{}
		}
		cur.Boundary[hash]++
	}
	if err = scanner.Err(); err != nil && parent.Err() == nil {
		observation.Outcome = "scan_error"
		observation.ErrorClass = logReadErrorClass(err)
		if cur.PendingAt.IsZero() {
			cur.PendingAt = maxLogTime(cur.Time, cur.DrainedThrough)
		}
		c.pipeline.kubernetesLogErrors.Add(1)
		c.pipeline.recordError(fmt.Errorf("scan Kubernetes cursor logs: %w", err))
	}
	if parent.Err() != nil {
		observation.Outcome = "canceled"
	}
	if err == nil && !invalid && !truncated {
		cur.PendingAt = time.Time{}
		cur.DrainedThrough = now
		cur.Complete = !finished.IsZero()
		cur.SourceUnavailable = false
		cur.NextAttempt = time.Time{}
	}
	if truncated {
		if kubernetesLogPriorityTarget(target) {
			c.pipeline.kubernetesPriorityTruncations.Add(1)
		}
	}
	cur.Visited = now
	c.cursorsMu.Lock()
	c.cursors[key] = cur
	c.cursorsMu.Unlock()
	return
}

func logReadErrorClass(err error) string {
	switch {
	case errors.Is(err, bufio.ErrTooLong):
		return "line_too_long"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "source_read"
	}
}

func maxLogTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return b
	}
	return a
}
