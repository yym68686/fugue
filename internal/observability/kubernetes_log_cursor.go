package observability

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
	"sync/atomic"
	"time"
)

type kubernetesLogCursor struct {
	Time     time.Time
	Boundary map[[32]byte]int
	Visited  time.Time
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
func (c *kubernetesLogCollector) collectCursorTarget(parent context.Context, target kubernetesLogTarget, budget *atomic.Int64) {
	if budget.Load() <= 0 || parent.Err() != nil {
		return
	}
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
	// Runtime retention is a finite bound, not permission to silently skip an
	// unbounded backlog. Make any gap visible before resetting the lower bound.
	if maxAge := c.pipeline.cfg.Retention; maxAge > 0 && now.Sub(cur.Time) > maxAge {
		cur.Time = now.Add(-maxAge)
		cur.Boundary = map[[32]byte]int{}
		c.pipeline.kubernetesLogCursorGaps.Add(1)
	}
	original := cur.Time
	boundary := cur.Boundary
	seen := map[[32]byte]int{}
	ctx, cancel := context.WithTimeout(parent, c.pipeline.cfg.KubernetesLogPollInterval)
	defer cancel()
	since := metav1.NewTime(cur.Time)
	stream, err := c.client.CoreV1().Pods(target.pod.Namespace).GetLogs(target.pod.Name, &corev1.PodLogOptions{Container: target.container, Timestamps: true, SinceTime: &since, Previous: target.previous}).Stream(ctx)
	if err != nil {
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
	scanner.Buffer(make([]byte, 0, 64<<10), int(c.pipeline.cfg.MaxPayloadBytes))
	n := 0
	truncated := false
	for scanner.Scan() {
		ts, msg := splitKubernetesLogLine(scanner.Text())
		if ts.IsZero() {
			c.pipeline.kubernetesLogCursorGaps.Add(1)
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
			truncated = true
			break
		}
		if budget.Add(-1) < 0 {
			budget.Add(1)
			truncated = true
			break
		}
		if !c.pipeline.IngestLogLineWithAttributes(ctx, source, msg, attrs, ts) {
			budget.Add(1)
			truncated = true
			break
		}
		n++
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
		c.pipeline.kubernetesLogErrors.Add(1)
		c.pipeline.recordError(fmt.Errorf("scan Kubernetes cursor logs: %w", err))
	}
	if truncated {
		if kubernetesLogPriorityTarget(target) {
			c.pipeline.kubernetesPriorityTruncations.Add(1)
		}
		lag := time.Since(cur.Time).Milliseconds()
		for old := c.pipeline.kubernetesLogBacklogMillis.Load(); lag > old && !c.pipeline.kubernetesLogBacklogMillis.CompareAndSwap(old, lag); old = c.pipeline.kubernetesLogBacklogMillis.Load() {
		}
	}
	cur.Visited = now
	c.cursorsMu.Lock()
	c.cursors[key] = cur
	c.cursorsMu.Unlock()
}
