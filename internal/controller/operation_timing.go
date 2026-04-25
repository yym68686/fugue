package controller

import (
	"log"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

type controllerOperationTimer struct {
	startedAt time.Time
	lastMark  time.Time
	now       func() time.Time
	segments  []controllerOperationTimingSegment
}

type controllerOperationTimingSegment struct {
	name     string
	duration time.Duration
}

func newControllerOperationTimer(now func() time.Time) *controllerOperationTimer {
	if now == nil {
		now = time.Now
	}
	startedAt := now()
	return &controllerOperationTimer{
		startedAt: startedAt,
		lastMark:  startedAt,
		now:       now,
	}
}

func (t *controllerOperationTimer) Mark(name string) {
	if t == nil {
		return
	}
	name = normalizeControllerTimingName(name)
	if name == "" {
		return
	}
	now := t.now()
	t.segments = append(t.segments, controllerOperationTimingSegment{
		name:     name,
		duration: now.Sub(t.lastMark),
	})
	t.lastMark = now
}

func (t *controllerOperationTimer) Log(logger *log.Logger, scope string, op model.Operation, err error) {
	if t == nil || logger == nil {
		return
	}
	status := "completed"
	if err != nil {
		status = "failed"
	}
	logger.Printf(
		"%s timing operation=%s app=%s type=%s status=%s total_ms=%d segments=%s",
		normalizeControllerTimingName(scope),
		strings.TrimSpace(op.ID),
		strings.TrimSpace(op.AppID),
		strings.TrimSpace(op.Type),
		status,
		t.now().Sub(t.startedAt).Milliseconds(),
		t.segmentString(),
	)
}

func (t *controllerOperationTimer) segmentString() string {
	if t == nil || len(t.segments) == 0 {
		return ""
	}
	parts := make([]string, 0, len(t.segments))
	for _, segment := range t.segments {
		if segment.name == "" {
			continue
		}
		parts = append(parts, segment.name+"="+timeDurationMilliseconds(segment.duration))
	}
	return strings.Join(parts, ",")
}

func normalizeControllerTimingName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

func timeDurationMilliseconds(value time.Duration) string {
	return strconv.FormatInt(value.Milliseconds(), 10) + "ms"
}
