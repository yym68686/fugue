package operationexecution

import (
	"log"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

type Timer struct {
	startedAt time.Time
	lastMark  time.Time
	now       func() time.Time
	segments  []timingSegment
}

type timingSegment struct {
	name     string
	duration time.Duration
}

func NewTimer(now func() time.Time) *Timer {
	if now == nil {
		now = time.Now
	}
	startedAt := now()
	return &Timer{
		startedAt: startedAt,
		lastMark:  startedAt,
		now:       now,
	}
}

func (t *Timer) Mark(name string) {
	if t == nil {
		return
	}
	name = normalizeTimingName(name)
	if name == "" {
		return
	}
	now := t.now()
	t.segments = append(t.segments, timingSegment{
		name:     name,
		duration: now.Sub(t.lastMark),
	})
	t.lastMark = now
}

func (t *Timer) Log(logger *log.Logger, scope string, op model.Operation, err error, pending bool) {
	if t == nil || logger == nil {
		return
	}
	status := "completed"
	if err != nil {
		status = "failed"
	}
	if pending {
		status = "pending"
	}
	logger.Printf(
		"%s timing operation=%s app=%s type=%s status=%s total_ms=%d segments=%s",
		normalizeTimingName(scope),
		strings.TrimSpace(op.ID),
		strings.TrimSpace(op.AppID),
		strings.TrimSpace(op.Type),
		status,
		t.now().Sub(t.startedAt).Milliseconds(),
		t.segmentString(),
	)
}

func (t *Timer) segmentString() string {
	if t == nil || len(t.segments) == 0 {
		return ""
	}
	parts := make([]string, 0, len(t.segments))
	for _, segment := range t.segments {
		if segment.name == "" {
			continue
		}
		parts = append(parts, segment.name+"="+formatMilliseconds(segment.duration))
	}
	return strings.Join(parts, ",")
}

func (t *Timer) Segments() []model.OperationControllerTimingSegment {
	if t == nil || len(t.segments) == 0 {
		return nil
	}
	out := make([]model.OperationControllerTimingSegment, 0, len(t.segments))
	for _, segment := range t.segments {
		if segment.name == "" {
			continue
		}
		out = append(out, model.OperationControllerTimingSegment{
			Name:                 segment.name,
			DurationMilliseconds: segment.duration.Milliseconds(),
		})
	}
	return out
}

func normalizeTimingName(value string) string {
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

func formatMilliseconds(value time.Duration) string {
	return strconv.FormatInt(value.Milliseconds(), 10) + "ms"
}
